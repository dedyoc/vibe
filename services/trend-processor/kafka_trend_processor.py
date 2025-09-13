#!/usr/bin/env python3
"""
Kafka-based Trend Processor

A simpler alternative to Flink that processes Bluesky posts directly from Kafka
and performs trend detection using sliding windows.
"""

import asyncio
import json
import logging
import re
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Set, Optional, Any
import hashlib

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import redis
import structlog

from shared.models import PostData, TrendAlert, WindowedKeywordCount

logger = structlog.get_logger(__name__)


class SlidingWindowTrendDetector:
    """Sliding window-based trend detector for real-time processing."""
    
    def __init__(
        self,
        window_size_minutes: int = 10,
        slide_interval_seconds: int = 30,
        min_frequency_threshold: int = 5,
        min_growth_rate: float = 1.5,
        min_confidence_score: float = 0.6
    ):
        self.window_size = timedelta(minutes=window_size_minutes)
        self.slide_interval = timedelta(seconds=slide_interval_seconds)
        self.min_frequency_threshold = min_frequency_threshold
        self.min_growth_rate = min_growth_rate
        self.min_confidence_score = min_confidence_score
        
        # Sliding window storage: keyword -> [(timestamp, author_did), ...]
        self.keyword_windows: Dict[str, deque] = defaultdict(lambda: deque())
        self.previous_counts: Dict[str, int] = {}
        
        # Regex patterns for keyword extraction
        self.hashtag_pattern = re.compile(r'#\w+', re.IGNORECASE)
        self.mention_pattern = re.compile(r'@[\w.-]+', re.IGNORECASE)
        self.keyword_pattern = re.compile(r'\b(?:AI|ML|tech|python|javascript|coding|programming|blockchain|crypto|NFT|web3|startup|innovation|climate|election|politics|sports|music|art|gaming|food|travel|health|fitness|education|science|news|breaking|trending)\b', re.IGNORECASE)
    
    def extract_keywords(self, post: PostData) -> Set[str]:
        """Extract keywords from a post."""
        keywords = set()
        
        # Add hashtags (without #)
        hashtags = self.hashtag_pattern.findall(post.text)
        keywords.update(tag[1:].lower() for tag in hashtags)
        
        # Add mentions (without @)
        mentions = self.mention_pattern.findall(post.text)
        keywords.update(mention[1:].lower() for mention in mentions)
        
        # Add common trending keywords
        common_keywords = self.keyword_pattern.findall(post.text)
        keywords.update(word.lower() for word in common_keywords)
        
        return keywords
    
    def add_post(self, post: PostData) -> List[TrendAlert]:
        """Add a post to the sliding window and detect trends."""
        keywords = self.extract_keywords(post)
        current_time = post.created_at
        trends = []
        
        # Add keywords to windows
        for keyword in keywords:
            self.keyword_windows[keyword].append((current_time, post.author_did))
        
        # Clean old entries and detect trends
        trends.extend(self._clean_windows_and_detect_trends(current_time))
        
        return trends
    
    def _clean_windows_and_detect_trends(self, current_time: datetime) -> List[TrendAlert]:
        """Clean old entries from windows and detect trends."""
        cutoff_time = current_time - self.window_size
        trends = []
        
        for keyword, window in self.keyword_windows.items():
            # Remove old entries
            while window and window[0][0] < cutoff_time:
                window.popleft()
            
            # Calculate current metrics
            if len(window) >= self.min_frequency_threshold:
                trend = self._calculate_trend_metrics(keyword, window, current_time)
                if trend and trend.confidence_score >= self.min_confidence_score:
                    trends.append(trend)
        
        return trends
    
    def _calculate_trend_metrics(
        self, 
        keyword: str, 
        window: deque, 
        current_time: datetime
    ) -> Optional[TrendAlert]:
        """Calculate trend metrics for a keyword."""
        if not window:
            return None
        
        # Current frequency
        frequency = len(window)
        
        # Unique authors
        unique_authors = len(set(author for _, author in window))
        
        # Growth rate calculation
        previous_count = self.previous_counts.get(keyword, 0)
        if previous_count > 0:
            growth_rate = ((frequency - previous_count) / previous_count) * 100
        else:
            growth_rate = frequency * 100  # 100% growth from 0
        
        # Update previous count
        self.previous_counts[keyword] = frequency
        
        # Skip if growth rate is too low
        if growth_rate < self.min_growth_rate:
            return None
        
        # Calculate confidence score based on multiple factors
        confidence_score = self._calculate_confidence_score(
            frequency, unique_authors, growth_rate, len(keyword)
        )
        
        # Create sample posts
        sample_posts = [
            f"Post mentioning '{keyword}'" for _ in range(min(3, len(window)))
        ]
        
        window_start = current_time - self.window_size
        
        return TrendAlert(
            keyword=keyword,
            frequency=frequency,
            growth_rate=growth_rate,
            confidence_score=confidence_score,
            window_start=window_start,
            window_end=current_time,
            sample_posts=sample_posts,
            unique_authors=unique_authors,
            rank=1  # Will be set later when ranking trends
        )
    
    def _calculate_confidence_score(
        self, 
        frequency: int, 
        unique_authors: int, 
        growth_rate: float, 
        keyword_length: int
    ) -> float:
        """Calculate confidence score for a trend."""
        # Base score from frequency (normalized)
        freq_score = min(frequency / 50.0, 1.0)  # Max score at 50 mentions
        
        # Author diversity score
        author_score = min(unique_authors / frequency, 1.0)  # Higher is better
        
        # Growth rate score (normalized)
        growth_score = min(growth_rate / 500.0, 1.0)  # Max score at 500% growth
        
        # Keyword quality score (longer keywords often more meaningful)
        quality_score = min(keyword_length / 10.0, 1.0)
        
        # Weighted average
        confidence = (
            freq_score * 0.3 +
            author_score * 0.3 +
            growth_score * 0.3 +
            quality_score * 0.1
        )
        
        return round(confidence, 2)


class KafkaTrendProcessor:
    """Main Kafka-based trend processor."""
    
    def __init__(
        self,
        kafka_bootstrap_servers: str,
        kafka_topic_posts: str,
        kafka_topic_trends: str,
        redis_host: str,
        redis_port: int,
        window_size_minutes: int = 10
    ):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic_posts = kafka_topic_posts
        self.kafka_topic_trends = kafka_topic_trends
        
        # Initialize components
        self.trend_detector = SlidingWindowTrendDetector(
            window_size_minutes=window_size_minutes
        )
        
        # Redis for caching trends
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        
        # Kafka clients
        self.consumer = None
        self.producer = None
        
        # Statistics
        self.posts_processed = 0
        self.trends_detected = 0
        self.last_processed_time = None
    
    async def start_processing(self) -> None:
        """Start the trend processing loop."""
        logger.info("Starting Kafka trend processor")
        
        try:
            # Initialize Kafka consumer
            self.consumer = KafkaConsumer(
                self.kafka_topic_posts,
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                group_id='trend-processor',
                auto_offset_reset='latest',  # Start from latest messages
                enable_auto_commit=True
            )
            
            # Initialize Kafka producer for trends
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            
            logger.info("Kafka clients initialized, starting message processing")
            
            # Process messages with timeout to allow other tasks to run
            while True:
                try:
                    # Poll for messages with timeout
                    message_batch = self.consumer.poll(timeout_ms=1000)
                    
                    if not message_batch:
                        # No messages, yield control to other tasks
                        await asyncio.sleep(0.1)
                        continue
                    
                    # Process all messages in the batch
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            try:
                                await self._process_message(message.value)
                            except Exception as e:
                                logger.error("Error processing message", error=str(e))
                                continue
                    
                    # Yield control to other tasks
                    await asyncio.sleep(0.01)
                    
                except Exception as e:
                    logger.error("Error in message polling", error=str(e))
                    await asyncio.sleep(1)
                    continue
                    
        except Exception as e:
            logger.error("Error in trend processing", error=str(e))
            raise
        finally:
            await self._cleanup()
    
    async def _process_message(self, message_data: Dict[str, Any]) -> None:
        """Process a single message from Kafka."""
        try:
            # Parse post data
            post = PostData(
                uri=message_data['uri'],
                author_did=message_data['author_did'],
                text=message_data['text'],
                created_at=datetime.fromisoformat(message_data['created_at']),
                language=message_data.get('language'),
                reply_to=message_data.get('reply_to'),
                mentions=message_data.get('mentions', []),
                hashtags=message_data.get('hashtags', [])
            )
            
            # Detect trends
            trends = self.trend_detector.add_post(post)
            
            # Process detected trends
            if trends:
                await self._handle_trends(trends)
            
            # Update statistics
            self.posts_processed += 1
            self.last_processed_time = datetime.now(timezone.utc)
            
            if self.posts_processed % 100 == 0:
                logger.info(
                    "Processing progress",
                    posts_processed=self.posts_processed,
                    trends_detected=self.trends_detected
                )
                
        except Exception as e:
            logger.error("Error processing post", error=str(e), message=message_data)
    
    async def _handle_trends(self, trends: List[TrendAlert]) -> None:
        """Handle detected trends by publishing and caching."""
        # Rank trends by confidence score
        ranked_trends = sorted(trends, key=lambda t: t.confidence_score, reverse=True)
        for i, trend in enumerate(ranked_trends):
            trend.rank = i + 1
        
        # Publish trends to Kafka
        for trend in ranked_trends:
            trend_data = {
                'keyword': trend.keyword,
                'frequency': trend.frequency,
                'growth_rate': trend.growth_rate,
                'confidence_score': trend.confidence_score,
                'window_start': trend.window_start.isoformat(),
                'window_end': trend.window_end.isoformat(),
                'sample_posts': trend.sample_posts,
                'unique_authors': trend.unique_authors,
                'rank': trend.rank
            }
            
            self.producer.send(self.kafka_topic_trends, trend_data)
            
            logger.info(
                "Trend detected",
                keyword=trend.keyword,
                frequency=trend.frequency,
                growth_rate=trend.growth_rate,
                confidence_score=trend.confidence_score,
                rank=trend.rank
            )
        
        # Cache trends in Redis
        await self._cache_trends(ranked_trends)
        
        self.trends_detected += len(trends)
    
    async def _cache_trends(self, trends: List[TrendAlert]) -> None:
        """Cache trends in Redis for quick access."""
        try:
            # Store current trends
            trend_data = {}
            for trend in trends[:10]:  # Top 10 trends
                trend_data[trend.keyword] = json.dumps({
                    'frequency': trend.frequency,
                    'growth_rate': trend.growth_rate,
                    'confidence_score': trend.confidence_score,
                    'window_start': trend.window_start.isoformat(),
                    'window_end': trend.window_end.isoformat(),
                    'unique_authors': trend.unique_authors,
                    'rank': trend.rank,
                    'sample_posts': trend.sample_posts
                })
            
            if trend_data:
                self.redis_client.hset('trends:current', mapping=trend_data)
                self.redis_client.expire('trends:current', 3600)  # Expire in 1 hour
                
        except Exception as e:
            logger.error("Error caching trends", error=str(e))
    
    async def _cleanup(self) -> None:
        """Cleanup resources."""
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.close()
        
        logger.info("Kafka trend processor stopped")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get processing statistics."""
        return {
            'posts_processed': self.posts_processed,
            'trends_detected': self.trends_detected,
            'last_processed_time': self.last_processed_time.isoformat() if self.last_processed_time else None,
            'active_keywords': len(self.trend_detector.keyword_windows)
        }


async def main():
    """Main entry point for the Kafka trend processor."""
    import os
    
    processor = KafkaTrendProcessor(
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        kafka_topic_posts=os.getenv("KAFKA_TOPIC_POSTS", "bluesky-posts"),
        kafka_topic_trends=os.getenv("KAFKA_TOPIC_TRENDS", "trend-alerts"),
        redis_host=os.getenv("REDIS_HOST", "redis"),
        redis_port=int(os.getenv("REDIS_PORT", "6379")),
        window_size_minutes=int(os.getenv("WINDOW_SIZE_MINUTES", "10"))
    )
    
    await processor.start_processing()


if __name__ == "__main__":
    asyncio.run(main())