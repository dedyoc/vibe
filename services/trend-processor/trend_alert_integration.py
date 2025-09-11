#!/usr/bin/env python3
"""
Trend Alert Integration

This module integrates the trend detection system with the Kafka alert publisher,
providing a complete pipeline from trend detection to alert publication.
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import structlog

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'shared'))
from models import TrendAlert, WindowedKeywordCount

from trend_detector import TrendDetector, TrendDetectionConfig, create_trend_detection_config
from trend_alert_publisher import TrendAlertPublisherManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TrendAlertPipeline:
    """
    Complete pipeline for trend detection and alert publishing.
    
    This class integrates trend detection with Kafka publishing to provide
    a complete end-to-end solution for real-time trend alerting.
    """
    
    def __init__(
        self,
        kafka_bootstrap_servers: str,
        trend_config: Optional[TrendDetectionConfig] = None,
        alert_topic: str = "trend-alerts",
        dedup_window_minutes: int = 30,
        max_alerts_per_keyword: int = 3
    ) -> None:
        """
        Initialize the trend alert pipeline.
        
        Args:
            kafka_bootstrap_servers: Kafka broker addresses
            trend_config: Configuration for trend detection
            alert_topic: Kafka topic for publishing alerts
            dedup_window_minutes: Deduplication window in minutes
            max_alerts_per_keyword: Maximum alerts per keyword in window
        """
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.alert_topic = alert_topic
        
        # Initialize trend detector
        self.trend_config = trend_config or create_trend_detection_config()
        self.trend_detector = TrendDetector(self.trend_config)
        
        # Initialize alert publisher manager
        self.publisher_manager = TrendAlertPublisherManager(kafka_bootstrap_servers)
        
        # Configuration
        self.dedup_window_minutes = dedup_window_minutes
        self.max_alerts_per_keyword = max_alerts_per_keyword
        
        # Statistics
        self.processed_batches = 0
        self.total_trends_detected = 0
        self.total_alerts_published = 0
        self.total_alerts_failed = 0
        
        self.logger = structlog.get_logger(__name__)
    
    async def process_windowed_counts_batch(
        self,
        windowed_counts: List[WindowedKeywordCount]
    ) -> Dict[str, Any]:
        """
        Process a batch of windowed counts and publish trend alerts.
        
        Args:
            windowed_counts: List of windowed keyword counts
            
        Returns:
            Dictionary with processing statistics
        """
        if not windowed_counts:
            return {
                'trends_detected': 0,
                'alerts_published': 0,
                'alerts_failed': 0,
                'processing_time_ms': 0
            }
        
        start_time = datetime.now()
        
        self.logger.info(
            "Processing windowed counts batch",
            batch_size=len(windowed_counts)
        )
        
        try:
            # Detect trends
            trends = self.trend_detector.process_windowed_counts(windowed_counts)
            
            if trends:
                # Publish trend alerts
                publish_result = await self._publish_trend_alerts(trends)
                
                # Update statistics
                self.total_trends_detected += len(trends)
                self.total_alerts_published += publish_result['sent']
                self.total_alerts_failed += publish_result['failed']
                
                self.logger.info(
                    "Batch processing completed",
                    trends_detected=len(trends),
                    alerts_published=publish_result['sent'],
                    alerts_failed=publish_result['failed'],
                    alerts_deduplicated=publish_result['deduplicated']
                )
            else:
                self.logger.debug("No trends detected in batch")
            
            self.processed_batches += 1
            processing_time = (datetime.now() - start_time).total_seconds() * 1000
            
            return {
                'trends_detected': len(trends),
                'alerts_published': publish_result['sent'] if trends else 0,
                'alerts_failed': publish_result['failed'] if trends else 0,
                'alerts_deduplicated': publish_result['deduplicated'] if trends else 0,
                'processing_time_ms': processing_time
            }
            
        except Exception as e:
            self.logger.error("Error processing windowed counts batch", error=str(e))
            raise
    
    async def _publish_trend_alerts(self, trends: List[TrendAlert]) -> Dict[str, int]:
        """
        Publish trend alerts to Kafka.
        
        Args:
            trends: List of trend alerts to publish
            
        Returns:
            Dictionary with publication statistics
        """
        try:
            # Get publisher with deduplication configuration
            publisher = await self.publisher_manager.get_publisher(
                topic_name=self.alert_topic,
                dedup_window_minutes=self.dedup_window_minutes,
                max_alerts_per_keyword=self.max_alerts_per_keyword
            )
            
            # Publish alerts in batch
            result = await publisher.publish_trend_alerts_batch(trends)
            
            self.logger.debug(
                "Trend alerts published",
                topic=self.alert_topic,
                sent=result['sent'],
                failed=result['failed'],
                deduplicated=result['deduplicated']
            )
            
            return result
            
        except Exception as e:
            self.logger.error("Error publishing trend alerts", error=str(e))
            return {
                'sent': 0,
                'failed': len(trends),
                'deduplicated': 0
            }
    
    async def process_single_trend_alert(self, alert: TrendAlert) -> bool:
        """
        Process and publish a single trend alert.
        
        Args:
            alert: TrendAlert to publish
            
        Returns:
            True if successfully published, False otherwise
        """
        try:
            result = await self.publisher_manager.publish_alert(
                alert=alert,
                topic_name=self.alert_topic,
                dedup_window_minutes=self.dedup_window_minutes,
                max_alerts_per_keyword=self.max_alerts_per_keyword
            )
            
            if result:
                self.total_alerts_published += 1
            else:
                self.total_alerts_failed += 1
            
            return result
            
        except Exception as e:
            self.logger.error("Error processing single trend alert", error=str(e))
            self.total_alerts_failed += 1
            return False
    
    def get_current_trends(self, limit: Optional[int] = None) -> List[TrendAlert]:
        """
        Get current trending keywords.
        
        Args:
            limit: Maximum number of trends to return
            
        Returns:
            List of current trend alerts
        """
        return self.trend_detector.get_current_trends(limit)
    
    def get_trend_statistics(self, keyword: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed statistics for a specific keyword.
        
        Args:
            keyword: Keyword to get statistics for
            
        Returns:
            Dictionary with trend statistics or None if not found
        """
        stats = self.trend_detector.get_trend_statistics(keyword)
        if stats:
            return {
                'keyword': stats.keyword,
                'current_frequency': stats.current_frequency,
                'current_unique_authors': stats.current_unique_authors,
                'historical_frequencies': stats.historical_frequencies,
                'window_start': stats.window_start.isoformat(),
                'window_end': stats.window_end.isoformat(),
                'sample_posts': stats.sample_posts,
                'growth_rate': stats.calculate_growth_rate()
            }
        return None
    
    def get_pipeline_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive pipeline statistics.
        
        Returns:
            Dictionary with pipeline statistics
        """
        # Get trend detector stats
        detection_stats = self.trend_detector.get_detection_summary()
        
        # Get publisher stats
        publisher_stats = self.publisher_manager.get_all_stats()
        
        return {
            'processed_batches': self.processed_batches,
            'total_trends_detected': self.total_trends_detected,
            'total_alerts_published': self.total_alerts_published,
            'total_alerts_failed': self.total_alerts_failed,
            'detection_stats': detection_stats,
            'publisher_stats': publisher_stats,
            'kafka_bootstrap_servers': self.kafka_bootstrap_servers,
            'alert_topic': self.alert_topic,
            'dedup_window_minutes': self.dedup_window_minutes,
            'max_alerts_per_keyword': self.max_alerts_per_keyword
        }
    
    async def cleanup_old_data(self, retention_hours: int = 24) -> None:
        """
        Clean up old data to prevent memory bloat.
        
        Args:
            retention_hours: Hours of data to retain
        """
        from datetime import timedelta
        
        cutoff_time = datetime.now() - timedelta(hours=retention_hours)
        self.trend_detector.clear_old_statistics(cutoff_time)
        
        self.logger.info("Pipeline cleanup completed", retention_hours=retention_hours)
    
    async def close(self) -> None:
        """Close the pipeline and clean up resources."""
        try:
            await self.publisher_manager.close_all()
            
            self.logger.info(
                "Trend alert pipeline closed",
                processed_batches=self.processed_batches,
                total_trends_detected=self.total_trends_detected,
                total_alerts_published=self.total_alerts_published,
                total_alerts_failed=self.total_alerts_failed
            )
            
        except Exception as e:
            self.logger.error("Error closing pipeline", error=str(e))


class FlinkTrendAlertSink:
    """
    Flink sink function for publishing trend alerts through the pipeline.
    
    This class provides a Flink-compatible interface for the trend alert pipeline,
    allowing seamless integration with Flink streaming jobs.
    """
    
    def __init__(
        self,
        kafka_bootstrap_servers: str,
        alert_topic: str = "trend-alerts",
        trend_config: Optional[TrendDetectionConfig] = None,
        dedup_window_minutes: int = 30,
        max_alerts_per_keyword: int = 3
    ) -> None:
        """
        Initialize the Flink trend alert sink.
        
        Args:
            kafka_bootstrap_servers: Kafka broker addresses
            alert_topic: Kafka topic for alerts
            trend_config: Trend detection configuration
            dedup_window_minutes: Deduplication window
            max_alerts_per_keyword: Max alerts per keyword
        """
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.alert_topic = alert_topic
        self.trend_config = trend_config
        self.dedup_window_minutes = dedup_window_minutes
        self.max_alerts_per_keyword = max_alerts_per_keyword
        
        self.pipeline: Optional[TrendAlertPipeline] = None
        self.logger = structlog.get_logger(__name__)
    
    def open(self, configuration) -> None:
        """Initialize the sink (called by Flink)."""
        self.pipeline = TrendAlertPipeline(
            kafka_bootstrap_servers=self.kafka_bootstrap_servers,
            trend_config=self.trend_config,
            alert_topic=self.alert_topic,
            dedup_window_minutes=self.dedup_window_minutes,
            max_alerts_per_keyword=self.max_alerts_per_keyword
        )
        
        self.logger.info("FlinkTrendAlertSink initialized")
    
    def invoke(self, trend_alert: TrendAlert, context) -> None:
        """
        Process a single trend alert (called by Flink for each alert).
        
        Args:
            trend_alert: TrendAlert to process
            context: Flink sink context
        """
        if self.pipeline:
            # Use asyncio to run the async method
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(
                    self.pipeline.process_single_trend_alert(trend_alert)
                )
                
                if not result:
                    self.logger.warning(
                        "Failed to publish trend alert",
                        keyword=trend_alert.keyword,
                        rank=trend_alert.rank
                    )
            finally:
                loop.close()
    
    def close(self) -> None:
        """Clean up resources (called by Flink)."""
        if self.pipeline:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self.pipeline.close())
            finally:
                loop.close()
            
            self.pipeline = None
        
        self.logger.info("FlinkTrendAlertSink closed")


def create_trend_alert_pipeline(
    kafka_bootstrap_servers: str,
    alert_topic: str = "trend-alerts",
    min_frequency: int = 5,
    min_unique_authors: int = 2,
    min_z_score: float = 2.0,
    min_growth_rate: float = 50.0,
    max_trends: int = 50,
    dedup_window_minutes: int = 30,
    max_alerts_per_keyword: int = 3
) -> TrendAlertPipeline:
    """
    Create a configured trend alert pipeline.
    
    Args:
        kafka_bootstrap_servers: Kafka broker addresses
        alert_topic: Kafka topic for alerts
        min_frequency: Minimum frequency threshold for trends
        min_unique_authors: Minimum unique authors threshold
        min_z_score: Minimum z-score for statistical significance
        min_growth_rate: Minimum growth rate percentage
        max_trends: Maximum number of trends to track
        dedup_window_minutes: Deduplication window in minutes
        max_alerts_per_keyword: Maximum alerts per keyword in window
        
    Returns:
        Configured TrendAlertPipeline instance
    """
    trend_config = create_trend_detection_config(
        min_frequency=min_frequency,
        min_unique_authors=min_unique_authors,
        min_z_score=min_z_score,
        min_growth_rate=min_growth_rate,
        max_trends=max_trends
    )
    
    return TrendAlertPipeline(
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        trend_config=trend_config,
        alert_topic=alert_topic,
        dedup_window_minutes=dedup_window_minutes,
        max_alerts_per_keyword=max_alerts_per_keyword
    )


def create_flink_trend_alert_sink(
    kafka_bootstrap_servers: str,
    alert_topic: str = "trend-alerts",
    **kwargs
) -> FlinkTrendAlertSink:
    """
    Create a Flink-compatible trend alert sink.
    
    Args:
        kafka_bootstrap_servers: Kafka broker addresses
        alert_topic: Kafka topic for alerts
        **kwargs: Additional configuration parameters
        
    Returns:
        FlinkTrendAlertSink instance
    """
    return FlinkTrendAlertSink(
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        alert_topic=alert_topic,
        **kwargs
    )