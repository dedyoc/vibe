#!/usr/bin/env python3
"""
Trend Alert Publisher

This module implements the Kafka producer for publishing TrendAlert messages
with deduplication, proper serialization, and retry logic.
"""

import asyncio
import json
import logging
import hashlib
from typing import Dict, Any, Optional, Set, List
from datetime import datetime, timedelta
import uuid
from collections import defaultdict

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError, NoBrokersAvailable
import structlog

from shared.models import TrendAlert

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TrendAlertDeduplicator:
    """
    Handles deduplication of trend alerts to prevent spam from repeated trends.
    
    This class maintains a sliding window of recently published trends and
    prevents duplicate alerts within a configurable time window.
    """
    
    def __init__(
        self,
        dedup_window_minutes: int = 30,
        max_alerts_per_keyword: int = 3,
        cleanup_interval_minutes: int = 60
    ) -> None:
        """
        Initialize the deduplicator.
        
        Args:
            dedup_window_minutes: Time window for deduplication in minutes
            max_alerts_per_keyword: Maximum alerts per keyword within window
            cleanup_interval_minutes: How often to clean up old entries
        """
        self.dedup_window = timedelta(minutes=dedup_window_minutes)
        self.max_alerts_per_keyword = max_alerts_per_keyword
        self.cleanup_interval = timedelta(minutes=cleanup_interval_minutes)
        
        # Track published alerts: keyword -> list of (timestamp, alert_hash)
        self.published_alerts: Dict[str, List[tuple]] = defaultdict(list)
        self.last_cleanup = datetime.now()
        
        self.logger = structlog.get_logger(__name__)
    
    def should_publish_alert(self, alert: TrendAlert) -> bool:
        """
        Check if an alert should be published based on deduplication rules.
        
        Args:
            alert: TrendAlert to check
            
        Returns:
            True if alert should be published, False if it's a duplicate
        """
        current_time = datetime.now()
        keyword = alert.keyword.lower()
        
        # Clean up old entries periodically
        if current_time - self.last_cleanup > self.cleanup_interval:
            self._cleanup_old_entries(current_time)
        
        # Generate alert hash for exact duplicate detection
        alert_hash = self._generate_alert_hash(alert)
        
        # Check recent alerts for this keyword
        recent_alerts = self.published_alerts[keyword]
        
        # Remove alerts outside the deduplication window
        cutoff_time = current_time - self.dedup_window
        recent_alerts = [
            (timestamp, hash_val) for timestamp, hash_val in recent_alerts
            if timestamp > cutoff_time
        ]
        self.published_alerts[keyword] = recent_alerts
        
        # Check for exact duplicates
        for _, existing_hash in recent_alerts:
            if existing_hash == alert_hash:
                self.logger.debug(
                    "Duplicate alert detected, skipping",
                    keyword=keyword,
                    alert_hash=alert_hash
                )
                return False
        
        # Check if we've exceeded the maximum alerts per keyword
        if len(recent_alerts) >= self.max_alerts_per_keyword:
            self.logger.debug(
                "Maximum alerts per keyword reached, skipping",
                keyword=keyword,
                recent_count=len(recent_alerts),
                max_allowed=self.max_alerts_per_keyword
            )
            return False
        
        # Alert should be published
        return True
    
    def mark_alert_published(self, alert: TrendAlert) -> None:
        """
        Mark an alert as published to prevent future duplicates.
        
        Args:
            alert: TrendAlert that was published
        """
        current_time = datetime.now()
        keyword = alert.keyword.lower()
        alert_hash = self._generate_alert_hash(alert)
        
        self.published_alerts[keyword].append((current_time, alert_hash))
        
        self.logger.debug(
            "Alert marked as published",
            keyword=keyword,
            alert_hash=alert_hash
        )
    
    def _generate_alert_hash(self, alert: TrendAlert) -> str:
        """
        Generate a hash for an alert to detect exact duplicates.
        
        Args:
            alert: TrendAlert to hash
            
        Returns:
            Hash string for the alert
        """
        # Create hash based on key alert properties (excluding timestamps for consistency)
        # Round window times to minute precision to handle slight timing differences
        window_start_minute = alert.window_start.replace(second=0, microsecond=0)
        hash_data = f"{alert.keyword}:{alert.frequency}:{alert.growth_rate:.2f}:{window_start_minute.isoformat()}"
        return hashlib.md5(hash_data.encode()).hexdigest()
    
    def _cleanup_old_entries(self, current_time: datetime) -> None:
        """
        Clean up old entries to prevent memory bloat.
        
        Args:
            current_time: Current timestamp for cleanup
        """
        cutoff_time = current_time - self.dedup_window * 2  # Keep extra buffer
        
        keywords_to_remove = []
        for keyword, alerts in self.published_alerts.items():
            # Filter out old alerts
            recent_alerts = [
                (timestamp, hash_val) for timestamp, hash_val in alerts
                if timestamp > cutoff_time
            ]
            
            if recent_alerts:
                self.published_alerts[keyword] = recent_alerts
            else:
                keywords_to_remove.append(keyword)
        
        # Remove keywords with no recent alerts
        for keyword in keywords_to_remove:
            del self.published_alerts[keyword]
        
        self.last_cleanup = current_time
        
        self.logger.debug(
            "Deduplicator cleanup completed",
            keywords_cleaned=len(keywords_to_remove),
            active_keywords=len(self.published_alerts)
        )
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get deduplicator statistics.
        
        Returns:
            Dictionary with deduplicator statistics
        """
        total_alerts = sum(len(alerts) for alerts in self.published_alerts.values())
        
        return {
            'active_keywords': len(self.published_alerts),
            'total_recent_alerts': total_alerts,
            'dedup_window_minutes': self.dedup_window.total_seconds() / 60,
            'max_alerts_per_keyword': self.max_alerts_per_keyword,
            'last_cleanup': self.last_cleanup.isoformat()
        }


class TrendAlertProducer:
    """
    Kafka producer for publishing TrendAlert messages with deduplication and retry logic.
    
    This producer handles JSON serialization, deduplication, retry logic with
    exponential backoff, and message replication for fault tolerance.
    """
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic_name: str = "trend-alerts",
        max_retries: int = 5,
        initial_backoff: float = 1.0,
        max_backoff: float = 60.0,
        backoff_multiplier: float = 2.0,
        batch_size: int = 16384,
        linger_ms: int = 10,
        acks: str = "all",
        retries: int = 3,
        enable_idempotence: bool = True,
        dedup_window_minutes: int = 30,
        max_alerts_per_keyword: int = 3
    ) -> None:
        """
        Initialize the trend alert producer.
        
        Args:
            bootstrap_servers: Comma-separated list of Kafka broker addresses
            topic_name: Name of the Kafka topic to publish to
            max_retries: Maximum number of retry attempts for failed sends
            initial_backoff: Initial backoff delay in seconds
            max_backoff: Maximum backoff delay in seconds
            backoff_multiplier: Multiplier for exponential backoff
            batch_size: Batch size for producer batching
            linger_ms: Time to wait for additional messages before sending batch
            acks: Acknowledgment level ('0', '1', or 'all')
            retries: Number of retries at Kafka client level
            enable_idempotence: Enable idempotent producer for exactly-once semantics
            dedup_window_minutes: Time window for alert deduplication
            max_alerts_per_keyword: Maximum alerts per keyword within window
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.backoff_multiplier = backoff_multiplier
        
        # Initialize deduplicator
        self.deduplicator = TrendAlertDeduplicator(
            dedup_window_minutes=dedup_window_minutes,
            max_alerts_per_keyword=max_alerts_per_keyword
        )
        
        # Logging with correlation IDs
        self.logger = structlog.get_logger(__name__)
        self._correlation_id = str(uuid.uuid4())
        
        # Statistics
        self._alerts_sent = 0
        self._alerts_failed = 0
        self._alerts_deduplicated = 0
        self._retry_attempts = 0
        
        # Producer configuration
        self._producer_config = {
            'bootstrap_servers': bootstrap_servers.split(','),
            'value_serializer': self._serialize_trend_alert,
            'key_serializer': self._serialize_partition_key,
            'batch_size': batch_size,
            'linger_ms': linger_ms,
            'acks': acks,
            'retries': retries,
            'enable_idempotence': enable_idempotence,
            'max_in_flight_requests_per_connection': 5 if enable_idempotence else 1,
            'compression_type': 'snappy',  # Efficient compression
            'request_timeout_ms': 30000,
            'delivery_timeout_ms': 120000,
        }
        
        self._producer: Optional[KafkaProducer] = None
        self._is_connected = False
    
    async def connect(self) -> None:
        """
        Connect to Kafka cluster and initialize producer.
        
        Raises:
            KafkaError: If connection to Kafka fails
        """
        self._correlation_id = str(uuid.uuid4())
        log = self.logger.bind(
            correlation_id=self._correlation_id,
            bootstrap_servers=self.bootstrap_servers,
            topic=self.topic_name
        )
        
        log.info("Connecting to Kafka cluster for trend alerts")
        
        try:
            self._producer = KafkaProducer(**self._producer_config)
            self._is_connected = True
            
            log.info("Successfully connected to Kafka cluster")
            
        except Exception as e:
            self._is_connected = False
            log.error("Failed to connect to Kafka", error=str(e))
            raise KafkaError(f"Kafka connection failed: {e}")
    
    async def publish_trend_alert(self, alert: TrendAlert) -> bool:
        """
        Publish a trend alert to Kafka with deduplication and retry logic.
        
        Args:
            alert: TrendAlert object to publish
            
        Returns:
            True if message was successfully sent, False otherwise
        """
        if not self._producer or not self._is_connected:
            raise KafkaError("Producer not connected. Call connect() first.")
        
        log = self.logger.bind(
            correlation_id=self._correlation_id,
            keyword=alert.keyword,
            rank=alert.rank,
            frequency=alert.frequency
        )
        
        # Check deduplication
        if not self.deduplicator.should_publish_alert(alert):
            self._alerts_deduplicated += 1
            log.debug("Alert deduplicated, skipping publication")
            return True  # Return True since this is expected behavior
        
        # Generate partition key based on keyword
        partition_key = self.get_partition_key(alert)
        
        retry_count = 0
        while retry_count <= self.max_retries:
            try:
                # Send message asynchronously
                future = self._producer.send(
                    topic=self.topic_name,
                    key=partition_key,
                    value=alert,
                    timestamp_ms=int(alert.window_end.timestamp() * 1000)
                )
                
                # Wait for acknowledgment (with timeout)
                record_metadata = future.get(timeout=30)
                
                # Mark alert as published for deduplication
                self.deduplicator.mark_alert_published(alert)
                
                self._alerts_sent += 1
                log.info(
                    "Trend alert sent successfully",
                    topic=record_metadata.topic,
                    partition=record_metadata.partition,
                    offset=record_metadata.offset,
                    growth_rate=alert.growth_rate,
                    confidence_score=alert.confidence_score
                )
                
                return True
                
            except (KafkaTimeoutError, KafkaError) as e:
                retry_count += 1
                self._retry_attempts += 1
                
                if retry_count > self.max_retries:
                    self._alerts_failed += 1
                    log.error(
                        "Trend alert send failed after all retries",
                        error=str(e),
                        retry_count=retry_count
                    )
                    return False
                
                # Calculate backoff delay
                backoff_delay = min(
                    self.initial_backoff * (self.backoff_multiplier ** (retry_count - 1)),
                    self.max_backoff
                )
                
                log.warning(
                    "Trend alert send failed, retrying",
                    error=str(e),
                    retry_count=retry_count,
                    backoff_delay=backoff_delay
                )
                
                await asyncio.sleep(backoff_delay)
                
                # Try to reconnect if connection was lost
                if isinstance(e, NoBrokersAvailable):
                    try:
                        await self.handle_reconnection()
                    except Exception as reconnect_error:
                        log.error("Reconnection failed", error=str(reconnect_error))
            
            except Exception as e:
                self._alerts_failed += 1
                log.error("Unexpected error sending trend alert", error=str(e))
                return False
        
        return False
    
    async def publish_trend_alerts_batch(self, alerts: List[TrendAlert]) -> Dict[str, int]:
        """
        Publish a batch of trend alerts.
        
        Args:
            alerts: List of TrendAlert objects to publish
            
        Returns:
            Dictionary with batch statistics
        """
        if not alerts:
            return {'sent': 0, 'failed': 0, 'deduplicated': 0}
        
        log = self.logger.bind(
            correlation_id=self._correlation_id,
            batch_size=len(alerts)
        )
        
        log.info("Publishing batch of trend alerts")
        
        sent_count = 0
        failed_count = 0
        dedup_count = 0
        
        for alert in alerts:
            try:
                # Check deduplication before attempting to publish
                if not self.deduplicator.should_publish_alert(alert):
                    dedup_count += 1
                    log.debug("Alert deduplicated in batch", keyword=alert.keyword)
                    continue
                
                success = await self.publish_trend_alert(alert)
                if success:
                    sent_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                failed_count += 1
                log.error("Error publishing alert in batch", keyword=alert.keyword, error=str(e))
        
        log.info(
            "Batch publication completed",
            sent=sent_count,
            failed=failed_count,
            deduplicated=dedup_count
        )
        
        return {
            'sent': sent_count,
            'failed': failed_count,
            'deduplicated': dedup_count
        }
    
    def get_partition_key(self, alert: TrendAlert) -> str:
        """
        Generate partition key based on keyword for load balancing.
        
        Args:
            alert: TrendAlert object
            
        Returns:
            Partition key string
        """
        # Use keyword as partition key to ensure alerts for same keyword
        # go to same partition, enabling ordered processing per keyword
        return alert.keyword.lower()
    
    async def handle_reconnection(self) -> None:
        """
        Handle producer reconnection with exponential backoff.
        
        Raises:
            KafkaError: If reconnection fails after all attempts
        """
        log = self.logger.bind(correlation_id=self._correlation_id)
        log.info("Attempting to reconnect trend alert producer")
        
        # Close existing producer
        if self._producer:
            try:
                self._producer.close(timeout=10)
            except Exception as e:
                log.warning("Error closing existing producer", error=str(e))
            finally:
                self._producer = None
                self._is_connected = False
        
        # Reconnect with exponential backoff
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                backoff_delay = min(
                    self.initial_backoff * (self.backoff_multiplier ** retry_count),
                    self.max_backoff
                )
                
                if retry_count > 0:
                    log.info("Waiting before reconnection attempt", backoff_delay=backoff_delay)
                    await asyncio.sleep(backoff_delay)
                
                await self.connect()
                log.info("Trend alert producer reconnection successful")
                return
                
            except Exception as e:
                retry_count += 1
                log.warning(
                    "Reconnection attempt failed",
                    error=str(e),
                    retry_count=retry_count
                )
        
        log.error("All reconnection attempts failed")
        raise KafkaError("Trend alert producer reconnection failed after all attempts")
    
    async def flush(self, timeout: float = 30.0) -> None:
        """
        Flush any pending messages.
        
        Args:
            timeout: Maximum time to wait for flush completion
        """
        if self._producer:
            try:
                self._producer.flush(timeout=timeout)
                self.logger.info("Trend alert producer flush completed", correlation_id=self._correlation_id)
            except Exception as e:
                self.logger.error("Trend alert producer flush failed", error=str(e))
    
    async def close(self) -> None:
        """Close the Kafka producer and clean up resources."""
        if self._producer:
            try:
                # Flush any pending messages
                await self.flush()
                
                # Close producer
                self._producer.close(timeout=10)
                self._producer = None
                self._is_connected = False
                
                self.logger.info(
                    "Trend alert producer closed",
                    correlation_id=self._correlation_id,
                    alerts_sent=self._alerts_sent,
                    alerts_failed=self._alerts_failed,
                    alerts_deduplicated=self._alerts_deduplicated,
                    retry_attempts=self._retry_attempts
                )
                
            except Exception as e:
                self.logger.error("Error closing trend alert producer", error=str(e))
    
    @staticmethod
    def _serialize_trend_alert(alert: TrendAlert) -> bytes:
        """
        Serialize TrendAlert to JSON bytes.
        
        Args:
            alert: TrendAlert object to serialize
            
        Returns:
            JSON-encoded bytes
        """
        # Convert TrendAlert to dictionary
        alert_dict = {
            'keyword': alert.keyword,
            'frequency': alert.frequency,
            'growth_rate': alert.growth_rate,
            'confidence_score': alert.confidence_score,
            'window_start': alert.window_start.isoformat(),
            'window_end': alert.window_end.isoformat(),
            'sample_posts': alert.sample_posts,
            'unique_authors': alert.unique_authors,
            'rank': alert.rank,
            'published_at': datetime.now().isoformat()  # Add publication timestamp
        }
        
        # Serialize to JSON bytes
        return json.dumps(alert_dict, ensure_ascii=False).encode('utf-8')
    
    @staticmethod
    def _serialize_partition_key(key: str) -> bytes:
        """
        Serialize partition key to bytes.
        
        Args:
            key: Partition key string
            
        Returns:
            UTF-8 encoded bytes
        """
        return key.encode('utf-8')
    
    @property
    def is_connected(self) -> bool:
        """Check if producer is connected to Kafka."""
        return self._is_connected
    
    @property
    def alerts_sent(self) -> int:
        """Get number of alerts successfully sent."""
        return self._alerts_sent
    
    @property
    def alerts_failed(self) -> int:
        """Get number of alerts that failed to send."""
        return self._alerts_failed
    
    @property
    def alerts_deduplicated(self) -> int:
        """Get number of alerts that were deduplicated."""
        return self._alerts_deduplicated
    
    @property
    def retry_attempts(self) -> int:
        """Get total number of retry attempts."""
        return self._retry_attempts
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get producer statistics.
        
        Returns:
            Dictionary containing producer statistics
        """
        return {
            'is_connected': self._is_connected,
            'alerts_sent': self._alerts_sent,
            'alerts_failed': self._alerts_failed,
            'alerts_deduplicated': self._alerts_deduplicated,
            'retry_attempts': self._retry_attempts,
            'topic_name': self.topic_name,
            'bootstrap_servers': self.bootstrap_servers,
            'correlation_id': self._correlation_id,
            'deduplicator_stats': self.deduplicator.get_stats()
        }


class TrendAlertPublisherManager:
    """
    Manager class for handling trend alert publishing with multiple topics and configurations.
    
    This class provides a higher-level interface for managing trend alert publishers
    and handling publisher lifecycle.
    """
    
    def __init__(self, bootstrap_servers: str) -> None:
        """
        Initialize the publisher manager.
        
        Args:
            bootstrap_servers: Comma-separated list of Kafka broker addresses
        """
        self.bootstrap_servers = bootstrap_servers
        self.publishers: Dict[str, TrendAlertProducer] = {}
        self.logger = structlog.get_logger(__name__)
    
    async def get_publisher(
        self,
        topic_name: str = "trend-alerts",
        **publisher_kwargs
    ) -> TrendAlertProducer:
        """
        Get or create a publisher for the specified topic.
        
        Args:
            topic_name: Name of the Kafka topic
            **publisher_kwargs: Additional arguments for publisher configuration
            
        Returns:
            TrendAlertProducer instance
        """
        if topic_name not in self.publishers:
            publisher = TrendAlertProducer(
                bootstrap_servers=self.bootstrap_servers,
                topic_name=topic_name,
                **publisher_kwargs
            )
            await publisher.connect()
            self.publishers[topic_name] = publisher
            
            self.logger.info("Created new trend alert publisher", topic=topic_name)
        
        return self.publishers[topic_name]
    
    async def publish_alert(
        self,
        alert: TrendAlert,
        topic_name: str = "trend-alerts",
        **publisher_kwargs
    ) -> bool:
        """
        Publish a trend alert to the specified topic.
        
        Args:
            alert: TrendAlert object to publish
            topic_name: Name of the Kafka topic
            **publisher_kwargs: Additional arguments for publisher configuration
            
        Returns:
            True if alert was successfully sent, False otherwise
        """
        publisher = await self.get_publisher(topic_name, **publisher_kwargs)
        return await publisher.publish_trend_alert(alert)
    
    async def publish_alerts_batch(
        self,
        alerts: List[TrendAlert],
        topic_name: str = "trend-alerts",
        **publisher_kwargs
    ) -> Dict[str, int]:
        """
        Publish a batch of trend alerts to the specified topic.
        
        Args:
            alerts: List of TrendAlert objects to publish
            topic_name: Name of the Kafka topic
            **publisher_kwargs: Additional arguments for publisher configuration
            
        Returns:
            Dictionary with batch statistics
        """
        publisher = await self.get_publisher(topic_name, **publisher_kwargs)
        return await publisher.publish_trend_alerts_batch(alerts)
    
    async def close_all(self) -> None:
        """Close all publishers and clean up resources."""
        for topic_name, publisher in self.publishers.items():
            try:
                await publisher.close()
                self.logger.info("Closed trend alert publisher", topic=topic_name)
            except Exception as e:
                self.logger.error("Error closing publisher", topic=topic_name, error=str(e))
        
        self.publishers.clear()
        self.logger.info("All trend alert publishers closed")
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """
        Get statistics for all publishers.
        
        Returns:
            Dictionary mapping topic names to publisher statistics
        """
        return {
            topic_name: publisher.get_stats()
            for topic_name, publisher in self.publishers.items()
        }