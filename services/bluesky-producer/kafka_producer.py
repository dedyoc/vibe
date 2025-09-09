"""
Kafka Producer for Bluesky Posts

This module implements the KafkaPostProducer class that publishes PostData
to Kafka topics with proper partitioning, serialization, and retry logic.
"""

import asyncio
import json
import logging
import hashlib
from typing import Dict, Any, Optional, List
from datetime import datetime
import uuid

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError, NoBrokersAvailable
import structlog

from shared.models import PostData


class KafkaPostProducer:
    """Kafka producer for publishing Bluesky posts with resilience features.
    
    This producer handles JSON serialization, partitioning by author DID,
    retry logic with exponential backoff, and message replication for fault tolerance.
    """
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic_name: str = "bluesky-posts",
        max_retries: int = 5,
        initial_backoff: float = 1.0,
        max_backoff: float = 60.0,
        backoff_multiplier: float = 2.0,
        batch_size: int = 16384,
        linger_ms: int = 10,
        acks: str = "all",
        retries: int = 3,
        enable_idempotence: bool = True
    ) -> None:
        """Initialize the Kafka producer.
        
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
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self.backoff_multiplier = backoff_multiplier
        
        # Logging with correlation IDs
        self.logger = structlog.get_logger(__name__)
        self._correlation_id = str(uuid.uuid4())
        
        # Statistics
        self._messages_sent = 0
        self._messages_failed = 0
        self._retry_attempts = 0
        
        # Producer configuration
        self._producer_config = {
            'bootstrap_servers': bootstrap_servers.split(','),
            'value_serializer': self._serialize_post_data,
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
        """Connect to Kafka cluster and initialize producer.
        
        Raises:
            KafkaError: If connection to Kafka fails
        """
        self._correlation_id = str(uuid.uuid4())
        log = self.logger.bind(
            correlation_id=self._correlation_id,
            bootstrap_servers=self.bootstrap_servers,
            topic=self.topic_name
        )
        
        log.info("Connecting to Kafka cluster")
        
        try:
            self._producer = KafkaProducer(**self._producer_config)
            self._is_connected = True
            
            log.info("Successfully connected to Kafka cluster")
            
        except Exception as e:
            self._is_connected = False
            log.error("Failed to connect to Kafka", error=str(e))
            raise KafkaError(f"Kafka connection failed: {e}")
    
    async def publish_post(self, post: PostData) -> bool:
        """Publish a post to Kafka with retry logic.
        
        Args:
            post: PostData object to publish
            
        Returns:
            True if message was successfully sent, False otherwise
        """
        if not self._producer or not self._is_connected:
            raise KafkaError("Producer not connected. Call connect() first.")
        
        log = self.logger.bind(
            correlation_id=self._correlation_id,
            post_uri=post.uri,
            author_did=post.author_did
        )
        
        # Generate partition key based on author DID
        partition_key = self.get_partition_key(post)
        
        retry_count = 0
        while retry_count <= self.max_retries:
            try:
                # Send message asynchronously
                future = self._producer.send(
                    topic=self.topic_name,
                    key=partition_key,
                    value=post,
                    timestamp_ms=int(post.created_at.timestamp() * 1000)
                )
                
                # Wait for acknowledgment (with timeout)
                record_metadata = future.get(timeout=30)
                
                self._messages_sent += 1
                log.info(
                    "Message sent successfully",
                    topic=record_metadata.topic,
                    partition=record_metadata.partition,
                    offset=record_metadata.offset
                )
                
                return True
                
            except (KafkaTimeoutError, KafkaError) as e:
                retry_count += 1
                self._retry_attempts += 1
                
                if retry_count > self.max_retries:
                    self._messages_failed += 1
                    log.error(
                        "Message send failed after all retries",
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
                    "Message send failed, retrying",
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
                self._messages_failed += 1
                log.error("Unexpected error sending message", error=str(e))
                return False
        
        return False
    
    def get_partition_key(self, post: PostData) -> str:
        """Generate partition key based on author DID for load balancing.
        
        Args:
            post: PostData object
            
        Returns:
            Partition key string
        """
        # Use author DID as partition key to ensure posts from same author
        # go to same partition, enabling ordered processing per author
        return post.author_did
    
    async def handle_reconnection(self) -> None:
        """Handle producer reconnection with exponential backoff.
        
        Raises:
            KafkaError: If reconnection fails after all attempts
        """
        log = self.logger.bind(correlation_id=self._correlation_id)
        log.info("Attempting to reconnect Kafka producer")
        
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
                log.info("Producer reconnection successful")
                return
                
            except Exception as e:
                retry_count += 1
                log.warning(
                    "Reconnection attempt failed",
                    error=str(e),
                    retry_count=retry_count
                )
        
        log.error("All reconnection attempts failed")
        raise KafkaError("Producer reconnection failed after all attempts")
    
    async def flush(self, timeout: float = 30.0) -> None:
        """Flush any pending messages.
        
        Args:
            timeout: Maximum time to wait for flush completion
        """
        if self._producer:
            try:
                self._producer.flush(timeout=timeout)
                self.logger.info("Producer flush completed", correlation_id=self._correlation_id)
            except Exception as e:
                self.logger.error("Producer flush failed", error=str(e))
    
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
                    "Kafka producer closed",
                    correlation_id=self._correlation_id,
                    messages_sent=self._messages_sent,
                    messages_failed=self._messages_failed,
                    retry_attempts=self._retry_attempts
                )
                
            except Exception as e:
                self.logger.error("Error closing Kafka producer", error=str(e))
    
    @staticmethod
    def _serialize_post_data(post_data: PostData) -> bytes:
        """Serialize PostData to JSON bytes.
        
        Args:
            post_data: PostData object to serialize
            
        Returns:
            JSON-encoded bytes
        """
        # Convert PostData to dictionary
        post_dict = {
            'uri': post_data.uri,
            'author_did': post_data.author_did,
            'text': post_data.text,
            'created_at': post_data.created_at.isoformat(),
            'language': post_data.language,
            'reply_to': post_data.reply_to,
            'mentions': post_data.mentions,
            'hashtags': post_data.hashtags
        }
        
        # Serialize to JSON bytes
        return json.dumps(post_dict, ensure_ascii=False).encode('utf-8')
    
    @staticmethod
    def _serialize_partition_key(key: str) -> bytes:
        """Serialize partition key to bytes.
        
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
    def messages_sent(self) -> int:
        """Get number of messages successfully sent."""
        return self._messages_sent
    
    @property
    def messages_failed(self) -> int:
        """Get number of messages that failed to send."""
        return self._messages_failed
    
    @property
    def retry_attempts(self) -> int:
        """Get total number of retry attempts."""
        return self._retry_attempts
    
    def get_stats(self) -> Dict[str, Any]:
        """Get producer statistics.
        
        Returns:
            Dictionary containing producer statistics
        """
        return {
            'is_connected': self._is_connected,
            'messages_sent': self._messages_sent,
            'messages_failed': self._messages_failed,
            'retry_attempts': self._retry_attempts,
            'topic_name': self.topic_name,
            'bootstrap_servers': self.bootstrap_servers,
            'correlation_id': self._correlation_id
        }


class KafkaProducerManager:
    """Manager class for handling multiple Kafka producers and topics.
    
    This class provides a higher-level interface for managing Kafka producers
    for different topics and handling producer lifecycle.
    """
    
    def __init__(self, bootstrap_servers: str) -> None:
        """Initialize the producer manager.
        
        Args:
            bootstrap_servers: Comma-separated list of Kafka broker addresses
        """
        self.bootstrap_servers = bootstrap_servers
        self.producers: Dict[str, KafkaPostProducer] = {}
        self.logger = structlog.get_logger(__name__)
    
    async def get_producer(
        self,
        topic_name: str,
        **producer_kwargs
    ) -> KafkaPostProducer:
        """Get or create a producer for the specified topic.
        
        Args:
            topic_name: Name of the Kafka topic
            **producer_kwargs: Additional arguments for producer configuration
            
        Returns:
            KafkaPostProducer instance
        """
        if topic_name not in self.producers:
            producer = KafkaPostProducer(
                bootstrap_servers=self.bootstrap_servers,
                topic_name=topic_name,
                **producer_kwargs
            )
            await producer.connect()
            self.producers[topic_name] = producer
            
            self.logger.info("Created new producer", topic=topic_name)
        
        return self.producers[topic_name]
    
    async def publish_to_topic(
        self,
        topic_name: str,
        post: PostData,
        **producer_kwargs
    ) -> bool:
        """Publish a post to the specified topic.
        
        Args:
            topic_name: Name of the Kafka topic
            post: PostData object to publish
            **producer_kwargs: Additional arguments for producer configuration
            
        Returns:
            True if message was successfully sent, False otherwise
        """
        producer = await self.get_producer(topic_name, **producer_kwargs)
        return await producer.publish_post(post)
    
    async def close_all(self) -> None:
        """Close all producers and clean up resources."""
        for topic_name, producer in self.producers.items():
            try:
                await producer.close()
                self.logger.info("Closed producer", topic=topic_name)
            except Exception as e:
                self.logger.error("Error closing producer", topic=topic_name, error=str(e))
        
        self.producers.clear()
        self.logger.info("All producers closed")
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all producers.
        
        Returns:
            Dictionary mapping topic names to producer statistics
        """
        return {
            topic_name: producer.get_stats()
            for topic_name, producer in self.producers.items()
        }