"""
Unit tests for Kafka producer functionality.

This module contains comprehensive tests for the KafkaPostProducer class,
including serialization, partitioning, retry logic, and error handling.
"""

import asyncio
import json
import pytest
from datetime import datetime, timezone
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from typing import Dict, Any

from kafka.errors import KafkaError, KafkaTimeoutError, NoBrokersAvailable
from kafka import KafkaProducer

from kafka_producer import KafkaPostProducer, KafkaProducerManager
from shared.models import PostData


class TestKafkaPostProducer:
    """Test cases for KafkaPostProducer class."""
    
    @pytest.fixture
    def sample_post_data(self) -> PostData:
        """Create sample PostData for testing."""
        return PostData(
            uri="at://did:plc:test123/app.bsky.feed.post/abc123",
            author_did="did:plc:test123",
            text="This is a test post with #hashtag and @mention.user",
            created_at=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
            language="en",
            reply_to=None,
            mentions=["mention.user"],
            hashtags=["hashtag"]
        )
    
    @pytest.fixture
    def producer_config(self) -> Dict[str, Any]:
        """Default producer configuration for testing."""
        return {
            'bootstrap_servers': 'localhost:9092',
            'topic_name': 'test-topic',
            'max_retries': 3,
            'initial_backoff': 0.1,
            'max_backoff': 1.0,
            'backoff_multiplier': 2.0
        }
    
    @pytest.fixture
    def mock_kafka_producer(self):
        """Mock KafkaProducer for testing."""
        with patch('kafka_producer.KafkaProducer') as mock_producer_class:
            mock_producer = Mock(spec=KafkaProducer)
            mock_producer_class.return_value = mock_producer
            yield mock_producer
    
    def test_initialization(self, producer_config):
        """Test producer initialization with configuration."""
        producer = KafkaPostProducer(**producer_config)
        
        assert producer.bootstrap_servers == producer_config['bootstrap_servers']
        assert producer.topic_name == producer_config['topic_name']
        assert producer.max_retries == producer_config['max_retries']
        assert producer.initial_backoff == producer_config['initial_backoff']
        assert producer.max_backoff == producer_config['max_backoff']
        assert producer.backoff_multiplier == producer_config['backoff_multiplier']
        assert not producer.is_connected
        assert producer.messages_sent == 0
        assert producer.messages_failed == 0
    
    @pytest.mark.asyncio
    async def test_connect_success(self, producer_config, mock_kafka_producer):
        """Test successful connection to Kafka."""
        producer = KafkaPostProducer(**producer_config)
        
        await producer.connect()
        
        assert producer.is_connected
        assert producer._producer is not None
    
    @pytest.mark.asyncio
    async def test_connect_failure(self, producer_config):
        """Test connection failure handling."""
        producer = KafkaPostProducer(**producer_config)
        
        with patch('kafka_producer.KafkaProducer', side_effect=Exception("Connection failed")):
            with pytest.raises(KafkaError, match="Kafka connection failed"):
                await producer.connect()
        
        assert not producer.is_connected
        assert producer._producer is None
    
    def test_get_partition_key(self, producer_config, sample_post_data):
        """Test partition key generation based on author DID."""
        producer = KafkaPostProducer(**producer_config)
        
        partition_key = producer.get_partition_key(sample_post_data)
        
        assert partition_key == sample_post_data.author_did
    
    def test_serialize_post_data(self, sample_post_data):
        """Test PostData serialization to JSON."""
        serialized = KafkaPostProducer._serialize_post_data(sample_post_data)
        
        assert isinstance(serialized, bytes)
        
        # Deserialize and verify content
        deserialized = json.loads(serialized.decode('utf-8'))
        assert deserialized['uri'] == sample_post_data.uri
        assert deserialized['author_did'] == sample_post_data.author_did
        assert deserialized['text'] == sample_post_data.text
        assert deserialized['created_at'] == sample_post_data.created_at.isoformat()
        assert deserialized['language'] == sample_post_data.language
        assert deserialized['mentions'] == sample_post_data.mentions
        assert deserialized['hashtags'] == sample_post_data.hashtags
    
    def test_serialize_partition_key(self):
        """Test partition key serialization."""
        key = "test-key"
        serialized = KafkaPostProducer._serialize_partition_key(key)
        
        assert isinstance(serialized, bytes)
        assert serialized == key.encode('utf-8')
    
    @pytest.mark.asyncio
    async def test_publish_post_success(self, producer_config, sample_post_data, mock_kafka_producer):
        """Test successful post publishing."""
        producer = KafkaPostProducer(**producer_config)
        await producer.connect()
        
        # Mock successful send
        mock_future = Mock()
        mock_record_metadata = Mock()
        mock_record_metadata.topic = producer_config['topic_name']
        mock_record_metadata.partition = 0
        mock_record_metadata.offset = 123
        mock_future.get.return_value = mock_record_metadata
        mock_kafka_producer.send.return_value = mock_future
        
        result = await producer.publish_post(sample_post_data)
        
        assert result is True
        assert producer.messages_sent == 1
        assert producer.messages_failed == 0
        
        # Verify send was called with correct parameters
        mock_kafka_producer.send.assert_called_once()
        call_args = mock_kafka_producer.send.call_args
        assert call_args[1]['topic'] == producer_config['topic_name']
        assert call_args[1]['key'] == sample_post_data.author_did
        assert call_args[1]['value'] == sample_post_data
    
    @pytest.mark.asyncio
    async def test_publish_post_not_connected(self, producer_config, sample_post_data):
        """Test publishing when producer is not connected."""
        producer = KafkaPostProducer(**producer_config)
        
        with pytest.raises(KafkaError, match="Producer not connected"):
            await producer.publish_post(sample_post_data)
    
    @pytest.mark.asyncio
    async def test_publish_post_timeout_with_retry(self, producer_config, sample_post_data, mock_kafka_producer):
        """Test post publishing with timeout and retry logic."""
        producer = KafkaPostProducer(**producer_config)
        await producer.connect()
        
        # Mock timeout on first attempt, success on second
        mock_future = Mock()
        mock_future.get.side_effect = [
            KafkaTimeoutError("Timeout"),
            Mock(topic=producer_config['topic_name'], partition=0, offset=123)
        ]
        mock_kafka_producer.send.return_value = mock_future
        
        result = await producer.publish_post(sample_post_data)
        
        assert result is True
        assert producer.messages_sent == 1
        assert producer.retry_attempts == 1
        assert mock_kafka_producer.send.call_count == 2
    
    @pytest.mark.asyncio
    async def test_publish_post_max_retries_exceeded(self, producer_config, sample_post_data, mock_kafka_producer):
        """Test publishing failure after max retries exceeded."""
        producer = KafkaPostProducer(**producer_config)
        await producer.connect()
        
        # Mock timeout on all attempts
        mock_future = Mock()
        mock_future.get.side_effect = KafkaTimeoutError("Timeout")
        mock_kafka_producer.send.return_value = mock_future
        
        result = await producer.publish_post(sample_post_data)
        
        assert result is False
        assert producer.messages_sent == 0
        assert producer.messages_failed == 1
        assert producer.retry_attempts == producer_config['max_retries'] + 1
        assert mock_kafka_producer.send.call_count == producer_config['max_retries'] + 1
    
    @pytest.mark.asyncio
    async def test_handle_reconnection_success(self, producer_config, mock_kafka_producer):
        """Test successful producer reconnection."""
        producer = KafkaPostProducer(**producer_config)
        await producer.connect()
        
        # Simulate connection loss
        producer._is_connected = False
        
        await producer.handle_reconnection()
        
        assert producer.is_connected
    
    @pytest.mark.asyncio
    async def test_handle_reconnection_failure(self, producer_config):
        """Test reconnection failure after max attempts."""
        producer = KafkaPostProducer(**producer_config)
        
        with patch('kafka_producer.KafkaProducer', side_effect=Exception("Connection failed")):
            with pytest.raises(KafkaError, match="Producer reconnection failed"):
                await producer.handle_reconnection()
    
    @pytest.mark.asyncio
    async def test_flush(self, producer_config, mock_kafka_producer):
        """Test producer flush operation."""
        producer = KafkaPostProducer(**producer_config)
        await producer.connect()
        
        await producer.flush(timeout=10.0)
        
        mock_kafka_producer.flush.assert_called_once_with(timeout=10.0)
    
    @pytest.mark.asyncio
    async def test_close(self, producer_config, mock_kafka_producer):
        """Test producer close operation."""
        producer = KafkaPostProducer(**producer_config)
        await producer.connect()
        
        await producer.close()
        
        mock_kafka_producer.flush.assert_called_once()
        mock_kafka_producer.close.assert_called_once_with(timeout=10)
        assert not producer.is_connected
        assert producer._producer is None
    
    def test_get_stats(self, producer_config):
        """Test statistics retrieval."""
        producer = KafkaPostProducer(**producer_config)
        
        stats = producer.get_stats()
        
        assert isinstance(stats, dict)
        assert 'is_connected' in stats
        assert 'messages_sent' in stats
        assert 'messages_failed' in stats
        assert 'retry_attempts' in stats
        assert 'topic_name' in stats
        assert 'bootstrap_servers' in stats
        assert 'correlation_id' in stats
        
        assert stats['is_connected'] == producer.is_connected
        assert stats['messages_sent'] == producer.messages_sent
        assert stats['topic_name'] == producer_config['topic_name']


class TestKafkaProducerManager:
    """Test cases for KafkaProducerManager class."""
    
    @pytest.fixture
    def manager_config(self) -> Dict[str, str]:
        """Configuration for producer manager."""
        return {
            'bootstrap_servers': 'localhost:9092'
        }
    
    @pytest.fixture
    def sample_post_data(self) -> PostData:
        """Create sample PostData for testing."""
        return PostData(
            uri="at://did:plc:test123/app.bsky.feed.post/abc123",
            author_did="did:plc:test123",
            text="Test post for manager",
            created_at=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        )
    
    @pytest.mark.asyncio
    async def test_get_producer_new_topic(self, manager_config):
        """Test creating producer for new topic."""
        manager = KafkaProducerManager(**manager_config)
        
        with patch.object(KafkaPostProducer, 'connect', new_callable=AsyncMock):
            producer = await manager.get_producer('test-topic')
        
        assert isinstance(producer, KafkaPostProducer)
        assert 'test-topic' in manager.producers
        assert manager.producers['test-topic'] is producer
    
    @pytest.mark.asyncio
    async def test_get_producer_existing_topic(self, manager_config):
        """Test getting existing producer for topic."""
        manager = KafkaProducerManager(**manager_config)
        
        with patch.object(KafkaPostProducer, 'connect', new_callable=AsyncMock):
            producer1 = await manager.get_producer('test-topic')
            producer2 = await manager.get_producer('test-topic')
        
        assert producer1 is producer2
        assert len(manager.producers) == 1
    
    @pytest.mark.asyncio
    async def test_publish_to_topic(self, manager_config, sample_post_data):
        """Test publishing to topic through manager."""
        manager = KafkaProducerManager(**manager_config)
        
        with patch.object(KafkaPostProducer, 'connect', new_callable=AsyncMock), \
             patch.object(KafkaPostProducer, 'publish_post', new_callable=AsyncMock, return_value=True) as mock_publish:
            
            result = await manager.publish_to_topic('test-topic', sample_post_data)
        
        assert result is True
        mock_publish.assert_called_once_with(sample_post_data)
    
    @pytest.mark.asyncio
    async def test_close_all(self, manager_config):
        """Test closing all producers."""
        manager = KafkaProducerManager(**manager_config)
        
        # Create multiple producers
        with patch.object(KafkaPostProducer, 'connect', new_callable=AsyncMock), \
             patch.object(KafkaPostProducer, 'close', new_callable=AsyncMock) as mock_close:
            
            await manager.get_producer('topic1')
            await manager.get_producer('topic2')
            
            assert len(manager.producers) == 2
            
            await manager.close_all()
            
            assert len(manager.producers) == 0
            assert mock_close.call_count == 2
    
    def test_get_all_stats(self, manager_config):
        """Test getting statistics for all producers."""
        manager = KafkaProducerManager(**manager_config)
        
        # Mock producers with stats
        mock_producer1 = Mock()
        mock_producer1.get_stats.return_value = {'messages_sent': 10}
        mock_producer2 = Mock()
        mock_producer2.get_stats.return_value = {'messages_sent': 20}
        
        manager.producers['topic1'] = mock_producer1
        manager.producers['topic2'] = mock_producer2
        
        stats = manager.get_all_stats()
        
        assert len(stats) == 2
        assert stats['topic1']['messages_sent'] == 10
        assert stats['topic2']['messages_sent'] == 20


class TestKafkaProducerIntegration:
    """Integration tests for Kafka producer with error scenarios."""
    
    @pytest.fixture
    def producer_config(self) -> Dict[str, Any]:
        """Producer configuration for integration tests."""
        return {
            'bootstrap_servers': 'localhost:9092',
            'topic_name': 'integration-test-topic',
            'max_retries': 2,
            'initial_backoff': 0.01,
            'max_backoff': 0.1
        }
    
    @pytest.fixture
    def sample_post_data(self) -> PostData:
        """Sample post data for integration tests."""
        return PostData(
            uri="at://did:plc:integration/app.bsky.feed.post/test123",
            author_did="did:plc:integration",
            text="Integration test post #test",
            created_at=datetime.now(timezone.utc),
            hashtags=["test"]
        )
    
    @pytest.mark.asyncio
    async def test_no_brokers_available_reconnection(self, producer_config, sample_post_data):
        """Test reconnection when no brokers are available."""
        producer = KafkaPostProducer(**producer_config)
        
        with patch('kafka_producer.KafkaProducer') as mock_producer_class:
            # First connection succeeds
            mock_producer = Mock()
            mock_producer_class.return_value = mock_producer
            await producer.connect()
            
            # Simulate NoBrokersAvailable error, then success
            mock_future = Mock()
            mock_future.get.side_effect = [
                NoBrokersAvailable("No brokers"),
                Mock(topic=producer_config['topic_name'], partition=0, offset=1)
            ]
            mock_producer.send.return_value = mock_future
            
            # Mock reconnection success
            with patch.object(producer, 'handle_reconnection', new_callable=AsyncMock):
                result = await producer.publish_post(sample_post_data)
            
            assert result is True
            assert producer.retry_attempts > 0
    
    @pytest.mark.asyncio
    async def test_serialization_with_unicode(self, producer_config):
        """Test serialization with Unicode characters."""
        producer = KafkaPostProducer(**producer_config)
        
        # Create post with Unicode content
        unicode_post = PostData(
            uri="at://did:plc:unicode/app.bsky.feed.post/test",
            author_did="did:plc:unicode",
            text="Test with émojis 🚀 and ñoñó characters",
            created_at=datetime.now(timezone.utc),
            language="es"
        )
        
        serialized = KafkaPostProducer._serialize_post_data(unicode_post)
        
        # Verify serialization preserves Unicode
        deserialized = json.loads(serialized.decode('utf-8'))
        assert deserialized['text'] == unicode_post.text
        assert "🚀" in deserialized['text']
        assert "ñoñó" in deserialized['text']
    
    @pytest.mark.asyncio
    async def test_concurrent_publishing(self, producer_config):
        """Test concurrent post publishing."""
        with patch('kafka_producer.KafkaProducer') as mock_producer_class:
            mock_kafka_producer = Mock(spec=KafkaProducer)
            mock_producer_class.return_value = mock_kafka_producer
            
            producer = KafkaPostProducer(**producer_config)
            await producer.connect()
            
            # Mock successful sends
            mock_future = Mock()
            mock_future.get.return_value = Mock(topic=producer_config['topic_name'], partition=0, offset=1)
            mock_kafka_producer.send.return_value = mock_future
        
            # Create multiple posts
            posts = [
                PostData(
                    uri=f"at://did:plc:concurrent/app.bsky.feed.post/{i}",
                    author_did=f"did:plc:concurrent{i}",
                    text=f"Concurrent test post {i}",
                    created_at=datetime.now(timezone.utc)
                )
                for i in range(5)
            ]
            
            # Publish concurrently
            tasks = [producer.publish_post(post) for post in posts]
            results = await asyncio.gather(*tasks)
            
            # Verify all succeeded
            assert all(results)
            assert producer.messages_sent == 5
            assert mock_kafka_producer.send.call_count == 5


if __name__ == "__main__":
    pytest.main([__file__, "-v"])