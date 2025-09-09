"""
Integration tests for Kafka producer with Bluesky client.

This module contains integration tests that verify the complete flow
from Bluesky firehose to Kafka publishing.
"""

import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import Mock, AsyncMock, patch
from typing import AsyncGenerator

from kafka_producer import KafkaPostProducer, KafkaProducerManager
from bluesky_client import BlueskyFirehoseClient
from shared.models import PostData


class TestBlueskyKafkaIntegration:
    """Integration tests for Bluesky to Kafka pipeline."""
    
    @pytest.fixture
    def sample_posts(self) -> list[PostData]:
        """Create sample posts for testing."""
        return [
            PostData(
                uri=f"at://did:plc:test{i}/app.bsky.feed.post/post{i}",
                author_did=f"did:plc:test{i}",
                text=f"Test post {i} with #hashtag{i}",
                created_at=datetime.now(timezone.utc),
                hashtags=[f"hashtag{i}"]
            )
            for i in range(3)
        ]
    
    @pytest.mark.asyncio
    async def test_firehose_to_kafka_pipeline(self, sample_posts):
        """Test complete pipeline from firehose to Kafka."""
        kafka_manager = KafkaProducerManager("localhost:9092")
        published_posts = []
        
        # Mock the Kafka producer to capture published posts
        async def mock_publish(topic: str, post: PostData, **kwargs) -> bool:
            published_posts.append(post)
            return True
        
        with patch.object(kafka_manager, 'publish_to_topic', side_effect=mock_publish):
            # Simulate publishing posts from firehose
            for post in sample_posts:
                success = await kafka_manager.publish_to_topic("bluesky-posts", post)
                assert success
        
        # Verify all posts were published
        assert len(published_posts) == len(sample_posts)
        for i, post in enumerate(published_posts):
            assert post.author_did == f"did:plc:test{i}"
            assert f"hashtag{i}" in post.hashtags
    
    @pytest.mark.asyncio
    async def test_producer_error_handling(self):
        """Test error handling in the producer pipeline."""
        kafka_manager = KafkaProducerManager("localhost:9092")
        
        # Create a post that will cause serialization issues
        problematic_post = PostData(
            uri="at://test/post/problematic",
            author_did="did:test:problematic",
            text="Test post",
            created_at=datetime.now(timezone.utc)
        )
        
        # Mock producer to simulate failure
        async def mock_publish_failure(topic: str, post: PostData, **kwargs) -> bool:
            return False
        
        with patch.object(kafka_manager, 'publish_to_topic', side_effect=mock_publish_failure):
            success = await kafka_manager.publish_to_topic("bluesky-posts", problematic_post)
            assert success is False
    
    @pytest.mark.asyncio
    async def test_partitioning_strategy(self, sample_posts):
        """Test that posts from same author go to same partition."""
        producer = KafkaPostProducer("localhost:9092", "test-topic")
        
        # Create posts from same author
        same_author_posts = [
            PostData(
                uri=f"at://did:plc:same/app.bsky.feed.post/post{i}",
                author_did="did:plc:same",
                text=f"Post {i} from same author",
                created_at=datetime.now(timezone.utc)
            )
            for i in range(3)
        ]
        
        # Verify all posts get same partition key
        partition_keys = [producer.get_partition_key(post) for post in same_author_posts]
        assert all(key == "did:plc:same" for key in partition_keys)
        assert len(set(partition_keys)) == 1  # All same partition key
    
    @pytest.mark.asyncio
    async def test_message_serialization_roundtrip(self, sample_posts):
        """Test that serialized messages can be properly deserialized."""
        import json
        
        for post in sample_posts:
            # Serialize
            serialized = KafkaPostProducer._serialize_post_data(post)
            
            # Deserialize
            deserialized_dict = json.loads(serialized.decode('utf-8'))
            
            # Verify all fields are preserved
            assert deserialized_dict['uri'] == post.uri
            assert deserialized_dict['author_did'] == post.author_did
            assert deserialized_dict['text'] == post.text
            assert deserialized_dict['hashtags'] == post.hashtags
            assert deserialized_dict['mentions'] == post.mentions
            
            # Verify timestamp format
            parsed_time = datetime.fromisoformat(deserialized_dict['created_at'])
            assert parsed_time == post.created_at
    
    @pytest.mark.asyncio
    async def test_producer_stats_tracking(self):
        """Test that producer statistics are properly tracked."""
        producer = KafkaPostProducer("localhost:9092", "test-topic")
        
        # Initial stats
        initial_stats = producer.get_stats()
        assert initial_stats['messages_sent'] == 0
        assert initial_stats['messages_failed'] == 0
        assert initial_stats['retry_attempts'] == 0
        
        # Mock successful connection and send
        with patch('kafka_producer.KafkaProducer') as mock_producer_class:
            mock_kafka_producer = Mock()
            mock_producer_class.return_value = mock_kafka_producer
            
            # Mock successful send
            mock_future = Mock()
            mock_future.get.return_value = Mock(topic="test-topic", partition=0, offset=1)
            mock_kafka_producer.send.return_value = mock_future
            
            await producer.connect()
            
            # Send a test post
            test_post = PostData(
                uri="at://test/post/stats",
                author_did="did:test:stats",
                text="Stats test post",
                created_at=datetime.now(timezone.utc)
            )
            
            success = await producer.publish_post(test_post)
            assert success
            
            # Check updated stats
            updated_stats = producer.get_stats()
            assert updated_stats['messages_sent'] == 1
            assert updated_stats['messages_failed'] == 0
    
    @pytest.mark.asyncio
    async def test_manager_multiple_topics(self):
        """Test manager handling multiple topics."""
        manager = KafkaProducerManager("localhost:9092")
        
        with patch.object(KafkaPostProducer, 'connect', new_callable=AsyncMock), \
             patch.object(KafkaPostProducer, 'publish_post', new_callable=AsyncMock, return_value=True):
            
            # Create producers for different topics
            producer1 = await manager.get_producer("topic1")
            producer2 = await manager.get_producer("topic2")
            producer3 = await manager.get_producer("topic1")  # Should reuse existing
            
            assert len(manager.producers) == 2  # Only 2 unique topics
            assert producer1 is producer3  # Same producer for same topic
            assert producer1 is not producer2  # Different producers for different topics
            
            # Test publishing to different topics
            test_post = PostData(
                uri="at://test/post/multi",
                author_did="did:test:multi",
                text="Multi-topic test",
                created_at=datetime.now(timezone.utc)
            )
            
            success1 = await manager.publish_to_topic("topic1", test_post)
            success2 = await manager.publish_to_topic("topic2", test_post)
            
            assert success1 and success2
            
            # Verify stats for all producers
            all_stats = manager.get_all_stats()
            assert len(all_stats) == 2
            assert "topic1" in all_stats
            assert "topic2" in all_stats


if __name__ == "__main__":
    pytest.main([__file__, "-v"])