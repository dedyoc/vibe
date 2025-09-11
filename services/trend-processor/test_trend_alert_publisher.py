#!/usr/bin/env python3
"""
Tests for Trend Alert Publisher

This module contains comprehensive tests for the trend alert publishing system,
including deduplication, serialization, and Kafka integration tests.
"""

import asyncio
import json
import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from typing import List, Dict, Any

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'shared'))
from models import TrendAlert

from trend_alert_publisher import (
    TrendAlertDeduplicator,
    TrendAlertProducer,
    TrendAlertPublisherManager
)


class TestTrendAlertDeduplicator:
    """Test cases for TrendAlertDeduplicator class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.deduplicator = TrendAlertDeduplicator(
            dedup_window_minutes=30,
            max_alerts_per_keyword=3,
            cleanup_interval_minutes=60
        )
    
    def create_test_alert(
        self,
        keyword: str = "test_keyword",
        frequency: int = 100,
        growth_rate: float = 150.0,
        window_start: datetime = None,
        window_end: datetime = None
    ) -> TrendAlert:
        """Create a test TrendAlert object."""
        if window_start is None:
            window_start = datetime.now() - timedelta(minutes=10)
        if window_end is None:
            window_end = datetime.now()
        
        return TrendAlert(
            keyword=keyword,
            frequency=frequency,
            growth_rate=growth_rate,
            confidence_score=0.95,
            window_start=window_start,
            window_end=window_end,
            sample_posts=["post1", "post2"],
            unique_authors=50,
            rank=1
        )
    
    def test_should_publish_new_alert(self):
        """Test that new alerts should be published."""
        alert = self.create_test_alert()
        
        assert self.deduplicator.should_publish_alert(alert) is True
    
    def test_should_not_publish_duplicate_alert(self):
        """Test that exact duplicate alerts should not be published."""
        alert = self.create_test_alert()
        
        # First alert should be published
        assert self.deduplicator.should_publish_alert(alert) is True
        self.deduplicator.mark_alert_published(alert)
        
        # Exact duplicate should not be published
        assert self.deduplicator.should_publish_alert(alert) is False
    
    def test_should_publish_different_keyword(self):
        """Test that alerts for different keywords should be published."""
        alert1 = self.create_test_alert(keyword="keyword1")
        alert2 = self.create_test_alert(keyword="keyword2")
        
        assert self.deduplicator.should_publish_alert(alert1) is True
        self.deduplicator.mark_alert_published(alert1)
        
        assert self.deduplicator.should_publish_alert(alert2) is True
    
    def test_should_publish_different_frequency(self):
        """Test that alerts with different frequencies should be published."""
        alert1 = self.create_test_alert(frequency=100)
        alert2 = self.create_test_alert(frequency=200)
        
        assert self.deduplicator.should_publish_alert(alert1) is True
        self.deduplicator.mark_alert_published(alert1)
        
        assert self.deduplicator.should_publish_alert(alert2) is True
    
    def test_max_alerts_per_keyword_limit(self):
        """Test that maximum alerts per keyword limit is enforced."""
        keyword = "test_keyword"
        
        # Publish maximum allowed alerts
        for i in range(3):  # max_alerts_per_keyword = 3
            alert = self.create_test_alert(
                keyword=keyword,
                frequency=100 + i * 10  # Different frequencies
            )
            assert self.deduplicator.should_publish_alert(alert) is True
            self.deduplicator.mark_alert_published(alert)
        
        # Next alert should be rejected
        alert = self.create_test_alert(keyword=keyword, frequency=500)
        assert self.deduplicator.should_publish_alert(alert) is False
    
    def test_dedup_window_expiry(self):
        """Test that alerts outside dedup window are allowed."""
        # Mock datetime to control time
        with patch('trend_alert_publisher.datetime') as mock_datetime:
            base_time = datetime(2024, 1, 1, 12, 0, 0)
            mock_datetime.now.return_value = base_time
            
            alert = self.create_test_alert()
            
            # Publish alert at base time
            assert self.deduplicator.should_publish_alert(alert) is True
            self.deduplicator.mark_alert_published(alert)
            
            # Same alert should be rejected within window
            assert self.deduplicator.should_publish_alert(alert) is False
            
            # Move time beyond dedup window (30 minutes + buffer)
            mock_datetime.now.return_value = base_time + timedelta(minutes=35)
            
            # Same alert should now be allowed
            assert self.deduplicator.should_publish_alert(alert) is True
    
    def test_generate_alert_hash_consistency(self):
        """Test that alert hash generation is consistent."""
        # Use fixed timestamps to ensure consistency
        fixed_start = datetime(2024, 1, 1, 12, 0, 0)
        fixed_end = datetime(2024, 1, 1, 12, 10, 0)
        
        alert1 = self.create_test_alert(window_start=fixed_start, window_end=fixed_end)
        alert2 = self.create_test_alert(window_start=fixed_start, window_end=fixed_end)
        
        hash1 = self.deduplicator._generate_alert_hash(alert1)
        hash2 = self.deduplicator._generate_alert_hash(alert2)
        
        assert hash1 == hash2
    
    def test_generate_alert_hash_uniqueness(self):
        """Test that different alerts generate different hashes."""
        alert1 = self.create_test_alert(frequency=100)
        alert2 = self.create_test_alert(frequency=200)
        
        hash1 = self.deduplicator._generate_alert_hash(alert1)
        hash2 = self.deduplicator._generate_alert_hash(alert2)
        
        assert hash1 != hash2
    
    def test_cleanup_old_entries(self):
        """Test cleanup of old entries."""
        with patch('trend_alert_publisher.datetime') as mock_datetime:
            base_time = datetime(2024, 1, 1, 12, 0, 0)
            mock_datetime.now.return_value = base_time
            
            # Add some alerts
            alert = self.create_test_alert()
            self.deduplicator.mark_alert_published(alert)
            
            # Move time forward significantly
            mock_datetime.now.return_value = base_time + timedelta(hours=2)
            
            # Trigger cleanup
            self.deduplicator._cleanup_old_entries(mock_datetime.now.return_value)
            
            # Check that old entries were cleaned up
            assert len(self.deduplicator.published_alerts) == 0
    
    def test_get_stats(self):
        """Test statistics retrieval."""
        alert = self.create_test_alert()
        self.deduplicator.mark_alert_published(alert)
        
        stats = self.deduplicator.get_stats()
        
        assert 'active_keywords' in stats
        assert 'total_recent_alerts' in stats
        assert 'dedup_window_minutes' in stats
        assert 'max_alerts_per_keyword' in stats
        assert 'last_cleanup' in stats
        
        assert stats['active_keywords'] == 1
        assert stats['total_recent_alerts'] == 1


class TestTrendAlertProducer:
    """Test cases for TrendAlertProducer class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.bootstrap_servers = "localhost:9092"
        self.topic_name = "test-trend-alerts"
        
    def create_test_alert(
        self, 
        keyword: str = "test",
        window_start: datetime = None,
        window_end: datetime = None
    ) -> TrendAlert:
        """Create a test TrendAlert object."""
        if window_start is None:
            window_start = datetime.now() - timedelta(minutes=10)
        if window_end is None:
            window_end = datetime.now()
            
        return TrendAlert(
            keyword=keyword,
            frequency=100,
            growth_rate=150.0,
            confidence_score=0.95,
            window_start=window_start,
            window_end=window_end,
            sample_posts=["post1", "post2"],
            unique_authors=50,
            rank=1
        )
    
    @pytest.mark.asyncio
    async def test_producer_initialization(self):
        """Test producer initialization."""
        producer = TrendAlertProducer(
            bootstrap_servers=self.bootstrap_servers,
            topic_name=self.topic_name
        )
        
        assert producer.bootstrap_servers == self.bootstrap_servers
        assert producer.topic_name == self.topic_name
        assert producer.is_connected is False
        assert producer.alerts_sent == 0
        assert producer.alerts_failed == 0
        assert producer.alerts_deduplicated == 0
    
    @pytest.mark.asyncio
    async def test_connect_success(self):
        """Test successful connection to Kafka."""
        with patch('trend_alert_publisher.KafkaProducer') as mock_kafka:
            mock_producer_instance = Mock()
            mock_kafka.return_value = mock_producer_instance
            
            producer = TrendAlertProducer(
                bootstrap_servers=self.bootstrap_servers,
                topic_name=self.topic_name
            )
            
            await producer.connect()
            
            assert producer.is_connected is True
            mock_kafka.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_connect_failure(self):
        """Test connection failure handling."""
        with patch('trend_alert_publisher.KafkaProducer') as mock_kafka:
            mock_kafka.side_effect = Exception("Connection failed")
            
            producer = TrendAlertProducer(
                bootstrap_servers=self.bootstrap_servers,
                topic_name=self.topic_name
            )
            
            with pytest.raises(Exception):
                await producer.connect()
            
            assert producer.is_connected is False
    
    @pytest.mark.asyncio
    async def test_publish_trend_alert_success(self):
        """Test successful trend alert publishing."""
        with patch('trend_alert_publisher.KafkaProducer') as mock_kafka:
            # Mock producer and future
            mock_producer_instance = Mock()
            mock_future = Mock()
            mock_metadata = Mock()
            mock_metadata.topic = self.topic_name
            mock_metadata.partition = 0
            mock_metadata.offset = 123
            mock_future.get.return_value = mock_metadata
            mock_producer_instance.send.return_value = mock_future
            mock_kafka.return_value = mock_producer_instance
            
            producer = TrendAlertProducer(
                bootstrap_servers=self.bootstrap_servers,
                topic_name=self.topic_name
            )
            await producer.connect()
            
            alert = self.create_test_alert()
            result = await producer.publish_trend_alert(alert)
            
            assert result is True
            assert producer.alerts_sent == 1
            assert producer.alerts_failed == 0
            mock_producer_instance.send.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_publish_trend_alert_deduplication(self):
        """Test trend alert deduplication."""
        with patch('trend_alert_publisher.KafkaProducer') as mock_kafka:
            mock_producer_instance = Mock()
            mock_kafka.return_value = mock_producer_instance
            
            producer = TrendAlertProducer(
                bootstrap_servers=self.bootstrap_servers,
                topic_name=self.topic_name,
                dedup_window_minutes=30,
                max_alerts_per_keyword=1  # Allow only 1 alert per keyword
            )
            await producer.connect()
            
            alert = self.create_test_alert()
            
            # First alert should succeed
            with patch.object(producer.deduplicator, 'should_publish_alert', return_value=True):
                result1 = await producer.publish_trend_alert(alert)
                assert result1 is True
            
            # Second identical alert should be deduplicated
            with patch.object(producer.deduplicator, 'should_publish_alert', return_value=False):
                result2 = await producer.publish_trend_alert(alert)
                assert result2 is True  # Returns True for deduplication (expected behavior)
                assert producer.alerts_deduplicated == 1
    
    @pytest.mark.asyncio
    async def test_publish_trend_alert_retry_logic(self):
        """Test retry logic for failed publishes."""
        from kafka.errors import KafkaTimeoutError
        
        with patch('trend_alert_publisher.KafkaProducer') as mock_kafka:
            mock_producer_instance = Mock()
            mock_future = Mock()
            
            # First call fails, second succeeds
            mock_future.get.side_effect = [
                KafkaTimeoutError("Timeout"),
                Mock(topic=self.topic_name, partition=0, offset=123)
            ]
            mock_producer_instance.send.return_value = mock_future
            mock_kafka.return_value = mock_producer_instance
            
            producer = TrendAlertProducer(
                bootstrap_servers=self.bootstrap_servers,
                topic_name=self.topic_name,
                max_retries=2,
                initial_backoff=0.01  # Fast retry for testing
            )
            await producer.connect()
            
            alert = self.create_test_alert()
            result = await producer.publish_trend_alert(alert)
            
            assert result is True
            assert producer.retry_attempts == 1
            assert mock_producer_instance.send.call_count == 2
    
    @pytest.mark.asyncio
    async def test_publish_trend_alerts_batch(self):
        """Test batch publishing of trend alerts."""
        with patch('trend_alert_publisher.KafkaProducer') as mock_kafka:
            mock_producer_instance = Mock()
            mock_future = Mock()
            mock_metadata = Mock(topic=self.topic_name, partition=0, offset=123)
            mock_future.get.return_value = mock_metadata
            mock_producer_instance.send.return_value = mock_future
            mock_kafka.return_value = mock_producer_instance
            
            producer = TrendAlertProducer(
                bootstrap_servers=self.bootstrap_servers,
                topic_name=self.topic_name
            )
            await producer.connect()
            
            alerts = [
                self.create_test_alert(keyword="keyword1"),
                self.create_test_alert(keyword="keyword2"),
                self.create_test_alert(keyword="keyword3")
            ]
            
            result = await producer.publish_trend_alerts_batch(alerts)
            
            assert result['sent'] == 3
            assert result['failed'] == 0
            assert result['deduplicated'] == 0
            assert mock_producer_instance.send.call_count == 3
    
    def test_get_partition_key(self):
        """Test partition key generation."""
        producer = TrendAlertProducer(
            bootstrap_servers=self.bootstrap_servers,
            topic_name=self.topic_name
        )
        
        alert = self.create_test_alert(keyword="TestKeyword")
        partition_key = producer.get_partition_key(alert)
        
        assert partition_key == "testkeyword"  # Should be lowercase
    
    def test_serialize_trend_alert(self):
        """Test trend alert serialization."""
        alert = self.create_test_alert()
        serialized = TrendAlertProducer._serialize_trend_alert(alert)
        
        assert isinstance(serialized, bytes)
        
        # Deserialize and check content
        deserialized = json.loads(serialized.decode('utf-8'))
        
        assert deserialized['keyword'] == alert.keyword
        assert deserialized['frequency'] == alert.frequency
        assert deserialized['growth_rate'] == alert.growth_rate
        assert deserialized['confidence_score'] == alert.confidence_score
        assert deserialized['unique_authors'] == alert.unique_authors
        assert deserialized['rank'] == alert.rank
        assert 'published_at' in deserialized
    
    def test_serialize_partition_key(self):
        """Test partition key serialization."""
        key = "test_key"
        serialized = TrendAlertProducer._serialize_partition_key(key)
        
        assert isinstance(serialized, bytes)
        assert serialized == key.encode('utf-8')
    
    @pytest.mark.asyncio
    async def test_close_producer(self):
        """Test producer cleanup."""
        with patch('trend_alert_publisher.KafkaProducer') as mock_kafka:
            mock_producer_instance = Mock()
            mock_producer_instance.flush = Mock()
            mock_producer_instance.close = Mock()
            mock_kafka.return_value = mock_producer_instance
            
            producer = TrendAlertProducer(
                bootstrap_servers=self.bootstrap_servers,
                topic_name=self.topic_name
            )
            await producer.connect()
            await producer.close()
            
            mock_producer_instance.flush.assert_called_once()
            mock_producer_instance.close.assert_called_once()
            assert producer.is_connected is False
    
    def test_get_stats(self):
        """Test statistics retrieval."""
        producer = TrendAlertProducer(
            bootstrap_servers=self.bootstrap_servers,
            topic_name=self.topic_name
        )
        
        stats = producer.get_stats()
        
        assert 'is_connected' in stats
        assert 'alerts_sent' in stats
        assert 'alerts_failed' in stats
        assert 'alerts_deduplicated' in stats
        assert 'retry_attempts' in stats
        assert 'topic_name' in stats
        assert 'bootstrap_servers' in stats
        assert 'correlation_id' in stats
        assert 'deduplicator_stats' in stats


class TestTrendAlertPublisherManager:
    """Test cases for TrendAlertPublisherManager class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.bootstrap_servers = "localhost:9092"
        self.manager = TrendAlertPublisherManager(self.bootstrap_servers)
    
    def create_test_alert(self, keyword: str = "test") -> TrendAlert:
        """Create a test TrendAlert object."""
        return TrendAlert(
            keyword=keyword,
            frequency=100,
            growth_rate=150.0,
            confidence_score=0.95,
            window_start=datetime.now() - timedelta(minutes=10),
            window_end=datetime.now(),
            sample_posts=["post1", "post2"],
            unique_authors=50,
            rank=1
        )
    
    @pytest.mark.asyncio
    async def test_get_publisher_creates_new(self):
        """Test that get_publisher creates new publisher when needed."""
        with patch('trend_alert_publisher.TrendAlertProducer') as mock_producer_class:
            mock_producer = AsyncMock()
            mock_producer_class.return_value = mock_producer
            
            topic_name = "test-topic"
            publisher = await self.manager.get_publisher(topic_name)
            
            assert topic_name in self.manager.publishers
            mock_producer_class.assert_called_once()
            mock_producer.connect.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_publisher_reuses_existing(self):
        """Test that get_publisher reuses existing publisher."""
        with patch('trend_alert_publisher.TrendAlertProducer') as mock_producer_class:
            mock_producer = AsyncMock()
            mock_producer_class.return_value = mock_producer
            
            topic_name = "test-topic"
            
            # First call creates publisher
            publisher1 = await self.manager.get_publisher(topic_name)
            
            # Second call reuses existing
            publisher2 = await self.manager.get_publisher(topic_name)
            
            assert publisher1 is publisher2
            mock_producer_class.assert_called_once()  # Only called once
    
    @pytest.mark.asyncio
    async def test_publish_alert(self):
        """Test publishing single alert through manager."""
        with patch('trend_alert_publisher.TrendAlertProducer') as mock_producer_class:
            mock_producer = AsyncMock()
            mock_producer.publish_trend_alert.return_value = True
            mock_producer_class.return_value = mock_producer
            
            alert = self.create_test_alert()
            result = await self.manager.publish_alert(alert)
            
            assert result is True
            mock_producer.publish_trend_alert.assert_called_once_with(alert)
    
    @pytest.mark.asyncio
    async def test_publish_alerts_batch(self):
        """Test publishing batch of alerts through manager."""
        with patch('trend_alert_publisher.TrendAlertProducer') as mock_producer_class:
            mock_producer = AsyncMock()
            mock_producer.publish_trend_alerts_batch.return_value = {
                'sent': 2, 'failed': 0, 'deduplicated': 0
            }
            mock_producer_class.return_value = mock_producer
            
            alerts = [
                self.create_test_alert(keyword="keyword1"),
                self.create_test_alert(keyword="keyword2")
            ]
            result = await self.manager.publish_alerts_batch(alerts)
            
            assert result['sent'] == 2
            mock_producer.publish_trend_alerts_batch.assert_called_once_with(alerts)
    
    @pytest.mark.asyncio
    async def test_close_all(self):
        """Test closing all publishers."""
        with patch('trend_alert_publisher.TrendAlertProducer') as mock_producer_class:
            mock_producer1 = AsyncMock()
            mock_producer2 = AsyncMock()
            mock_producer_class.side_effect = [mock_producer1, mock_producer2]
            
            # Create two publishers
            await self.manager.get_publisher("topic1")
            await self.manager.get_publisher("topic2")
            
            # Close all
            await self.manager.close_all()
            
            mock_producer1.close.assert_called_once()
            mock_producer2.close.assert_called_once()
            assert len(self.manager.publishers) == 0
    
    def test_get_all_stats(self):
        """Test getting statistics for all publishers."""
        # Mock publishers with stats
        mock_producer1 = Mock()
        mock_producer1.get_stats.return_value = {'alerts_sent': 10}
        mock_producer2 = Mock()
        mock_producer2.get_stats.return_value = {'alerts_sent': 20}
        
        self.manager.publishers = {
            'topic1': mock_producer1,
            'topic2': mock_producer2
        }
        
        stats = self.manager.get_all_stats()
        
        assert 'topic1' in stats
        assert 'topic2' in stats
        assert stats['topic1']['alerts_sent'] == 10
        assert stats['topic2']['alerts_sent'] == 20


class TestIntegrationScenarios:
    """Integration test scenarios for the trend alert publishing system."""
    
    def create_test_alert(
        self,
        keyword: str = "test",
        frequency: int = 100,
        growth_rate: float = 150.0,
        window_start: datetime = None,
        window_end: datetime = None
    ) -> TrendAlert:
        """Create a test TrendAlert object."""
        if window_start is None:
            window_start = datetime.now() - timedelta(minutes=10)
        if window_end is None:
            window_end = datetime.now()
            
        return TrendAlert(
            keyword=keyword,
            frequency=frequency,
            growth_rate=growth_rate,
            confidence_score=0.95,
            window_start=window_start,
            window_end=window_end,
            sample_posts=["post1", "post2"],
            unique_authors=50,
            rank=1
        )
    
    @pytest.mark.asyncio
    async def test_end_to_end_alert_publishing(self):
        """Test complete end-to-end alert publishing workflow."""
        with patch('trend_alert_publisher.KafkaProducer') as mock_kafka:
            # Mock successful Kafka interaction
            mock_producer_instance = Mock()
            mock_future = Mock()
            mock_metadata = Mock(topic="trend-alerts", partition=0, offset=123)
            mock_future.get.return_value = mock_metadata
            mock_producer_instance.send.return_value = mock_future
            mock_kafka.return_value = mock_producer_instance
            
            # Create manager and publish alerts
            manager = TrendAlertPublisherManager("localhost:9092")
            
            alerts = [
                self.create_test_alert(keyword="trending_topic", frequency=500),
                self.create_test_alert(keyword="viral_content", frequency=300),
                self.create_test_alert(keyword="breaking_news", frequency=800)
            ]
            
            # Publish batch
            result = await manager.publish_alerts_batch(alerts)
            
            # Verify results
            assert result['sent'] == 3
            assert result['failed'] == 0
            
            # Verify Kafka interactions
            assert mock_producer_instance.send.call_count == 3
            
            # Clean up
            await manager.close_all()
    
    @pytest.mark.asyncio
    async def test_deduplication_with_real_scenarios(self):
        """Test deduplication with realistic trending scenarios."""
        deduplicator = TrendAlertDeduplicator(
            dedup_window_minutes=30,
            max_alerts_per_keyword=2
        )
        
        # Use fixed timestamps for consistent hashing
        fixed_start = datetime(2024, 1, 1, 12, 0, 0)
        fixed_end = datetime(2024, 1, 1, 12, 10, 0)
        
        # Scenario: Same trending topic detected multiple times
        base_alert = self.create_test_alert(
            keyword="breaking_news", 
            frequency=100,
            window_start=fixed_start,
            window_end=fixed_end
        )
        
        # First detection should be allowed
        assert deduplicator.should_publish_alert(base_alert) is True
        deduplicator.mark_alert_published(base_alert)
        
        # Exact duplicate should be blocked
        duplicate_alert = self.create_test_alert(
            keyword="breaking_news", 
            frequency=100,
            window_start=fixed_start,
            window_end=fixed_end
        )
        assert deduplicator.should_publish_alert(duplicate_alert) is False
        
        # Similar but different alert (higher frequency) should be allowed
        evolved_alert = self.create_test_alert(
            keyword="breaking_news", 
            frequency=200,
            window_start=fixed_start,
            window_end=fixed_end
        )
        assert deduplicator.should_publish_alert(evolved_alert) is True
        deduplicator.mark_alert_published(evolved_alert)
        
        # Third alert for same keyword should be blocked (max_alerts_per_keyword=2)
        third_alert = self.create_test_alert(
            keyword="breaking_news", 
            frequency=300,
            window_start=fixed_start,
            window_end=fixed_end
        )
        assert deduplicator.should_publish_alert(third_alert) is False
    
    @pytest.mark.asyncio
    async def test_serialization_roundtrip(self):
        """Test that serialized alerts can be properly deserialized."""
        alert = self.create_test_alert(
            keyword="test_serialization",
            frequency=150,
            growth_rate=75.5
        )
        
        # Serialize
        serialized = TrendAlertProducer._serialize_trend_alert(alert)
        
        # Deserialize
        deserialized_dict = json.loads(serialized.decode('utf-8'))
        
        # Verify all fields are preserved
        assert deserialized_dict['keyword'] == alert.keyword
        assert deserialized_dict['frequency'] == alert.frequency
        assert deserialized_dict['growth_rate'] == alert.growth_rate
        assert deserialized_dict['confidence_score'] == alert.confidence_score
        assert deserialized_dict['unique_authors'] == alert.unique_authors
        assert deserialized_dict['rank'] == alert.rank
        assert deserialized_dict['sample_posts'] == alert.sample_posts
        
        # Verify timestamps are ISO format
        window_start = datetime.fromisoformat(deserialized_dict['window_start'])
        window_end = datetime.fromisoformat(deserialized_dict['window_end'])
        published_at = datetime.fromisoformat(deserialized_dict['published_at'])
        
        assert isinstance(window_start, datetime)
        assert isinstance(window_end, datetime)
        assert isinstance(published_at, datetime)


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])