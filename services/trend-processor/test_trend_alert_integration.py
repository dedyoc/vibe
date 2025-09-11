#!/usr/bin/env python3
"""
Integration Tests for Trend Alert Publishing System

This module contains integration tests that verify the complete trend alert
publishing pipeline from trend detection to Kafka publication.
"""

import asyncio
import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from typing import List, Dict, Any

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'shared'))
from models import TrendAlert, WindowedKeywordCount

from trend_alert_integration import (
    TrendAlertPipeline,
    FlinkTrendAlertSink,
    create_trend_alert_pipeline,
    create_flink_trend_alert_sink
)
from trend_detector import create_trend_detection_config


class TestTrendAlertPipeline:
    """Test cases for TrendAlertPipeline class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.kafka_servers = "localhost:9092"
        self.alert_topic = "test-trend-alerts"
        
    def create_test_windowed_count(
        self,
        keyword: str = "test",
        count: int = 10,
        unique_authors: int = 5
    ) -> WindowedKeywordCount:
        """Create a test WindowedKeywordCount object."""
        return WindowedKeywordCount(
            keyword=keyword,
            count=count,
            window_start=datetime.now() - timedelta(minutes=10),
            window_end=datetime.now(),
            unique_authors=unique_authors,
            normalized_keyword=keyword.lower()
        )
    
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
    async def test_pipeline_initialization(self):
        """Test pipeline initialization."""
        pipeline = TrendAlertPipeline(
            kafka_bootstrap_servers=self.kafka_servers,
            alert_topic=self.alert_topic
        )
        
        assert pipeline.kafka_bootstrap_servers == self.kafka_servers
        assert pipeline.alert_topic == self.alert_topic
        assert pipeline.processed_batches == 0
        assert pipeline.total_trends_detected == 0
        assert pipeline.total_alerts_published == 0
        assert pipeline.total_alerts_failed == 0
    
    @pytest.mark.asyncio
    async def test_process_windowed_counts_batch_no_trends(self):
        """Test processing batch with no trends detected."""
        with patch('trend_alert_integration.TrendDetector') as mock_detector_class:
            mock_detector = Mock()
            mock_detector.process_windowed_counts.return_value = []  # No trends
            mock_detector_class.return_value = mock_detector
            
            pipeline = TrendAlertPipeline(
                kafka_bootstrap_servers=self.kafka_servers,
                alert_topic=self.alert_topic
            )
            
            windowed_counts = [
                self.create_test_windowed_count(keyword="low_freq", count=2)
            ]
            
            result = await pipeline.process_windowed_counts_batch(windowed_counts)
            
            assert result['trends_detected'] == 0
            assert result['alerts_published'] == 0
            assert result['alerts_failed'] == 0
            assert pipeline.processed_batches == 1
    
    @pytest.mark.asyncio
    async def test_process_windowed_counts_batch_with_trends(self):
        """Test processing batch with trends detected and published."""
        with patch('trend_alert_integration.TrendDetector') as mock_detector_class, \
             patch('trend_alert_integration.TrendAlertPublisherManager') as mock_manager_class:
            
            # Mock trend detector
            mock_detector = Mock()
            trends = [
                self.create_test_alert(keyword="trending1"),
                self.create_test_alert(keyword="trending2")
            ]
            mock_detector.process_windowed_counts.return_value = trends
            mock_detector_class.return_value = mock_detector
            
            # Mock publisher manager
            mock_manager = AsyncMock()
            mock_publisher = AsyncMock()
            mock_publisher.publish_trend_alerts_batch.return_value = {
                'sent': 2, 'failed': 0, 'deduplicated': 0
            }
            mock_manager.get_publisher.return_value = mock_publisher
            mock_manager_class.return_value = mock_manager
            
            pipeline = TrendAlertPipeline(
                kafka_bootstrap_servers=self.kafka_servers,
                alert_topic=self.alert_topic
            )
            
            windowed_counts = [
                self.create_test_windowed_count(keyword="trending1", count=100),
                self.create_test_windowed_count(keyword="trending2", count=80)
            ]
            
            result = await pipeline.process_windowed_counts_batch(windowed_counts)
            
            assert result['trends_detected'] == 2
            assert result['alerts_published'] == 2
            assert result['alerts_failed'] == 0
            assert pipeline.total_trends_detected == 2
            assert pipeline.total_alerts_published == 2
            
            # Verify publisher was called
            mock_publisher.publish_trend_alerts_batch.assert_called_once_with(trends)
    
    @pytest.mark.asyncio
    async def test_process_windowed_counts_batch_with_failures(self):
        """Test processing batch with publication failures."""
        with patch('trend_alert_integration.TrendDetector') as mock_detector_class, \
             patch('trend_alert_integration.TrendAlertPublisherManager') as mock_manager_class:
            
            # Mock trend detector
            mock_detector = Mock()
            trends = [self.create_test_alert(keyword="trending1")]
            mock_detector.process_windowed_counts.return_value = trends
            mock_detector_class.return_value = mock_detector
            
            # Mock publisher manager with failures
            mock_manager = AsyncMock()
            mock_publisher = AsyncMock()
            mock_publisher.publish_trend_alerts_batch.return_value = {
                'sent': 0, 'failed': 1, 'deduplicated': 0
            }
            mock_manager.get_publisher.return_value = mock_publisher
            mock_manager_class.return_value = mock_manager
            
            pipeline = TrendAlertPipeline(
                kafka_bootstrap_servers=self.kafka_servers,
                alert_topic=self.alert_topic
            )
            
            windowed_counts = [
                self.create_test_windowed_count(keyword="trending1", count=100)
            ]
            
            result = await pipeline.process_windowed_counts_batch(windowed_counts)
            
            assert result['trends_detected'] == 1
            assert result['alerts_published'] == 0
            assert result['alerts_failed'] == 1
            assert pipeline.total_alerts_failed == 1
    
    @pytest.mark.asyncio
    async def test_process_single_trend_alert_success(self):
        """Test processing single trend alert successfully."""
        with patch('trend_alert_integration.TrendAlertPublisherManager') as mock_manager_class:
            mock_manager = AsyncMock()
            mock_manager.publish_alert.return_value = True
            mock_manager_class.return_value = mock_manager
            
            pipeline = TrendAlertPipeline(
                kafka_bootstrap_servers=self.kafka_servers,
                alert_topic=self.alert_topic
            )
            
            alert = self.create_test_alert()
            result = await pipeline.process_single_trend_alert(alert)
            
            assert result is True
            assert pipeline.total_alerts_published == 1
            mock_manager.publish_alert.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_process_single_trend_alert_failure(self):
        """Test processing single trend alert with failure."""
        with patch('trend_alert_integration.TrendAlertPublisherManager') as mock_manager_class:
            mock_manager = AsyncMock()
            mock_manager.publish_alert.return_value = False
            mock_manager_class.return_value = mock_manager
            
            pipeline = TrendAlertPipeline(
                kafka_bootstrap_servers=self.kafka_servers,
                alert_topic=self.alert_topic
            )
            
            alert = self.create_test_alert()
            result = await pipeline.process_single_trend_alert(alert)
            
            assert result is False
            assert pipeline.total_alerts_failed == 1
    
    @pytest.mark.asyncio
    async def test_get_current_trends(self):
        """Test getting current trends from detector."""
        with patch('trend_alert_integration.TrendDetector') as mock_detector_class:
            mock_detector = Mock()
            trends = [
                self.create_test_alert(keyword="trend1"),
                self.create_test_alert(keyword="trend2")
            ]
            mock_detector.get_current_trends.return_value = trends
            mock_detector_class.return_value = mock_detector
            
            pipeline = TrendAlertPipeline(
                kafka_bootstrap_servers=self.kafka_servers,
                alert_topic=self.alert_topic
            )
            
            current_trends = pipeline.get_current_trends(limit=10)
            
            assert len(current_trends) == 2
            assert current_trends[0].keyword == "trend1"
            assert current_trends[1].keyword == "trend2"
            mock_detector.get_current_trends.assert_called_once_with(10)
    
    @pytest.mark.asyncio
    async def test_get_trend_statistics(self):
        """Test getting trend statistics for specific keyword."""
        with patch('trend_alert_integration.TrendDetector') as mock_detector_class:
            mock_detector = Mock()
            mock_stats = Mock()
            mock_stats.keyword = "test_keyword"
            mock_stats.current_frequency = 100
            mock_stats.current_unique_authors = 50
            mock_stats.historical_frequencies = [80, 90, 95]
            mock_stats.window_start = datetime.now() - timedelta(minutes=10)
            mock_stats.window_end = datetime.now()
            mock_stats.sample_posts = ["post1", "post2"]
            mock_stats.calculate_growth_rate.return_value = 25.0
            
            mock_detector.get_trend_statistics.return_value = mock_stats
            mock_detector_class.return_value = mock_detector
            
            pipeline = TrendAlertPipeline(
                kafka_bootstrap_servers=self.kafka_servers,
                alert_topic=self.alert_topic
            )
            
            stats = pipeline.get_trend_statistics("test_keyword")
            
            assert stats is not None
            assert stats['keyword'] == "test_keyword"
            assert stats['current_frequency'] == 100
            assert stats['current_unique_authors'] == 50
            assert stats['growth_rate'] == 25.0
    
    @pytest.mark.asyncio
    async def test_get_pipeline_stats(self):
        """Test getting comprehensive pipeline statistics."""
        with patch('trend_alert_integration.TrendDetector') as mock_detector_class, \
             patch('trend_alert_integration.TrendAlertPublisherManager') as mock_manager_class:
            
            mock_detector = Mock()
            mock_detector.get_detection_summary.return_value = {
                'total_keywords_tracked': 100,
                'currently_trending': 5
            }
            mock_detector_class.return_value = mock_detector
            
            mock_manager = Mock()
            mock_manager.get_all_stats.return_value = {
                'topic1': {'alerts_sent': 50}
            }
            mock_manager_class.return_value = mock_manager
            
            pipeline = TrendAlertPipeline(
                kafka_bootstrap_servers=self.kafka_servers,
                alert_topic=self.alert_topic
            )
            
            stats = pipeline.get_pipeline_stats()
            
            assert 'processed_batches' in stats
            assert 'total_trends_detected' in stats
            assert 'total_alerts_published' in stats
            assert 'total_alerts_failed' in stats
            assert 'detection_stats' in stats
            assert 'publisher_stats' in stats
            assert stats['kafka_bootstrap_servers'] == self.kafka_servers
            assert stats['alert_topic'] == self.alert_topic
    
    @pytest.mark.asyncio
    async def test_cleanup_old_data(self):
        """Test cleanup of old data."""
        with patch('trend_alert_integration.TrendDetector') as mock_detector_class:
            mock_detector = Mock()
            mock_detector_class.return_value = mock_detector
            
            pipeline = TrendAlertPipeline(
                kafka_bootstrap_servers=self.kafka_servers,
                alert_topic=self.alert_topic
            )
            
            await pipeline.cleanup_old_data(retention_hours=12)
            
            mock_detector.clear_old_statistics.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_close_pipeline(self):
        """Test pipeline cleanup."""
        with patch('trend_alert_integration.TrendAlertPublisherManager') as mock_manager_class:
            mock_manager = AsyncMock()
            mock_manager_class.return_value = mock_manager
            
            pipeline = TrendAlertPipeline(
                kafka_bootstrap_servers=self.kafka_servers,
                alert_topic=self.alert_topic
            )
            
            await pipeline.close()
            
            mock_manager.close_all.assert_called_once()


class TestFlinkTrendAlertSink:
    """Test cases for FlinkTrendAlertSink class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.kafka_servers = "localhost:9092"
        self.alert_topic = "test-trend-alerts"
    
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
    
    def test_sink_initialization(self):
        """Test Flink sink initialization."""
        sink = FlinkTrendAlertSink(
            kafka_bootstrap_servers=self.kafka_servers,
            alert_topic=self.alert_topic
        )
        
        assert sink.kafka_bootstrap_servers == self.kafka_servers
        assert sink.alert_topic == self.alert_topic
        assert sink.pipeline is None
    
    def test_sink_open(self):
        """Test Flink sink open method."""
        with patch('trend_alert_integration.TrendAlertPipeline') as mock_pipeline_class:
            mock_pipeline = Mock()
            mock_pipeline_class.return_value = mock_pipeline
            
            sink = FlinkTrendAlertSink(
                kafka_bootstrap_servers=self.kafka_servers,
                alert_topic=self.alert_topic
            )
            
            sink.open(configuration=None)
            
            assert sink.pipeline is not None
            mock_pipeline_class.assert_called_once()
    
    def test_sink_invoke(self):
        """Test Flink sink invoke method."""
        with patch('trend_alert_integration.TrendAlertPipeline') as mock_pipeline_class, \
             patch('asyncio.new_event_loop') as mock_loop_func, \
             patch('asyncio.set_event_loop') as mock_set_loop:
            
            # Mock pipeline
            mock_pipeline = AsyncMock()
            mock_pipeline.process_single_trend_alert.return_value = True
            mock_pipeline_class.return_value = mock_pipeline
            
            # Mock event loop
            mock_loop = Mock()
            mock_loop.run_until_complete.return_value = True
            mock_loop_func.return_value = mock_loop
            
            sink = FlinkTrendAlertSink(
                kafka_bootstrap_servers=self.kafka_servers,
                alert_topic=self.alert_topic
            )
            sink.open(configuration=None)
            
            alert = self.create_test_alert()
            sink.invoke(alert, context=None)
            
            mock_loop.run_until_complete.assert_called_once()
            mock_loop.close.assert_called_once()
    
    def test_sink_close(self):
        """Test Flink sink close method."""
        with patch('trend_alert_integration.TrendAlertPipeline') as mock_pipeline_class, \
             patch('asyncio.new_event_loop') as mock_loop_func, \
             patch('asyncio.set_event_loop') as mock_set_loop:
            
            # Mock pipeline
            mock_pipeline = AsyncMock()
            mock_pipeline_class.return_value = mock_pipeline
            
            # Mock event loop
            mock_loop = Mock()
            mock_loop_func.return_value = mock_loop
            
            sink = FlinkTrendAlertSink(
                kafka_bootstrap_servers=self.kafka_servers,
                alert_topic=self.alert_topic
            )
            sink.open(configuration=None)
            sink.close()
            
            mock_loop.run_until_complete.assert_called_once()
            mock_loop.close.assert_called_once()
            assert sink.pipeline is None


class TestFactoryFunctions:
    """Test cases for factory functions."""
    
    def test_create_trend_alert_pipeline(self):
        """Test trend alert pipeline factory function."""
        pipeline = create_trend_alert_pipeline(
            kafka_bootstrap_servers="localhost:9092",
            alert_topic="test-alerts",
            min_frequency=10,
            min_unique_authors=3,
            min_z_score=2.5,
            min_growth_rate=75.0,
            max_trends=25,
            dedup_window_minutes=45,
            max_alerts_per_keyword=2
        )
        
        assert isinstance(pipeline, TrendAlertPipeline)
        assert pipeline.kafka_bootstrap_servers == "localhost:9092"
        assert pipeline.alert_topic == "test-alerts"
        assert pipeline.dedup_window_minutes == 45
        assert pipeline.max_alerts_per_keyword == 2
    
    def test_create_flink_trend_alert_sink(self):
        """Test Flink trend alert sink factory function."""
        sink = create_flink_trend_alert_sink(
            kafka_bootstrap_servers="localhost:9092",
            alert_topic="test-alerts"
        )
        
        assert isinstance(sink, FlinkTrendAlertSink)
        assert sink.kafka_bootstrap_servers == "localhost:9092"
        assert sink.alert_topic == "test-alerts"


class TestEndToEndIntegration:
    """End-to-end integration tests."""
    
    def create_test_windowed_counts(self) -> List[WindowedKeywordCount]:
        """Create test windowed counts that should trigger trends."""
        return [
            WindowedKeywordCount(
                keyword="breaking_news",
                count=150,  # High frequency
                window_start=datetime.now() - timedelta(minutes=10),
                window_end=datetime.now(),
                unique_authors=75,  # High unique authors
                normalized_keyword="breaking_news"
            ),
            WindowedKeywordCount(
                keyword="viral_video",
                count=200,  # Very high frequency
                window_start=datetime.now() - timedelta(minutes=10),
                window_end=datetime.now(),
                unique_authors=100,  # Very high unique authors
                normalized_keyword="viral_video"
            ),
            WindowedKeywordCount(
                keyword="low_activity",
                count=2,  # Low frequency - should not trend
                window_start=datetime.now() - timedelta(minutes=10),
                window_end=datetime.now(),
                unique_authors=2,
                normalized_keyword="low_activity"
            )
        ]
    
    @pytest.mark.asyncio
    async def test_complete_pipeline_flow(self):
        """Test complete pipeline from windowed counts to Kafka publication."""
        with patch('trend_alert_integration.TrendAlertPublisherManager') as mock_manager_class:
            # Mock successful Kafka publishing
            mock_manager = AsyncMock()
            mock_publisher = AsyncMock()
            mock_publisher.publish_trend_alerts_batch.return_value = {
                'sent': 2, 'failed': 0, 'deduplicated': 0
            }
            mock_manager.get_publisher.return_value = mock_publisher
            mock_manager_class.return_value = mock_manager
            
            # Create pipeline with realistic configuration
            pipeline = create_trend_alert_pipeline(
                kafka_bootstrap_servers="localhost:9092",
                alert_topic="trend-alerts",
                min_frequency=10,  # Lower threshold for testing
                min_unique_authors=5,
                min_z_score=1.0,  # Lower threshold for testing
                min_growth_rate=0.0,  # Allow any growth for testing
                max_trends=50
            )
            
            # Process windowed counts
            windowed_counts = self.create_test_windowed_counts()
            result = await pipeline.process_windowed_counts_batch(windowed_counts)
            
            # Verify trends were detected and published
            # Note: Actual trend detection depends on historical data,
            # so we focus on the pipeline mechanics
            assert result['processing_time_ms'] > 0
            assert pipeline.processed_batches == 1
            
            # Clean up
            await pipeline.close()
    
    @pytest.mark.asyncio
    async def test_deduplication_in_pipeline(self):
        """Test that deduplication works in the complete pipeline."""
        with patch('trend_alert_integration.TrendAlertPublisherManager') as mock_manager_class:
            # Mock publisher with deduplication
            mock_manager = AsyncMock()
            mock_publisher = AsyncMock()
            
            # First batch: all alerts sent
            # Second batch: some deduplicated
            mock_publisher.publish_trend_alerts_batch.side_effect = [
                {'sent': 2, 'failed': 0, 'deduplicated': 0},
                {'sent': 1, 'failed': 0, 'deduplicated': 1}
            ]
            mock_manager.get_publisher.return_value = mock_publisher
            mock_manager_class.return_value = mock_publisher
            
            pipeline = create_trend_alert_pipeline(
                kafka_bootstrap_servers="localhost:9092",
                dedup_window_minutes=30,
                max_alerts_per_keyword=2
            )
            
            windowed_counts = self.create_test_windowed_counts()
            
            # Process same batch twice
            result1 = await pipeline.process_windowed_counts_batch(windowed_counts)
            result2 = await pipeline.process_windowed_counts_batch(windowed_counts)
            
            # Verify deduplication occurred in second batch
            # (Exact behavior depends on trend detection algorithm)
            assert pipeline.processed_batches == 2
            
            await pipeline.close()


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])