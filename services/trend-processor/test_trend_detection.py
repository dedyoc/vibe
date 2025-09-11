#!/usr/bin/env python3
"""
Comprehensive tests for trend detection algorithm.

These tests verify statistical trend detection with synthetic datasets,
z-score calculations, confidence metrics, and trend ranking functionality.
"""

import pytest
import math
from datetime import datetime, timedelta
from typing import List, Dict, Any
from unittest.mock import Mock

# Import our models and trend detection classes
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'shared'))
sys.path.append(os.path.dirname(__file__))

from models import WindowedKeywordCount, TrendAlert
from trend_detector import (
    TrendDetector, 
    TrendDetectionConfig, 
    TrendStatistics,
    TrendDetectionProcessor,
    create_trend_detection_config
)


class TestTrendDetectionConfig:
    """Test trend detection configuration."""
    
    def test_default_config(self):
        """Test default configuration values."""
        config = TrendDetectionConfig()
        
        assert config.min_frequency_threshold == 5
        assert config.min_unique_authors_threshold == 2
        assert config.min_z_score_threshold == 2.0
        assert config.min_growth_rate_threshold == 50.0
        assert config.max_trends_to_rank == 50
        assert config.velocity_weight + config.frequency_weight == 1.0
    
    def test_custom_config(self):
        """Test custom configuration values."""
        config = TrendDetectionConfig(
            min_frequency_threshold=10,
            min_z_score_threshold=3.0,
            velocity_weight=0.7,
            frequency_weight=0.3
        )
        
        assert config.min_frequency_threshold == 10
        assert config.min_z_score_threshold == 3.0
        assert config.velocity_weight == 0.7
        assert config.frequency_weight == 0.3
    
    def test_config_validation(self):
        """Test configuration validation."""
        # Test invalid weight sum
        with pytest.raises(ValueError, match="must sum to 1.0"):
            TrendDetectionConfig(velocity_weight=0.5, frequency_weight=0.6)
        
        # Test negative z-score threshold
        with pytest.raises(ValueError, match="Z-score threshold must be non-negative"):
            TrendDetectionConfig(min_z_score_threshold=-1.0)
        
        # Test invalid frequency threshold
        with pytest.raises(ValueError, match="Minimum frequency threshold must be at least 1"):
            TrendDetectionConfig(min_frequency_threshold=0)


class TestTrendStatistics:
    """Test trend statistics calculations."""
    
    def test_create_trend_statistics(self):
        """Test creation of trend statistics."""
        stats = TrendStatistics(
            keyword="python",
            current_frequency=10,
            current_unique_authors=5
        )
        
        assert stats.keyword == "python"
        assert stats.current_frequency == 10
        assert stats.current_unique_authors == 5
        assert len(stats.historical_frequencies) == 0
    
    def test_add_historical_data(self):
        """Test adding historical frequency data."""
        stats = TrendStatistics(keyword="test", current_frequency=0)
        
        # Add some historical data
        for i in range(10):
            stats.add_historical_frequency(i + 1, i + 1)
        
        assert len(stats.historical_frequencies) == 10
        assert len(stats.historical_unique_authors) == 10
        assert stats.historical_frequencies == list(range(1, 11))
    
    def test_historical_data_limit(self):
        """Test that historical data is limited to prevent memory bloat."""
        stats = TrendStatistics(keyword="test", current_frequency=0)
        
        # Add more than the maximum history
        for i in range(150):
            stats.add_historical_frequency(i, i)
        
        # Should be limited to 100 entries
        assert len(stats.historical_frequencies) == 100
        assert len(stats.historical_unique_authors) == 100
        
        # Should keep the most recent entries
        assert stats.historical_frequencies[-1] == 149
        assert stats.historical_frequencies[0] == 50  # 150 - 100
    
    def test_baseline_stats_calculation(self):
        """Test baseline statistics calculation."""
        stats = TrendStatistics(keyword="test", current_frequency=0)
        
        # Test with insufficient data
        mean, std = stats.calculate_baseline_stats()
        assert mean == 0.0
        assert std == 1.0
        
        # Add some data
        frequencies = [5, 7, 6, 8, 5, 9, 7, 6]
        for freq in frequencies:
            stats.add_historical_frequency(freq, freq)
        
        mean, std = stats.calculate_baseline_stats()
        expected_mean = sum(frequencies) / len(frequencies)
        
        assert abs(mean - expected_mean) < 0.001
        assert std > 0  # Should have some variation
    
    def test_growth_rate_calculation(self):
        """Test growth rate calculation."""
        stats = TrendStatistics(keyword="test", current_frequency=20)
        
        # Test with no historical data
        growth_rate = stats.calculate_growth_rate()
        assert growth_rate == 0.0
        
        # Add historical data with average of 10
        for _ in range(5):
            stats.add_historical_frequency(10, 5)
        
        # Current frequency is 20, historical average is 10
        # Growth rate should be 100%
        growth_rate = stats.calculate_growth_rate()
        assert abs(growth_rate - 100.0) < 0.001
        
        # Test with zero historical average
        stats.historical_frequencies = [0, 0, 0]
        stats.current_frequency = 5
        growth_rate = stats.calculate_growth_rate()
        assert growth_rate == 100.0


class TestTrendDetector:
    """Test the main trend detector functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.config = TrendDetectionConfig(
            min_frequency_threshold=3,
            min_unique_authors_threshold=2,
            min_z_score_threshold=1.5,
            min_growth_rate_threshold=25.0,
            min_historical_windows_for_stats=3
        )
        self.detector = TrendDetector(self.config)
        self.base_time = datetime(2024, 1, 1, 12, 0, 0)
    
    def create_windowed_count(self, keyword: str, count: int, unique_authors: int, 
                            time_offset_minutes: int = 0) -> WindowedKeywordCount:
        """Helper to create windowed keyword counts."""
        window_start = self.base_time + timedelta(minutes=time_offset_minutes)
        window_end = window_start + timedelta(minutes=10)
        
        return WindowedKeywordCount(
            keyword=keyword,
            count=count,
            window_start=window_start,
            window_end=window_end,
            unique_authors=unique_authors,
            normalized_keyword=keyword.lower()
        )
    
    def test_process_windowed_counts_basic(self):
        """Test basic processing of windowed counts."""
        counts = [
            self.create_windowed_count("python", 5, 3),
            self.create_windowed_count("java", 3, 2),
        ]
        
        trends = self.detector.process_windowed_counts(counts)
        
        # Should not detect trends yet (no historical data)
        assert len(trends) == 0
        
        # But should have updated statistics
        assert "python" in self.detector.trend_statistics
        assert "java" in self.detector.trend_statistics
    
    def test_trend_detection_with_historical_data(self):
        """Test trend detection with sufficient historical data."""
        keyword = "python"
        
        # Build up historical data (low frequency)
        for i in range(5):
            counts = [self.create_windowed_count(keyword, 2, 2, i)]
            self.detector.process_windowed_counts(counts)
        
        # Now send a high frequency count (should trigger trend)
        high_count = [self.create_windowed_count(keyword, 10, 5, 5)]
        trends = self.detector.process_windowed_counts(high_count)
        
        # Should detect a trend
        assert len(trends) > 0
        
        trend = trends[0]
        assert trend.keyword == keyword
        assert trend.frequency == 10
        assert trend.unique_authors == 5
        assert trend.growth_rate > 0
        assert trend.confidence_score > 0
        assert trend.rank == 1
    
    def test_z_score_calculation(self):
        """Test z-score calculation for statistical significance."""
        stats = TrendStatistics(keyword="test", current_frequency=15)
        
        # Add historical data with mean=5, std≈2
        historical_data = [3, 4, 5, 6, 7, 4, 5, 6, 5, 4]
        for freq in historical_data:
            stats.add_historical_frequency(freq, 3)
        
        z_score = self.detector._calculate_z_score(stats)
        
        # Current frequency (15) should be significantly higher than historical mean (~5)
        assert z_score > 2.0  # Should be statistically significant
    
    def test_confidence_score_conversion(self):
        """Test conversion of z-score to confidence score."""
        # Test various z-scores
        test_cases = [
            (0.0, 0.0),  # No significance
            (1.0, 0.5),  # Low significance (approximately)
            (2.0, 0.95),  # 95% confidence (approximately)
            (3.0, 0.99),  # 99% confidence (approximately)
        ]
        
        for z_score, expected_min_confidence in test_cases:
            confidence = self.detector._z_score_to_confidence(z_score)
            
            if z_score == 0.0:
                assert confidence == 0.0
            else:
                assert confidence >= expected_min_confidence * 0.8  # Allow some tolerance
                assert confidence <= 1.0
    
    def test_trend_ranking_system(self):
        """Test trend ranking based on frequency and velocity."""
        # Create trends with different characteristics
        trends = [
            TrendAlert(
                keyword="high_freq_low_growth",
                frequency=100,
                growth_rate=30.0,
                confidence_score=0.9,
                window_start=self.base_time,
                window_end=self.base_time + timedelta(minutes=10),
                sample_posts=["post1"],
                unique_authors=10,
                rank=1  # Temporary rank for testing
            ),
            TrendAlert(
                keyword="low_freq_high_growth",
                frequency=20,
                growth_rate=200.0,
                confidence_score=0.95,
                window_start=self.base_time,
                window_end=self.base_time + timedelta(minutes=10),
                sample_posts=["post2"],
                unique_authors=15,
                rank=2  # Temporary rank for testing
            ),
            TrendAlert(
                keyword="balanced",
                frequency=50,
                growth_rate=100.0,
                confidence_score=0.85,
                window_start=self.base_time,
                window_end=self.base_time + timedelta(minutes=10),
                sample_posts=["post3"],
                unique_authors=8,
                rank=3  # Temporary rank for testing
            )
        ]
        
        ranked_trends = self.detector._rank_trends(trends)
        
        # Should have 3 ranked trends
        assert len(ranked_trends) == 3
        
        # Ranks should be assigned (1, 2, 3)
        ranks = [trend.rank for trend in ranked_trends]
        assert ranks == [1, 2, 3]
        
        # Higher ranked trends should have higher composite scores
        # (This depends on the weighting, but we can check basic ordering)
        assert all(trend.rank > 0 for trend in ranked_trends)
    
    def test_simultaneous_trending_keywords(self):
        """Test handling of multiple simultaneous trending keywords."""
        keywords = ["python", "javascript", "rust", "go", "java"]
        
        # Build historical data for all keywords
        for window in range(5):
            counts = []
            for keyword in keywords:
                # Base frequency around 3-5
                base_freq = 3 + (hash(keyword) % 3)
                counts.append(self.create_windowed_count(keyword, base_freq, 2, window))
            
            self.detector.process_windowed_counts(counts)
        
        # Now create a trending scenario with different growth patterns
        trending_counts = [
            self.create_windowed_count("python", 15, 8, 5),    # High growth
            self.create_windowed_count("javascript", 12, 6, 5), # Medium growth
            self.create_windowed_count("rust", 20, 10, 5),     # Highest growth
            self.create_windowed_count("go", 8, 4, 5),         # Low growth
            self.create_windowed_count("java", 4, 2, 5),       # No significant growth
        ]
        
        trends = self.detector.process_windowed_counts(trending_counts)
        
        # Should detect multiple trends
        assert len(trends) >= 2
        
        # Should be properly ranked
        for i in range(len(trends) - 1):
            assert trends[i].rank < trends[i + 1].rank
        
        # Top trend should be one of the high-growth keywords
        top_keyword = trends[0].keyword
        assert top_keyword in ["python", "rust", "javascript"]
    
    def test_get_current_trends(self):
        """Test retrieval of current trends."""
        # Process some data to generate trends
        keyword = "trending_topic"
        
        # Build historical baseline
        for i in range(5):
            counts = [self.create_windowed_count(keyword, 3, 2, i)]
            self.detector.process_windowed_counts(counts)
        
        # Create trending scenario
        trending_count = [self.create_windowed_count(keyword, 15, 8, 5)]
        trends = self.detector.process_windowed_counts(trending_count)
        
        # Test getting all current trends
        current_trends = self.detector.get_current_trends()
        assert len(current_trends) == len(trends)
        
        # Test getting limited trends
        limited_trends = self.detector.get_current_trends(limit=1)
        assert len(limited_trends) == 1
        assert limited_trends[0].rank == 1
    
    def test_clear_old_statistics(self):
        """Test clearing of old statistical data."""
        # Add some statistics
        old_time = self.base_time - timedelta(hours=2)
        recent_time = self.base_time
        
        # Create old statistics
        old_count = WindowedKeywordCount(
            keyword="old_keyword",
            count=5,
            window_start=old_time,
            window_end=old_time + timedelta(minutes=10),
            unique_authors=3,
            normalized_keyword="old_keyword"
        )
        
        # Create recent statistics
        recent_count = WindowedKeywordCount(
            keyword="recent_keyword",
            count=7,
            window_start=recent_time,
            window_end=recent_time + timedelta(minutes=10),
            unique_authors=4,
            normalized_keyword="recent_keyword"
        )
        
        self.detector.process_windowed_counts([old_count, recent_count])
        
        # Should have both keywords
        assert len(self.detector.trend_statistics) == 2
        
        # Clear old statistics (cutoff 1 hour ago)
        cutoff_time = self.base_time - timedelta(hours=1)
        self.detector.clear_old_statistics(cutoff_time)
        
        # Should only have recent keyword
        assert len(self.detector.trend_statistics) == 1
        assert "recent_keyword" in self.detector.trend_statistics
        assert "old_keyword" not in self.detector.trend_statistics


class TestSyntheticTrendingDatasets:
    """Test trend detection with comprehensive synthetic datasets."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.config = TrendDetectionConfig(
            min_frequency_threshold=5,
            min_unique_authors_threshold=3,
            min_z_score_threshold=2.0,
            min_growth_rate_threshold=50.0,
            min_historical_windows_for_stats=4
        )
        self.detector = TrendDetector(self.config)
        self.base_time = datetime(2024, 1, 1, 12, 0, 0)
    
    def create_synthetic_baseline(self, keywords: List[str], windows: int = 10) -> None:
        """Create synthetic baseline data for keywords."""
        for window in range(windows):
            counts = []
            for keyword in keywords:
                # Create stable baseline frequency (3-7 range)
                base_freq = 3 + (hash(keyword + str(window)) % 5)
                unique_authors = min(base_freq, 2 + (hash(keyword) % 3))
                
                count = WindowedKeywordCount(
                    keyword=keyword,
                    count=base_freq,
                    window_start=self.base_time + timedelta(minutes=window * 10),
                    window_end=self.base_time + timedelta(minutes=(window + 1) * 10),
                    unique_authors=unique_authors,
                    normalized_keyword=keyword.lower()
                )
                counts.append(count)
            
            self.detector.process_windowed_counts(counts)
    
    def test_viral_content_pattern(self):
        """Test detection of viral content pattern (exponential growth)."""
        keyword = "viral_topic"
        
        # Create baseline
        self.create_synthetic_baseline([keyword], 5)
        
        # Simulate viral growth pattern
        viral_frequencies = [8, 15, 30, 60, 100]  # Exponential growth
        viral_authors = [5, 10, 20, 35, 50]       # Growing author engagement
        
        detected_trends_per_window = []
        
        for i, (freq, authors) in enumerate(zip(viral_frequencies, viral_authors)):
            count = WindowedKeywordCount(
                keyword=keyword,
                count=freq,
                window_start=self.base_time + timedelta(minutes=(5 + i) * 10),
                window_end=self.base_time + timedelta(minutes=(6 + i) * 10),
                unique_authors=authors,
                normalized_keyword=keyword.lower()
            )
            
            trends = self.detector.process_windowed_counts([count])
            detected_trends_per_window.append(len(trends))
        
        # Should detect trending behavior in later windows
        assert sum(detected_trends_per_window) > 0
        
        # Final trend should have high confidence and growth rate
        final_trends = self.detector.get_current_trends()
        if final_trends:
            top_trend = final_trends[0]
            assert top_trend.keyword == keyword
            assert top_trend.confidence_score > 0.8
            assert top_trend.growth_rate > 100.0
    
    def test_breaking_news_pattern(self):
        """Test detection of breaking news pattern (sudden spike)."""
        keyword = "breaking_news"
        
        # Create stable baseline
        self.create_synthetic_baseline([keyword], 8)
        
        # Simulate breaking news spike
        normal_count = WindowedKeywordCount(
            keyword=keyword,
            count=4,  # Normal frequency
            window_start=self.base_time + timedelta(minutes=80),
            window_end=self.base_time + timedelta(minutes=90),
            unique_authors=3,
            normalized_keyword=keyword.lower()
        )
        
        spike_count = WindowedKeywordCount(
            keyword=keyword,
            count=50,  # Sudden spike
            window_start=self.base_time + timedelta(minutes=90),
            window_end=self.base_time + timedelta(minutes=100),
            unique_authors=25,
            normalized_keyword=keyword.lower()
        )
        
        # Process normal window (should not trend)
        trends_normal = self.detector.process_windowed_counts([normal_count])
        assert len(trends_normal) == 0
        
        # Process spike window (should trend)
        trends_spike = self.detector.process_windowed_counts([spike_count])
        assert len(trends_spike) > 0
        
        trend = trends_spike[0]
        assert trend.keyword == keyword
        assert trend.frequency == 50
        assert trend.unique_authors == 25
        assert trend.growth_rate > 200.0  # Massive growth
    
    def test_competing_trends_scenario(self):
        """Test scenario with multiple competing trends."""
        keywords = ["tech_news", "sports_update", "celebrity_gossip", "political_event"]
        
        # Create baseline for all keywords
        self.create_synthetic_baseline(keywords, 6)
        
        # Create competing trends with different characteristics
        competing_counts = [
            # Tech news: High frequency, moderate authors
            WindowedKeywordCount(
                keyword="tech_news",
                count=40,
                window_start=self.base_time + timedelta(minutes=60),
                window_end=self.base_time + timedelta(minutes=70),
                unique_authors=15,
                normalized_keyword="tech_news"
            ),
            # Sports: Moderate frequency, high authors (broad appeal)
            WindowedKeywordCount(
                keyword="sports_update",
                count=25,
                window_start=self.base_time + timedelta(minutes=60),
                window_end=self.base_time + timedelta(minutes=70),
                unique_authors=20,
                normalized_keyword="sports_update"
            ),
            # Celebrity: Very high frequency, moderate authors (niche but intense)
            WindowedKeywordCount(
                keyword="celebrity_gossip",
                count=60,
                window_start=self.base_time + timedelta(minutes=60),
                window_end=self.base_time + timedelta(minutes=70),
                unique_authors=12,
                normalized_keyword="celebrity_gossip"
            ),
            # Political: Low frequency, high authors (controversial but engaging)
            WindowedKeywordCount(
                keyword="political_event",
                count=15,
                window_start=self.base_time + timedelta(minutes=60),
                window_end=self.base_time + timedelta(minutes=70),
                unique_authors=18,
                normalized_keyword="political_event"
            )
        ]
        
        trends = self.detector.process_windowed_counts(competing_counts)
        
        # Should detect multiple trends
        assert len(trends) >= 2
        
        # Verify ranking makes sense
        trend_keywords = [trend.keyword for trend in trends]
        
        # Celebrity gossip should rank high due to very high frequency
        # Tech news should also rank high due to good frequency and growth
        assert "celebrity_gossip" in trend_keywords[:2]
        assert "tech_news" in trend_keywords[:3]
        
        # Verify all trends meet thresholds
        for trend in trends:
            assert trend.frequency >= self.config.min_frequency_threshold
            assert trend.unique_authors >= self.config.min_unique_authors_threshold
            assert trend.confidence_score > 0.0
    
    def test_seasonal_trend_pattern(self):
        """Test detection of seasonal/cyclical trend patterns."""
        keyword = "seasonal_topic"
        
        # Create a pattern that simulates seasonal interest
        # Low baseline, periodic spikes
        window_frequencies = [
            3, 4, 3, 5, 15, 20, 8, 4,   # Spike in middle
            3, 5, 4, 3, 18, 25, 6, 5,   # Another spike
            4, 3, 5, 4, 22, 30, 7, 4    # Growing spike
        ]
        
        detected_spikes = 0
        
        for i, freq in enumerate(window_frequencies):
            authors = min(freq, 3 + freq // 5)  # Authors scale with frequency
            
            count = WindowedKeywordCount(
                keyword=keyword,
                count=freq,
                window_start=self.base_time + timedelta(minutes=i * 10),
                window_end=self.base_time + timedelta(minutes=(i + 1) * 10),
                unique_authors=authors,
                normalized_keyword=keyword.lower()
            )
            
            trends = self.detector.process_windowed_counts([count])
            if trends:
                detected_spikes += 1
        
        # Should detect the periodic spikes
        assert detected_spikes >= 2
        
        # Final spike should be the strongest
        final_trends = self.detector.get_current_trends()
        if final_trends:
            assert final_trends[0].keyword == keyword
    
    def test_false_positive_resistance(self):
        """Test resistance to false positives with noisy data."""
        keywords = ["noise1", "noise2", "noise3", "legitimate_trend"]
        
        # Create baseline
        self.create_synthetic_baseline(keywords, 8)
        
        # Create noisy data that shouldn't trigger trends
        noisy_counts = [
            # Small random variations (shouldn't trend)
            WindowedKeywordCount(
                keyword="noise1",
                count=7,  # Slight increase from baseline ~4
                window_start=self.base_time + timedelta(minutes=80),
                window_end=self.base_time + timedelta(minutes=90),
                unique_authors=3,
                normalized_keyword="noise1"
            ),
            # Single author spike (shouldn't trend due to low author diversity)
            WindowedKeywordCount(
                keyword="noise2",
                count=15,
                window_start=self.base_time + timedelta(minutes=80),
                window_end=self.base_time + timedelta(minutes=90),
                unique_authors=1,  # Only one author
                normalized_keyword="noise2"
            ),
            # Legitimate trend (should trend)
            WindowedKeywordCount(
                keyword="legitimate_trend",
                count=25,
                window_start=self.base_time + timedelta(minutes=80),
                window_end=self.base_time + timedelta(minutes=90),
                unique_authors=12,
                normalized_keyword="legitimate_trend"
            )
        ]
        
        trends = self.detector.process_windowed_counts(noisy_counts)
        
        # Should only detect the legitimate trend
        assert len(trends) <= 1
        
        if trends:
            assert trends[0].keyword == "legitimate_trend"
            assert trends[0].unique_authors >= self.config.min_unique_authors_threshold


class TestTrendDetectionProcessor:
    """Test the high-level trend detection processor."""
    
    def test_processor_initialization(self):
        """Test processor initialization."""
        processor = TrendDetectionProcessor()
        
        assert processor.detector is not None
        assert processor.processed_windows == 0
        assert processor.total_trends_detected == 0
    
    def test_process_stream_batch(self):
        """Test processing of stream batches."""
        config = TrendDetectionConfig(min_historical_windows_for_stats=2)
        processor = TrendDetectionProcessor(config)
        
        base_time = datetime(2024, 1, 1, 12, 0, 0)
        
        # Create batch of windowed counts
        batch1 = [
            WindowedKeywordCount(
                keyword="test",
                count=3,
                window_start=base_time,
                window_end=base_time + timedelta(minutes=10),
                unique_authors=2,
                normalized_keyword="test"
            )
        ]
        
        batch2 = [
            WindowedKeywordCount(
                keyword="test",
                count=4,
                window_start=base_time + timedelta(minutes=10),
                window_end=base_time + timedelta(minutes=20),
                unique_authors=3,
                normalized_keyword="test"
            )
        ]
        
        batch3 = [
            WindowedKeywordCount(
                keyword="test",
                count=15,  # Trending
                window_start=base_time + timedelta(minutes=20),
                window_end=base_time + timedelta(minutes=30),
                unique_authors=8,
                normalized_keyword="test"
            )
        ]
        
        # Process batches
        trends1 = processor.process_stream_batch(batch1)
        trends2 = processor.process_stream_batch(batch2)
        trends3 = processor.process_stream_batch(batch3)
        
        # Should detect trend in third batch
        assert len(trends1) == 0
        assert len(trends2) == 0
        assert len(trends3) > 0
        
        # Check processing stats
        stats = processor.get_processing_stats()
        assert stats["processed_windows"] == 3
        assert stats["total_trends_detected"] >= 1
    
    def test_cleanup_old_data(self):
        """Test cleanup of old data."""
        processor = TrendDetectionProcessor()
        
        # Add some old data
        old_time = datetime.now() - timedelta(hours=25)
        old_count = WindowedKeywordCount(
            keyword="old",
            count=5,
            window_start=old_time,
            window_end=old_time + timedelta(minutes=10),
            unique_authors=3,
            normalized_keyword="old"
        )
        
        processor.process_stream_batch([old_count])
        
        # Should have the keyword
        assert len(processor.detector.trend_statistics) == 1
        
        # Cleanup with 24-hour retention
        processor.cleanup_old_data(retention_hours=24)
        
        # Should be cleaned up
        assert len(processor.detector.trend_statistics) == 0


class TestTrendDetectionConfigFactory:
    """Test the configuration factory function."""
    
    def test_create_default_config(self):
        """Test creating default configuration."""
        config = create_trend_detection_config()
        
        assert config.min_frequency_threshold == 5
        assert config.min_unique_authors_threshold == 2
        assert config.min_z_score_threshold == 2.0
        assert config.min_growth_rate_threshold == 50.0
        assert config.max_trends_to_rank == 50
    
    def test_create_custom_config(self):
        """Test creating custom configuration."""
        config = create_trend_detection_config(
            min_frequency=10,
            min_unique_authors=5,
            min_z_score=3.0,
            min_growth_rate=100.0,
            max_trends=25
        )
        
        assert config.min_frequency_threshold == 10
        assert config.min_unique_authors_threshold == 5
        assert config.min_z_score_threshold == 3.0
        assert config.min_growth_rate_threshold == 100.0
        assert config.max_trends_to_rank == 25


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])