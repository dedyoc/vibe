#!/usr/bin/env python3
"""
Integration tests for trend detection with Flink stream processing.

These tests verify the complete pipeline from windowed keyword counts
to trend detection and alert generation.
"""

import pytest
from datetime import datetime, timedelta
from typing import List

# Import our models and classes
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'shared'))
sys.path.append(os.path.dirname(__file__))

from models import WindowedKeywordCount, TrendAlert
from trend_detector import TrendDetector, TrendDetectionConfig
from flink_job import TrendDetectionFunction, MockRuntimeContext, MockProcessingContext


class TestTrendDetectionIntegration:
    """Integration tests for the complete trend detection pipeline."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.config = TrendDetectionConfig(
            min_frequency_threshold=3,
            min_unique_authors_threshold=2,
            min_z_score_threshold=1.5,
            min_growth_rate_threshold=25.0,
            min_historical_windows_for_stats=3
        )
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
    
    def test_end_to_end_trend_detection(self):
        """Test complete end-to-end trend detection pipeline."""
        detector = TrendDetector(self.config)
        
        # Simulate a realistic trending scenario
        keyword = "breaking_news"
        
        # Phase 1: Build baseline (normal activity)
        baseline_counts = []
        for i in range(5):
            count = self.create_windowed_count(keyword, 3, 2, i * 10)
            baseline_counts.append(count)
        
        # Process baseline - should not detect trends
        for count in baseline_counts:
            trends = detector.process_windowed_counts([count])
            assert len(trends) == 0, "Should not detect trends during baseline"
        
        # Phase 2: Trending spike
        trending_count = self.create_windowed_count(keyword, 20, 12, 50)
        trends = detector.process_windowed_counts([trending_count])
        
        # Should detect trend
        assert len(trends) == 1, "Should detect one trend"
        
        trend = trends[0]
        assert trend.keyword == keyword
        assert trend.frequency == 20
        assert trend.unique_authors == 12
        assert trend.growth_rate > 100.0  # Significant growth
        assert trend.confidence_score > 0.8  # High confidence
        assert trend.rank == 1
        
        # Verify trend statistics
        stats = detector.get_trend_statistics(keyword)
        assert stats is not None
        assert stats.current_frequency == 20
        assert len(stats.historical_frequencies) == 5
    
    def test_flink_trend_detection_function(self):
        """Test the Flink trend detection function integration."""
        trend_function = TrendDetectionFunction(self.config)
        runtime_context = MockRuntimeContext()
        trend_function.open(runtime_context)
        
        # Mock output collector
        output_alerts = []
        
        class MockOutput:
            def collect(self, item):
                output_alerts.append(item)
        
        mock_out = MockOutput()
        
        # Simulate processing windowed counts
        keyword = "viral_content"
        
        # Build baseline
        for i in range(4):
            count = self.create_windowed_count(keyword, 2, 2, i * 10)
            timestamp_ms = int(count.window_start.timestamp() * 1000)
            ctx = MockProcessingContext(timestamp_ms)
            
            trend_function.process_element(count, ctx, mock_out)
        
        # Should not have emitted trends yet (buffering)
        assert len(output_alerts) == 0
        
        # Add trending data and trigger detection
        trending_count = self.create_windowed_count(keyword, 15, 8, 40)
        timestamp_ms = int(trending_count.window_start.timestamp() * 1000)
        
        # Simulate time passing to trigger detection
        ctx = MockProcessingContext(timestamp_ms + 35000)  # 35 seconds later
        trend_function.process_element(trending_count, ctx, mock_out)
        
        # Should have detected and emitted trend
        assert len(output_alerts) > 0, "Should have emitted trend alerts"
        
        # Verify the trend alert
        trend_alert = output_alerts[0]
        assert isinstance(trend_alert, TrendAlert)
        assert trend_alert.keyword == keyword
        assert trend_alert.frequency == 15
        assert trend_alert.rank >= 1
    
    def test_multiple_keywords_competition(self):
        """Test trend detection with multiple competing keywords."""
        detector = TrendDetector(self.config)
        
        keywords = ["tech", "sports", "politics"]
        
        # Build baseline for all keywords
        for window in range(4):
            counts = []
            for keyword in keywords:
                count = self.create_windowed_count(keyword, 3, 2, window * 10)
                counts.append(count)
            
            trends = detector.process_windowed_counts(counts)
            assert len(trends) == 0, "Should not detect trends during baseline"
        
        # Create competitive trending scenario
        competitive_counts = [
            self.create_windowed_count("tech", 25, 15, 40),      # High frequency, high authors
            self.create_windowed_count("sports", 20, 18, 40),    # Medium frequency, very high authors
            self.create_windowed_count("politics", 30, 10, 40),  # Very high frequency, medium authors
        ]
        
        trends = detector.process_windowed_counts(competitive_counts)
        
        # Should detect multiple trends
        assert len(trends) >= 2, "Should detect multiple trends"
        
        # Verify ranking
        trend_keywords = [trend.keyword for trend in trends]
        assert "politics" in trend_keywords, "Politics should trend (highest frequency)"
        assert "tech" in trend_keywords, "Tech should trend (good balance)"
        
        # Verify proper ranking order
        for i in range(len(trends) - 1):
            assert trends[i].rank < trends[i + 1].rank, "Trends should be properly ranked"
    
    def test_trend_persistence_and_decay(self):
        """Test trend persistence and natural decay over time."""
        detector = TrendDetector(self.config)
        
        keyword = "temporary_trend"
        
        # Build baseline
        for i in range(4):
            count = self.create_windowed_count(keyword, 3, 2, i * 10)
            detector.process_windowed_counts([count])
        
        # Create trending spike
        spike_count = self.create_windowed_count(keyword, 20, 12, 40)
        trends = detector.process_windowed_counts([spike_count])
        
        assert len(trends) == 1, "Should detect initial trend"
        initial_trend = trends[0]
        
        # Simulate trend decay (frequency returns to baseline)
        decay_counts = [
            self.create_windowed_count(keyword, 15, 8, 50),   # Still elevated
            self.create_windowed_count(keyword, 10, 6, 60),   # Declining
            self.create_windowed_count(keyword, 5, 4, 70),    # Near baseline
            self.create_windowed_count(keyword, 3, 2, 80),    # Back to baseline
        ]
        
        trend_detected_count = 0
        for count in decay_counts:
            trends = detector.process_windowed_counts([count])
            if trends:
                trend_detected_count += 1
        
        # Should detect fewer trends as it decays
        assert trend_detected_count <= 2, "Trend should decay over time"
    
    def test_statistical_significance_thresholds(self):
        """Test that statistical significance thresholds work correctly."""
        # Use stricter configuration
        strict_config = TrendDetectionConfig(
            min_frequency_threshold=5,
            min_unique_authors_threshold=3,
            min_z_score_threshold=3.0,  # Very strict
            min_growth_rate_threshold=100.0,  # Very strict
            min_historical_windows_for_stats=3
        )
        
        detector = TrendDetector(strict_config)
        keyword = "marginal_trend"
        
        # Build baseline
        for i in range(4):
            count = self.create_windowed_count(keyword, 4, 3, i * 10)
            detector.process_windowed_counts([count])
        
        # Test marginal increase (should not trend with strict config)
        marginal_count = self.create_windowed_count(keyword, 6, 4, 40)  # 1.5x increase (50% growth)
        trends = detector.process_windowed_counts([marginal_count])
        
        assert len(trends) == 0, "Marginal increase should not trend with strict config"
        
        # Test significant increase (should trend)
        significant_count = self.create_windowed_count(keyword, 25, 15, 50)  # 6x increase
        trends = detector.process_windowed_counts([significant_count])
        
        assert len(trends) == 1, "Significant increase should trend even with strict config"
        trend = trends[0]
        assert trend.confidence_score > 0.9, "Should have very high confidence"
    
    def test_trend_detection_performance_metrics(self):
        """Test trend detection performance and summary metrics."""
        detector = TrendDetector(self.config)
        
        # Process various scenarios
        scenarios = [
            ("baseline_keyword", [(3, 2), (4, 2), (3, 2), (5, 3)]),  # No trend
            ("trending_keyword", [(2, 2), (3, 2), (2, 2), (15, 8)]),  # Clear trend
            ("noisy_keyword", [(5, 3), (7, 4), (6, 3), (8, 4)]),     # Noisy, no clear trend
        ]
        
        total_processed = 0
        total_trends = 0
        
        for keyword, frequency_author_pairs in scenarios:
            for i, (freq, authors) in enumerate(frequency_author_pairs):
                count = self.create_windowed_count(keyword, freq, authors, i * 10)
                trends = detector.process_windowed_counts([count])
                
                total_processed += 1
                total_trends += len(trends)
        
        # Get detection summary
        summary = detector.get_detection_summary()
        
        assert summary["total_keywords_tracked"] == 3
        assert summary["currently_trending"] >= 0
        assert summary["average_frequency"] > 0
        assert summary["config"]["min_frequency_threshold"] == self.config.min_frequency_threshold
        
        # Should have detected at least one trend (from trending_keyword)
        assert total_trends >= 1, "Should have detected at least one trend"


class TestTrendDetectionEdgeCases:
    """Test edge cases and error conditions in trend detection."""
    
    def test_empty_input_handling(self):
        """Test handling of empty input."""
        detector = TrendDetector()
        
        # Empty list should return empty results
        trends = detector.process_windowed_counts([])
        assert len(trends) == 0
        
        # None input should be handled gracefully
        trends = detector.process_windowed_counts(None)
        assert len(trends) == 0
    
    def test_single_window_data(self):
        """Test behavior with insufficient historical data."""
        detector = TrendDetector()
        
        base_time = datetime(2024, 1, 1, 12, 0, 0)
        count = WindowedKeywordCount(
            keyword="new_keyword",
            count=100,  # High frequency
            window_start=base_time,
            window_end=base_time + timedelta(minutes=10),
            unique_authors=50,
            normalized_keyword="new_keyword"
        )
        
        # Should not detect trend with insufficient history
        trends = detector.process_windowed_counts([count])
        assert len(trends) == 0, "Should not detect trends without sufficient history"
    
    def test_zero_frequency_handling(self):
        """Test handling of zero frequency counts."""
        detector = TrendDetector()
        
        base_time = datetime(2024, 1, 1, 12, 0, 0)
        zero_count = WindowedKeywordCount(
            keyword="zero_keyword",
            count=0,
            window_start=base_time,
            window_end=base_time + timedelta(minutes=10),
            unique_authors=0,
            normalized_keyword="zero_keyword"
        )
        
        # Should handle zero counts gracefully
        trends = detector.process_windowed_counts([zero_count])
        assert len(trends) == 0, "Zero frequency should not cause errors"
    
    def test_cleanup_functionality(self):
        """Test cleanup of old statistical data."""
        detector = TrendDetector()
        
        # Add some old data
        old_time = datetime(2024, 1, 1, 12, 0, 0)
        old_count = WindowedKeywordCount(
            keyword="old_keyword",
            count=5,
            window_start=old_time,
            window_end=old_time + timedelta(minutes=10),
            unique_authors=3,
            normalized_keyword="old_keyword"
        )
        
        detector.process_windowed_counts([old_count])
        assert len(detector.trend_statistics) == 1
        
        # Cleanup old data
        cutoff_time = old_time + timedelta(hours=1)
        detector.clear_old_statistics(cutoff_time)
        
        assert len(detector.trend_statistics) == 0, "Old statistics should be cleaned up"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])