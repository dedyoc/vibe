#!/usr/bin/env python3
"""
Trend Detection Algorithm

This module implements statistical trend detection with configurable thresholds,
z-score calculations, confidence metrics, and trend ranking system.
"""

import logging
import math
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from statistics import mean, stdev

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'shared'))
from models import WindowedKeywordCount, TrendAlert

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class TrendStatistics:
    """Statistical data for trend detection calculations."""
    
    keyword: str
    current_frequency: int
    historical_frequencies: List[int] = field(default_factory=list)
    current_unique_authors: int = 0
    historical_unique_authors: List[int] = field(default_factory=list)
    window_start: datetime = field(default_factory=datetime.now)
    window_end: datetime = field(default_factory=datetime.now)
    sample_posts: List[str] = field(default_factory=list)
    
    def add_historical_frequency(self, frequency: int, unique_authors: int) -> None:
        """Add historical frequency data for baseline calculation."""
        self.historical_frequencies.append(frequency)
        self.historical_unique_authors.append(unique_authors)
        
        # Keep only recent history to avoid memory bloat
        max_history = 100
        if len(self.historical_frequencies) > max_history:
            self.historical_frequencies = self.historical_frequencies[-max_history:]
            self.historical_unique_authors = self.historical_unique_authors[-max_history:]
    
    def calculate_baseline_stats(self) -> Tuple[float, float]:
        """
        Calculate baseline mean and standard deviation from historical data.
        
        Returns:
            Tuple of (mean, standard_deviation)
        """
        if len(self.historical_frequencies) < 2:
            # Not enough data for statistical analysis
            return 0.0, 1.0
        
        baseline_mean = mean(self.historical_frequencies)
        baseline_std = stdev(self.historical_frequencies) if len(self.historical_frequencies) > 1 else 1.0
        
        # Ensure std is not zero to avoid division by zero
        if baseline_std == 0:
            baseline_std = 1.0
            
        return baseline_mean, baseline_std
    
    def calculate_growth_rate(self) -> float:
        """
        Calculate growth rate compared to recent historical average.
        
        Returns:
            Growth rate as percentage (positive for growth, negative for decline)
        """
        if not self.historical_frequencies:
            return 0.0
        
        # Use recent history for growth calculation (last 10 windows)
        recent_history = self.historical_frequencies[-10:]
        recent_average = mean(recent_history)
        
        if recent_average == 0:
            return 100.0 if self.current_frequency > 0 else 0.0
        
        growth_rate = ((self.current_frequency - recent_average) / recent_average) * 100
        return growth_rate


@dataclass
class TrendDetectionConfig:
    """Configuration parameters for trend detection algorithm."""
    
    # Minimum frequency thresholds
    min_frequency_threshold: int = 5
    min_unique_authors_threshold: int = 2
    
    # Statistical significance thresholds
    min_z_score_threshold: float = 2.0  # 95% confidence
    high_confidence_z_score: float = 3.0  # 99.7% confidence
    
    # Growth rate thresholds
    min_growth_rate_threshold: float = 50.0  # 50% growth
    high_growth_rate_threshold: float = 200.0  # 200% growth
    
    # Ranking parameters
    max_trends_to_rank: int = 50
    velocity_weight: float = 0.6  # Weight for growth velocity in ranking
    frequency_weight: float = 0.4  # Weight for absolute frequency in ranking
    
    # Historical data management
    max_historical_windows: int = 100
    min_historical_windows_for_stats: int = 5
    
    def __post_init__(self) -> None:
        """Validate configuration parameters."""
        if self.velocity_weight + self.frequency_weight != 1.0:
            raise ValueError("Velocity weight and frequency weight must sum to 1.0")
        
        if self.min_z_score_threshold < 0:
            raise ValueError("Z-score threshold must be non-negative")
        
        if self.min_frequency_threshold < 1:
            raise ValueError("Minimum frequency threshold must be at least 1")


class TrendDetector:
    """
    Statistical trend detection engine with configurable thresholds and ranking.
    
    This class implements trend detection using z-score analysis, growth rate calculations,
    and a sophisticated ranking system that considers both frequency and velocity.
    """
    
    def __init__(self, config: Optional[TrendDetectionConfig] = None):
        """
        Initialize the trend detector.
        
        Args:
            config: Configuration parameters for trend detection
        """
        self.config = config or TrendDetectionConfig()
        self.trend_statistics: Dict[str, TrendStatistics] = {}
        self.current_trends: List[TrendAlert] = []
        
        logger.info(f"TrendDetector initialized with config: {self.config}")
    
    def process_windowed_counts(self, windowed_counts: List[WindowedKeywordCount]) -> List[TrendAlert]:
        """
        Process windowed keyword counts and detect trends.
        
        Args:
            windowed_counts: List of windowed keyword counts from stream processing
            
        Returns:
            List of detected trend alerts, ranked by significance
        """
        if not windowed_counts:
            return []
        
        logger.info(f"Processing {len(windowed_counts)} windowed counts for trend detection")
        
        # Update statistics for each keyword
        for count in windowed_counts:
            self._update_keyword_statistics(count)
        
        # Detect trends using statistical analysis
        detected_trends = self._detect_statistical_trends()
        
        # Rank trends by significance
        ranked_trends = self._rank_trends(detected_trends)
        
        # Update current trends
        self.current_trends = ranked_trends
        
        logger.info(f"Detected {len(ranked_trends)} trending keywords")
        
        return ranked_trends
    
    def _update_keyword_statistics(self, count: WindowedKeywordCount) -> None:
        """
        Update statistical data for a keyword.
        
        Args:
            count: Windowed keyword count data
        """
        keyword = count.normalized_keyword
        
        if keyword not in self.trend_statistics:
            self.trend_statistics[keyword] = TrendStatistics(
                keyword=keyword,
                current_frequency=0,
                current_unique_authors=0
            )
        
        stats = self.trend_statistics[keyword]
        
        # Store previous frequency as historical data
        if stats.current_frequency > 0:
            stats.add_historical_frequency(stats.current_frequency, stats.current_unique_authors)
        
        # Update current statistics
        stats.current_frequency = count.count
        stats.current_unique_authors = count.unique_authors
        stats.window_start = count.window_start
        stats.window_end = count.window_end
        
        # Add sample post (we'll need to modify this when we have actual post URIs)
        # For now, create a placeholder
        sample_uri = f"sample_post_for_{keyword}_{count.window_start.isoformat()}"
        if sample_uri not in stats.sample_posts:
            stats.sample_posts.append(sample_uri)
            # Keep only recent samples
            if len(stats.sample_posts) > 10:
                stats.sample_posts = stats.sample_posts[-10:]
    
    def _detect_statistical_trends(self) -> List[TrendAlert]:
        """
        Detect trends using statistical analysis (z-scores and growth rates).
        
        Returns:
            List of detected trend alerts (unranked)
        """
        trends = []
        
        for keyword, stats in self.trend_statistics.items():
            # Skip if below minimum thresholds
            if (stats.current_frequency < self.config.min_frequency_threshold or
                stats.current_unique_authors < self.config.min_unique_authors_threshold):
                continue
            
            # Skip if insufficient historical data for statistical analysis
            if len(stats.historical_frequencies) < self.config.min_historical_windows_for_stats:
                continue
            
            # Calculate statistical significance
            z_score = self._calculate_z_score(stats)
            confidence_score = self._z_score_to_confidence(z_score)
            
            # Calculate growth rate
            growth_rate = stats.calculate_growth_rate()
            
            # Check if meets trending criteria
            if (z_score >= self.config.min_z_score_threshold and
                growth_rate >= self.config.min_growth_rate_threshold):
                
                trend_alert = TrendAlert(
                    keyword=stats.keyword,
                    frequency=stats.current_frequency,
                    growth_rate=growth_rate,
                    confidence_score=confidence_score,
                    window_start=stats.window_start,
                    window_end=stats.window_end,
                    sample_posts=stats.sample_posts.copy(),
                    unique_authors=stats.current_unique_authors,
                    rank=1  # Temporary rank, will be updated during ranking
                )
                
                trends.append(trend_alert)
                
                logger.debug(f"Detected trend: {keyword} (freq={stats.current_frequency}, "
                           f"z_score={z_score:.2f}, growth={growth_rate:.1f}%)")
        
        return trends
    
    def _calculate_z_score(self, stats: TrendStatistics) -> float:
        """
        Calculate z-score for statistical significance testing.
        
        Args:
            stats: Trend statistics for a keyword
            
        Returns:
            Z-score indicating how many standard deviations above the mean
        """
        baseline_mean, baseline_std = stats.calculate_baseline_stats()
        
        if baseline_std == 0:
            return 0.0
        
        z_score = (stats.current_frequency - baseline_mean) / baseline_std
        return max(0.0, z_score)  # Only positive z-scores indicate trending
    
    def _z_score_to_confidence(self, z_score: float) -> float:
        """
        Convert z-score to confidence score (0.0 to 1.0).
        
        Args:
            z_score: Statistical z-score
            
        Returns:
            Confidence score between 0.0 and 1.0
        """
        # Use cumulative distribution function approximation
        # For z-score of 2.0 (95% confidence) -> 0.95
        # For z-score of 3.0 (99.7% confidence) -> 0.997
        
        if z_score <= 0:
            return 0.0
        
        # Approximate confidence using error function
        # This is a simplified approximation
        confidence = 0.5 * (1 + math.erf(z_score / math.sqrt(2)))
        
        # Adjust to make it more intuitive (higher z-scores get higher confidence)
        if z_score >= self.config.high_confidence_z_score:
            confidence = min(0.99, confidence + 0.1)
        elif z_score >= self.config.min_z_score_threshold:
            confidence = min(0.95, confidence + 0.05)
        
        return min(1.0, confidence)
    
    def _rank_trends(self, trends: List[TrendAlert]) -> List[TrendAlert]:
        """
        Rank trends by significance considering frequency and growth velocity.
        
        Args:
            trends: List of unranked trend alerts
            
        Returns:
            List of ranked trend alerts (sorted by rank)
        """
        if not trends:
            return []
        
        # Calculate composite scores for ranking
        scored_trends = []
        
        # Normalize frequency and growth rate for fair comparison
        max_frequency = max(trend.frequency for trend in trends)
        max_growth_rate = max(abs(trend.growth_rate) for trend in trends)
        
        for trend in trends:
            # Normalize frequency (0.0 to 1.0)
            normalized_frequency = trend.frequency / max_frequency if max_frequency > 0 else 0.0
            
            # Normalize growth rate (0.0 to 1.0)
            normalized_growth = abs(trend.growth_rate) / max_growth_rate if max_growth_rate > 0 else 0.0
            
            # Calculate composite score
            composite_score = (
                self.config.frequency_weight * normalized_frequency +
                self.config.velocity_weight * normalized_growth
            )
            
            # Boost score based on confidence
            composite_score *= trend.confidence_score
            
            # Boost score for high unique author count (indicates broader engagement)
            author_boost = min(1.2, 1.0 + (trend.unique_authors - 1) * 0.05)
            composite_score *= author_boost
            
            scored_trends.append((composite_score, trend))
        
        # Sort by composite score (descending)
        scored_trends.sort(key=lambda x: x[0], reverse=True)
        
        # Assign ranks and return top trends
        ranked_trends = []
        for rank, (score, trend) in enumerate(scored_trends[:self.config.max_trends_to_rank], 1):
            trend.rank = rank
            ranked_trends.append(trend)
            
            logger.debug(f"Ranked trend #{rank}: {trend.keyword} (score={score:.3f})")
        
        return ranked_trends
    
    def get_current_trends(self, limit: Optional[int] = None) -> List[TrendAlert]:
        """
        Get current trending keywords.
        
        Args:
            limit: Maximum number of trends to return
            
        Returns:
            List of current trend alerts, ranked by significance
        """
        trends = self.current_trends.copy()
        
        if limit is not None:
            trends = trends[:limit]
        
        return trends
    
    def get_trend_statistics(self, keyword: str) -> Optional[TrendStatistics]:
        """
        Get detailed statistics for a specific keyword.
        
        Args:
            keyword: Normalized keyword to get statistics for
            
        Returns:
            TrendStatistics object or None if keyword not found
        """
        return self.trend_statistics.get(keyword)
    
    def clear_old_statistics(self, cutoff_time: datetime) -> None:
        """
        Clear statistics for keywords that haven't been seen recently.
        
        Args:
            cutoff_time: Remove statistics older than this time
        """
        keywords_to_remove = []
        
        for keyword, stats in self.trend_statistics.items():
            if stats.window_end < cutoff_time:
                keywords_to_remove.append(keyword)
        
        for keyword in keywords_to_remove:
            del self.trend_statistics[keyword]
        
        logger.info(f"Cleared statistics for {len(keywords_to_remove)} old keywords")
    
    def get_detection_summary(self) -> Dict[str, Any]:
        """
        Get summary statistics about trend detection.
        
        Returns:
            Dictionary with detection summary information
        """
        total_keywords = len(self.trend_statistics)
        trending_keywords = len(self.current_trends)
        
        # Calculate average statistics
        if self.trend_statistics:
            avg_frequency = mean(stats.current_frequency for stats in self.trend_statistics.values())
            avg_unique_authors = mean(stats.current_unique_authors for stats in self.trend_statistics.values())
        else:
            avg_frequency = 0.0
            avg_unique_authors = 0.0
        
        return {
            "total_keywords_tracked": total_keywords,
            "currently_trending": trending_keywords,
            "average_frequency": avg_frequency,
            "average_unique_authors": avg_unique_authors,
            "config": {
                "min_frequency_threshold": self.config.min_frequency_threshold,
                "min_z_score_threshold": self.config.min_z_score_threshold,
                "min_growth_rate_threshold": self.config.min_growth_rate_threshold
            }
        }


class TrendDetectionProcessor:
    """
    High-level processor that integrates trend detection with stream processing.
    
    This class provides a convenient interface for processing streams of windowed
    keyword counts and producing trend alerts.
    """
    
    def __init__(self, config: Optional[TrendDetectionConfig] = None):
        """
        Initialize the trend detection processor.
        
        Args:
            config: Configuration for trend detection
        """
        self.detector = TrendDetector(config)
        self.processed_windows = 0
        self.total_trends_detected = 0
        
    def process_stream_batch(self, windowed_counts: List[WindowedKeywordCount]) -> List[TrendAlert]:
        """
        Process a batch of windowed counts from the stream processor.
        
        Args:
            windowed_counts: Batch of windowed keyword counts
            
        Returns:
            List of detected trend alerts
        """
        trends = self.detector.process_windowed_counts(windowed_counts)
        
        self.processed_windows += len(windowed_counts)
        self.total_trends_detected += len(trends)
        
        return trends
    
    def cleanup_old_data(self, retention_hours: int = 24) -> None:
        """
        Clean up old statistical data to prevent memory bloat.
        
        Args:
            retention_hours: Hours of data to retain
        """
        cutoff_time = datetime.now() - timedelta(hours=retention_hours)
        self.detector.clear_old_statistics(cutoff_time)
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """
        Get processing statistics.
        
        Returns:
            Dictionary with processing statistics
        """
        detection_summary = self.detector.get_detection_summary()
        
        return {
            "processed_windows": self.processed_windows,
            "total_trends_detected": self.total_trends_detected,
            "detection_summary": detection_summary
        }


def create_trend_detection_config(
    min_frequency: int = 5,
    min_unique_authors: int = 2,
    min_z_score: float = 2.0,
    min_growth_rate: float = 50.0,
    max_trends: int = 50
) -> TrendDetectionConfig:
    """
    Create a trend detection configuration with common parameters.
    
    Args:
        min_frequency: Minimum frequency threshold
        min_unique_authors: Minimum unique authors threshold
        min_z_score: Minimum z-score for statistical significance
        min_growth_rate: Minimum growth rate percentage
        max_trends: Maximum number of trends to track
        
    Returns:
        Configured TrendDetectionConfig object
    """
    return TrendDetectionConfig(
        min_frequency_threshold=min_frequency,
        min_unique_authors_threshold=min_unique_authors,
        min_z_score_threshold=min_z_score,
        min_growth_rate_threshold=min_growth_rate,
        max_trends_to_rank=max_trends
    )