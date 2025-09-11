#!/usr/bin/env python3
"""
Example Usage of Trend Alert Publishing System

This module demonstrates how to use the trend alert publishing system
for detecting and publishing trending topics from social media data.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'shared'))
from models import WindowedKeywordCount, TrendAlert

from trend_alert_integration import create_trend_alert_pipeline
from trend_detector import create_trend_detection_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_sample_windowed_counts() -> List[WindowedKeywordCount]:
    """Create sample windowed keyword counts for demonstration."""
    base_time = datetime.now()
    window_start = base_time - timedelta(minutes=10)
    window_end = base_time
    
    return [
        # High-frequency trending topics
        WindowedKeywordCount(
            keyword="breaking_news",
            count=250,
            window_start=window_start,
            window_end=window_end,
            unique_authors=125,
            normalized_keyword="breaking_news"
        ),
        WindowedKeywordCount(
            keyword="viral_video",
            count=180,
            window_start=window_start,
            window_end=window_end,
            unique_authors=90,
            normalized_keyword="viral_video"
        ),
        WindowedKeywordCount(
            keyword="tech_announcement",
            count=150,
            window_start=window_start,
            window_end=window_end,
            unique_authors=75,
            normalized_keyword="tech_announcement"
        ),
        # Medium-frequency topics
        WindowedKeywordCount(
            keyword="sports_update",
            count=80,
            window_start=window_start,
            window_end=window_end,
            unique_authors=40,
            normalized_keyword="sports_update"
        ),
        WindowedKeywordCount(
            keyword="weather_alert",
            count=60,
            window_start=window_start,
            window_end=window_end,
            unique_authors=30,
            normalized_keyword="weather_alert"
        ),
        # Low-frequency topics (should not trend)
        WindowedKeywordCount(
            keyword="random_topic",
            count=5,
            window_start=window_start,
            window_end=window_end,
            unique_authors=3,
            normalized_keyword="random_topic"
        ),
        WindowedKeywordCount(
            keyword="niche_discussion",
            count=3,
            window_start=window_start,
            window_end=window_end,
            unique_authors=2,
            normalized_keyword="niche_discussion"
        )
    ]


async def demonstrate_basic_pipeline():
    """Demonstrate basic trend alert pipeline usage."""
    logger.info("=== Basic Pipeline Demonstration ===")
    
    # Create pipeline with custom configuration
    pipeline = create_trend_alert_pipeline(
        kafka_bootstrap_servers="localhost:9092",
        alert_topic="demo-trend-alerts",
        min_frequency=10,  # Lower threshold for demo
        min_unique_authors=5,
        min_z_score=1.0,  # Lower threshold for demo
        min_growth_rate=0.0,  # Allow any growth for demo
        max_trends=10,
        dedup_window_minutes=30,
        max_alerts_per_keyword=3
    )
    
    # Create sample data
    windowed_counts = create_sample_windowed_counts()
    
    logger.info(f"Processing {len(windowed_counts)} windowed keyword counts")
    
    try:
        # Process the batch
        result = await pipeline.process_windowed_counts_batch(windowed_counts)
        
        logger.info("Batch processing results:")
        logger.info(f"  - Trends detected: {result['trends_detected']}")
        logger.info(f"  - Alerts published: {result['alerts_published']}")
        logger.info(f"  - Alerts failed: {result['alerts_failed']}")
        logger.info(f"  - Alerts deduplicated: {result['alerts_deduplicated']}")
        logger.info(f"  - Processing time: {result['processing_time_ms']:.2f}ms")
        
        # Get current trends
        current_trends = pipeline.get_current_trends(limit=5)
        if current_trends:
            logger.info(f"\nTop {len(current_trends)} trending topics:")
            for i, trend in enumerate(current_trends, 1):
                logger.info(f"  {i}. {trend.keyword} (freq={trend.frequency}, "
                           f"growth={trend.growth_rate:.1f}%, rank={trend.rank})")
        else:
            logger.info("No trends detected (may need historical data for comparison)")
        
        # Get pipeline statistics
        stats = pipeline.get_pipeline_stats()
        logger.info(f"\nPipeline Statistics:")
        logger.info(f"  - Processed batches: {stats['processed_batches']}")
        logger.info(f"  - Total trends detected: {stats['total_trends_detected']}")
        logger.info(f"  - Total alerts published: {stats['total_alerts_published']}")
        
    finally:
        # Clean up
        await pipeline.close()


async def demonstrate_deduplication():
    """Demonstrate alert deduplication functionality."""
    logger.info("\n=== Deduplication Demonstration ===")
    
    # Create pipeline with strict deduplication
    pipeline = create_trend_alert_pipeline(
        kafka_bootstrap_servers="localhost:9092",
        alert_topic="demo-dedup-alerts",
        min_frequency=5,
        min_unique_authors=2,
        min_z_score=0.5,
        min_growth_rate=0.0,
        dedup_window_minutes=15,  # Short window for demo
        max_alerts_per_keyword=2  # Allow max 2 alerts per keyword
    )
    
    # Create sample data
    windowed_counts = create_sample_windowed_counts()
    
    try:
        # Process the same batch multiple times to demonstrate deduplication
        logger.info("Processing batch #1...")
        result1 = await pipeline.process_windowed_counts_batch(windowed_counts)
        
        logger.info("Processing batch #2 (should have deduplication)...")
        result2 = await pipeline.process_windowed_counts_batch(windowed_counts)
        
        logger.info("Processing batch #3 (should have more deduplication)...")
        result3 = await pipeline.process_windowed_counts_batch(windowed_counts)
        
        logger.info("\nDeduplication Results:")
        logger.info(f"Batch 1 - Published: {result1['alerts_published']}, "
                   f"Deduplicated: {result1['alerts_deduplicated']}")
        logger.info(f"Batch 2 - Published: {result2['alerts_published']}, "
                   f"Deduplicated: {result2['alerts_deduplicated']}")
        logger.info(f"Batch 3 - Published: {result3['alerts_published']}, "
                   f"Deduplicated: {result3['alerts_deduplicated']}")
        
    finally:
        await pipeline.close()


async def demonstrate_single_alert_publishing():
    """Demonstrate publishing individual trend alerts."""
    logger.info("\n=== Single Alert Publishing Demonstration ===")
    
    pipeline = create_trend_alert_pipeline(
        kafka_bootstrap_servers="localhost:9092",
        alert_topic="demo-single-alerts"
    )
    
    # Create a sample trend alert
    sample_alert = TrendAlert(
        keyword="demo_trending_topic",
        frequency=200,
        growth_rate=150.0,
        confidence_score=0.95,
        window_start=datetime.now() - timedelta(minutes=10),
        window_end=datetime.now(),
        sample_posts=["post1", "post2", "post3"],
        unique_authors=100,
        rank=1
    )
    
    try:
        logger.info(f"Publishing single alert for keyword: {sample_alert.keyword}")
        
        success = await pipeline.process_single_trend_alert(sample_alert)
        
        if success:
            logger.info("✓ Alert published successfully")
        else:
            logger.info("✗ Alert publication failed")
        
        # Try to publish the same alert again (should be deduplicated)
        logger.info("Attempting to publish the same alert again...")
        success2 = await pipeline.process_single_trend_alert(sample_alert)
        
        if success2:
            logger.info("✓ Alert published (or deduplicated)")
        else:
            logger.info("✗ Alert publication failed")
        
    finally:
        await pipeline.close()


async def demonstrate_trend_statistics():
    """Demonstrate trend statistics and monitoring."""
    logger.info("\n=== Trend Statistics Demonstration ===")
    
    pipeline = create_trend_alert_pipeline(
        kafka_bootstrap_servers="localhost:9092",
        alert_topic="demo-stats-alerts"
    )
    
    # Process some data first
    windowed_counts = create_sample_windowed_counts()
    
    try:
        await pipeline.process_windowed_counts_batch(windowed_counts)
        
        # Get statistics for specific keywords
        keywords_to_check = ["breaking_news", "viral_video", "tech_announcement"]
        
        logger.info("Keyword Statistics:")
        for keyword in keywords_to_check:
            stats = pipeline.get_trend_statistics(keyword)
            if stats:
                logger.info(f"\n{keyword}:")
                logger.info(f"  - Current frequency: {stats['current_frequency']}")
                logger.info(f"  - Unique authors: {stats['current_unique_authors']}")
                logger.info(f"  - Growth rate: {stats['growth_rate']:.1f}%")
                logger.info(f"  - Historical frequencies: {stats['historical_frequencies']}")
            else:
                logger.info(f"\n{keyword}: No statistics available")
        
        # Get overall pipeline statistics
        pipeline_stats = pipeline.get_pipeline_stats()
        logger.info(f"\nOverall Pipeline Statistics:")
        logger.info(f"  - Processed batches: {pipeline_stats['processed_batches']}")
        logger.info(f"  - Total trends detected: {pipeline_stats['total_trends_detected']}")
        logger.info(f"  - Total alerts published: {pipeline_stats['total_alerts_published']}")
        logger.info(f"  - Total alerts failed: {pipeline_stats['total_alerts_failed']}")
        
        detection_stats = pipeline_stats['detection_stats']
        logger.info(f"  - Keywords tracked: {detection_stats['total_keywords_tracked']}")
        logger.info(f"  - Currently trending: {detection_stats['currently_trending']}")
        
    finally:
        await pipeline.close()


async def main():
    """Run all demonstrations."""
    logger.info("Starting Trend Alert Publishing System Demonstration")
    logger.info("=" * 60)
    
    try:
        # Note: These demos use mock Kafka connections since we're not running actual Kafka
        # In a real environment, make sure Kafka is running on localhost:9092
        
        await demonstrate_basic_pipeline()
        await demonstrate_deduplication()
        await demonstrate_single_alert_publishing()
        await demonstrate_trend_statistics()
        
        logger.info("\n" + "=" * 60)
        logger.info("Demonstration completed successfully!")
        
    except Exception as e:
        logger.error(f"Demonstration failed: {e}")
        raise


if __name__ == "__main__":
    # Run the demonstration
    asyncio.run(main())