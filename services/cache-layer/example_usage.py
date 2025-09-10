"""Example usage of the Redis caching layer.

This script demonstrates how to use the CacheLayer class for caching
trending keywords, user metrics, and windowed data.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List

from cache_layer import CacheLayer
from shared.models import TrendAlert, WindowedKeywordCount


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def create_sample_trends() -> List[TrendAlert]:
    """Create sample trend alerts for demonstration."""
    base_time = datetime.utcnow()
    
    return [
        TrendAlert(
            keyword="python",
            frequency=250,
            growth_rate=35.2,
            confidence_score=0.92,
            window_start=base_time - timedelta(minutes=15),
            window_end=base_time,
            sample_posts=[
                "at://did:plc:example1/app.bsky.feed.post/abc123",
                "at://did:plc:example2/app.bsky.feed.post/def456",
                "at://did:plc:example3/app.bsky.feed.post/ghi789",
            ],
            unique_authors=125,
            rank=1,
        ),
        TrendAlert(
            keyword="ai",
            frequency=180,
            growth_rate=28.7,
            confidence_score=0.87,
            window_start=base_time - timedelta(minutes=15),
            window_end=base_time,
            sample_posts=[
                "at://did:plc:example4/app.bsky.feed.post/jkl012",
                "at://did:plc:example5/app.bsky.feed.post/mno345",
            ],
            unique_authors=95,
            rank=2,
        ),
        TrendAlert(
            keyword="blockchain",
            frequency=145,
            growth_rate=22.1,
            confidence_score=0.81,
            window_start=base_time - timedelta(minutes=15),
            window_end=base_time,
            sample_posts=[
                "at://did:plc:example6/app.bsky.feed.post/pqr678",
            ],
            unique_authors=72,
            rank=3,
        ),
        TrendAlert(
            keyword="machine_learning",
            frequency=120,
            growth_rate=18.5,
            confidence_score=0.78,
            window_start=base_time - timedelta(minutes=15),
            window_end=base_time,
            sample_posts=[
                "at://did:plc:example7/app.bsky.feed.post/stu901",
                "at://did:plc:example8/app.bsky.feed.post/vwx234",
            ],
            unique_authors=60,
            rank=4,
        ),
    ]


def create_sample_windowed_data() -> List[WindowedKeywordCount]:
    """Create sample windowed keyword count data."""
    base_time = datetime.utcnow()
    
    return [
        WindowedKeywordCount(
            keyword="Python",
            count=85,
            window_start=base_time - timedelta(minutes=5),
            window_end=base_time,
            unique_authors=42,
            normalized_keyword="python",
        ),
        WindowedKeywordCount(
            keyword="AI",
            count=67,
            window_start=base_time - timedelta(minutes=5),
            window_end=base_time,
            unique_authors=35,
            normalized_keyword="ai",
        ),
        WindowedKeywordCount(
            keyword="Blockchain",
            count=53,
            window_start=base_time - timedelta(minutes=5),
            window_end=base_time,
            unique_authors=28,
            normalized_keyword="blockchain",
        ),
    ]


async def demonstrate_trend_caching(cache: CacheLayer) -> None:
    """Demonstrate trend caching functionality."""
    logger.info("=== Demonstrating Trend Caching ===")
    
    # Create sample trends
    trends = create_sample_trends()
    logger.info(f"Created {len(trends)} sample trends")
    
    # Cache the trends with 10-minute TTL
    await cache.cache_current_trends(trends, ttl=600)
    logger.info("Cached trends successfully")
    
    # Retrieve trending keywords
    retrieved_trends = await cache.get_trending_keywords(limit=10)
    logger.info(f"Retrieved {len(retrieved_trends)} trending keywords:")
    
    for trend in retrieved_trends:
        logger.info(
            f"  #{trend['rank']}: {trend['keyword']} "
            f"(freq: {trend['frequency']}, growth: {trend['growth_rate']:.1f}%)"
        )
    
    # Get detailed information for a specific trend
    details = await cache.get_trend_details("python")
    if details:
        logger.info(f"Details for 'python': {details['frequency']} occurrences, "
                   f"{details['unique_authors']} unique authors")
        logger.info(f"Sample posts: {details['sample_posts'][:2]}...")
    
    logger.info("")


async def demonstrate_user_metrics_caching(cache: CacheLayer) -> None:
    """Demonstrate user metrics caching functionality."""
    logger.info("=== Demonstrating User Metrics Caching ===")
    
    # Create sample user metrics
    user_stats = {
        "total_active_users": 2500,
        "posts_per_minute": 67.3,
        "average_post_length": 142,
        "top_contributors": [
            {"user_id": "did:plc:user1", "post_count": 45},
            {"user_id": "did:plc:user2", "post_count": 38},
            {"user_id": "did:plc:user3", "post_count": 32},
        ],
        "active_users": [f"did:plc:user{i}" for i in range(1, 101)],  # 100 active users
        "language_distribution": {
            "en": 0.72,
            "es": 0.15,
            "fr": 0.08,
            "de": 0.05,
        },
    }
    
    # Cache user metrics with 5-minute TTL
    await cache.cache_user_metrics(user_stats, ttl=300)
    logger.info("Cached user metrics successfully")
    
    # Retrieve user metrics
    retrieved_stats = await cache.get_user_metrics()
    if retrieved_stats:
        logger.info(f"Total active users: {retrieved_stats['total_active_users']}")
        logger.info(f"Posts per minute: {retrieved_stats['posts_per_minute']}")
        logger.info(f"Top contributors: {len(retrieved_stats['top_contributors'])}")
        logger.info(f"Language distribution: {retrieved_stats['language_distribution']}")
    
    logger.info("")


async def demonstrate_windowed_data_caching(cache: CacheLayer) -> None:
    """Demonstrate windowed data caching functionality."""
    logger.info("=== Demonstrating Windowed Data Caching ===")
    
    # Create sample windowed data
    windowed_data = create_sample_windowed_data()
    logger.info(f"Created {len(windowed_data)} windowed keyword counts")
    
    # Cache windowed data with unique window ID
    window_id = f"window_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    await cache.cache_windowed_data(windowed_data, window_id, ttl=300)
    logger.info(f"Cached windowed data for window: {window_id}")
    
    logger.info("")


async def demonstrate_cache_management(cache: CacheLayer) -> None:
    """Demonstrate cache management functionality."""
    logger.info("=== Demonstrating Cache Management ===")
    
    # Get cache statistics
    stats = await cache.get_cache_stats()
    logger.info("Cache Statistics:")
    logger.info(f"  Redis version: {stats['redis_version']}")
    logger.info(f"  Connected clients: {stats['connected_clients']}")
    logger.info(f"  Used memory: {stats['used_memory_human']}")
    logger.info(f"  Total keys: {stats['total_keys']}")
    logger.info(f"  Trend keys: {stats['trend_keys']}")
    logger.info(f"  User keys: {stats['user_keys']}")
    logger.info(f"  Window keys: {stats['window_keys']}")
    logger.info(f"  Uptime: {stats['uptime_in_seconds']} seconds")
    
    # Demonstrate cache invalidation
    logger.info("\nTesting cache invalidation...")
    
    # Check current trends before invalidation
    trends_before = await cache.get_trending_keywords()
    logger.info(f"Trends before invalidation: {len(trends_before)}")
    
    # Invalidate expired trends (this won't remove anything since TTL is still valid)
    invalidated_count = await cache.invalidate_expired_trends()
    logger.info(f"Invalidated {invalidated_count} expired trend keys")
    
    # Clear specific pattern (be careful in production!)
    # cleared_count = await cache.clear_cache_pattern("trends:keyword:*")
    # logger.info(f"Cleared {cleared_count} individual trend keys")
    
    logger.info("")


async def demonstrate_error_handling(cache: CacheLayer) -> None:
    """Demonstrate error handling scenarios."""
    logger.info("=== Demonstrating Error Handling ===")
    
    # Test retrieving non-existent data
    non_existent_trend = await cache.get_trend_details("non_existent_keyword")
    logger.info(f"Non-existent trend details: {non_existent_trend}")
    
    # Test retrieving from empty cache
    await cache.clear_cache_pattern("trends:current")
    empty_trends = await cache.get_trending_keywords()
    logger.info(f"Trends from empty cache: {len(empty_trends)}")
    
    # Test caching empty data
    await cache.cache_current_trends([])  # Should handle gracefully
    await cache.cache_user_metrics({})    # Should handle gracefully
    logger.info("Handled empty data caching gracefully")
    
    logger.info("")


async def main() -> None:
    """Main demonstration function."""
    logger.info("Starting Redis Cache Layer Demonstration")
    logger.info("=" * 50)
    
    # Initialize cache layer
    cache = CacheLayer(
        redis_host="localhost",
        redis_port=6379,
        redis_db=0,
        default_ttl=300,  # 5 minutes default TTL
    )
    
    try:
        # Check Redis connection
        if not await cache.health_check():
            logger.error("Redis is not available. Please start Redis server.")
            return
        
        logger.info("Redis connection successful!")
        logger.info("")
        
        # Run demonstrations
        await demonstrate_trend_caching(cache)
        await demonstrate_user_metrics_caching(cache)
        await demonstrate_windowed_data_caching(cache)
        await demonstrate_cache_management(cache)
        await demonstrate_error_handling(cache)
        
        logger.info("Demonstration completed successfully!")
        
    except Exception as e:
        logger.error(f"Demonstration failed: {e}")
        raise
    
    finally:
        # Clean up
        await cache.close()
        logger.info("Cache connection closed")


if __name__ == "__main__":
    # Run the demonstration
    asyncio.run(main())