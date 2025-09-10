"""Integration tests for the Redis caching layer.

These tests require a running Redis instance and test the actual
Redis operations, data consistency, and TTL behavior.
"""

import asyncio
import pytest
from datetime import datetime, timedelta
from typing import List

from cache_layer import CacheLayer
from shared.models import TrendAlert, WindowedKeywordCount


# Skip integration tests if Redis is not available
pytest_plugins = ["pytest_asyncio"]


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
async def redis_cache():
    """Create a CacheLayer instance connected to test Redis."""
    cache = CacheLayer(
        redis_host="localhost",
        redis_port=6379,
        redis_db=15,  # Use a separate test database
        default_ttl=60,  # Short TTL for testing
    )
    
    # Check if Redis is available
    if not await cache.health_check():
        pytest.skip("Redis is not available for integration tests")
    
    # Clear test database before tests
    await cache.clear_cache_pattern("*")
    
    yield cache
    
    # Cleanup after tests
    await cache.clear_cache_pattern("*")
    await cache.close()


@pytest.fixture
def sample_trends() -> List[TrendAlert]:
    """Create sample trend alerts for integration testing."""
    base_time = datetime.utcnow()
    return [
        TrendAlert(
            keyword="integration_test_python",
            frequency=100,
            growth_rate=15.5,
            confidence_score=0.80,
            window_start=base_time - timedelta(minutes=5),
            window_end=base_time,
            sample_posts=["post1", "post2"],
            unique_authors=50,
            rank=1,
        ),
        TrendAlert(
            keyword="integration_test_ai",
            frequency=85,
            growth_rate=12.3,
            confidence_score=0.75,
            window_start=base_time - timedelta(minutes=5),
            window_end=base_time,
            sample_posts=["post3"],
            unique_authors=40,
            rank=2,
        ),
    ]


@pytest.fixture
def sample_windowed_data() -> List[WindowedKeywordCount]:
    """Create sample windowed data for integration testing."""
    base_time = datetime.utcnow()
    return [
        WindowedKeywordCount(
            keyword="IntegrationTest",
            count=25,
            window_start=base_time - timedelta(minutes=3),
            window_end=base_time,
            unique_authors=15,
            normalized_keyword="integrationtest",
        ),
    ]


class TestRedisIntegration:
    """Test actual Redis operations and data consistency."""
    
    @pytest.mark.asyncio
    async def test_health_check_real_redis(self, redis_cache):
        """Test health check with real Redis instance."""
        result = await redis_cache.health_check()
        assert result is True
    
    @pytest.mark.asyncio
    async def test_cache_and_retrieve_trends(self, redis_cache, sample_trends):
        """Test caching and retrieving trends with real Redis."""
        # Cache trends
        await redis_cache.cache_current_trends(sample_trends, ttl=30)
        
        # Retrieve trends
        retrieved_trends = await redis_cache.get_trending_keywords()
        
        assert len(retrieved_trends) == 2
        assert retrieved_trends[0]["keyword"] == "integration_test_python"
        assert retrieved_trends[0]["rank"] == 1
        assert retrieved_trends[1]["keyword"] == "integration_test_ai"
        assert retrieved_trends[1]["rank"] == 2
    
    @pytest.mark.asyncio
    async def test_trend_details_consistency(self, redis_cache, sample_trends):
        """Test that trend details are consistent with cached trends."""
        # Cache trends
        await redis_cache.cache_current_trends(sample_trends, ttl=30)
        
        # Get details for specific trend
        details = await redis_cache.get_trend_details("integration_test_python")
        
        assert details is not None
        assert details["keyword"] == "integration_test_python"
        assert details["frequency"] == 100
        assert details["growth_rate"] == 15.5
        assert details["confidence_score"] == 0.80
        assert details["unique_authors"] == 50
        assert details["rank"] == 1
        assert isinstance(details["window_start"], datetime)
        assert isinstance(details["window_end"], datetime)
    
    @pytest.mark.asyncio
    async def test_user_metrics_round_trip(self, redis_cache):
        """Test caching and retrieving user metrics."""
        user_stats = {
            "total_users": 1000,
            "active_users": ["user1", "user2", "user3"],
            "posts_per_minute": 25.5,
        }
        
        # Cache user metrics
        await redis_cache.cache_user_metrics(user_stats, ttl=30)
        
        # Retrieve user metrics
        retrieved_stats = await redis_cache.get_user_metrics()
        
        assert retrieved_stats == user_stats
    
    @pytest.mark.asyncio
    async def test_windowed_data_caching(self, redis_cache, sample_windowed_data):
        """Test caching windowed data."""
        window_id = "integration_test_window_123"
        
        # Cache windowed data
        await redis_cache.cache_windowed_data(
            sample_windowed_data, 
            window_id, 
            ttl=30
        )
        
        # Verify data was cached (by checking if key exists)
        stats = await redis_cache.get_cache_stats()
        assert stats["window_keys"] >= 1
    
    @pytest.mark.asyncio
    async def test_ttl_behavior(self, redis_cache, sample_trends):
        """Test TTL behavior with real Redis."""
        # Cache trends with very short TTL
        await redis_cache.cache_current_trends(sample_trends, ttl=2)
        
        # Immediately retrieve - should be available
        trends = await redis_cache.get_trending_keywords()
        assert len(trends) == 2
        
        # Wait for TTL to expire
        await asyncio.sleep(3)
        
        # Should be expired now
        trends = await redis_cache.get_trending_keywords()
        assert len(trends) == 0
    
    @pytest.mark.asyncio
    async def test_cache_invalidation(self, redis_cache, sample_trends):
        """Test cache invalidation functionality."""
        # Cache some trends
        await redis_cache.cache_current_trends(sample_trends, ttl=60)
        
        # Verify trends are cached
        trends = await redis_cache.get_trending_keywords()
        assert len(trends) == 2
        
        # Clear trend cache
        cleared_count = await redis_cache.clear_cache_pattern("trends:*")
        assert cleared_count > 0
        
        # Verify trends are gone
        trends = await redis_cache.get_trending_keywords()
        assert len(trends) == 0
    
    @pytest.mark.asyncio
    async def test_cache_stats_accuracy(self, redis_cache, sample_trends):
        """Test that cache statistics are accurate."""
        # Start with clean cache
        await redis_cache.clear_cache_pattern("*")
        
        # Cache some data
        await redis_cache.cache_current_trends(sample_trends, ttl=60)
        await redis_cache.cache_user_metrics({"test": "data"}, ttl=60)
        
        # Get stats
        stats = await redis_cache.get_cache_stats()
        
        assert stats["trend_keys"] >= 3  # individual trends + current list + last_update
        assert stats["user_keys"] >= 2   # metrics + last_update
        assert stats["total_keys"] >= 5
        assert "redis_version" in stats
        assert "used_memory" in stats
    
    @pytest.mark.asyncio
    async def test_concurrent_operations(self, redis_cache, sample_trends):
        """Test concurrent cache operations."""
        # Create multiple trend sets
        trend_sets = []
        for i in range(5):
            base_time = datetime.utcnow()
            trends = [
                TrendAlert(
                    keyword=f"concurrent_test_{i}_{j}",
                    frequency=10 + j,
                    growth_rate=5.0 + j,
                    confidence_score=0.7 + (j * 0.1),
                    window_start=base_time - timedelta(minutes=5),
                    window_end=base_time,
                    sample_posts=[f"post_{i}_{j}"],
                    unique_authors=5 + j,
                    rank=j + 1,
                )
                for j in range(3)
            ]
            trend_sets.append(trends)
        
        # Cache all trend sets concurrently
        cache_tasks = [
            redis_cache.cache_current_trends(trends, ttl=60)
            for trends in trend_sets
        ]
        await asyncio.gather(*cache_tasks)
        
        # The last cached trends should be available
        # (since they all use the same key, the last one wins)
        retrieved_trends = await redis_cache.get_trending_keywords()
        assert len(retrieved_trends) == 3
    
    @pytest.mark.asyncio
    async def test_large_data_handling(self, redis_cache):
        """Test handling of large data sets."""
        # Create a large trend alert
        base_time = datetime.utcnow()
        large_trend = TrendAlert(
            keyword="large_test_keyword",
            frequency=10000,
            growth_rate=50.0,
            confidence_score=0.95,
            window_start=base_time - timedelta(hours=1),
            window_end=base_time,
            sample_posts=[f"post_{i}" for i in range(1000)],  # Large sample list
            unique_authors=5000,
            rank=1,
        )
        
        # Cache the large trend
        await redis_cache.cache_current_trends([large_trend], ttl=60)
        
        # Retrieve and verify
        details = await redis_cache.get_trend_details("large_test_keyword")
        assert details is not None
        assert len(details["sample_posts"]) == 1000
        assert details["unique_authors"] == 5000
    
    @pytest.mark.asyncio
    async def test_special_characters_handling(self, redis_cache):
        """Test handling of special characters in keywords."""
        base_time = datetime.utcnow()
        special_trend = TrendAlert(
            keyword="test-keyword_with.special@chars!",
            frequency=50,
            growth_rate=10.0,
            confidence_score=0.8,
            window_start=base_time - timedelta(minutes=5),
            window_end=base_time,
            sample_posts=["post1"],
            unique_authors=25,
            rank=1,
        )
        
        # Cache trend with special characters
        await redis_cache.cache_current_trends([special_trend], ttl=60)
        
        # Retrieve and verify
        details = await redis_cache.get_trend_details("test-keyword_with.special@chars!")
        assert details is not None
        assert details["keyword"] == "test-keyword_with.special@chars!"
    
    @pytest.mark.asyncio
    async def test_unicode_handling(self, redis_cache):
        """Test handling of Unicode characters in keywords."""
        base_time = datetime.utcnow()
        unicode_trend = TrendAlert(
            keyword="测试关键词",  # Chinese characters
            frequency=30,
            growth_rate=8.0,
            confidence_score=0.75,
            window_start=base_time - timedelta(minutes=5),
            window_end=base_time,
            sample_posts=["post1"],
            unique_authors=15,
            rank=1,
        )
        
        # Cache trend with Unicode characters
        await redis_cache.cache_current_trends([unicode_trend], ttl=60)
        
        # Retrieve and verify
        details = await redis_cache.get_trend_details("测试关键词")
        assert details is not None
        assert details["keyword"] == "测试关键词"


class TestDataConsistency:
    """Test data consistency across cache operations."""
    
    @pytest.mark.asyncio
    async def test_datetime_serialization_consistency(self, redis_cache):
        """Test that datetime objects are consistently serialized/deserialized."""
        base_time = datetime.utcnow().replace(microsecond=0)  # Remove microseconds for consistency
        
        trend = TrendAlert(
            keyword="datetime_test",
            frequency=100,
            growth_rate=15.0,
            confidence_score=0.8,
            window_start=base_time - timedelta(minutes=10),
            window_end=base_time,
            sample_posts=["post1"],
            unique_authors=50,
            rank=1,
        )
        
        # Cache the trend
        await redis_cache.cache_current_trends([trend], ttl=60)
        
        # Retrieve details
        details = await redis_cache.get_trend_details("datetime_test")
        
        assert details is not None
        # Check that datetime objects are properly reconstructed
        assert isinstance(details["window_start"], datetime)
        assert isinstance(details["window_end"], datetime)
        
        # Check that the values are correct (within 1 second due to potential precision loss)
        start_diff = abs((details["window_start"] - trend.window_start).total_seconds())
        end_diff = abs((details["window_end"] - trend.window_end).total_seconds())
        assert start_diff < 1.0
        assert end_diff < 1.0
    
    @pytest.mark.asyncio
    async def test_trend_ranking_consistency(self, redis_cache):
        """Test that trend rankings are maintained consistently."""
        base_time = datetime.utcnow()
        
        # Create trends with specific rankings
        trends = [
            TrendAlert(
                keyword=f"rank_test_{rank}",
                frequency=100 - (rank * 10),
                growth_rate=20.0 - rank,
                confidence_score=0.9 - (rank * 0.1),
                window_start=base_time - timedelta(minutes=5),
                window_end=base_time,
                sample_posts=[f"post_{rank}"],
                unique_authors=50 - (rank * 5),
                rank=rank,
            )
            for rank in [3, 1, 2]  # Intentionally out of order
        ]
        
        # Cache trends
        await redis_cache.cache_current_trends(trends, ttl=60)
        
        # Retrieve trends
        retrieved_trends = await redis_cache.get_trending_keywords()
        
        # Should be sorted by rank
        assert len(retrieved_trends) == 3
        assert retrieved_trends[0]["rank"] == 1
        assert retrieved_trends[1]["rank"] == 2
        assert retrieved_trends[2]["rank"] == 3
        assert retrieved_trends[0]["keyword"] == "rank_test_1"
        assert retrieved_trends[1]["keyword"] == "rank_test_2"
        assert retrieved_trends[2]["keyword"] == "rank_test_3"