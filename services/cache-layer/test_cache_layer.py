"""Unit tests for the Redis caching layer.

This module contains comprehensive tests for the CacheLayer class,
including tests for caching operations, TTL management, and error handling.
"""

import json
import pytest
from datetime import datetime, timedelta
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

from cache_layer import CacheLayer
from shared.models import TrendAlert, WindowedKeywordCount


@pytest.fixture
def sample_trend_alerts() -> List[TrendAlert]:
    """Create sample trend alerts for testing."""
    base_time = datetime.utcnow()
    return [
        TrendAlert(
            keyword="python",
            frequency=150,
            growth_rate=25.5,
            confidence_score=0.85,
            window_start=base_time - timedelta(minutes=10),
            window_end=base_time,
            sample_posts=["post1", "post2", "post3"],
            unique_authors=75,
            rank=1,
        ),
        TrendAlert(
            keyword="ai",
            frequency=120,
            growth_rate=18.2,
            confidence_score=0.78,
            window_start=base_time - timedelta(minutes=10),
            window_end=base_time,
            sample_posts=["post4", "post5"],
            unique_authors=60,
            rank=2,
        ),
        TrendAlert(
            keyword="blockchain",
            frequency=95,
            growth_rate=12.1,
            confidence_score=0.72,
            window_start=base_time - timedelta(minutes=10),
            window_end=base_time,
            sample_posts=["post6"],
            unique_authors=45,
            rank=3,
        ),
    ]


@pytest.fixture
def sample_windowed_data() -> List[WindowedKeywordCount]:
    """Create sample windowed keyword count data for testing."""
    base_time = datetime.utcnow()
    return [
        WindowedKeywordCount(
            keyword="Python",
            count=50,
            window_start=base_time - timedelta(minutes=5),
            window_end=base_time,
            unique_authors=25,
            normalized_keyword="python",
        ),
        WindowedKeywordCount(
            keyword="AI",
            count=35,
            window_start=base_time - timedelta(minutes=5),
            window_end=base_time,
            unique_authors=20,
            normalized_keyword="ai",
        ),
    ]


@pytest.fixture
def sample_user_stats() -> Dict[str, Any]:
    """Create sample user statistics for testing."""
    return {
        "total_active_users": 1500,
        "posts_per_minute": 45.2,
        "average_post_length": 128,
        "active_users": ["user1", "user2", "user3"],
        "top_contributors": [
            {"user_id": "user1", "post_count": 25},
            {"user_id": "user2", "post_count": 18},
        ],
    }


class MockAsyncContextManager:
    """Mock async context manager for Redis pipeline."""
    
    def __init__(self, mock_pipeline):
        self.mock_pipeline = mock_pipeline
    
    async def __aenter__(self):
        return self.mock_pipeline
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return None


@pytest.fixture
async def cache_layer():
    """Create a CacheLayer instance with mocked Redis client."""
    with patch("cache_layer.redis.Redis") as mock_redis_class:
        mock_redis = AsyncMock()
        mock_redis_class.return_value = mock_redis
        
        # Set up pipeline mock properly
        mock_pipeline = AsyncMock()
        mock_context_manager = MockAsyncContextManager(mock_pipeline)
        mock_redis.pipeline = MagicMock(return_value=mock_context_manager)
        
        cache = CacheLayer(
            redis_host="localhost",
            redis_port=6379,
            default_ttl=300,
        )
        cache.redis_client = mock_redis
        
        yield cache, mock_redis, mock_pipeline
        
        await cache.close()


class TestCacheLayerInitialization:
    """Test CacheLayer initialization and configuration."""
    
    def test_init_with_defaults(self):
        """Test initialization with default parameters."""
        with patch("cache_layer.redis.Redis") as mock_redis:
            cache = CacheLayer()
            
            mock_redis.assert_called_once_with(
                host="localhost",
                port=6379,
                db=0,
                password=None,
                max_connections=20,
                socket_timeout=5.0,
                socket_connect_timeout=5.0,
                decode_responses=True,
            )
            assert cache.default_ttl == 300
            assert cache.trend_key_prefix == "trends"
            assert cache.user_key_prefix == "users"
            assert cache.window_key_prefix == "windows"
    
    def test_init_with_custom_params(self):
        """Test initialization with custom parameters."""
        with patch("cache_layer.redis.Redis") as mock_redis:
            cache = CacheLayer(
                redis_host="redis.example.com",
                redis_port=6380,
                redis_db=1,
                redis_password="secret",
                default_ttl=600,
                max_connections=50,
                socket_timeout=10.0,
                socket_connect_timeout=15.0,
            )
            
            mock_redis.assert_called_once_with(
                host="redis.example.com",
                port=6380,
                db=1,
                password="secret",
                max_connections=50,
                socket_timeout=10.0,
                socket_connect_timeout=15.0,
                decode_responses=True,
            )
            assert cache.default_ttl == 600


class TestHealthCheck:
    """Test Redis health check functionality."""
    
    @pytest.mark.asyncio
    async def test_health_check_success(self, cache_layer):
        """Test successful health check."""
        cache, mock_redis, _ = cache_layer
        mock_redis.ping.return_value = True
        
        result = await cache.health_check()
        
        assert result is True
        mock_redis.ping.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_health_check_failure(self, cache_layer):
        """Test health check failure."""
        from redis.exceptions import ConnectionError
        
        cache, mock_redis, _ = cache_layer
        mock_redis.ping.side_effect = ConnectionError("Connection failed")
        
        result = await cache.health_check()
        
        assert result is False
        mock_redis.ping.assert_called_once()


class TestTrendCaching:
    """Test trend caching functionality."""
    
    @pytest.mark.asyncio
    async def test_cache_current_trends_success(self, cache_layer, sample_trend_alerts):
        """Test successful caching of current trends."""
        cache, mock_redis, mock_pipeline = cache_layer
        
        await cache.cache_current_trends(sample_trend_alerts, ttl=600)
        
        # Verify pipeline was used
        mock_redis.pipeline.assert_called_once()
        
        # Verify setex calls were made for individual trends
        assert mock_pipeline.setex.call_count == 5  # 3 trends + current list + last_update
        
        # Verify execute was called
        mock_pipeline.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_cache_current_trends_empty_list(self, cache_layer):
        """Test caching with empty trends list."""
        cache, mock_redis, _ = cache_layer
        
        await cache.cache_current_trends([])
        
        # Should not interact with Redis for empty list
        mock_redis.pipeline.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_cache_current_trends_connection_error(self, cache_layer, sample_trend_alerts):
        """Test caching with Redis connection error."""
        from redis.exceptions import ConnectionError
        
        cache, mock_redis, mock_pipeline = cache_layer
        mock_pipeline.execute.side_effect = ConnectionError("Connection failed")
        
        with pytest.raises(ConnectionError):
            await cache.cache_current_trends(sample_trend_alerts)
    
    @pytest.mark.asyncio
    async def test_get_trending_keywords_success(self, cache_layer):
        """Test successful retrieval of trending keywords."""
        cache, mock_redis, _ = cache_layer
        
        # Mock cached data
        cached_trends = [
            {"keyword": "python", "frequency": 150, "rank": 1},
            {"keyword": "ai", "frequency": 120, "rank": 2},
        ]
        mock_redis.get.return_value = json.dumps(cached_trends)
        
        result = await cache.get_trending_keywords(limit=10)
        
        assert result == cached_trends
        mock_redis.get.assert_called_once_with("trends:current")
    
    @pytest.mark.asyncio
    async def test_get_trending_keywords_no_cache(self, cache_layer):
        """Test retrieval when no cached data exists."""
        cache, mock_redis, _ = cache_layer
        mock_redis.get.return_value = None
        
        result = await cache.get_trending_keywords()
        
        assert result == []
        mock_redis.get.assert_called_once_with("trends:current")
    
    @pytest.mark.asyncio
    async def test_get_trending_keywords_with_limit(self, cache_layer):
        """Test retrieval with limit parameter."""
        cache, mock_redis, _ = cache_layer
        
        # Mock cached data with more items than limit
        cached_trends = [
            {"keyword": f"keyword{i}", "frequency": 100 - i, "rank": i + 1}
            for i in range(10)
        ]
        mock_redis.get.return_value = json.dumps(cached_trends)
        
        result = await cache.get_trending_keywords(limit=3)
        
        assert len(result) == 3
        assert result == cached_trends[:3]
    
    @pytest.mark.asyncio
    async def test_get_trend_details_success(self, cache_layer):
        """Test successful retrieval of trend details."""
        cache, mock_redis, _ = cache_layer
        
        # Mock cached trend details
        base_time = datetime.utcnow()
        trend_data = {
            "keyword": "python",
            "frequency": 150,
            "growth_rate": 25.5,
            "confidence_score": 0.85,
            "window_start": base_time.isoformat(),
            "window_end": (base_time + timedelta(minutes=10)).isoformat(),
            "sample_posts": ["post1", "post2"],
            "unique_authors": 75,
            "rank": 1,
        }
        mock_redis.get.return_value = json.dumps(trend_data)
        
        result = await cache.get_trend_details("python")
        
        assert result is not None
        assert result["keyword"] == "python"
        assert isinstance(result["window_start"], datetime)
        assert isinstance(result["window_end"], datetime)
        mock_redis.get.assert_called_once_with("trends:keyword:python")
    
    @pytest.mark.asyncio
    async def test_get_trend_details_not_found(self, cache_layer):
        """Test retrieval when trend details not found."""
        cache, mock_redis, _ = cache_layer
        mock_redis.get.return_value = None
        
        result = await cache.get_trend_details("nonexistent")
        
        assert result is None
        mock_redis.get.assert_called_once_with("trends:keyword:nonexistent")


class TestUserMetricsCaching:
    """Test user metrics caching functionality."""
    
    @pytest.mark.asyncio
    async def test_cache_user_metrics_success(self, cache_layer, sample_user_stats):
        """Test successful caching of user metrics."""
        cache, mock_redis, mock_pipeline = cache_layer
        
        await cache.cache_user_metrics(sample_user_stats, ttl=300)
        
        # Verify pipeline was used
        mock_redis.pipeline.assert_called_once()
        
        # Verify setex calls were made
        assert mock_pipeline.setex.call_count == 3  # metrics + active_users + last_update
        
        # Verify execute was called
        mock_pipeline.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_cache_user_metrics_empty_dict(self, cache_layer):
        """Test caching with empty user stats."""
        cache, mock_redis, _ = cache_layer
        
        await cache.cache_user_metrics({})
        
        # Should not interact with Redis for empty dict
        mock_redis.pipeline.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_get_user_metrics_success(self, cache_layer, sample_user_stats):
        """Test successful retrieval of user metrics."""
        cache, mock_redis, _ = cache_layer
        mock_redis.get.return_value = json.dumps(sample_user_stats)
        
        result = await cache.get_user_metrics()
        
        assert result == sample_user_stats
        mock_redis.get.assert_called_once_with("users:metrics")
    
    @pytest.mark.asyncio
    async def test_get_user_metrics_not_found(self, cache_layer):
        """Test retrieval when user metrics not found."""
        cache, mock_redis, _ = cache_layer
        mock_redis.get.return_value = None
        
        result = await cache.get_user_metrics()
        
        assert result is None
        mock_redis.get.assert_called_once_with("users:metrics")


class TestWindowedDataCaching:
    """Test windowed data caching functionality."""
    
    @pytest.mark.asyncio
    async def test_cache_windowed_data_success(self, cache_layer, sample_windowed_data):
        """Test successful caching of windowed data."""
        cache, mock_redis, _ = cache_layer
        
        window_id = "window_2024_01_01_12_00"
        await cache.cache_windowed_data(sample_windowed_data, window_id, ttl=600)
        
        # Verify setex was called with correct parameters
        mock_redis.setex.assert_called_once()
        call_args = mock_redis.setex.call_args
        
        assert call_args[0][0] == f"windows:{window_id}"
        assert call_args[0][1] == 600
        
        # Verify data was serialized correctly
        cached_data = json.loads(call_args[0][2])
        assert len(cached_data) == 2
        assert cached_data[0]["keyword"] == "Python"
        assert cached_data[0]["normalized_keyword"] == "python"
    
    @pytest.mark.asyncio
    async def test_cache_windowed_data_empty_list(self, cache_layer):
        """Test caching with empty windowed data."""
        cache, mock_redis, _ = cache_layer
        
        await cache.cache_windowed_data([], "window_id")
        
        # Should not interact with Redis for empty list
        mock_redis.setex.assert_not_called()


class TestCacheInvalidation:
    """Test cache invalidation functionality."""
    
    @pytest.mark.asyncio
    async def test_invalidate_expired_trends_success(self, cache_layer):
        """Test successful invalidation of expired trends."""
        cache, mock_redis, _ = cache_layer
        
        # Mock keys and TTL responses
        trend_keys = ["trends:keyword:python", "trends:keyword:ai", "trends:current"]
        mock_redis.keys.return_value = trend_keys
        
        # Mock TTL responses: first key expired (-2), second key has TTL, third key no TTL (-1)
        mock_redis.ttl.side_effect = [-2, 300, -1]
        
        # Mock delete response
        mock_redis.delete.return_value = 2
        
        result = await cache.invalidate_expired_trends()
        
        assert result == 2
        mock_redis.keys.assert_called_once_with("trends:*")
        assert mock_redis.ttl.call_count == 3
        mock_redis.delete.assert_called_once_with("trends:keyword:python", "trends:current")
    
    @pytest.mark.asyncio
    async def test_invalidate_expired_trends_no_keys(self, cache_layer):
        """Test invalidation when no trend keys exist."""
        cache, mock_redis, _ = cache_layer
        mock_redis.keys.return_value = []
        
        result = await cache.invalidate_expired_trends()
        
        assert result == 0
        mock_redis.keys.assert_called_once_with("trends:*")
        mock_redis.ttl.assert_not_called()
        mock_redis.delete.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_clear_cache_pattern_success(self, cache_layer):
        """Test successful clearing of cache pattern."""
        cache, mock_redis, _ = cache_layer
        
        # Mock keys matching pattern
        matching_keys = ["trends:keyword:python", "trends:keyword:ai"]
        mock_redis.keys.return_value = matching_keys
        mock_redis.delete.return_value = 2
        
        result = await cache.clear_cache_pattern("trends:*")
        
        assert result == 2
        mock_redis.keys.assert_called_once_with("trends:*")
        mock_redis.delete.assert_called_once_with(*matching_keys)
    
    @pytest.mark.asyncio
    async def test_clear_cache_pattern_no_matches(self, cache_layer):
        """Test clearing when no keys match pattern."""
        cache, mock_redis, _ = cache_layer
        mock_redis.keys.return_value = []
        
        result = await cache.clear_cache_pattern("nonexistent:*")
        
        assert result == 0
        mock_redis.keys.assert_called_once_with("nonexistent:*")
        mock_redis.delete.assert_not_called()


class TestCacheStats:
    """Test cache statistics functionality."""
    
    @pytest.mark.asyncio
    async def test_get_cache_stats_success(self, cache_layer):
        """Test successful retrieval of cache statistics."""
        cache, mock_redis, _ = cache_layer
        
        # Mock Redis info
        redis_info = {
            "redis_version": "7.0.0",
            "connected_clients": 5,
            "used_memory": 1048576,
            "used_memory_human": "1.00M",
            "uptime_in_seconds": 3600,
        }
        mock_redis.info.return_value = redis_info
        
        # Mock keys for different prefixes
        mock_redis.keys.side_effect = [
            ["trends:keyword:python", "trends:current"],  # trend keys
            ["users:metrics", "users:active"],  # user keys
            ["windows:window1"],  # window keys
        ]
        
        result = await cache.get_cache_stats()
        
        assert result["redis_version"] == "7.0.0"
        assert result["connected_clients"] == 5
        assert result["used_memory"] == 1048576
        assert result["used_memory_human"] == "1.00M"
        assert result["total_keys"] == 5
        assert result["trend_keys"] == 2
        assert result["user_keys"] == 2
        assert result["window_keys"] == 1
        assert result["uptime_in_seconds"] == 3600
        
        # Verify all keys calls were made
        assert mock_redis.keys.call_count == 3


class TestErrorHandling:
    """Test error handling in cache operations."""
    
    @pytest.mark.asyncio
    async def test_connection_error_handling(self, cache_layer, sample_trend_alerts):
        """Test handling of Redis connection errors."""
        from redis.exceptions import ConnectionError
        
        cache, mock_redis, mock_pipeline = cache_layer
        mock_pipeline.execute.side_effect = ConnectionError("Connection lost")
        
        with pytest.raises(ConnectionError):
            await cache.cache_current_trends(sample_trend_alerts)
    
    @pytest.mark.asyncio
    async def test_timeout_error_handling(self, cache_layer):
        """Test handling of Redis timeout errors."""
        from redis.exceptions import TimeoutError
        
        cache, mock_redis, _ = cache_layer
        mock_redis.get.side_effect = TimeoutError("Operation timed out")
        
        with pytest.raises(TimeoutError):
            await cache.get_trending_keywords()
    
    @pytest.mark.asyncio
    async def test_json_decode_error_handling(self, cache_layer):
        """Test handling of JSON decode errors."""
        cache, mock_redis, _ = cache_layer
        mock_redis.get.return_value = "invalid json"
        
        # Should return empty list instead of raising exception
        result = await cache.get_trending_keywords()
        assert result == []
        
        # Should return None for trend details
        result = await cache.get_trend_details("python")
        assert result is None