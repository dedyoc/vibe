"""Redis caching layer implementation for real-time trend data.

This module provides the CacheLayer class for caching trending keywords,
user metrics, and other real-time data with TTL-based management and
cache invalidation strategies.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set
from dataclasses import asdict

import redis.asyncio as redis
from redis.asyncio import Redis
from redis.exceptions import ConnectionError, TimeoutError

from shared.models import TrendAlert, WindowedKeywordCount


logger = logging.getLogger(__name__)


class CacheLayer:
    """Redis-based caching layer for real-time trend data.
    
    This class provides methods for caching trending keywords, user metrics,
    and other real-time data with TTL-based management and cache invalidation.
    
    Attributes:
        redis_client: Async Redis client instance
        default_ttl: Default TTL in seconds for cached data
        trend_key_prefix: Prefix for trend-related cache keys
        user_key_prefix: Prefix for user-related cache keys
        window_key_prefix: Prefix for windowed data cache keys
    """
    
    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0,
        redis_password: Optional[str] = None,
        default_ttl: int = 300,  # 5 minutes default TTL
        max_connections: int = 20,
        socket_timeout: float = 5.0,
        socket_connect_timeout: float = 5.0,
    ) -> None:
        """Initialize the Redis caching layer.
        
        Args:
            redis_host: Redis server hostname
            redis_port: Redis server port
            redis_db: Redis database number
            redis_password: Optional Redis password
            default_ttl: Default TTL in seconds for cached data
            max_connections: Maximum number of Redis connections
            socket_timeout: Socket timeout in seconds
            socket_connect_timeout: Socket connection timeout in seconds
        """
        self.redis_client: Redis = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            password=redis_password,
            max_connections=max_connections,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            decode_responses=True,
        )
        self.default_ttl = default_ttl
        self.trend_key_prefix = "trends"
        self.user_key_prefix = "users"
        self.window_key_prefix = "windows"
        
    async def close(self) -> None:
        """Close the Redis connection."""
        await self.redis_client.close()
        
    async def health_check(self) -> bool:
        """Check if Redis connection is healthy.
        
        Returns:
            True if Redis is accessible, False otherwise
        """
        try:
            await self.redis_client.ping()
            return True
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Redis health check failed: {e}")
            return False
            
    async def cache_current_trends(
        self, 
        trends: List[TrendAlert], 
        ttl: Optional[int] = None
    ) -> None:
        """Cache current trending keywords with TTL.
        
        Args:
            trends: List of trend alerts to cache
            ttl: Time-to-live in seconds (uses default_ttl if None)
            
        Raises:
            ConnectionError: If Redis connection fails
        """
        if not trends:
            logger.warning("No trends provided for caching")
            return
            
        cache_ttl = ttl or self.default_ttl
        current_time = datetime.utcnow()
        
        try:
            # Use pipeline for atomic operations
            async with self.redis_client.pipeline() as pipe:
                # Cache individual trends
                for trend in trends:
                    trend_key = f"{self.trend_key_prefix}:keyword:{trend.keyword}"
                    trend_data = asdict(trend)
                    # Convert datetime objects to ISO strings for JSON serialization
                    trend_data["window_start"] = trend.window_start.isoformat()
                    trend_data["window_end"] = trend.window_end.isoformat()
                    
                    await pipe.setex(
                        trend_key,
                        cache_ttl,
                        json.dumps(trend_data)
                    )
                
                # Cache current trends list (sorted by rank)
                sorted_trends = sorted(trends, key=lambda t: t.rank)
                trends_list_key = f"{self.trend_key_prefix}:current"
                trends_data = [
                    {
                        "keyword": trend.keyword,
                        "frequency": trend.frequency,
                        "growth_rate": trend.growth_rate,
                        "confidence_score": trend.confidence_score,
                        "rank": trend.rank,
                        "unique_authors": trend.unique_authors,
                    }
                    for trend in sorted_trends
                ]
                
                await pipe.setex(
                    trends_list_key,
                    cache_ttl,
                    json.dumps(trends_data)
                )
                
                # Cache timestamp of last update
                last_update_key = f"{self.trend_key_prefix}:last_update"
                await pipe.setex(
                    last_update_key,
                    cache_ttl,
                    current_time.isoformat()
                )
                
                await pipe.execute()
                
            logger.info(f"Cached {len(trends)} trends with TTL {cache_ttl}s")
            
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Failed to cache trends: {e}")
            raise
            
    async def get_trending_keywords(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Retrieve current trending keywords from cache.
        
        Args:
            limit: Maximum number of trends to return
            
        Returns:
            List of trending keyword data dictionaries
            
        Raises:
            ConnectionError: If Redis connection fails
        """
        try:
            trends_list_key = f"{self.trend_key_prefix}:current"
            cached_data = await self.redis_client.get(trends_list_key)
            
            if not cached_data:
                logger.info("No cached trends found")
                return []
                
            trends_data = json.loads(cached_data)
            
            # Apply limit and return
            limited_trends = trends_data[:limit] if limit > 0 else trends_data
            
            logger.info(f"Retrieved {len(limited_trends)} trending keywords from cache")
            return limited_trends
            
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Failed to retrieve trending keywords: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode cached trends data: {e}")
            return []
            
    async def get_trend_details(self, keyword: str) -> Optional[Dict[str, Any]]:
        """Get detailed information for a specific trending keyword.
        
        Args:
            keyword: The keyword to get details for
            
        Returns:
            Trend details dictionary or None if not found
            
        Raises:
            ConnectionError: If Redis connection fails
        """
        try:
            trend_key = f"{self.trend_key_prefix}:keyword:{keyword}"
            cached_data = await self.redis_client.get(trend_key)
            
            if not cached_data:
                logger.info(f"No cached details found for keyword: {keyword}")
                return None
                
            trend_data = json.loads(cached_data)
            
            # Convert ISO strings back to datetime objects
            trend_data["window_start"] = datetime.fromisoformat(trend_data["window_start"])
            trend_data["window_end"] = datetime.fromisoformat(trend_data["window_end"])
            
            logger.info(f"Retrieved details for trending keyword: {keyword}")
            return trend_data
            
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Failed to retrieve trend details for {keyword}: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode trend details for {keyword}: {e}")
            return None
            
    async def cache_user_metrics(
        self, 
        user_stats: Dict[str, Any], 
        ttl: Optional[int] = None
    ) -> None:
        """Cache user activity metrics.
        
        Args:
            user_stats: Dictionary containing user metrics
            ttl: Time-to-live in seconds (uses default_ttl if None)
            
        Raises:
            ConnectionError: If Redis connection fails
        """
        if not user_stats:
            logger.warning("No user stats provided for caching")
            return
            
        cache_ttl = ttl or self.default_ttl
        current_time = datetime.utcnow()
        
        try:
            async with self.redis_client.pipeline() as pipe:
                # Cache overall user metrics
                user_metrics_key = f"{self.user_key_prefix}:metrics"
                await pipe.setex(
                    user_metrics_key,
                    cache_ttl,
                    json.dumps(user_stats)
                )
                
                # Cache individual user data if provided
                if "active_users" in user_stats:
                    active_users_key = f"{self.user_key_prefix}:active"
                    await pipe.setex(
                        active_users_key,
                        cache_ttl,
                        json.dumps(user_stats["active_users"])
                    )
                
                # Cache timestamp of last update
                last_update_key = f"{self.user_key_prefix}:last_update"
                await pipe.setex(
                    last_update_key,
                    cache_ttl,
                    current_time.isoformat()
                )
                
                await pipe.execute()
                
            logger.info(f"Cached user metrics with TTL {cache_ttl}s")
            
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Failed to cache user metrics: {e}")
            raise
            
    async def get_user_metrics(self) -> Optional[Dict[str, Any]]:
        """Retrieve cached user metrics.
        
        Returns:
            User metrics dictionary or None if not found
            
        Raises:
            ConnectionError: If Redis connection fails
        """
        try:
            user_metrics_key = f"{self.user_key_prefix}:metrics"
            cached_data = await self.redis_client.get(user_metrics_key)
            
            if not cached_data:
                logger.info("No cached user metrics found")
                return None
                
            user_metrics = json.loads(cached_data)
            
            logger.info("Retrieved user metrics from cache")
            return user_metrics
            
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Failed to retrieve user metrics: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode user metrics: {e}")
            return None
            
    async def cache_windowed_data(
        self,
        window_data: List[WindowedKeywordCount],
        window_id: str,
        ttl: Optional[int] = None
    ) -> None:
        """Cache windowed keyword count data.
        
        Args:
            window_data: List of windowed keyword counts
            window_id: Unique identifier for the time window
            ttl: Time-to-live in seconds (uses default_ttl if None)
            
        Raises:
            ConnectionError: If Redis connection fails
        """
        if not window_data:
            logger.warning("No window data provided for caching")
            return
            
        cache_ttl = ttl or self.default_ttl
        
        try:
            window_key = f"{self.window_key_prefix}:{window_id}"
            
            # Convert windowed data to serializable format
            serializable_data = []
            for item in window_data:
                item_data = asdict(item)
                item_data["window_start"] = item.window_start.isoformat()
                item_data["window_end"] = item.window_end.isoformat()
                serializable_data.append(item_data)
            
            await self.redis_client.setex(
                window_key,
                cache_ttl,
                json.dumps(serializable_data)
            )
            
            logger.info(f"Cached {len(window_data)} windowed items for window {window_id}")
            
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Failed to cache windowed data: {e}")
            raise
            
    async def invalidate_expired_trends(self) -> int:
        """Remove expired trend data from cache.
        
        Returns:
            Number of keys that were invalidated
            
        Raises:
            ConnectionError: If Redis connection fails
        """
        try:
            # Get all trend-related keys
            trend_pattern = f"{self.trend_key_prefix}:*"
            trend_keys = await self.redis_client.keys(trend_pattern)
            
            if not trend_keys:
                logger.info("No trend keys found for invalidation")
                return 0
            
            # Check TTL for each key and collect expired ones
            expired_keys = []
            for key in trend_keys:
                ttl = await self.redis_client.ttl(key)
                if ttl == -2:  # Key doesn't exist
                    expired_keys.append(key)
                elif ttl == -1:  # Key exists but has no TTL (shouldn't happen)
                    logger.warning(f"Key {key} has no TTL, considering for cleanup")
                    expired_keys.append(key)
            
            # Delete expired keys
            if expired_keys:
                deleted_count = await self.redis_client.delete(*expired_keys)
                logger.info(f"Invalidated {deleted_count} expired trend keys")
                return deleted_count
            else:
                logger.info("No expired trend keys found")
                return 0
                
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Failed to invalidate expired trends: {e}")
            raise
            
    async def clear_cache_pattern(self, pattern: str) -> int:
        """Clear all cache keys matching a pattern.
        
        Args:
            pattern: Redis key pattern (e.g., "trends:*")
            
        Returns:
            Number of keys that were deleted
            
        Raises:
            ConnectionError: If Redis connection fails
        """
        try:
            keys = await self.redis_client.keys(pattern)
            
            if not keys:
                logger.info(f"No keys found matching pattern: {pattern}")
                return 0
            
            deleted_count = await self.redis_client.delete(*keys)
            logger.info(f"Cleared {deleted_count} keys matching pattern: {pattern}")
            return deleted_count
            
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Failed to clear cache pattern {pattern}: {e}")
            raise
            
    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics and health information.
        
        Returns:
            Dictionary containing cache statistics
            
        Raises:
            ConnectionError: If Redis connection fails
        """
        try:
            # Get Redis info
            redis_info = await self.redis_client.info()
            
            # Count keys by prefix
            trend_keys = await self.redis_client.keys(f"{self.trend_key_prefix}:*")
            user_keys = await self.redis_client.keys(f"{self.user_key_prefix}:*")
            window_keys = await self.redis_client.keys(f"{self.window_key_prefix}:*")
            
            stats = {
                "redis_version": redis_info.get("redis_version", "unknown"),
                "connected_clients": redis_info.get("connected_clients", 0),
                "used_memory": redis_info.get("used_memory", 0),
                "used_memory_human": redis_info.get("used_memory_human", "0B"),
                "total_keys": len(trend_keys) + len(user_keys) + len(window_keys),
                "trend_keys": len(trend_keys),
                "user_keys": len(user_keys),
                "window_keys": len(window_keys),
                "uptime_in_seconds": redis_info.get("uptime_in_seconds", 0),
            }
            
            logger.info("Retrieved cache statistics")
            return stats
            
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Failed to get cache stats: {e}")
            raise