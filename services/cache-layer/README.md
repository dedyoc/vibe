# Redis Cache Layer Service

The Redis Cache Layer service provides high-performance caching for the Bluesky streaming pipeline, enabling real-time access to trending keywords, user metrics, and windowed aggregation data.

## Features

- **Trend Caching**: Cache trending keywords with TTL-based expiration
- **User Metrics**: Store and retrieve user activity statistics
- **Windowed Data**: Cache time-windowed keyword counts and aggregations
- **Cache Management**: Automatic invalidation and cleanup of expired data
- **Health Monitoring**: Built-in health checks and statistics
- **Error Handling**: Robust error handling with connection resilience
- **Async Support**: Full async/await support for high-performance operations

## Architecture

The cache layer uses Redis as the backend storage with the following key patterns:

- `trends:current` - Current trending keywords list
- `trends:keyword:{keyword}` - Individual trend details
- `trends:last_update` - Timestamp of last trend update
- `users:metrics` - User activity metrics
- `users:active` - Active user list
- `users:last_update` - Timestamp of last user metrics update
- `windows:{window_id}` - Windowed keyword count data

## Installation

### Prerequisites

- Python 3.9+
- Redis server (6.0+)
- Required Python packages (see requirements.txt)

### Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Start Redis server:
```bash
redis-server
```

3. Run the example usage:
```bash
python example_usage.py
```

## Usage

### Basic Usage

```python
import asyncio
from cache_layer import CacheLayer
from shared.models import TrendAlert

async def main():
    # Initialize cache layer
    cache = CacheLayer(
        redis_host="localhost",
        redis_port=6379,
        default_ttl=300  # 5 minutes
    )
    
    # Check health
    if await cache.health_check():
        print("Redis is healthy!")
    
    # Cache trending keywords
    trends = [
        TrendAlert(
            keyword="python",
            frequency=150,
            growth_rate=25.5,
            confidence_score=0.85,
            window_start=datetime.utcnow() - timedelta(minutes=10),
            window_end=datetime.utcnow(),
            sample_posts=["post1", "post2"],
            unique_authors=75,
            rank=1,
        )
    ]
    
    await cache.cache_current_trends(trends, ttl=600)
    
    # Retrieve trending keywords
    trending = await cache.get_trending_keywords(limit=10)
    print(f"Found {len(trending)} trending keywords")
    
    # Get specific trend details
    details = await cache.get_trend_details("python")
    if details:
        print(f"Python trend: {details['frequency']} occurrences")
    
    # Clean up
    await cache.close()

asyncio.run(main())
```

### Caching User Metrics

```python
# Cache user activity metrics
user_stats = {
    "total_active_users": 1500,
    "posts_per_minute": 45.2,
    "top_contributors": [
        {"user_id": "user1", "post_count": 25},
        {"user_id": "user2", "post_count": 18},
    ]
}

await cache.cache_user_metrics(user_stats, ttl=300)

# Retrieve user metrics
metrics = await cache.get_user_metrics()
print(f"Active users: {metrics['total_active_users']}")
```

### Windowed Data Caching

```python
from shared.models import WindowedKeywordCount

# Create windowed data
windowed_data = [
    WindowedKeywordCount(
        keyword="Python",
        count=50,
        window_start=datetime.utcnow() - timedelta(minutes=5),
        window_end=datetime.utcnow(),
        unique_authors=25,
        normalized_keyword="python",
    )
]

# Cache windowed data
window_id = "window_2024_01_01_12_00"
await cache.cache_windowed_data(windowed_data, window_id, ttl=300)
```

### Cache Management

```python
# Get cache statistics
stats = await cache.get_cache_stats()
print(f"Total keys: {stats['total_keys']}")
print(f"Used memory: {stats['used_memory_human']}")

# Invalidate expired trends
invalidated = await cache.invalidate_expired_trends()
print(f"Invalidated {invalidated} expired keys")

# Clear specific pattern
cleared = await cache.clear_cache_pattern("trends:*")
print(f"Cleared {cleared} trend keys")
```

## Configuration

### Environment Variables

- `REDIS_HOST`: Redis server hostname (default: localhost)
- `REDIS_PORT`: Redis server port (default: 6379)
- `REDIS_DB`: Redis database number (default: 0)
- `REDIS_PASSWORD`: Redis password (optional)
- `CACHE_DEFAULT_TTL`: Default TTL in seconds (default: 300)

### CacheLayer Parameters

```python
cache = CacheLayer(
    redis_host="localhost",           # Redis hostname
    redis_port=6379,                  # Redis port
    redis_db=0,                       # Redis database
    redis_password=None,              # Redis password
    default_ttl=300,                  # Default TTL (seconds)
    max_connections=20,               # Max Redis connections
    socket_timeout=5.0,               # Socket timeout
    socket_connect_timeout=5.0,       # Connection timeout
)
```

## Testing

### Unit Tests

Run unit tests with mocked Redis:

```bash
pytest test_cache_layer.py -v
```

### Integration Tests

Run integration tests with real Redis (requires Redis server):

```bash
pytest test_integration.py -v
```

### Test Coverage

Generate test coverage report:

```bash
pytest --cov=cache_layer --cov-report=html
```

## Performance Considerations

### Memory Usage

- Use appropriate TTL values to prevent memory bloat
- Monitor Redis memory usage with `get_cache_stats()`
- Consider Redis eviction policies for production

### Connection Management

- Use connection pooling (enabled by default)
- Configure appropriate timeouts for your network
- Monitor connection count in Redis

### Data Serialization

- JSON serialization is used for compatibility
- Large objects may impact performance
- Consider data structure optimization for frequently accessed data

## Monitoring

### Health Checks

```python
# Basic health check
healthy = await cache.health_check()

# Detailed statistics
stats = await cache.get_cache_stats()
```

### Metrics

The cache layer provides metrics for:

- Total keys by category (trends, users, windows)
- Redis memory usage
- Connection count
- Cache hit/miss ratios (via Redis INFO)

### Logging

The service uses structured logging with the following levels:

- `INFO`: Normal operations and statistics
- `WARNING`: Non-critical issues (empty data, cache misses)
- `ERROR`: Connection errors and failures

## Error Handling

### Connection Errors

- Automatic retry with exponential backoff
- Graceful degradation when Redis is unavailable
- Connection pooling for resilience

### Data Errors

- JSON decode errors are handled gracefully
- Invalid data structures are logged and skipped
- TTL validation prevents negative values

### Recovery Strategies

- Health checks before critical operations
- Fallback to empty results when cache is unavailable
- Automatic reconnection on connection loss

## Production Deployment

### Docker

Build and run with Docker:

```bash
docker build -t cache-layer .
docker run -d --name cache-layer \
  -e REDIS_HOST=redis \
  -e REDIS_PORT=6379 \
  cache-layer
```

### Docker Compose

The service is included in the main docker-compose.yml:

```yaml
services:
  redis:
    image: redis:alpine
    ports:
      - "6379:6379"
    
  cache-layer:
    build: ./services/cache-layer
    depends_on:
      - redis
    environment:
      - REDIS_HOST=redis
```

### Scaling

- Redis clustering for horizontal scaling
- Multiple cache layer instances for load distribution
- Consistent hashing for data partitioning

## Troubleshooting

### Common Issues

1. **Connection Refused**
   - Ensure Redis server is running
   - Check host/port configuration
   - Verify network connectivity

2. **Memory Issues**
   - Monitor Redis memory usage
   - Adjust TTL values
   - Configure Redis eviction policies

3. **Performance Issues**
   - Check connection pool settings
   - Monitor network latency
   - Optimize data structures

### Debug Mode

Enable debug logging:

```python
import logging
logging.getLogger('cache_layer').setLevel(logging.DEBUG)
```

### Redis CLI

Connect to Redis for debugging:

```bash
redis-cli
> KEYS trends:*
> TTL trends:current
> INFO memory
```

## Contributing

1. Follow PEP 8 style guidelines
2. Add type hints to all functions
3. Write comprehensive tests
4. Update documentation for new features
5. Use structured logging for debugging

## License

This project is licensed under the MIT License.