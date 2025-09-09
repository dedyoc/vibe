# Data Lake Service

This service provides data lake management functionality for the Bluesky streaming pipeline using MinIO S3-compatible object storage.

## Overview

The DataLakeManager class handles:
- Partitioned storage of raw posts and processed trend data
- Automatic lifecycle management and cleanup policies
- Querying historical data with date range filtering
- Storage statistics and monitoring

## Features

### Data Storage
- **Raw Posts**: Stored in partitioned structure by date/hour
- **Processed Trends**: Stored separately with trend analysis data
- **JSON Format**: All data stored as JSON for easy querying
- **Partitioning**: Automatic partitioning by year/month/day/hour

### Lifecycle Management
- **Automatic Cleanup**: Configurable retention policies
- **Storage Classes**: Transition to infrequent access storage
- **Expiration Rules**: Automatic deletion of old data

### Query Capabilities
- **Historical Queries**: Query data by date range
- **Pagination**: Support for large result sets with limits
- **Statistics**: Storage usage and object count metrics

## Usage

### Basic Setup

```python
from data_lake_manager import DataLakeManager
from shared.models import PostData, TrendAlert
from datetime import datetime

# Initialize the data lake manager
data_lake = DataLakeManager(
    endpoint_url="http://localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin123",
    bucket_name="bluesky-data-lake"
)

# Initialize bucket and lifecycle policies
await data_lake.initialize()
```

### Storing Data

```python
# Store raw posts
posts = [
    PostData(
        uri="at://did:plc:user/app.bsky.feed.post/123",
        author_did="did:plc:user",
        text="Sample post content",
        created_at=datetime.utcnow()
    )
]

await data_lake.store_raw_posts(posts)

# Store trend aggregations
trends = [
    TrendAlert(
        keyword="trending_topic",
        frequency=150,
        growth_rate=25.5,
        confidence_score=0.85,
        window_start=datetime.utcnow() - timedelta(minutes=10),
        window_end=datetime.utcnow(),
        sample_posts=["at://did:plc:user/app.bsky.feed.post/123"],
        unique_authors=75,
        rank=1
    )
]

await data_lake.store_trend_aggregations(trends)
```

### Querying Data

```python
from datetime import datetime, timedelta

# Query historical trends
start_date = datetime.utcnow() - timedelta(days=1)
end_date = datetime.utcnow()

trends = await data_lake.query_historical_trends(start_date, end_date)
posts = await data_lake.query_raw_posts(start_date, end_date, limit=1000)

# Get storage statistics
stats = await data_lake.get_storage_stats()
print(f"Total objects: {stats['total_objects']}")
print(f"Total size: {stats['total_size_bytes']} bytes")
```

### Cleanup Operations

```python
# Clean up data older than 90 days
await data_lake.cleanup_old_data(retention_days=90)

# Get partition path for a specific date
partition_path = data_lake.get_partition_path(
    datetime(2024, 1, 15, 14, 30, 0), 
    "raw/posts"
)
# Returns: "raw/posts/year=2024/month=01/day=15/hour=14/"
```

## Storage Structure

The data lake uses a hierarchical partitioning scheme:

```
bucket/
├── raw/
│   └── posts/
│       └── year=2024/
│           └── month=01/
│               └── day=15/
│                   └── hour=14/
│                       ├── posts_1705327800.json
│                       └── posts_1705327860.json
└── processed/
    └── trends/
        └── year=2024/
            └── month=01/
                └── day=15/
                    └── hour=14/
                        ├── trends_1705327800.json
                        └── trends_1705327860.json
```

## Configuration

### Environment Variables

- `MINIO_ENDPOINT`: MinIO endpoint URL (default: http://localhost:9000)
- `MINIO_ACCESS_KEY`: MinIO access key (default: minioadmin)
- `MINIO_SECRET_KEY`: MinIO secret key (default: minioadmin123)
- `DATA_LAKE_BUCKET`: Bucket name (default: bluesky-data-lake)

### Lifecycle Policies

The service automatically configures lifecycle policies:

- **Raw Posts**: Retained for 90 days
- **Processed Trends**: Retained for 365 days
- **Transition to IA**: After 30 days for processed data

## Testing

### Unit Tests

```bash
# Run basic functionality test
python test_basic.py

# Run comprehensive unit tests
python test_unit.py

# Run pytest (if available)
pytest test_data_lake_manager.py -v
```

### Integration Tests

```bash
# Start MinIO first
docker run -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin123" \
  minio/minio server /data --console-address ":9001"

# Run integration tests
python example_usage.py
```

## Dependencies

- `boto3>=1.34.0`: AWS SDK for S3 operations
- `botocore>=1.34.0`: Core AWS functionality
- `asyncio`: Asynchronous I/O support

## Error Handling

The service includes comprehensive error handling:

- **Connection Errors**: Automatic retry with exponential backoff
- **Missing Objects**: Graceful handling of non-existent data
- **Malformed Data**: Skip invalid records and continue processing
- **Storage Errors**: Detailed logging and error propagation

## Performance Considerations

- **Thread Pool**: Configurable worker threads for async operations
- **Batch Operations**: Efficient bulk operations for large datasets
- **Connection Pooling**: Reuse connections for better performance
- **Memory Management**: Streaming operations for large files

## Monitoring

The service provides metrics for:

- Storage usage and object counts
- Operation latencies and error rates
- Data freshness and completeness
- Lifecycle policy effectiveness

## Docker Support

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
ENV PYTHONPATH=/app

CMD ["python", "example_usage.py"]
```

## Contributing

1. Follow Python coding standards (PEP 8)
2. Include type hints for all functions
3. Add comprehensive tests for new features
4. Update documentation for API changes
5. Use descriptive commit messages

## License

This service is part of the Bluesky Streaming Pipeline project.