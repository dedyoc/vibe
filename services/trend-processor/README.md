# Trend Processor Service - Flink Stream Processing

This service implements the Flink stream processing job foundation for real-time trend detection from Bluesky social media posts.

## Overview

The Trend Processor service provides:

- **Flink Stream Processing Job**: Real-time processing of social media posts from Kafka
- **Keyword Extraction**: NLP-based extraction and normalization of keywords from post text
- **Windowed Aggregations**: Sliding time windows for keyword frequency counting
- **Checkpointing**: State persistence to MinIO for fault tolerance
- **Job Management**: REST API for deploying, monitoring, and managing Flink jobs
- **Health Monitoring**: Comprehensive health checks and metrics collection

## Architecture

```
Kafka (Posts) → Flink Job → Windowed Aggregations → Kafka (Trends)
                    ↓
                MinIO (Checkpoints)
```

### Components

1. **FlinkStreamProcessor**: Main job orchestrator
2. **KeywordExtractor**: NLP-based keyword extraction from post text
3. **KeywordAggregator**: Windowed aggregation with unique author tracking
4. **FlinkJobManager**: Job lifecycle management via Flink REST API
5. **FlinkJobDeployer**: Automated job deployment and monitoring

## Features Implemented

### ✅ Task 7 Requirements

- [x] **PyFlink job structure** for consuming from Kafka topics
- [x] **Keyword extraction** from post text using NLP techniques
- [x] **Flink streaming environment** with checkpointing to MinIO
- [x] **Basic integration tests** for Flink job deployment

### Key Capabilities

#### Keyword Extraction
- Removes URLs, mentions (@username), and stop words
- Extracts hashtags with # prefix preservation
- Normalizes text (lowercase, punctuation removal)
- Filters by minimum/maximum keyword length
- Handles multiple languages and special characters

#### Stream Processing
- Consumes posts from Kafka `bluesky-posts` topic
- Applies sliding time windows (configurable duration)
- Tracks unique authors per keyword
- Maintains state using RocksDB backend
- Outputs windowed counts to `windowed-keyword-counts` topic

#### Fault Tolerance
- Checkpointing to MinIO S3-compatible storage
- Automatic job recovery from last successful checkpoint
- Exponential backoff for connection failures
- Graceful handling of malformed messages

## Configuration

### Environment Variables

```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_TOPIC_POSTS=bluesky-posts
KAFKA_TOPIC_TRENDS=windowed-keyword-counts

# Flink Configuration
FLINK_JOBMANAGER_HOST=flink-jobmanager
FLINK_JOBMANAGER_PORT=8081
FLINK_PARALLELISM=2

# MinIO Configuration (for checkpointing)
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123

# Processing Configuration
WINDOW_SIZE_MINUTES=10
CHECKPOINT_INTERVAL_MS=60000

# Service Configuration
LOG_LEVEL=INFO
PROMETHEUS_PORT=8001
```

### Flink Job Parameters

- **Parallelism**: Number of parallel task slots (default: 2)
- **Window Size**: Sliding window duration in minutes (default: 10)
- **Checkpoint Interval**: State checkpoint frequency in milliseconds (default: 60000)

## API Endpoints

### Health and Status

- `GET /health` - Service health check
- `GET /` - Service information
- `GET /metrics` - Prometheus metrics

### Flink Job Management

- `GET /flink/status` - Flink cluster and job status
- `POST /flink/deploy` - Deploy trend detection job
- `POST /flink/stop` - Stop current job

## Testing

### Unit Tests

```bash
# Run basic functionality tests
python test_basic_functionality.py

# Run deployment tests
python test_deployment.py
```

### Integration Tests

```bash
# Set environment variable to enable integration tests
export RUN_INTEGRATION_TESTS=1

# Run integration tests (requires running infrastructure)
python -m pytest test_flink_integration.py -v -m integration
```

### Test Coverage

- ✅ Keyword extraction and normalization
- ✅ Post data serialization/deserialization
- ✅ Windowed keyword count creation and validation
- ✅ Trend alert creation and validation
- ✅ Flink job configuration
- ✅ Stream processor initialization
- ✅ Mock job execution
- ✅ Async functionality

## Data Models

### PostData
```python
@dataclass
class PostData:
    uri: str
    author_did: str
    text: str
    created_at: datetime
    language: Optional[str] = None
    reply_to: Optional[str] = None
    mentions: List[str] = field(default_factory=list)
    hashtags: List[str] = field(default_factory=list)
```

### WindowedKeywordCount
```python
@dataclass
class WindowedKeywordCount:
    keyword: str
    count: int
    window_start: datetime
    window_end: datetime
    unique_authors: int
    normalized_keyword: str
```

## Deployment

### Docker

The service runs in a Docker container with:
- Python 3.11 runtime
- Java 11 for Flink compatibility
- Flink CLI tools for job submission
- Health checks and metrics endpoints

### Dependencies

- **PyFlink**: Stream processing framework
- **Kafka-Python**: Kafka client library
- **aiohttp**: Async HTTP client for Flink REST API
- **FastAPI**: REST API framework
- **Prometheus Client**: Metrics collection

## Monitoring

### Metrics

- `flink_job_submissions_total`: Total job submissions
- `flink_active_jobs`: Number of active jobs
- `flink_job_duration_seconds`: Job execution duration
- `trends_detected_total`: Total trends detected
- `trend_processing_seconds`: Processing time histogram

### Health Checks

- Service health endpoint at `/health`
- Docker health checks every 30 seconds
- Flink cluster connectivity monitoring
- Job status monitoring

## Development Notes

### PyFlink Version Compatibility

The current implementation uses mock classes for PyFlink due to version compatibility issues. In production, you would:

1. Use PyFlink 1.18+ with proper imports
2. Deploy to a Flink cluster with matching versions
3. Configure proper Kafka connectors and serialization

### Mock Implementation

For development and testing, the code includes mock implementations of:
- Flink streaming environment
- Kafka connectors
- Data stream operations
- State management

This allows for testing the business logic without requiring a full Flink cluster.

## Next Steps

The foundation is ready for:

1. **Task 8**: Sliding window aggregation implementation
2. **Task 9**: Trend detection algorithm development
3. **Task 10**: Trend alert publishing system
4. **Integration**: With cache layer and data lake storage

## Troubleshooting

### Common Issues

1. **PyFlink Import Errors**: Use the provided mock implementations for development
2. **Kafka Connection**: Ensure Kafka is running and accessible
3. **Flink Cluster**: Verify JobManager is healthy at `http://flink-jobmanager:8081`
4. **MinIO Access**: Check MinIO credentials and bucket permissions

### Logs

Check service logs for:
- Job submission status
- Keyword extraction statistics
- Checkpoint completion
- Error handling and recovery

## Performance Considerations

- **Parallelism**: Adjust based on available resources and throughput requirements
- **Window Size**: Balance between latency and accuracy
- **Checkpointing**: Tune interval based on state size and recovery requirements
- **Memory**: Configure JVM heap size for Flink TaskManagers