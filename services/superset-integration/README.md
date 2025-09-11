# Superset Integration Service

This service manages the integration between the Bluesky Streaming Pipeline and Apache Superset for business intelligence and data visualization.

## Overview

The Superset Integration Service provides:

- **Data Source Configuration**: Automatically configures Redis and MinIO connections in Superset
- **Dashboard Management**: Creates and manages real-time and historical trend analysis dashboards
- **Data Synchronization**: Continuously syncs trend data from Redis and historical data from MinIO to Superset-accessible formats
- **Automated Reporting**: Sets up scheduled reports for trend analysis
- **User Access Controls**: Manages dashboard sharing and user permissions

## Features

### Real-time Dashboards

1. **Current Trending Topics Dashboard**
   - Live view of trending keywords from Bluesky firehose
   - Keyword frequency heatmaps
   - Growth rate visualizations
   - Confidence score metrics
   - Auto-refresh every 30 seconds

2. **Historical Trend Analysis Dashboard**
   - Daily trend patterns over time
   - Keyword evolution timelines
   - Trend frequency distributions
   - Growth pattern analysis
   - Weekly and monthly aggregations

### Data Sources

- **Redis Cache**: Real-time trending data with TTL-based management
- **MinIO Data Lake**: Historical trend data stored in partitioned S3-compatible storage
- **Automated Sync**: Continuous data synchronization every 5 minutes

### Automated Reports

- **Daily Trend Summary**: PDF reports sent daily at 9 AM
- **Weekly Trend Analysis**: Comprehensive weekly reports every Monday
- **Custom Schedules**: Configurable cron-based scheduling
- **Multiple Formats**: PDF, PNG, and CSV export options

## Configuration

### Environment Variables

```bash
# Superset Configuration
SUPERSET_URL=http://superset:8088
SUPERSET_SECRET_KEY=your-secret-key-here

# Data Sources
REDIS_HOST=redis
REDIS_PORT=6379
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123

# Service Configuration
LOG_LEVEL=INFO
PROMETHEUS_PORT=8002
```

### Dashboard Configuration

Dashboards are configured through JSON files in `config/superset/dashboards/`:

- `realtime_trends.json`: Real-time trending topics dashboard
- `historical_analysis.json`: Historical trend analysis dashboard

### Data Source Setup

The service automatically configures:

1. **Redis Database Connection**
   - Connection to Redis cache for real-time data
   - Optimized for fast queries and low latency
   - TTL-based cache management

2. **MinIO S3 Database Connection**
   - S3-compatible object storage for historical data
   - Partitioned by date/hour for efficient querying
   - Lifecycle policies for data archival

## API Endpoints

### Health Check
```
GET /health
```
Returns service health status and dependency checks.

### Metrics
```
GET /metrics
```
Prometheus-compatible metrics for monitoring.

### Status
```
GET /status
```
Detailed service status including data source information.

## Data Formats

### Trending Keywords (Redis)
```json
{
  "keyword": "python",
  "frequency": 150,
  "growth_rate": 2.5,
  "confidence_score": 0.85,
  "window_start": "2024-01-01T10:00:00Z",
  "window_end": "2024-01-01T11:00:00Z",
  "unique_authors": 75,
  "rank": 1,
  "sample_posts": ["Post 1", "Post 2"],
  "last_updated": "2024-01-01T10:30:00Z"
}
```

### Historical Trends (MinIO)
```json
{
  "trend_date": "2024-01-01",
  "keyword": "python",
  "avg_frequency": 125.5,
  "max_growth_rate": 3.2,
  "trend_occurrences": 24,
  "confidence_score": 0.82,
  "unique_authors": 1200
}
```

## Usage

### Starting the Service

```bash
# Using Docker Compose
docker-compose up superset superset-integration

# Direct Python execution
cd services/superset-integration
python superset_manager.py
```

### Accessing Dashboards

1. **Superset Web Interface**: http://localhost:8088
   - Username: `admin`
   - Password: `admin123`

2. **Available Dashboards**:
   - Real-time Trending Topics: `/dashboard/realtime-trending-topics/`
   - Historical Trend Analysis: `/dashboard/historical-trend-analysis/`

### Creating Custom Dashboards

1. Use the Superset web interface to create new dashboards
2. Connect to the configured Redis and MinIO data sources
3. Use the synced data formats for consistent visualization

## Monitoring

### Metrics

The service exposes Prometheus metrics:

- `superset_integration_trending_keywords_total`: Current trending keywords count
- `superset_integration_historical_records_total`: Historical trend records count
- `superset_integration_last_sync_timestamp`: Last successful data sync timestamp

### Logging

Structured logging with correlation IDs for tracing:

```python
logger.info("Data sync completed", extra={
    "trending_count": 25,
    "historical_count": 1500,
    "sync_duration": 2.3
})
```

## Development

### Running Tests

```bash
# Install test dependencies
pip install -r requirements.txt

# Run tests
pytest test_superset_integration.py -v

# Run with coverage
pytest --cov=superset_manager test_superset_integration.py
```

### Adding New Dashboards

1. Create dashboard configuration in `config/superset/dashboards/`
2. Update `SupersetManager.setup_trend_dashboards()` method
3. Add corresponding data sync logic if needed
4. Test with sample data

### Extending Data Sources

1. Add new data source configuration in `SupersetManager`
2. Implement data sync methods for the new source
3. Update dashboard configurations to use new data
4. Add appropriate tests

## Troubleshooting

### Common Issues

1. **Superset Login Failures**
   - Check Superset service health
   - Verify credentials in configuration
   - Check network connectivity

2. **Data Sync Issues**
   - Verify Redis and MinIO connectivity
   - Check data format compatibility
   - Review sync interval configuration

3. **Dashboard Loading Problems**
   - Verify data source connections in Superset
   - Check data availability in Redis/MinIO
   - Review dashboard configuration JSON

### Debug Mode

Enable debug logging:

```bash
export LOG_LEVEL=DEBUG
python superset_manager.py
```

### Health Checks

Monitor service health:

```bash
# Check service health
curl http://localhost:8002/health

# Check detailed status
curl http://localhost:8002/status

# Check metrics
curl http://localhost:8002/metrics
```

## Security Considerations

- Change default Superset admin credentials in production
- Use secure secret keys for Superset configuration
- Implement proper network security for data source connections
- Regular security updates for Superset and dependencies
- Access control and user management for dashboard sharing

## Performance Optimization

- Configure appropriate cache TTL values for data freshness vs performance
- Optimize data sync intervals based on data volume and latency requirements
- Use connection pooling for database connections
- Monitor and tune Superset query performance
- Implement data partitioning strategies for large datasets