# Bluesky Streaming Pipeline

A real-time streaming data pipeline that ingests social media posts from Bluesky's firehose, processes them through a scalable streaming architecture, and detects trending topics using sliding time windows.

## Project Structure

```
├── services/           # Microservices for data processing
├── shared/            # Shared data models and utilities
├── config/            # Configuration files for services
├── tests/             # Unit and integration tests
├── pyproject.toml     # Python project configuration
├── ruff.toml          # Code linting and formatting configuration
└── README.md          # This file
```

## Technology Stack

- **Python 3.9+** - Primary programming language
- **ATProto** - Bluesky firehose connection
- **Apache Kafka** - Message streaming platform
- **Apache Flink** - Stream processing engine
- **Apache Superset** - Business intelligence and data visualization
- **Redis** - Caching layer for real-time data
- **MinIO** - S3-compatible object storage for data lake
- **Prometheus & Grafana** - Monitoring and observability
- **Docker Compose** - Local development environment
- **Ruff** - Code linting and formatting

## Development Setup

1. Install dependencies:
   ```bash
   pip install -e ".[dev]"
   ```

2. Run code formatting:
   ```bash
   ruff format .
   ```

3. Run linting:
   ```bash
   ruff check .
   ```

4. Run tests:
   ```bash
   pytest
   ```

## Data Models

The pipeline uses three core data models defined in `shared/models.py`:

- **PostData**: Represents social media posts from Bluesky
- **TrendAlert**: Contains detected trending keywords with statistics
- **WindowedKeywordCount**: Tracks keyword frequencies in time windows

## Architecture

The system follows an event-driven architecture with:

1. **Data Ingestion**: Bluesky firehose → Kafka topics
2. **Stream Processing**: Flink jobs for windowed aggregations
3. **Trend Detection**: Statistical analysis for trending keywords
4. **Storage**: Data lake (MinIO) and caching layers (Redis)
5. **Business Intelligence**: Apache Superset for dashboards and reporting
6. **Monitoring**: Comprehensive observability stack (Prometheus/Grafana)

## Services

### Core Processing Services
- **bluesky-producer**: Connects to Bluesky firehose and publishes to Kafka
- **trend-processor**: Flink-based stream processing for trend detection
- **cache-layer**: Redis caching service for real-time data
- **data-lake**: MinIO-based object storage for historical data

### Business Intelligence
- **superset**: Apache Superset for data visualization and dashboards
- **superset-integration**: Service managing Superset data sources and sync

### Infrastructure Services
- **kafka**: Message streaming platform (KRaft mode)
- **flink-jobmanager/taskmanager**: Stream processing engine
- **redis**: In-memory caching and real-time data store
- **minio**: S3-compatible object storage
- **prometheus**: Metrics collection and monitoring
- **grafana**: Visualization and alerting for system metrics

## Web Interfaces

When running the full stack:

- **Superset Dashboards**: http://localhost:8088 (admin/admin123)
- **Grafana Monitoring**: http://localhost:3000 (admin/admin123)
- **Flink Web UI**: http://localhost:8081
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin123)
- **Prometheus**: http://localhost:9090

## Getting Started

See the implementation tasks in `.kiro/specs/bluesky-streaming-pipeline/tasks.md` for detailed development steps.