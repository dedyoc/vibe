# Docker Infrastructure Setup

This document describes the Docker Compose infrastructure for the Bluesky Streaming Pipeline.

## Architecture Overview

The infrastructure consists of the following services:

### Core Infrastructure
- **Kafka** (KRaft mode): Message streaming platform
- **Flink**: Stream processing engine (JobManager + TaskManager)
- **MinIO**: S3-compatible object storage for data lake
- **Redis**: Caching layer for real-time data

### Monitoring Stack
- **Prometheus**: Metrics collection
- **Grafana**: Visualization and dashboards
- **AlertManager**: (Future) Alert management

### Application Services
- **Bluesky Producer**: Ingests data from Bluesky firehose
- **Trend Processor**: Processes streams and detects trends

## Quick Start

1. **Start all services:**
   ```bash
   make up
   ```

2. **Check service health:**
   ```bash
   make health
   ```

3. **View logs:**
   ```bash
   make logs
   ```

4. **Access web interfaces:**
   ```bash
   make urls
   ```

## Service Details

### Kafka (Port 9092)
- **Mode**: KRaft (no Zookeeper required)
- **Topics**: Auto-created as needed
- **JMX Metrics**: Port 9101
- **Health Check**: Broker API versions check

### Flink (Port 8081)
- **JobManager**: Cluster coordination and web UI
- **TaskManager**: Job execution with 4 task slots
- **State Backend**: RocksDB with S3 checkpointing
- **Checkpoints**: Stored in MinIO

### MinIO (Ports 9000/9001)
- **API**: Port 9000 (S3-compatible)
- **Console**: Port 9001 (web interface)
- **Credentials**: minioadmin/minioadmin123
- **Buckets**: Auto-created (data-lake, flink-checkpoints, flink-savepoints)

### Redis (Port 6379)
- **Mode**: Standalone with persistence
- **Memory**: 512MB limit with LRU eviction
- **Persistence**: AOF enabled

### Prometheus (Port 9090)
- **Scrape Interval**: 15s
- **Retention**: 200h
- **Targets**: All services with metrics endpoints

### Grafana (Port 3000)
- **Credentials**: admin/admin123
- **Datasources**: Prometheus, Redis
- **Dashboards**: System overview pre-configured

## Development Tools

Enable development tools with:
```bash
make dev-tools
```

This starts:
- **Kafka UI** (Port 8080): Kafka cluster management
- **Redis Commander** (Port 8081): Redis data browser

## Configuration

### Environment Variables

Application services use these environment variables:

**Bluesky Producer:**
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka connection
- `REDIS_HOST/REDIS_PORT`: Redis connection
- `LOG_LEVEL`: Logging level
- `PROMETHEUS_PORT`: Metrics endpoint port

**Trend Processor:**
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka connection
- `REDIS_HOST/REDIS_PORT`: Redis connection
- `MINIO_ENDPOINT/ACCESS_KEY/SECRET_KEY`: MinIO connection
- `FLINK_JOBMANAGER_HOST/PORT`: Flink connection

### Volume Mounts

Persistent data is stored in Docker volumes:
- `kafka_data`: Kafka logs and metadata
- `minio_data`: Object storage data
- `redis_data`: Redis persistence
- `prometheus_data`: Metrics storage
- `grafana_data`: Dashboards and configuration
- `flink_*_data`: Flink logs

### Networking

All services communicate via the `streaming-network` bridge network (172.20.0.0/16).

## Health Checks

All services include health checks:
- **Interval**: 30s
- **Timeout**: 10s
- **Retries**: 5
- **Start Period**: 30-60s depending on service

## Resource Requirements

Minimum system requirements:
- **CPU**: 4 cores
- **Memory**: 8GB RAM
- **Storage**: 10GB free space
- **Network**: Docker networking enabled

## Troubleshooting

### Common Issues

1. **Services won't start:**
   ```bash
   make clean  # Remove all data and restart
   make up
   ```

2. **Port conflicts:**
   - Check if ports 3000, 6379, 8081, 9000, 9001, 9090, 9092 are available
   - Modify port mappings in docker-compose.yml if needed

3. **Memory issues:**
   - Increase Docker memory limit to at least 8GB
   - Monitor with `docker stats`

4. **Network connectivity:**
   ```bash
   docker network ls
   docker network inspect streaming-pipeline_streaming-network
   ```

### Logs and Debugging

View specific service logs:
```bash
make kafka-logs      # Kafka logs
make flink-logs      # Flink logs
make app-logs        # Application logs
make monitoring-logs # Prometheus/Grafana logs
```

### Data Reset

To completely reset all data:
```bash
make clean  # WARNING: Deletes all volumes and data
```

## Security Notes

**Development Setup Only:**
- Default credentials are used (not production-ready)
- No TLS/SSL encryption
- No authentication on most services
- All ports exposed to localhost

For production deployment, implement:
- Strong authentication and authorization
- TLS encryption for all communications
- Network segmentation and firewalls
- Secrets management
- Resource limits and monitoring

## Next Steps

After infrastructure is running:
1. Implement Bluesky firehose client (Task 3)
2. Build Kafka producer (Task 4)
3. Set up data lake storage (Task 5)
4. Implement Redis caching (Task 6)
5. Create Flink stream processing jobs (Task 7+)