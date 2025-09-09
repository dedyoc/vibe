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
4. **Storage**: Data lake and caching layers
5. **Monitoring**: Comprehensive observability stack

## Getting Started

See the implementation tasks in `.kiro/specs/bluesky-streaming-pipeline/tasks.md` for detailed development steps.