# Project Context - Streaming Data Pipeline

## Project Overview
This is a streaming data pipeline focused on trend detection using sliding time windows for:
- **Primary Source**: Bluesky firehose (ATProto) for social media posts
- **Future Sources**: Stock ticker data, political keyword monitoring
- Real-time pattern recognition and trend analysis

## Technology Stack
- **Primary Language**: Python
- **Data Source**: Bluesky firehose via `atproto` library
- **Streaming Platform**: Apache Kafka for message queuing
- **Stream Processing**: Apache Flink or Spark Streaming
- **Containerization**: Docker Compose for local development
- **Tools**: ruff (linting), uv (package management)
- **Architecture**: Event-driven streaming with proper message queues and stream processors

## Development Workflow
- **Branching**: feat/* for features, dev for development, main for production
- **Code Quality**: Use ruff for linting and formatting
- **Package Management**: Use uv for dependency management

## Key Architectural Patterns
When implementing streaming data solutions:
- **Kafka Producers**: Bluesky firehose data ingestion to Kafka topics
- **Stream Processing**: Use Flink/Spark for windowed aggregations and trend detection
- **Microservices**: Containerized components with Docker Compose
- **Event Sourcing**: Store raw events for replay and reprocessing
- **CQRS**: Separate read/write models for real-time vs analytical queries
- Design for horizontal scalability and fault tolerance

## Performance Considerations
- Optimize for low-latency data processing with Kafka partitioning
- Use Flink/Spark for efficient windowed operations at scale
- Implement efficient serialization (Avro/Protobuf) for Kafka messages
- Design for backpressure handling across the pipeline
- Use Docker resource limits and monitoring for local development

## Deployment Strategy
- **Local Development**: Docker Compose with Kafka, Flink, and supporting services
- **Future Cloud**: AWS deployment (EKS, MSK, Kinesis Analytics)
- **Infrastructure as Code**: Prepare for cloud migration with proper abstractions