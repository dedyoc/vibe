---
inclusion: fileMatch
fileMatchPattern: '*kafka*|*flink*|*spark*|*docker*|*compose*'
---

# Kafka & Stream Processing Patterns

## Kafka Integration
When working with Kafka for streaming data:

### Producer Patterns
```python
from kafka import KafkaProducer
import json
from typing import Dict, Any

class BlueskyKafkaProducer:
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Wait for all replicas
            retries=3,
            batch_size=16384,
            linger_ms=10  # Small batching for low latency
        )
    
    async def send_post(self, post_data: Dict[str, Any]) -> None:
        """Send Bluesky post to Kafka topic."""
        topic = 'bluesky-posts'
        key = post_data.get('author')  # Partition by author
        await self.producer.send(topic, value=post_data, key=key)
```

### Topic Design
- **bluesky-posts**: Raw post data from firehose
- **bluesky-likes**: Like events
- **bluesky-follows**: Follow events
- **trending-keywords**: Processed trend data
- **alerts**: High-priority trend alerts

### Consumer Patterns
```python
from kafka import KafkaConsumer
import json

def create_consumer(topic: str, group_id: str) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        group_id=group_id,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True
    )
```

## Flink Stream Processing
When using Apache Flink for stream processing:

### Windowing Operations
```python
# PyFlink example for sliding windows
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common import Time

def setup_trend_detection_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Read from Kafka
    posts_stream = env.add_source(kafka_source)
    
    # Extract keywords and apply sliding window
    keyword_counts = (posts_stream
        .map(extract_keywords)
        .key_by(lambda x: x[0])  # Key by keyword
        .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
        .aggregate(CountAggregator())
    )
    
    # Sink back to Kafka
    keyword_counts.add_sink(kafka_sink)
    
    env.execute("Bluesky Trend Detection")
```

### State Management
- Use Flink's managed state for sliding window data
- Implement checkpointing for fault tolerance
- Design stateful functions for trend detection algorithms

## Docker Compose Architecture
Structure your services for local development:

```yaml
# docker-compose.yml structure
services:
  zookeeper:
    # Kafka coordination
  
  kafka:
    # Message broker
    
  flink-jobmanager:
    # Flink cluster manager
    
  flink-taskmanager:
    # Flink workers
    
  bluesky-producer:
    # Your Python service ingesting from firehose
    
  trend-processor:
    # Flink job or Spark streaming job
    
  postgres:
    # State storage for results
    
  redis:
    # Caching layer for real-time queries
```

## Data Serialization
- Use Avro schemas for Kafka message structure
- Define schemas for posts, likes, follows, and trend data
- Implement schema evolution for backward compatibility

## Monitoring & Observability
- Kafka metrics: throughput, lag, partition distribution
- Flink metrics: checkpoint duration, backpressure, watermarks
- Custom metrics: trend detection accuracy, processing latency
- Use Prometheus + Grafana in Docker Compose setup