---
inclusion: fileMatch
fileMatchPattern: '*stream*|*pipeline*|*processor*|*consumer*|*producer*'
---

# Streaming Data Pipeline Patterns

## Core Streaming Concepts
When working with streaming data processing:

### Sliding Window Implementation
- **Flink Windows**: Use Flink's built-in windowing for scalable operations
- **Window Types**: Tumbling, sliding, and session windows based on use case
- **State Management**: Leverage Flink's managed state for window data
- **Memory Efficiency**: Let Flink handle memory management and spilling to disk

Example Flink windowing pattern:
```python
from pyflink.datastream.window import TumblingProcessingTimeWindows, SlidingProcessingTimeWindows
from pyflink.common import Time

# Tumbling window for periodic trend reports
posts_stream.window(TumblingProcessingTimeWindows.of(Time.minutes(5)))

# Sliding window for real-time trend detection
posts_stream.window(SlidingProcessingTimeWindows.of(Time.minutes(10), Time.minutes(1)))
```

For simple Python components, still use deque for local buffering:
```python
from collections import deque
from datetime import datetime, timedelta

class LocalBuffer:
    def __init__(self, max_size: int = 1000):
        self.buffer: deque = deque(maxlen=max_size)
```

### Producer-Consumer Patterns
- **Kafka Producers**: Send Bluesky data to Kafka topics with proper partitioning
- **Stream Processors**: Use Flink/Spark for windowed operations and aggregations
- **Backpressure**: Handle via Kafka consumer groups and Flink's built-in mechanisms
- **Fault Tolerance**: Leverage Kafka's replication and Flink's checkpointing
- **Batch Processing**: Configure appropriate batch sizes for throughput vs latency

### Data Processing Best Practices
- Validate incoming data before processing
- Implement circuit breakers for external dependencies
- Use structured logging with correlation IDs
- Design for idempotent operations where possible

## Trend Detection Specifics
- Implement statistical measures (moving averages, standard deviation)
- Use appropriate data structures for time-series analysis
- Consider different trend detection algorithms (momentum, regression)
- Handle missing or delayed data points gracefully
- For social media: track hashtag frequency, mention patterns, viral content spread
- Implement keyword extraction and clustering for emerging topic detection