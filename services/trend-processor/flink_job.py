#!/usr/bin/env python3
"""
Flink Stream Processing Job for Trend Detection

This module implements the core Flink streaming job that consumes posts from Kafka,
extracts keywords using NLP techniques, and performs windowed aggregations for trend detection.
"""

import json
import logging
import re
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

# Note: Using mock implementations for PyFlink since version 1.0 has limited functionality
# In a production environment, you would use the actual PyFlink 1.18+ imports:
# from pyflink.common import Configuration, Types
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream import StreamExecutionEnvironment, DataStream
# from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
# # Mock base classes for Flink functions
class MapFunction:
    def map(self, value):
        raise NotImplementedError

class FlatMapFunction:
    def flat_map(self, value):
        raise NotImplementedError

class KeyedProcessFunction:
    def open(self, runtime_context):
        pass
    
    def process_element(self, value, ctx, out):
        raise NotImplementedError
# from pyflink.datastream.state import ValueStateDescriptor
# from pyflink.datastream.window import TumblingProcessingTimeWindows, SlidingProcessingTimeWindows
# from pyflink.common.time import Time
# from pyflink.common.typeinfo import Types as FlinkTypes

# Mock implementations for development/testing with PyFlink 1.0
class MockConfiguration:
    def __init__(self):
        self.params = {}
    
    def set_string(self, key: str, value: str):
        self.params[key] = value
    
    def set_global_job_parameters(self, config):
        pass

class MockTypes:
    @staticmethod
    def STRING():
        return "STRING"
    
    @staticmethod
    def INT():
        return "INT"
    
    @staticmethod
    def PICKLED_BYTE_ARRAY():
        return "PICKLED_BYTE_ARRAY"
    
    @staticmethod
    def TUPLE(types):
        return f"TUPLE({types})"

class MockSimpleStringSchema:
    pass

class MockStreamExecutionEnvironment:
    def __init__(self):
        self.parallelism = 1
        self.config = MockConfiguration()
    
    @staticmethod
    def get_execution_environment():
        return MockStreamExecutionEnvironment()
    
    def set_parallelism(self, parallelism: int):
        self.parallelism = parallelism
    
    def get_parallelism(self):
        return self.parallelism
    
    def enable_checkpointing(self, interval_ms: int):
        pass
    
    def get_config(self):
        return self.config
    
    def add_source(self, source):
        return MockDataStream()
    
    def execute(self, job_name: str):
        logger.info(f"Mock execution of job: {job_name}")

class MockDataStream:
    def map(self, func, output_type=None):
        return MockDataStream()
    
    def filter(self, func):
        return MockDataStream()
    
    def flat_map(self, func, output_type=None):
        return MockDataStream()
    
    def key_by(self, func):
        return MockKeyedStream()
    
    def add_sink(self, sink):
        pass

class MockKeyedStream:
    def process(self, func, output_type=None):
        return MockDataStream()

class MockFlinkKafkaConsumer:
    def __init__(self, topics, deserialization_schema, properties):
        self.topics = topics
        self.properties = properties
    
    def set_start_from_latest(self):
        pass

class MockFlinkKafkaProducer:
    def __init__(self, topic, serialization_schema, producer_config):
        self.topic = topic
        self.config = producer_config

class MockValueStateDescriptor:
    def __init__(self, name, type_info):
        self.name = name
        self.type_info = type_info

# Use mock implementations
Configuration = MockConfiguration
Types = MockTypes
SimpleStringSchema = MockSimpleStringSchema
StreamExecutionEnvironment = MockStreamExecutionEnvironment
DataStream = MockDataStream
FlinkKafkaConsumer = MockFlinkKafkaConsumer
FlinkKafkaProducer = MockFlinkKafkaProducer
ValueStateDescriptor = MockValueStateDescriptor
FlinkTypes = MockTypes

from shared.models import PostData, WindowedKeywordCount, TrendAlert

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PostDataDeserializer(MapFunction):
    """Deserializes JSON messages from Kafka into PostData objects."""
    
    def map(self, value: str) -> Optional[PostData]:
        """
        Convert JSON string to PostData object.
        
        Args:
            value: JSON string from Kafka message
            
        Returns:
            PostData object or None if deserialization fails
        """
        try:
            data = json.loads(value)
            return PostData(
                uri=data['uri'],
                author_did=data['author_did'],
                text=data['text'],
                created_at=datetime.fromisoformat(data['created_at']),
                language=data.get('language'),
                reply_to=data.get('reply_to'),
                mentions=data.get('mentions', []),
                hashtags=data.get('hashtags', [])
            )
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.warning(f"Failed to deserialize post data: {e}")
            return None


class KeywordExtractor(FlatMapFunction):
    """Extracts and normalizes keywords from post text using NLP techniques."""
    
    # Common English stop words to filter out
    STOP_WORDS = {
        'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from',
        'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on', 'that', 'the',
        'to', 'was', 'will', 'with', 'the', 'this', 'but', 'they', 'have',
        'had', 'what', 'said', 'each', 'which', 'she', 'do', 'how', 'their',
        'if', 'up', 'out', 'many', 'then', 'them', 'these', 'so', 'some',
        'her', 'would', 'make', 'like', 'into', 'him', 'time', 'two', 'more',
        'go', 'no', 'way', 'could', 'my', 'than', 'first', 'been', 'call',
        'who', 'oil', 'sit', 'now', 'find', 'down', 'day', 'did', 'get',
        'come', 'made', 'may', 'part'
    }
    
    # Minimum keyword length
    MIN_KEYWORD_LENGTH = 3
    
    # Maximum keyword length
    MAX_KEYWORD_LENGTH = 50
    
    def flat_map(self, post: PostData) -> List[Tuple[str, PostData]]:
        """
        Extract keywords from post text and emit (keyword, post) pairs.
        
        Args:
            post: PostData object containing the post text
            
        Returns:
            List of (keyword, post) tuples for each extracted keyword
        """
        if not post or not post.text:
            return []
        
        keywords = self._extract_keywords(post.text)
        return [(keyword, post) for keyword in keywords]
    
    def _extract_keywords(self, text: str) -> List[str]:
        """
        Extract and normalize keywords from text.
        
        Args:
            text: Raw post text
            
        Returns:
            List of normalized keywords
        """
        # Convert to lowercase
        text = text.lower()
        
        # Remove URLs
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        
        # Remove mentions (@username)
        text = re.sub(r'@\w+', '', text)
        
        # Extract hashtags separately (they're important for trends)
        hashtags = re.findall(r'#(\w+)', text)
        
        # Remove hashtags from text to avoid duplication
        text = re.sub(r'#\w+', '', text)
        
        # Extract words using regex (alphanumeric + some special chars)
        words = re.findall(r'\b[a-zA-Z][a-zA-Z0-9_-]*\b', text)
        
        # Filter and normalize words
        keywords = []
        for word in words:
            normalized = word.lower().strip()
            if (self.MIN_KEYWORD_LENGTH <= len(normalized) <= self.MAX_KEYWORD_LENGTH and
                normalized not in self.STOP_WORDS and
                not normalized.isdigit()):
                keywords.append(normalized)
        
        # Add hashtags (they're already normalized)
        for hashtag in hashtags:
            if (self.MIN_KEYWORD_LENGTH <= len(hashtag) <= self.MAX_KEYWORD_LENGTH and
                hashtag.lower() not in self.STOP_WORDS):
                keywords.append(f"#{hashtag.lower()}")
        
        return list(set(keywords))  # Remove duplicates


class KeywordAggregator(KeyedProcessFunction):
    """Aggregates keyword counts within time windows and tracks unique authors."""
    
    def __init__(self, window_size_minutes: int = 10):
        """
        Initialize the keyword aggregator.
        
        Args:
            window_size_minutes: Size of the sliding window in minutes
        """
        self.window_size_minutes = window_size_minutes
        self.count_state = None
        self.authors_state = None
    
    def open(self, runtime_context):
        """Initialize state descriptors."""
        self.count_state = runtime_context.get_state(
            ValueStateDescriptor("keyword_count", FlinkTypes.INT())
        )
        self.authors_state = runtime_context.get_state(
            ValueStateDescriptor("unique_authors", FlinkTypes.PICKLED_BYTE_ARRAY())
        )
    
    def process_element(self, value: Tuple[str, PostData], ctx, out):
        """
        Process each keyword-post pair and maintain aggregated counts.
        
        Args:
            value: Tuple of (keyword, post)
            ctx: Processing context
            out: Output collector
        """
        keyword, post = value
        
        # Get current count
        current_count = self.count_state.value()
        if current_count is None:
            current_count = 0
        
        # Get current authors set
        authors_set = self.authors_state.value()
        if authors_set is None:
            authors_set = set()
        
        # Update count and authors
        current_count += 1
        authors_set.add(post.author_did)
        
        # Update state
        self.count_state.update(current_count)
        self.authors_state.update(authors_set)
        
        # Create windowed keyword count
        window_start = datetime.fromtimestamp(ctx.timestamp() / 1000)
        window_end = datetime.fromtimestamp(
            (ctx.timestamp() + self.window_size_minutes * 60 * 1000) / 1000
        )
        
        windowed_count = WindowedKeywordCount(
            keyword=keyword,
            count=current_count,
            window_start=window_start,
            window_end=window_end,
            unique_authors=len(authors_set),
            normalized_keyword=keyword.lower()
        )
        
        # Emit the windowed count
        out.collect(windowed_count)


class WindowedCountSerializer(MapFunction):
    """Serializes WindowedKeywordCount objects to JSON for output."""
    
    def map(self, windowed_count: WindowedKeywordCount) -> str:
        """
        Convert WindowedKeywordCount to JSON string.
        
        Args:
            windowed_count: WindowedKeywordCount object
            
        Returns:
            JSON string representation
        """
        return json.dumps({
            'keyword': windowed_count.keyword,
            'count': windowed_count.count,
            'window_start': windowed_count.window_start.isoformat(),
            'window_end': windowed_count.window_end.isoformat(),
            'unique_authors': windowed_count.unique_authors,
            'normalized_keyword': windowed_count.normalized_keyword
        })


class FlinkStreamProcessor:
    """Main Flink stream processing job for trend detection."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Flink stream processor.
        
        Args:
            config: Configuration dictionary containing Kafka, MinIO, and other settings
        """
        self.config = config
        self.env = None
        
    def setup_environment(self) -> StreamExecutionEnvironment:
        """
        Set up the Flink streaming environment with checkpointing and state backend.
        
        Returns:
            Configured StreamExecutionEnvironment
        """
        # Create streaming environment
        self.env = StreamExecutionEnvironment.get_execution_environment()
        
        # Set parallelism
        self.env.set_parallelism(self.config.get('parallelism', 2))
        
        # Configure checkpointing
        self.env.enable_checkpointing(
            self.config.get('checkpoint_interval_ms', 60000)  # 1 minute
        )
        
        # Configure state backend and checkpoints to MinIO
        configuration = Configuration()
        
        # RocksDB state backend for large state
        configuration.set_string("state.backend", "rocksdb")
        
        # MinIO S3 configuration for checkpoints
        minio_endpoint = self.config.get('minio_endpoint', 'http://minio:9000')
        minio_access_key = self.config.get('minio_access_key', 'minioadmin')
        minio_secret_key = self.config.get('minio_secret_key', 'minioadmin123')
        
        configuration.set_string("state.checkpoints.dir", f"{minio_endpoint}/flink-checkpoints")
        configuration.set_string("state.savepoints.dir", f"{minio_endpoint}/flink-savepoints")
        configuration.set_string("s3.endpoint", minio_endpoint)
        configuration.set_string("s3.access-key", minio_access_key)
        configuration.set_string("s3.secret-key", minio_secret_key)
        configuration.set_string("s3.path-style-access", "true")
        
        # Apply configuration
        self.env.get_config().set_global_job_parameters(configuration)
        
        logger.info(f"Flink environment configured with parallelism: {self.env.get_parallelism()}")
        logger.info(f"Checkpointing enabled with interval: {self.config.get('checkpoint_interval_ms', 60000)}ms")
        
        return self.env
    
    def create_kafka_source(self) -> DataStream:
        """
        Create Kafka source for consuming posts.
        
        Returns:
            DataStream of raw post messages from Kafka
        """
        kafka_props = {
            'bootstrap.servers': self.config.get('kafka_bootstrap_servers', 'kafka:29092'),
            'group.id': self.config.get('kafka_consumer_group', 'trend-processor'),
            'auto.offset.reset': 'latest'
        }
        
        kafka_consumer = FlinkKafkaConsumer(
            topics=self.config.get('kafka_topic_posts', 'bluesky-posts'),
            deserialization_schema=SimpleStringSchema(),
            properties=kafka_props
        )
        
        # Start from latest messages
        kafka_consumer.set_start_from_latest()
        
        source_stream = self.env.add_source(kafka_consumer)
        logger.info(f"Kafka source created for topic: {self.config.get('kafka_topic_posts', 'bluesky-posts')}")
        
        return source_stream
    
    def create_processing_pipeline(self, source_stream: DataStream) -> DataStream:
        """
        Create the main processing pipeline for keyword extraction and aggregation.
        
        Args:
            source_stream: Input stream of raw Kafka messages
            
        Returns:
            DataStream of WindowedKeywordCount objects
        """
        # Deserialize posts from JSON
        posts_stream = source_stream.map(
            PostDataDeserializer(),
            output_type=FlinkTypes.PICKLED_BYTE_ARRAY()
        ).filter(lambda post: post is not None)
        
        # Extract keywords from posts
        keywords_stream = posts_stream.flat_map(
            KeywordExtractor(),
            output_type=FlinkTypes.TUPLE([FlinkTypes.STRING(), FlinkTypes.PICKLED_BYTE_ARRAY()])
        )
        
        # Key by keyword for parallel processing
        keyed_stream = keywords_stream.key_by(lambda x: x[0])
        
        # Apply sliding window aggregation
        window_size_minutes = self.config.get('window_size_minutes', 10)
        windowed_counts = keyed_stream.process(
            KeywordAggregator(window_size_minutes),
            output_type=FlinkTypes.PICKLED_BYTE_ARRAY()
        )
        
        logger.info(f"Processing pipeline created with {window_size_minutes}-minute windows")
        
        return windowed_counts
    
    def create_output_sinks(self, processed_stream: DataStream) -> None:
        """
        Create output sinks for processed data.
        
        Args:
            processed_stream: Stream of WindowedKeywordCount objects
        """
        # Serialize for Kafka output
        serialized_stream = processed_stream.map(
            WindowedCountSerializer(),
            output_type=FlinkTypes.STRING()
        )
        
        # Kafka sink for trend alerts
        kafka_props = {
            'bootstrap.servers': self.config.get('kafka_bootstrap_servers', 'kafka:29092'),
        }
        
        kafka_producer = FlinkKafkaProducer(
            topic=self.config.get('kafka_topic_trends', 'windowed-keyword-counts'),
            serialization_schema=SimpleStringSchema(),
            producer_config=kafka_props
        )
        
        serialized_stream.add_sink(kafka_producer)
        
        logger.info(f"Output sink created for topic: {self.config.get('kafka_topic_trends', 'windowed-keyword-counts')}")
    
    def run_job(self) -> None:
        """Execute the complete Flink streaming job."""
        try:
            # Setup environment
            env = self.setup_environment()
            
            # Create source
            source_stream = self.create_kafka_source()
            
            # Create processing pipeline
            processed_stream = self.create_processing_pipeline(source_stream)
            
            # Create output sinks
            self.create_output_sinks(processed_stream)
            
            # Execute the job
            logger.info("Starting Flink streaming job...")
            env.execute("Bluesky Trend Detection Job")
            
        except Exception as e:
            logger.error(f"Failed to run Flink job: {e}")
            raise


def create_flink_job_config(
    kafka_bootstrap_servers: str = "kafka:29092",
    kafka_topic_posts: str = "bluesky-posts",
    kafka_topic_trends: str = "windowed-keyword-counts",
    kafka_consumer_group: str = "trend-processor",
    minio_endpoint: str = "http://minio:9000",
    minio_access_key: str = "minioadmin",
    minio_secret_key: str = "minioadmin123",
    parallelism: int = 2,
    checkpoint_interval_ms: int = 60000,
    window_size_minutes: int = 10
) -> Dict[str, Any]:
    """
    Create configuration dictionary for Flink job.
    
    Args:
        kafka_bootstrap_servers: Kafka bootstrap servers
        kafka_topic_posts: Input topic for posts
        kafka_topic_trends: Output topic for trends
        kafka_consumer_group: Consumer group ID
        minio_endpoint: MinIO endpoint URL
        minio_access_key: MinIO access key
        minio_secret_key: MinIO secret key
        parallelism: Flink job parallelism
        checkpoint_interval_ms: Checkpointing interval in milliseconds
        window_size_minutes: Window size for aggregation in minutes
        
    Returns:
        Configuration dictionary
    """
    return {
        'kafka_bootstrap_servers': kafka_bootstrap_servers,
        'kafka_topic_posts': kafka_topic_posts,
        'kafka_topic_trends': kafka_topic_trends,
        'kafka_consumer_group': kafka_consumer_group,
        'minio_endpoint': minio_endpoint,
        'minio_access_key': minio_access_key,
        'minio_secret_key': minio_secret_key,
        'parallelism': parallelism,
        'checkpoint_interval_ms': checkpoint_interval_ms,
        'window_size_minutes': window_size_minutes
    }


if __name__ == "__main__":
    # Example usage
    config = create_flink_job_config()
    processor = FlinkStreamProcessor(config)
    processor.run_job()