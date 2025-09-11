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

class MockRuntimeContext:
    """Mock runtime context for state management."""
    
    def __init__(self):
        self._state_storage = {}
        self._map_state_storage = {}
        self._list_state_storage = {}
    
    def get_state(self, descriptor):
        return MockValueState(descriptor.name, self._state_storage)
    
    def get_value_state(self, descriptor):
        return MockValueState(descriptor.name, self._state_storage)
    
    def get_map_state(self, descriptor):
        return MockMapState(descriptor.name, self._map_state_storage)
    
    def get_list_state(self, descriptor):
        return MockListState(descriptor.name, self._list_state_storage)

class MockValueState:
    """Mock value state for testing."""
    
    def __init__(self, name: str, storage: Dict[str, Any]):
        self.name = name
        self.storage = storage
    
    def value(self):
        return self.storage.get(self.name)
    
    def update(self, value):
        self.storage[self.name] = value
    
    def clear(self):
        if self.name in self.storage:
            del self.storage[self.name]

class MockMapState:
    """Mock map state for testing."""
    
    def __init__(self, name: str, storage: Dict[str, Dict[str, Any]]):
        self.name = name
        self.storage = storage
        if name not in self.storage:
            self.storage[name] = {}
    
    def get(self, key):
        return self.storage[self.name].get(key)
    
    def put(self, key, value):
        self.storage[self.name][key] = value
    
    def remove(self, key):
        if key in self.storage[self.name]:
            del self.storage[self.name][key]
    
    def keys(self):
        return self.storage[self.name].keys()
    
    def clear(self):
        self.storage[self.name].clear()

class MockListState:
    """Mock list state for testing."""
    
    def __init__(self, name: str, storage: Dict[str, List[Any]]):
        self.name = name
        self.storage = storage
        if name not in self.storage:
            self.storage[name] = []
    
    def get(self):
        return self.storage[self.name]
    
    def add(self, value):
        self.storage[self.name].append(value)
    
    def clear(self):
        self.storage[self.name].clear()

class MockTimerService:
    """Mock timer service for testing."""
    
    def __init__(self):
        self.timers = []
    
    def register_event_time_timer(self, timestamp: int):
        self.timers.append(timestamp)
    
    def register_processing_time_timer(self, timestamp: int):
        self.timers.append(timestamp)

class MockProcessingContext:
    """Mock processing context for testing."""
    
    def __init__(self, timestamp: int = None):
        self._timestamp = timestamp or int(datetime.now().timestamp() * 1000)
        self._timer_service = MockTimerService()
    
    def timestamp(self):
        return self._timestamp
    
    def timer_service(self):
        return self._timer_service

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
from trend_detector import TrendDetector, TrendDetectionConfig, create_trend_detection_config

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


class SlidingWindowAggregator(KeyedProcessFunction):
    """
    Aggregates keyword counts within sliding time windows and tracks unique authors.
    
    This class implements a sliding window aggregation using Flink's process function
    with RocksDB state backend for persistence. It maintains separate state for each
    time window and handles window expiration automatically.
    """
    
    def __init__(self, window_size_minutes: int = 10, slide_interval_minutes: int = 1):
        """
        Initialize the sliding window aggregator.
        
        Args:
            window_size_minutes: Size of the sliding window in minutes
            slide_interval_minutes: Slide interval for the window in minutes
        """
        self.window_size_minutes = window_size_minutes
        self.slide_interval_minutes = slide_interval_minutes
        self.window_size_ms = window_size_minutes * 60 * 1000
        self.slide_interval_ms = slide_interval_minutes * 60 * 1000
        
        # State descriptors for RocksDB backend
        self.window_counts_state = None
        self.window_authors_state = None
        self.cleanup_timer_state = None
    
    def open(self, runtime_context):
        """Initialize state descriptors for RocksDB backend."""
        # State for storing counts per window
        self.window_counts_state = runtime_context.get_map_state(
            ValueStateDescriptor("window_counts", FlinkTypes.PICKLED_BYTE_ARRAY())
        )
        
        # State for storing unique authors per window
        self.window_authors_state = runtime_context.get_map_state(
            ValueStateDescriptor("window_authors", FlinkTypes.PICKLED_BYTE_ARRAY())
        )
        
        # State for cleanup timers
        self.cleanup_timer_state = runtime_context.get_value_state(
            ValueStateDescriptor("cleanup_timer", FlinkTypes.PICKLED_BYTE_ARRAY())
        )
    
    def process_element(self, value: Tuple[str, PostData], ctx, out):
        """
        Process each keyword-post pair and maintain sliding window aggregations.
        
        Args:
            value: Tuple of (keyword, post)
            ctx: Processing context with timestamp and timer service
            out: Output collector for emitting windowed counts
        """
        keyword, post = value
        event_time = ctx.timestamp()
        
        # Calculate which windows this event belongs to
        windows = self._get_windows_for_timestamp(event_time)
        
        for window_start, window_end in windows:
            window_key = f"{window_start}_{window_end}"
            
            # Update count for this window
            current_count = self.window_counts_state.get(window_key)
            if current_count is None:
                current_count = 0
            current_count += 1
            self.window_counts_state.put(window_key, current_count)
            
            # Update unique authors for this window
            authors_set = self.window_authors_state.get(window_key)
            if authors_set is None:
                authors_set = set()
            authors_set.add(post.author_did)
            self.window_authors_state.put(window_key, authors_set)
            
            # Create windowed keyword count
            windowed_count = WindowedKeywordCount(
                keyword=keyword,
                count=current_count,
                window_start=datetime.fromtimestamp(window_start / 1000),
                window_end=datetime.fromtimestamp(window_end / 1000),
                unique_authors=len(authors_set),
                normalized_keyword=self._normalize_keyword(keyword)
            )
            
            # Emit the windowed count
            out.collect(windowed_count)
            
            # Register cleanup timer for window expiration
            cleanup_time = window_end + 1000  # 1 second after window ends
            ctx.timer_service().register_event_time_timer(cleanup_time)
    
    def on_timer(self, timestamp: int, ctx, out):
        """
        Handle timer events for window cleanup.
        
        Args:
            timestamp: Timer timestamp
            ctx: Processing context
            out: Output collector
        """
        # Clean up expired windows
        current_time = ctx.timestamp()
        expired_windows = []
        
        # Find windows that have expired
        for window_key in self.window_counts_state.keys():
            window_start, window_end = map(int, window_key.split('_'))
            if window_end <= current_time - self.window_size_ms:
                expired_windows.append(window_key)
        
        # Remove expired windows from state
        for window_key in expired_windows:
            self.window_counts_state.remove(window_key)
            self.window_authors_state.remove(window_key)
    
    def _get_windows_for_timestamp(self, timestamp: int) -> List[Tuple[int, int]]:
        """
        Calculate which sliding windows contain the given timestamp.
        
        Args:
            timestamp: Event timestamp in milliseconds
            
        Returns:
            List of (window_start, window_end) tuples in milliseconds
        """
        windows = []
        
        # Find the most recent window boundary that is <= timestamp
        # Align to slide interval boundaries
        latest_window_end = ((timestamp // self.slide_interval_ms) + 1) * self.slide_interval_ms
        
        # Generate windows that contain this timestamp
        # A window contains the timestamp if: window_start <= timestamp < window_end
        window_end = latest_window_end
        while window_end - self.window_size_ms <= timestamp:
            window_start = window_end - self.window_size_ms
            if window_start <= timestamp < window_end:
                windows.append((window_start, window_end))
            window_end += self.slide_interval_ms
            
            # Prevent infinite loop - only look at reasonable number of future windows
            if len(windows) > 20:
                break
        
        return windows
    
    def _normalize_keyword(self, keyword: str) -> str:
        """
        Normalize keyword for consistent matching.
        
        Args:
            keyword: Original keyword
            
        Returns:
            Normalized keyword (lowercase, trimmed)
        """
        return keyword.lower().strip()


class WindowedCountAggregator(KeyedProcessFunction):
    """
    Alternative implementation using explicit window management for better control.
    
    This aggregator maintains a more explicit sliding window implementation
    with configurable window parameters and efficient state management.
    """
    
    def __init__(self, window_size_minutes: int = 10, slide_interval_minutes: int = 1):
        """
        Initialize the windowed count aggregator.
        
        Args:
            window_size_minutes: Size of each window in minutes
            slide_interval_minutes: How often to slide the window in minutes
        """
        self.window_size_minutes = window_size_minutes
        self.slide_interval_minutes = slide_interval_minutes
        self.window_size_ms = window_size_minutes * 60 * 1000
        self.slide_interval_ms = slide_interval_minutes * 60 * 1000
        
        # State for window data
        self.events_state = None
        self.last_cleanup_state = None
    
    def open(self, runtime_context):
        """Initialize state for window management."""
        # Store events with their timestamps for window calculations
        self.events_state = runtime_context.get_list_state(
            ValueStateDescriptor("window_events", FlinkTypes.PICKLED_BYTE_ARRAY())
        )
        
        # Track last cleanup time
        self.last_cleanup_state = runtime_context.get_value_state(
            ValueStateDescriptor("last_cleanup", FlinkTypes.PICKLED_BYTE_ARRAY())
        )
    
    def process_element(self, value: Tuple[str, PostData], ctx, out):
        """
        Process keyword-post pairs with sliding window logic.
        
        Args:
            value: Tuple of (keyword, post)
            ctx: Processing context
            out: Output collector
        """
        keyword, post = value
        event_time = ctx.timestamp()
        
        # Add new event to state
        event_data = {
            'timestamp': event_time,
            'author_did': post.author_did,
            'post_uri': post.uri
        }
        self.events_state.add(event_data)
        
        # Clean up old events periodically
        self._cleanup_old_events(event_time)
        
        # Calculate current window statistics
        window_stats = self._calculate_window_stats(event_time)
        
        # Emit windowed count for each active window
        for window_start, window_end, count, unique_authors in window_stats:
            windowed_count = WindowedKeywordCount(
                keyword=keyword,
                count=count,
                window_start=datetime.fromtimestamp(window_start / 1000),
                window_end=datetime.fromtimestamp(window_end / 1000),
                unique_authors=unique_authors,
                normalized_keyword=self._normalize_keyword(keyword)
            )
            out.collect(windowed_count)
    
    def _cleanup_old_events(self, current_time: int) -> None:
        """
        Remove events that are outside all possible windows.
        
        Args:
            current_time: Current event timestamp
        """
        last_cleanup = self.last_cleanup_state.value()
        if last_cleanup is None:
            last_cleanup = 0
        
        # Only cleanup every slide interval to avoid excessive processing
        if current_time - last_cleanup < self.slide_interval_ms:
            return
        
        cutoff_time = current_time - self.window_size_ms
        events = list(self.events_state.get())
        
        # Filter out old events
        recent_events = [
            event for event in events 
            if event['timestamp'] > cutoff_time
        ]
        
        # Update state with filtered events
        self.events_state.clear()
        for event in recent_events:
            self.events_state.add(event)
        
        self.last_cleanup_state.update(current_time)
    
    def _calculate_window_stats(self, current_time: int) -> List[Tuple[int, int, int, int]]:
        """
        Calculate statistics for all active windows.
        
        Args:
            current_time: Current timestamp
            
        Returns:
            List of (window_start, window_end, count, unique_authors) tuples
        """
        events = list(self.events_state.get())
        window_stats = []
        
        # Calculate window boundaries
        latest_window_start = current_time - (current_time % self.slide_interval_ms)
        
        # Generate statistics for each window
        window_start = latest_window_start
        while window_start > current_time - self.window_size_ms:
            window_end = window_start + self.window_size_ms
            
            # Count events in this window
            window_events = [
                event for event in events
                if window_start <= event['timestamp'] < window_end
            ]
            
            if window_events:
                count = len(window_events)
                unique_authors = len(set(event['author_did'] for event in window_events))
                window_stats.append((window_start, window_end, count, unique_authors))
            
            window_start -= self.slide_interval_ms
        
        return window_stats
    
    def _normalize_keyword(self, keyword: str) -> str:
        """Normalize keyword for consistent processing."""
        return keyword.lower().strip()


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


class TrendAlertSerializer(MapFunction):
    """Serializes TrendAlert objects to JSON for Kafka output."""
    
    def map(self, trend_alert: TrendAlert) -> str:
        """
        Convert TrendAlert to JSON string.
        
        Args:
            trend_alert: TrendAlert object
            
        Returns:
            JSON string representation
        """
        return json.dumps({
            'keyword': trend_alert.keyword,
            'frequency': trend_alert.frequency,
            'growth_rate': trend_alert.growth_rate,
            'confidence_score': trend_alert.confidence_score,
            'window_start': trend_alert.window_start.isoformat(),
            'window_end': trend_alert.window_end.isoformat(),
            'sample_posts': trend_alert.sample_posts,
            'unique_authors': trend_alert.unique_authors,
            'rank': trend_alert.rank
        })


class TrendDetectionFunction(KeyedProcessFunction):
    """
    Flink process function that integrates trend detection algorithm.
    
    This function collects windowed keyword counts and applies statistical
    trend detection to identify trending topics in real-time.
    """
    
    def __init__(self, trend_config: Optional[TrendDetectionConfig] = None):
        """
        Initialize trend detection function.
        
        Args:
            trend_config: Configuration for trend detection algorithm
        """
        self.trend_config = trend_config or create_trend_detection_config()
        self.trend_detector = None
        self.windowed_counts_buffer = None
        self.last_detection_time = None
        self.detection_interval_ms = 30000  # Run trend detection every 30 seconds
        
    def open(self, runtime_context):
        """Initialize trend detector and state."""
        self.trend_detector = TrendDetector(self.trend_config)
        
        # State for buffering windowed counts
        self.windowed_counts_buffer = runtime_context.get_list_state(
            ValueStateDescriptor("windowed_counts_buffer", FlinkTypes.PICKLED_BYTE_ARRAY())
        )
        
        # State for tracking last detection time
        self.last_detection_time = runtime_context.get_value_state(
            ValueStateDescriptor("last_detection_time", FlinkTypes.PICKLED_BYTE_ARRAY())
        )
        
        logger.info("TrendDetectionFunction initialized with trend detection algorithm")
    
    def process_element(self, value: WindowedKeywordCount, ctx, out):
        """
        Process windowed keyword counts and detect trends periodically.
        
        Args:
            value: WindowedKeywordCount from sliding window aggregation
            ctx: Processing context
            out: Output collector for trend alerts
        """
        current_time = ctx.timestamp()
        
        # Add windowed count to buffer
        self.windowed_counts_buffer.add(value)
        
        # Check if it's time to run trend detection
        last_detection = self.last_detection_time.value()
        if (last_detection is None or 
            current_time - last_detection >= self.detection_interval_ms):
            
            # Get all buffered counts
            buffered_counts = list(self.windowed_counts_buffer.get())
            
            if buffered_counts:
                # Run trend detection
                trends = self.trend_detector.process_windowed_counts(buffered_counts)
                
                # Emit trend alerts
                for trend in trends:
                    out.collect(trend)
                    logger.debug(f"Emitted trend alert: {trend.keyword} (rank={trend.rank})")
                
                # Clear buffer and update detection time
                self.windowed_counts_buffer.clear()
                self.last_detection_time.update(current_time)
                
                logger.info(f"Trend detection completed: {len(trends)} trends detected from "
                           f"{len(buffered_counts)} windowed counts")
    
    def on_timer(self, timestamp: int, ctx, out):
        """Handle timer events for periodic trend detection."""
        # This could be used for additional periodic cleanup or detection
        pass


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
    
    def create_processing_pipeline(self, source_stream: DataStream) -> Tuple[DataStream, DataStream]:
        """
        Create the main processing pipeline for keyword extraction, sliding window aggregation, and trend detection.
        
        Args:
            source_stream: Input stream of raw Kafka messages
            
        Returns:
            Tuple of (windowed_counts_stream, trend_alerts_stream)
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
        
        # Apply sliding window aggregation with configurable parameters
        window_size_minutes = self.config.get('window_size_minutes', 10)
        slide_interval_minutes = self.config.get('slide_interval_minutes', 1)
        
        # Use the sliding window aggregator for better window management
        windowed_counts = keyed_stream.process(
            SlidingWindowAggregator(
                window_size_minutes=window_size_minutes,
                slide_interval_minutes=slide_interval_minutes
            ),
            output_type=FlinkTypes.PICKLED_BYTE_ARRAY()
        )
        
        # Apply trend detection algorithm
        trend_detection_config = create_trend_detection_config(
            min_frequency=self.config.get('trend_min_frequency', 5),
            min_unique_authors=self.config.get('trend_min_unique_authors', 2),
            min_z_score=self.config.get('trend_min_z_score', 2.0),
            min_growth_rate=self.config.get('trend_min_growth_rate', 50.0),
            max_trends=self.config.get('trend_max_trends', 50)
        )
        
        # Key windowed counts by normalized keyword for trend detection
        keyed_windowed_counts = windowed_counts.key_by(lambda wc: wc.normalized_keyword)
        
        # Apply trend detection function
        trend_alerts = keyed_windowed_counts.process(
            TrendDetectionFunction(trend_detection_config),
            output_type=FlinkTypes.PICKLED_BYTE_ARRAY()
        )
        
        logger.info(f"Processing pipeline created with {window_size_minutes}-minute sliding windows, "
                   f"{slide_interval_minutes}-minute slide interval, and trend detection")
        
        return windowed_counts, trend_alerts
    
    def create_output_sinks(self, windowed_counts_stream: DataStream, trend_alerts_stream: DataStream) -> None:
        """
        Create output sinks for processed data.
        
        Args:
            windowed_counts_stream: Stream of WindowedKeywordCount objects
            trend_alerts_stream: Stream of TrendAlert objects
        """
        # Serialize windowed counts for Kafka output
        serialized_counts = windowed_counts_stream.map(
            WindowedCountSerializer(),
            output_type=FlinkTypes.STRING()
        )
        
        # Serialize trend alerts for Kafka output
        serialized_alerts = trend_alerts_stream.map(
            TrendAlertSerializer(),
            output_type=FlinkTypes.STRING()
        )
        
        # Kafka producer configuration
        kafka_props = {
            'bootstrap.servers': self.config.get('kafka_bootstrap_servers', 'kafka:29092'),
        }
        
        # Kafka sink for windowed keyword counts
        windowed_counts_producer = FlinkKafkaProducer(
            topic=self.config.get('kafka_topic_windowed_counts', 'windowed-keyword-counts'),
            serialization_schema=SimpleStringSchema(),
            producer_config=kafka_props
        )
        
        # Kafka sink for trend alerts
        trend_alerts_producer = FlinkKafkaProducer(
            topic=self.config.get('kafka_topic_trend_alerts', 'trend-alerts'),
            serialization_schema=SimpleStringSchema(),
            producer_config=kafka_props
        )
        
        # Add sinks to streams
        serialized_counts.add_sink(windowed_counts_producer)
        serialized_alerts.add_sink(trend_alerts_producer)
        
        logger.info(f"Output sinks created for windowed counts and trend alerts")
        logger.info(f"Windowed counts topic: {self.config.get('kafka_topic_windowed_counts', 'windowed-keyword-counts')}")
        logger.info(f"Trend alerts topic: {self.config.get('kafka_topic_trend_alerts', 'trend-alerts')}")
    
    def run_job(self) -> None:
        """Execute the complete Flink streaming job with trend detection."""
        try:
            # Setup environment
            env = self.setup_environment()
            
            # Create source
            source_stream = self.create_kafka_source()
            
            # Create processing pipeline (returns both windowed counts and trend alerts)
            windowed_counts_stream, trend_alerts_stream = self.create_processing_pipeline(source_stream)
            
            # Create output sinks for both streams
            self.create_output_sinks(windowed_counts_stream, trend_alerts_stream)
            
            # Execute the job
            logger.info("Starting Flink streaming job with trend detection...")
            env.execute("Bluesky Trend Detection Job")
            
        except Exception as e:
            logger.error(f"Failed to run Flink job: {e}")
            raise


def create_flink_job_config(
    kafka_bootstrap_servers: str = "kafka:29092",
    kafka_topic_posts: str = "bluesky-posts",
    kafka_topic_windowed_counts: str = "windowed-keyword-counts",
    kafka_topic_trend_alerts: str = "trend-alerts",
    kafka_consumer_group: str = "trend-processor",
    minio_endpoint: str = "http://minio:9000",
    minio_access_key: str = "minioadmin",
    minio_secret_key: str = "minioadmin123",
    parallelism: int = 2,
    checkpoint_interval_ms: int = 60000,
    window_size_minutes: int = 10,
    slide_interval_minutes: int = 1,
    trend_min_frequency: int = 5,
    trend_min_unique_authors: int = 2,
    trend_min_z_score: float = 2.0,
    trend_min_growth_rate: float = 50.0,
    trend_max_trends: int = 50
) -> Dict[str, Any]:
    """
    Create configuration dictionary for Flink job with sliding window and trend detection parameters.
    
    Args:
        kafka_bootstrap_servers: Kafka bootstrap servers
        kafka_topic_posts: Input topic for posts
        kafka_topic_windowed_counts: Output topic for windowed keyword counts
        kafka_topic_trend_alerts: Output topic for trend alerts
        kafka_consumer_group: Consumer group ID
        minio_endpoint: MinIO endpoint URL
        minio_access_key: MinIO access key
        minio_secret_key: MinIO secret key
        parallelism: Flink job parallelism
        checkpoint_interval_ms: Checkpointing interval in milliseconds
        window_size_minutes: Window size for aggregation in minutes
        slide_interval_minutes: Slide interval for windows in minutes
        trend_min_frequency: Minimum frequency threshold for trend detection
        trend_min_unique_authors: Minimum unique authors threshold for trend detection
        trend_min_z_score: Minimum z-score threshold for statistical significance
        trend_min_growth_rate: Minimum growth rate percentage for trend detection
        trend_max_trends: Maximum number of trends to track and rank
        
    Returns:
        Configuration dictionary
    """
    return {
        'kafka_bootstrap_servers': kafka_bootstrap_servers,
        'kafka_topic_posts': kafka_topic_posts,
        'kafka_topic_windowed_counts': kafka_topic_windowed_counts,
        'kafka_topic_trend_alerts': kafka_topic_trend_alerts,
        'kafka_consumer_group': kafka_consumer_group,
        'minio_endpoint': minio_endpoint,
        'minio_access_key': minio_access_key,
        'minio_secret_key': minio_secret_key,
        'parallelism': parallelism,
        'checkpoint_interval_ms': checkpoint_interval_ms,
        'window_size_minutes': window_size_minutes,
        'slide_interval_minutes': slide_interval_minutes,
        'trend_min_frequency': trend_min_frequency,
        'trend_min_unique_authors': trend_min_unique_authors,
        'trend_min_z_score': trend_min_z_score,
        'trend_min_growth_rate': trend_min_growth_rate,
        'trend_max_trends': trend_max_trends
    }


if __name__ == "__main__":
    # Example usage
    config = create_flink_job_config()
    processor = FlinkStreamProcessor(config)
    processor.run_job()