#!/usr/bin/env python3
"""
Integration test demonstrating complete sliding window functionality.

This test shows how the sliding window aggregation works end-to-end
with realistic data patterns and validates the requirements.
"""

import pytest
from datetime import datetime, timedelta
from typing import List, Dict, Any
from unittest.mock import Mock

# Import our models and classes
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from shared.models import PostData, WindowedKeywordCount
from flink_job import (
    SlidingWindowAggregator,
    KeywordExtractor,
    PostDataDeserializer,
    MockRuntimeContext,
    MockProcessingContext,
    create_flink_job_config
)


class TestSlidingWindowIntegration:
    """Integration test for sliding window functionality."""
    
    def test_end_to_end_keyword_counting(self):
        """
        Test complete end-to-end sliding window keyword counting.
        
        This test validates:
        - Requirement 3.3: Sliding window operations with configurable duration
        - Requirement 3.4: Keyword frequency counting with normalization
        - Requirement 4.1: Unique author counting within time windows
        - Requirement 4.2: Keyword normalization and filtering
        """
        # Setup sliding window aggregator
        aggregator = SlidingWindowAggregator(
            window_size_minutes=5,  # 5-minute windows
            slide_interval_minutes=1  # 1-minute slides
        )
        runtime_context = MockRuntimeContext()
        aggregator.open(runtime_context)
        
        # Setup keyword extractor
        extractor = KeywordExtractor()
        
        # Create realistic test data
        base_time = datetime(2024, 1, 1, 12, 0, 0)
        test_posts = [
            # Python trending posts
            PostData(
                uri="at://did:plc:alice/app.bsky.feed.post/1",
                author_did="did:plc:alice",
                text="Just learned #Python programming! Amazing language for data science.",
                created_at=base_time
            ),
            PostData(
                uri="at://did:plc:bob/app.bsky.feed.post/1",
                author_did="did:plc:bob",
                text="Python is great for machine learning and AI development.",
                created_at=base_time + timedelta(minutes=1)
            ),
            PostData(
                uri="at://did:plc:charlie/app.bsky.feed.post/1",
                author_did="did:plc:charlie",
                text="Working on a Python project with Django framework.",
                created_at=base_time + timedelta(minutes=2)
            ),
            PostData(
                uri="at://did:plc:alice/app.bsky.feed.post/2",
                author_did="did:plc:alice",
                text="Python vs JavaScript: which is better for beginners?",
                created_at=base_time + timedelta(minutes=3)
            ),
            # JavaScript posts (less frequent)
            PostData(
                uri="at://did:plc:dave/app.bsky.feed.post/1",
                author_did="did:plc:dave",
                text="JavaScript frameworks are evolving so fast! React, Vue, Angular...",
                created_at=base_time + timedelta(minutes=4)
            ),
        ]
        
        # Process posts through the pipeline
        output_collector = []
        
        def collect(item):
            output_collector.append(item)
        
        mock_out = Mock()
        mock_out.collect = collect
        
        # Extract keywords and process through sliding windows
        for post in test_posts:
            # Extract keywords from post
            keyword_post_pairs = extractor.flat_map(post)
            
            # Process each keyword through sliding window aggregator
            for keyword, post_data in keyword_post_pairs:
                timestamp_ms = int(post_data.created_at.timestamp() * 1000)
                ctx = MockProcessingContext(timestamp_ms)
                
                aggregator.process_element((keyword, post_data), ctx, mock_out)
        
        # Analyze results
        assert len(output_collector) > 0
        
        # Group results by keyword
        keyword_counts = {}
        for windowed_count in output_collector:
            keyword = windowed_count.keyword
            if keyword not in keyword_counts:
                keyword_counts[keyword] = []
            keyword_counts[keyword].append(windowed_count)
        
        # Validate keyword normalization (Requirement 4.2)
        for keyword in keyword_counts.keys():
            assert keyword == keyword.lower()  # Should be normalized to lowercase
        
        # Validate Python trending (should appear more frequently)
        assert "python" in keyword_counts
        python_counts = keyword_counts["python"]
        
        # Should have multiple window counts for Python
        assert len(python_counts) > 1
        
        # Find maximum count for Python
        max_python_count = max(wc.count for wc in python_counts)
        assert max_python_count >= 3  # Should appear in at least 3 posts
        
        # Validate unique author counting (Requirement 4.1)
        max_python_authors = max(wc.unique_authors for wc in python_counts)
        assert max_python_authors >= 2  # Should have at least 2 unique authors
        assert max_python_authors <= 3  # But not more than the actual unique authors
        
        # Validate that we can distinguish between different keyword frequencies
        # Note: Due to windowing effects, absolute counts may vary, but we should
        # see that Python appears in more posts overall
        if "javascript" in keyword_counts:
            javascript_counts = keyword_counts["javascript"]
            # Count total occurrences across all windows for comparison
            total_python_occurrences = len(python_counts)
            total_javascript_occurrences = len(javascript_counts)
            
            # Python should appear in more windows since it's in more posts
            assert total_python_occurrences >= total_javascript_occurrences
        
        # Validate window timing (Requirement 3.3)
        for windowed_count in output_collector:
            window_duration = windowed_count.window_end - windowed_count.window_start
            assert window_duration == timedelta(minutes=5)  # 5-minute windows
            
            # Validate that windows are reasonable (not in the far future or past)
            assert windowed_count.window_start >= base_time - timedelta(minutes=10)
            assert windowed_count.window_end <= base_time + timedelta(minutes=20)
    
    def test_configurable_window_parameters(self):
        """Test that window parameters are configurable as required."""
        # Test different configurations
        configs = [
            (3, 1),   # 3-minute windows, 1-minute slide
            (10, 2),  # 10-minute windows, 2-minute slide
            (15, 5),  # 15-minute windows, 5-minute slide
        ]
        
        for window_size, slide_interval in configs:
            config = create_flink_job_config(
                window_size_minutes=window_size,
                slide_interval_minutes=slide_interval
            )
            
            assert config['window_size_minutes'] == window_size
            assert config['slide_interval_minutes'] == slide_interval
            
            # Test aggregator with these parameters
            aggregator = SlidingWindowAggregator(
                window_size_minutes=window_size,
                slide_interval_minutes=slide_interval
            )
            
            assert aggregator.window_size_minutes == window_size
            assert aggregator.slide_interval_minutes == slide_interval
    
    def test_keyword_frequency_normalization(self):
        """Test keyword frequency counting with proper normalization."""
        aggregator = SlidingWindowAggregator(
            window_size_minutes=10,
            slide_interval_minutes=1
        )
        runtime_context = MockRuntimeContext()
        aggregator.open(runtime_context)
        
        extractor = KeywordExtractor()
        
        # Create posts with various keyword formats
        base_time = datetime(2024, 1, 1, 12, 0, 0)
        test_posts = [
            PostData(
                uri="at://did:plc:user1/app.bsky.feed.post/1",
                author_did="did:plc:user1",
                text="PYTHON Programming is awesome!",
                created_at=base_time
            ),
            PostData(
                uri="at://did:plc:user2/app.bsky.feed.post/1",
                author_did="did:plc:user2",
                text="python development with Machine-Learning",
                created_at=base_time + timedelta(minutes=1)
            ),
            PostData(
                uri="at://did:plc:user3/app.bsky.feed.post/1",
                author_did="did:plc:user3",
                text="Learning Python and machine-learning together",
                created_at=base_time + timedelta(minutes=2)
            ),
        ]
        
        output_collector = []
        
        def collect(item):
            output_collector.append(item)
        
        mock_out = Mock()
        mock_out.collect = collect
        
        # Process posts
        for post in test_posts:
            keyword_post_pairs = extractor.flat_map(post)
            
            for keyword, post_data in keyword_post_pairs:
                timestamp_ms = int(post_data.created_at.timestamp() * 1000)
                ctx = MockProcessingContext(timestamp_ms)
                
                aggregator.process_element((keyword, post_data), ctx, mock_out)
        
        # Analyze normalization
        keywords_found = set(wc.keyword for wc in output_collector)
        
        # Should find normalized keywords
        assert "python" in keywords_found  # Normalized from PYTHON and python
        assert "programming" in keywords_found
        
        # Should not find non-normalized versions
        assert "PYTHON" not in keywords_found
        assert "Programming" not in keywords_found
        
        # Check that machine-learning is handled consistently
        ml_keywords = [k for k in keywords_found if "machine" in k or "learning" in k]
        assert len(ml_keywords) > 0  # Should extract machine learning related terms
    
    def test_rocksdb_state_backend_simulation(self):
        """Test simulation of RocksDB state backend behavior."""
        # This test simulates the behavior we expect from RocksDB state backend
        aggregator = SlidingWindowAggregator(
            window_size_minutes=5,
            slide_interval_minutes=1
        )
        runtime_context = MockRuntimeContext()
        aggregator.open(runtime_context)
        
        # Simulate processing many events to test state persistence
        keyword = "persistence_test"
        base_time = datetime(2024, 1, 1, 12, 0, 0)
        
        output_collector = []
        
        def collect(item):
            output_collector.append(item)
        
        mock_out = Mock()
        mock_out.collect = collect
        
        # Process events over time
        for i in range(20):
            post = PostData(
                uri=f"at://did:plc:user{i % 5}/app.bsky.feed.post/{i}",
                author_did=f"did:plc:user{i % 5}",  # 5 different users
                text=f"Post {i} about persistence testing",
                created_at=base_time + timedelta(minutes=i * 0.5)  # Every 30 seconds
            )
            
            timestamp_ms = int(post.created_at.timestamp() * 1000)
            ctx = MockProcessingContext(timestamp_ms)
            
            aggregator.process_element((keyword, post), ctx, mock_out)
        
        # Verify state persistence behavior
        window_counts = aggregator.window_counts_state.storage['window_counts']
        window_authors = aggregator.window_authors_state.storage['window_authors']
        
        # Should have multiple windows with accumulated state
        assert len(window_counts) > 1
        assert len(window_authors) > 1
        
        # Should have emitted many windowed counts
        assert len(output_collector) > 10
        
        # Verify unique author tracking across windows
        max_unique_authors = max(wc.unique_authors for wc in output_collector)
        assert max_unique_authors <= 5  # Can't exceed actual number of unique users
        assert max_unique_authors >= 1  # Should track at least one author


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])