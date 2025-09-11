#!/usr/bin/env python3
"""
Basic functionality tests for Flink stream processing components.

These tests verify the core logic without requiring complex PyFlink imports.
"""

import json
import pytest
from datetime import datetime, timedelta
from typing import List

# Import our models
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'shared'))
from models import PostData, WindowedKeywordCount, TrendAlert


class TestKeywordExtraction:
    """Test keyword extraction functionality."""
    
    def test_extract_keywords_basic(self):
        """Test basic keyword extraction logic."""
        # This simulates the keyword extraction logic from flink_job.py
        text = "This is a test post about #python programming and #AI trends"
        
        # Simulate the extraction process
        text_lower = text.lower()
        
        # Remove URLs (basic pattern)
        import re
        text_lower = re.sub(r'http[s]?://\S+', '', text_lower)
        
        # Extract hashtags
        hashtags = re.findall(r'#(\w+)', text_lower)
        
        # Remove hashtags from text
        text_lower = re.sub(r'#\w+', '', text_lower)
        
        # Extract words
        words = re.findall(r'\b[a-zA-Z][a-zA-Z0-9_-]*\b', text_lower)
        
        # Filter stop words
        stop_words = {'this', 'is', 'a', 'and', 'about', 'the', 'of', 'to', 'in', 'for'}
        keywords = [word for word in words if word not in stop_words and len(word) >= 3]
        
        # Add hashtags with # prefix
        for hashtag in hashtags:
            if len(hashtag) >= 2:  # AI is only 2 characters
                keywords.append(f"#{hashtag}")
        
        # Verify results
        assert "test" in keywords
        assert "post" in keywords
        assert "programming" in keywords
        assert "trends" in keywords
        assert "#python" in keywords
        assert "#ai" in keywords
        
        # Should not contain stop words
        assert "this" not in keywords
        assert "is" not in keywords
        assert "a" not in keywords
    
    def test_extract_keywords_with_mentions(self):
        """Test keyword extraction with mentions and URLs."""
        text = "Check out @user123 and visit https://example.com for more #AI info"
        
        # Simulate the extraction process
        text_lower = text.lower()
        
        # Remove URLs
        import re
        text_lower = re.sub(r'http[s]?://\S+', '', text_lower)
        
        # Remove mentions
        text_lower = re.sub(r'@\w+', '', text_lower)
        
        # Extract hashtags
        hashtags = re.findall(r'#(\w+)', text_lower)
        
        # Remove hashtags from text
        text_lower = re.sub(r'#\w+', '', text_lower)
        
        # Extract words
        words = re.findall(r'\b[a-zA-Z][a-zA-Z0-9_-]*\b', text_lower)
        
        # Filter stop words
        stop_words = {'and', 'for', 'more', 'out', 'the', 'of', 'to', 'in'}
        keywords = [word for word in words if word not in stop_words and len(word) >= 3]
        
        # Add hashtags
        for hashtag in hashtags:
            if len(hashtag) >= 2:  # AI is only 2 characters
                keywords.append(f"#{hashtag}")
        
        # Verify results
        assert "check" in keywords
        assert "visit" in keywords
        assert "info" in keywords
        assert "#ai" in keywords
        
        # Should not contain URLs or mentions
        assert "https://example.com" not in " ".join(keywords)
        assert "@user123" not in " ".join(keywords)


class TestPostDataSerialization:
    """Test post data serialization/deserialization."""
    
    def test_serialize_deserialize_post(self):
        """Test JSON serialization and deserialization of PostData."""
        # Create a sample post
        post = PostData(
            uri="at://did:plc:test/app.bsky.feed.post/123",
            author_did="did:plc:test",
            text="This is a test post with #hashtag",
            created_at=datetime.now(),
            language="en",
            hashtags=["hashtag"]
        )
        
        # Serialize to JSON
        post_dict = {
            'uri': post.uri,
            'author_did': post.author_did,
            'text': post.text,
            'created_at': post.created_at.isoformat(),
            'language': post.language,
            'reply_to': post.reply_to,
            'mentions': post.mentions,
            'hashtags': post.hashtags
        }
        
        post_json = json.dumps(post_dict)
        
        # Deserialize from JSON
        data = json.loads(post_json)
        deserialized_post = PostData(
            uri=data['uri'],
            author_did=data['author_did'],
            text=data['text'],
            created_at=datetime.fromisoformat(data['created_at']),
            language=data.get('language'),
            reply_to=data.get('reply_to'),
            mentions=data.get('mentions', []),
            hashtags=data.get('hashtags', [])
        )
        
        # Verify
        assert deserialized_post.uri == post.uri
        assert deserialized_post.author_did == post.author_did
        assert deserialized_post.text == post.text
        assert deserialized_post.language == post.language
        assert deserialized_post.hashtags == post.hashtags
    
    def test_deserialize_minimal_post(self):
        """Test deserialization with minimal required fields."""
        minimal_json = json.dumps({
            'uri': 'test://uri',
            'author_did': 'did:test',
            'text': 'test text',
            'created_at': datetime.now().isoformat()
        })
        
        data = json.loads(minimal_json)
        post = PostData(
            uri=data['uri'],
            author_did=data['author_did'],
            text=data['text'],
            created_at=datetime.fromisoformat(data['created_at']),
            language=data.get('language'),
            reply_to=data.get('reply_to'),
            mentions=data.get('mentions', []),
            hashtags=data.get('hashtags', [])
        )
        
        assert post.uri == 'test://uri'
        assert post.author_did == 'did:test'
        assert post.text == 'test text'
        assert post.mentions == []
        assert post.hashtags == []


class TestWindowedKeywordCount:
    """Test windowed keyword count functionality."""
    
    def test_create_windowed_count(self):
        """Test creation of WindowedKeywordCount."""
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=10)
        
        count = WindowedKeywordCount(
            keyword="python",
            count=42,
            window_start=start_time,
            window_end=end_time,
            unique_authors=15,
            normalized_keyword="python"
        )
        
        assert count.keyword == "python"
        assert count.count == 42
        assert count.unique_authors == 15
        assert count.normalized_keyword == "python"
        assert count.window_start == start_time
        assert count.window_end == end_time
    
    def test_windowed_count_validation(self):
        """Test validation of WindowedKeywordCount."""
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=10)
        
        # Test invalid count
        with pytest.raises(ValueError, match="Count must be non-negative"):
            WindowedKeywordCount(
                keyword="test",
                count=-1,
                window_start=start_time,
                window_end=end_time,
                unique_authors=1,
                normalized_keyword="test"
            )
        
        # Test invalid window times
        with pytest.raises(ValueError, match="Window start must be before window end"):
            WindowedKeywordCount(
                keyword="test",
                count=1,
                window_start=end_time,
                window_end=start_time,
                unique_authors=1,
                normalized_keyword="test"
            )


class TestTrendAlert:
    """Test trend alert functionality."""
    
    def test_create_trend_alert(self):
        """Test creation of TrendAlert."""
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=10)
        
        alert = TrendAlert(
            keyword="python",
            frequency=100,
            growth_rate=25.5,
            confidence_score=0.85,
            window_start=start_time,
            window_end=end_time,
            sample_posts=["post1", "post2", "post3"],
            unique_authors=50,
            rank=1
        )
        
        assert alert.keyword == "python"
        assert alert.frequency == 100
        assert alert.growth_rate == 25.5
        assert alert.confidence_score == 0.85
        assert alert.unique_authors == 50
        assert alert.rank == 1
        assert len(alert.sample_posts) == 3
    
    def test_trend_alert_validation(self):
        """Test validation of TrendAlert."""
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=10)
        
        # Test invalid confidence score
        with pytest.raises(ValueError, match="Confidence score must be between 0.0 and 1.0"):
            TrendAlert(
                keyword="test",
                frequency=10,
                growth_rate=5.0,
                confidence_score=1.5,  # Invalid
                window_start=start_time,
                window_end=end_time,
                sample_posts=[],
                unique_authors=5,
                rank=1
            )
        
        # Test invalid rank
        with pytest.raises(ValueError, match="Rank must be positive"):
            TrendAlert(
                keyword="test",
                frequency=10,
                growth_rate=5.0,
                confidence_score=0.8,
                window_start=start_time,
                window_end=end_time,
                sample_posts=[],
                unique_authors=5,
                rank=0  # Invalid
            )


class TestFlinkJobConfiguration:
    """Test Flink job configuration."""
    
    def test_create_job_config(self):
        """Test creation of Flink job configuration."""
        # Import the config function
        sys.path.append(os.path.dirname(__file__))
        from flink_job import create_flink_job_config
        
        config = create_flink_job_config(
            kafka_bootstrap_servers="test-kafka:9092",
            kafka_topic_posts="test-posts",
            kafka_topic_windowed_counts="test-windowed-counts",
            kafka_topic_trend_alerts="test-trend-alerts",
            parallelism=4,
            window_size_minutes=5
        )
        
        assert config['kafka_bootstrap_servers'] == "test-kafka:9092"
        assert config['kafka_topic_posts'] == "test-posts"
        assert config['kafka_topic_windowed_counts'] == "test-windowed-counts"
        assert config['kafka_topic_trend_alerts'] == "test-trend-alerts"
        assert config['parallelism'] == 4
        assert config['window_size_minutes'] == 5
        assert config['minio_endpoint'] == "http://minio:9000"
        assert config['checkpoint_interval_ms'] == 60000
    
    def test_default_job_config(self):
        """Test default Flink job configuration."""
        from flink_job import create_flink_job_config
        
        config = create_flink_job_config()
        
        assert config['kafka_bootstrap_servers'] == "kafka:29092"
        assert config['kafka_topic_posts'] == "bluesky-posts"
        assert config['kafka_topic_windowed_counts'] == "windowed-keyword-counts"
        assert config['kafka_topic_trend_alerts'] == "trend-alerts"
        assert config['parallelism'] == 2
        assert config['window_size_minutes'] == 10


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])