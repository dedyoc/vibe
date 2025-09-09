#!/usr/bin/env python3
"""Unit tests for DataLakeManager."""

import sys
import os
import asyncio
import json
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from data_lake_manager import DataLakeManager
from shared.models import PostData, TrendAlert


def create_sample_posts():
    """Create sample PostData objects for testing."""
    return [
        PostData(
            uri="at://did:plc:test1/app.bsky.feed.post/123",
            author_did="did:plc:test1",
            text="This is a test post #trending",
            created_at=datetime(2024, 1, 15, 10, 30, 0),
            language="en",
            hashtags=["trending"]
        ),
        PostData(
            uri="at://did:plc:test2/app.bsky.feed.post/456",
            author_did="did:plc:test2",
            text="Another test post with @mention",
            created_at=datetime(2024, 1, 15, 10, 31, 0),
            mentions=["did:plc:mentioned"]
        )
    ]


def create_sample_trends():
    """Create sample TrendAlert objects for testing."""
    return [
        TrendAlert(
            keyword="trending",
            frequency=150,
            growth_rate=25.5,
            confidence_score=0.85,
            window_start=datetime(2024, 1, 15, 10, 0, 0),
            window_end=datetime(2024, 1, 15, 10, 10, 0),
            sample_posts=["at://did:plc:test1/app.bsky.feed.post/123"],
            unique_authors=75,
            rank=1
        )
    ]


async def test_store_raw_posts():
    """Test storing raw posts."""
    print("Testing store_raw_posts...")
    
    with patch('data_lake_manager.boto3.client') as mock_boto3:
        mock_client = Mock()
        mock_boto3.return_value = mock_client
        
        manager = DataLakeManager(
            endpoint_url="http://localhost:9000",
            access_key="test",
            secret_key="test"
        )
        
        posts = create_sample_posts()
        partition_date = datetime(2024, 1, 15, 10, 30, 0)
        
        await manager.store_raw_posts(posts, partition_date)
        
        # Verify S3 put_object was called
        mock_client.put_object.assert_called_once()
        call_args = mock_client.put_object.call_args
        
        # Check the object key format
        expected_key_prefix = "raw/posts/year=2024/month=01/day=15/hour=10/posts_"
        assert call_args[1]['Key'].startswith(expected_key_prefix)
        assert call_args[1]['Bucket'] == "bluesky-data-lake"
        assert call_args[1]['ContentType'] == 'application/json'
        
        # Verify the stored data
        stored_data = json.loads(call_args[1]['Body'].decode('utf-8'))
        assert len(stored_data) == 2
        assert stored_data[0]['uri'] == posts[0].uri
        
        print("✓ store_raw_posts working correctly")


async def test_store_trend_aggregations():
    """Test storing trend aggregations."""
    print("Testing store_trend_aggregations...")
    
    with patch('data_lake_manager.boto3.client') as mock_boto3:
        mock_client = Mock()
        mock_boto3.return_value = mock_client
        
        manager = DataLakeManager(
            endpoint_url="http://localhost:9000",
            access_key="test",
            secret_key="test"
        )
        
        trends = create_sample_trends()
        window_time = datetime(2024, 1, 15, 10, 0, 0)
        
        await manager.store_trend_aggregations(trends, window_time)
        
        # Verify S3 put_object was called
        mock_client.put_object.assert_called_once()
        call_args = mock_client.put_object.call_args
        
        # Check the object key format
        expected_key_prefix = "processed/trends/year=2024/month=01/day=15/hour=10/trends_"
        assert call_args[1]['Key'].startswith(expected_key_prefix)
        
        # Verify the stored data
        stored_data = json.loads(call_args[1]['Body'].decode('utf-8'))
        assert len(stored_data) == 1
        assert stored_data[0]['keyword'] == trends[0].keyword
        
        print("✓ store_trend_aggregations working correctly")


async def test_query_historical_trends():
    """Test querying historical trends."""
    print("Testing query_historical_trends...")
    
    with patch('data_lake_manager.boto3.client') as mock_boto3:
        mock_client = Mock()
        mock_boto3.return_value = mock_client
        
        # Mock S3 responses
        mock_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'processed/trends/year=2024/month=01/day=15/hour=10/trends_123.json'}
            ]
        }
        
        # Mock the trend data
        trend_json = json.dumps([
            {
                'keyword': 'trending',
                'frequency': 150,
                'growth_rate': 25.5,
                'confidence_score': 0.85,
                'window_start': '2024-01-15T10:00:00',
                'window_end': '2024-01-15T10:10:00',
                'sample_posts': ['at://did:plc:test1/app.bsky.feed.post/123'],
                'unique_authors': 75,
                'rank': 1
            }
        ])
        
        mock_response = Mock()
        mock_response.read.return_value = trend_json.encode('utf-8')
        mock_client.get_object.return_value = {'Body': mock_response}
        
        manager = DataLakeManager(
            endpoint_url="http://localhost:9000",
            access_key="test",
            secret_key="test"
        )
        
        start_date = datetime(2024, 1, 15, 10, 0, 0)
        end_date = datetime(2024, 1, 15, 10, 30, 0)  # Same hour to avoid duplicate calls
        
        results = await manager.query_historical_trends(start_date, end_date)
        
        assert len(results) == 1
        assert results[0]['keyword'] == 'trending'
        assert results[0]['frequency'] == 150
        
        print("✓ query_historical_trends working correctly")


async def test_empty_lists():
    """Test handling of empty lists."""
    print("Testing empty list handling...")
    
    with patch('data_lake_manager.boto3.client') as mock_boto3:
        mock_client = Mock()
        mock_boto3.return_value = mock_client
        
        manager = DataLakeManager(
            endpoint_url="http://localhost:9000",
            access_key="test",
            secret_key="test"
        )
        
        # Test empty posts
        await manager.store_raw_posts([])
        mock_client.put_object.assert_not_called()
        
        # Test empty trends
        await manager.store_trend_aggregations([])
        mock_client.put_object.assert_not_called()
        
        print("✓ Empty list handling working correctly")


async def run_all_tests():
    """Run all unit tests."""
    print("Running DataLakeManager unit tests...")
    print("=" * 50)
    
    await test_store_raw_posts()
    await test_store_trend_aggregations()
    await test_query_historical_trends()
    await test_empty_lists()
    
    print("=" * 50)
    print("🎉 All unit tests passed!")


if __name__ == "__main__":
    try:
        asyncio.run(run_all_tests())
    except Exception as e:
        print(f"\n❌ Tests failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)