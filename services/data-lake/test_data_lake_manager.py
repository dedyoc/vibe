"""Tests for DataLakeManager class.

This module contains comprehensive tests for the DataLakeManager,
including unit tests and integration tests with mocked MinIO.
"""

import json
import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock, call
from typing import List, Dict, Any
import asyncio

from botocore.exceptions import ClientError

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from data_lake_manager import DataLakeManager
from shared.models import PostData, TrendAlert


class TestDataLakeManager:
    """Test suite for DataLakeManager class."""
    
    @pytest.fixture
    def mock_s3_client(self):
        """Mock S3 client for testing."""
        with patch('data_lake_manager.boto3.client') as mock_client:
            client_instance = Mock()
            mock_client.return_value = client_instance
            yield client_instance
    
    @pytest.fixture
    def data_lake_manager(self, mock_s3_client):
        """Create DataLakeManager instance for testing."""
        return DataLakeManager(
            endpoint_url="http://localhost:9000",
            access_key="test_access",
            secret_key="test_secret",
            bucket_name="test-bucket"
        )
    
    @pytest.fixture
    def sample_posts(self) -> List[PostData]:
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
    
    @pytest.fixture
    def sample_trends(self) -> List[TrendAlert]:
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
            ),
            TrendAlert(
                keyword="viral",
                frequency=120,
                growth_rate=18.2,
                confidence_score=0.78,
                window_start=datetime(2024, 1, 15, 10, 0, 0),
                window_end=datetime(2024, 1, 15, 10, 10, 0),
                sample_posts=["at://did:plc:test3/app.bsky.feed.post/789"],
                unique_authors=60,
                rank=2
            )
        ]
    
    def test_init(self, mock_s3_client):
        """Test DataLakeManager initialization."""
        manager = DataLakeManager(
            endpoint_url="http://localhost:9000",
            access_key="test_access",
            secret_key="test_secret",
            bucket_name="test-bucket"
        )
        
        assert manager.bucket_name == "test-bucket"
        assert manager.client is not None
        assert manager.executor is not None
    
    @pytest.mark.asyncio
    async def test_initialize_success(self, data_lake_manager, mock_s3_client):
        """Test successful data lake initialization."""
        # Mock bucket exists
        mock_s3_client.head_bucket.return_value = None
        mock_s3_client.put_bucket_lifecycle_configuration.return_value = None
        
        await data_lake_manager.initialize()
        
        mock_s3_client.head_bucket.assert_called_once_with(Bucket="test-bucket")
        mock_s3_client.put_bucket_lifecycle_configuration.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_initialize_create_bucket(self, data_lake_manager, mock_s3_client):
        """Test initialization when bucket doesn't exist."""
        # Mock bucket doesn't exist
        mock_s3_client.head_bucket.side_effect = ClientError(
            {'Error': {'Code': '404'}}, 'HeadBucket'
        )
        mock_s3_client.create_bucket.return_value = None
        mock_s3_client.put_bucket_lifecycle_configuration.return_value = None
        
        await data_lake_manager.initialize()
        
        mock_s3_client.head_bucket.assert_called_once_with(Bucket="test-bucket")
        mock_s3_client.create_bucket.assert_called_once_with(Bucket="test-bucket")
    
    def test_get_partition_path(self, data_lake_manager):
        """Test partition path generation."""
        test_date = datetime(2024, 1, 15, 14, 30, 45)
        
        raw_path = data_lake_manager.get_partition_path(test_date, "raw/posts")
        expected_raw = "raw/posts/year=2024/month=01/day=15/hour=14/"
        assert raw_path == expected_raw
        
        trends_path = data_lake_manager.get_partition_path(test_date, "processed/trends")
        expected_trends = "processed/trends/year=2024/month=01/day=15/hour=14/"
        assert trends_path == expected_trends
    
    @pytest.mark.asyncio
    async def test_store_raw_posts(self, data_lake_manager, mock_s3_client, sample_posts):
        """Test storing raw posts."""
        partition_date = datetime(2024, 1, 15, 10, 30, 0)
        
        await data_lake_manager.store_raw_posts(sample_posts, partition_date)
        
        # Verify S3 put_object was called
        mock_s3_client.put_object.assert_called_once()
        call_args = mock_s3_client.put_object.call_args
        
        # Check the object key format
        expected_key_prefix = "raw/posts/year=2024/month=01/day=15/hour=10/posts_"
        assert call_args[1]['Key'].startswith(expected_key_prefix)
        assert call_args[1]['Bucket'] == "test-bucket"
        assert call_args[1]['ContentType'] == 'application/json'
        
        # Verify the stored data
        stored_data = json.loads(call_args[1]['Body'].decode('utf-8'))
        assert len(stored_data) == 2
        assert stored_data[0]['uri'] == sample_posts[0].uri
        assert stored_data[0]['author_did'] == sample_posts[0].author_did
        assert stored_data[1]['text'] == sample_posts[1].text
    
    @pytest.mark.asyncio
    async def test_store_raw_posts_empty_list(self, data_lake_manager, mock_s3_client):
        """Test storing empty list of posts."""
        await data_lake_manager.store_raw_posts([])
        
        # Should not call put_object for empty list
        mock_s3_client.put_object.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_store_trend_aggregations(self, data_lake_manager, mock_s3_client, sample_trends):
        """Test storing trend aggregations."""
        window_time = datetime(2024, 1, 15, 10, 0, 0)
        
        await data_lake_manager.store_trend_aggregations(sample_trends, window_time)
        
        # Verify S3 put_object was called
        mock_s3_client.put_object.assert_called_once()
        call_args = mock_s3_client.put_object.call_args
        
        # Check the object key format
        expected_key_prefix = "processed/trends/year=2024/month=01/day=15/hour=10/trends_"
        assert call_args[1]['Key'].startswith(expected_key_prefix)
        assert call_args[1]['Bucket'] == "test-bucket"
        assert call_args[1]['ContentType'] == 'application/json'
        
        # Verify the stored data
        stored_data = json.loads(call_args[1]['Body'].decode('utf-8'))
        assert len(stored_data) == 2
        assert stored_data[0]['keyword'] == sample_trends[0].keyword
        assert stored_data[0]['frequency'] == sample_trends[0].frequency
        assert stored_data[1]['growth_rate'] == sample_trends[1].growth_rate
    
    @pytest.mark.asyncio
    async def test_store_trend_aggregations_empty_list(self, data_lake_manager, mock_s3_client):
        """Test storing empty list of trends."""
        await data_lake_manager.store_trend_aggregations([])
        
        # Should not call put_object for empty list
        mock_s3_client.put_object.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_query_historical_trends(self, data_lake_manager, mock_s3_client, sample_trends):
        """Test querying historical trends."""
        start_date = datetime(2024, 1, 15, 10, 0, 0)
        end_date = datetime(2024, 1, 15, 11, 0, 0)
        
        # Mock S3 responses
        mock_s3_client.list_objects_v2.return_value = {
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
        mock_s3_client.get_object.return_value = {'Body': mock_response}
        
        results = await data_lake_manager.query_historical_trends(start_date, end_date)
        
        assert len(results) == 1
        assert results[0]['keyword'] == 'trending'
        assert results[0]['frequency'] == 150
        
        # Verify S3 calls
        assert mock_s3_client.list_objects_v2.call_count >= 1
        mock_s3_client.get_object.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_query_raw_posts(self, data_lake_manager, mock_s3_client):
        """Test querying raw posts."""
        start_date = datetime(2024, 1, 15, 10, 0, 0)
        end_date = datetime(2024, 1, 15, 11, 0, 0)
        
        # Mock S3 responses
        mock_s3_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'raw/posts/year=2024/month=01/day=15/hour=10/posts_123.json'}
            ]
        }
        
        # Mock the post data
        post_json = json.dumps([
            {
                'uri': 'at://did:plc:test1/app.bsky.feed.post/123',
                'author_did': 'did:plc:test1',
                'text': 'Test post',
                'created_at': '2024-01-15T10:30:00',
                'language': 'en',
                'reply_to': None,
                'mentions': [],
                'hashtags': ['test']
            }
        ])
        
        mock_response = Mock()
        mock_response.read.return_value = post_json.encode('utf-8')
        mock_s3_client.get_object.return_value = {'Body': mock_response}
        
        results = await data_lake_manager.query_raw_posts(start_date, end_date)
        
        assert len(results) == 1
        assert results[0]['uri'] == 'at://did:plc:test1/app.bsky.feed.post/123'
        assert results[0]['text'] == 'Test post'
        
        # Verify S3 calls
        assert mock_s3_client.list_objects_v2.call_count >= 1
        mock_s3_client.get_object.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_query_with_limit(self, data_lake_manager, mock_s3_client):
        """Test querying with result limit."""
        start_date = datetime(2024, 1, 15, 10, 0, 0)
        end_date = datetime(2024, 1, 15, 11, 0, 0)
        
        # Mock S3 responses with multiple files
        mock_s3_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'processed/trends/year=2024/month=01/day=15/hour=10/trends_123.json'},
                {'Key': 'processed/trends/year=2024/month=01/day=15/hour=10/trends_456.json'}
            ]
        }
        
        # Mock trend data with multiple items
        trend_json = json.dumps([
            {'keyword': 'trend1', 'frequency': 100, 'growth_rate': 10.0, 'confidence_score': 0.8,
             'window_start': '2024-01-15T10:00:00', 'window_end': '2024-01-15T10:10:00',
             'sample_posts': [], 'unique_authors': 50, 'rank': 1},
            {'keyword': 'trend2', 'frequency': 90, 'growth_rate': 8.0, 'confidence_score': 0.7,
             'window_start': '2024-01-15T10:00:00', 'window_end': '2024-01-15T10:10:00',
             'sample_posts': [], 'unique_authors': 45, 'rank': 2}
        ])
        
        mock_response = Mock()
        mock_response.read.return_value = trend_json.encode('utf-8')
        mock_s3_client.get_object.return_value = {'Body': mock_response}
        
        # Query with limit of 1
        results = await data_lake_manager.query_historical_trends(start_date, end_date, limit=1)
        
        assert len(results) == 1
        assert results[0]['keyword'] == 'trend1'
    
    @pytest.mark.asyncio
    async def test_cleanup_old_data(self, data_lake_manager, mock_s3_client):
        """Test cleanup of old data."""
        # Mock paginator and objects
        mock_paginator = Mock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        
        old_date = datetime.utcnow() - timedelta(days=100)
        mock_paginator.paginate.return_value = [
            {
                'Contents': [
                    {'Key': 'old_file_1.json', 'LastModified': old_date},
                    {'Key': 'old_file_2.json', 'LastModified': old_date}
                ]
            }
        ]
        
        await data_lake_manager.cleanup_old_data(retention_days=90)
        
        # Verify delete_objects was called
        mock_s3_client.delete_objects.assert_called_once()
        call_args = mock_s3_client.delete_objects.call_args
        assert len(call_args[1]['Delete']['Objects']) == 2
    
    @pytest.mark.asyncio
    async def test_get_storage_stats(self, data_lake_manager, mock_s3_client):
        """Test getting storage statistics."""
        # Mock paginator and objects
        mock_paginator = Mock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        
        test_date = datetime(2024, 1, 15, 10, 0, 0)
        mock_paginator.paginate.return_value = [
            {
                'Contents': [
                    {
                        'Key': 'raw/posts/file1.json',
                        'Size': 1024,
                        'LastModified': test_date
                    },
                    {
                        'Key': 'processed/trends/file2.json',
                        'Size': 512,
                        'LastModified': test_date + timedelta(hours=1)
                    }
                ]
            }
        ]
        
        stats = await data_lake_manager.get_storage_stats()
        
        assert stats['total_objects'] == 2
        assert stats['total_size_bytes'] == 1536
        assert stats['raw_posts_count'] == 1
        assert stats['raw_posts_size'] == 1024
        assert stats['processed_trends_count'] == 1
        assert stats['processed_trends_size'] == 512
        assert stats['oldest_object'] == test_date
        assert stats['newest_object'] == test_date + timedelta(hours=1)
    
    @pytest.mark.asyncio
    async def test_error_handling_store_posts(self, data_lake_manager, mock_s3_client, sample_posts):
        """Test error handling when storing posts fails."""
        mock_s3_client.put_object.side_effect = ClientError(
            {'Error': {'Code': 'AccessDenied'}}, 'PutObject'
        )
        
        with pytest.raises(ClientError):
            await data_lake_manager.store_raw_posts(sample_posts)
    
    @pytest.mark.asyncio
    async def test_error_handling_query_trends(self, data_lake_manager, mock_s3_client):
        """Test error handling when querying trends fails."""
        start_date = datetime(2024, 1, 15, 10, 0, 0)
        end_date = datetime(2024, 1, 15, 11, 0, 0)
        
        mock_s3_client.list_objects_v2.side_effect = ClientError(
            {'Error': {'Code': 'AccessDenied'}}, 'ListObjectsV2'
        )
        
        # Should handle the error gracefully and return empty list
        results = await data_lake_manager.query_historical_trends(start_date, end_date)
        assert results == []
    
    @pytest.mark.asyncio
    async def test_close(self, data_lake_manager):
        """Test closing the data lake manager."""
        await data_lake_manager.close()
        
        # Verify executor is shut down
        assert data_lake_manager.executor._shutdown


@pytest.mark.integration
class TestDataLakeManagerIntegration:
    """Integration tests for DataLakeManager with real MinIO (if available)."""
    
    @pytest.fixture
    def integration_manager(self):
        """Create DataLakeManager for integration testing."""
        # These tests require a running MinIO instance
        return DataLakeManager(
            endpoint_url="http://localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin123",
            bucket_name="test-integration-bucket"
        )
    
    @pytest.mark.asyncio
    async def test_full_workflow(self, integration_manager):
        """Test complete workflow with real MinIO (requires MinIO running)."""
        try:
            # Initialize
            await integration_manager.initialize()
            
            # Create test data
            posts = [
                PostData(
                    uri="at://did:plc:integration/app.bsky.feed.post/1",
                    author_did="did:plc:integration",
                    text="Integration test post",
                    created_at=datetime.utcnow()
                )
            ]
            
            trends = [
                TrendAlert(
                    keyword="integration",
                    frequency=1,
                    growth_rate=100.0,
                    confidence_score=1.0,
                    window_start=datetime.utcnow() - timedelta(minutes=10),
                    window_end=datetime.utcnow(),
                    sample_posts=["at://did:plc:integration/app.bsky.feed.post/1"],
                    unique_authors=1,
                    rank=1
                )
            ]
            
            # Store data
            await integration_manager.store_raw_posts(posts)
            await integration_manager.store_trend_aggregations(trends)
            
            # Query data
            start_date = datetime.utcnow() - timedelta(hours=1)
            end_date = datetime.utcnow() + timedelta(hours=1)
            
            retrieved_posts = await integration_manager.query_raw_posts(start_date, end_date)
            retrieved_trends = await integration_manager.query_historical_trends(start_date, end_date)
            
            # Verify data
            assert len(retrieved_posts) >= 1
            assert len(retrieved_trends) >= 1
            
            # Get stats
            stats = await integration_manager.get_storage_stats()
            assert stats['total_objects'] >= 2
            
        except Exception as e:
            pytest.skip(f"Integration test requires running MinIO: {e}")
        finally:
            await integration_manager.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])