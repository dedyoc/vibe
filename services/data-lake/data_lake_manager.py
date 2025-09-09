"""Data Lake Manager for MinIO S3-compatible object storage.

This module provides the DataLakeManager class for storing and retrieving
raw posts and processed trend data in a partitioned object storage system.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from pathlib import Path
import asyncio
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from botocore.config import Config

from shared.models import PostData, TrendAlert


logger = logging.getLogger(__name__)


class DataLakeManager:
    """Manages data lake operations using MinIO S3-compatible storage.
    
    This class handles partitioned storage of raw posts and processed trends,
    with automatic lifecycle management and cleanup policies.
    
    Attributes:
        bucket_name: Name of the S3 bucket for data storage
        client: Boto3 S3 client configured for MinIO
        executor: Thread pool for async operations
    """
    
    def __init__(
        self,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket_name: str = "bluesky-data-lake",
        region_name: str = "us-east-1",
        max_workers: int = 10
    ) -> None:
        """Initialize the DataLakeManager.
        
        Args:
            endpoint_url: MinIO endpoint URL (e.g., 'http://localhost:9000')
            access_key: MinIO access key
            secret_key: MinIO secret key
            bucket_name: Name of the bucket to use for storage
            region_name: AWS region name (for compatibility)
            max_workers: Maximum number of worker threads for async operations
        """
        self.bucket_name = bucket_name
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # Configure boto3 client for MinIO
        config = Config(
            region_name=region_name,
            retries={'max_attempts': 3, 'mode': 'adaptive'},
            max_pool_connections=max_workers
        )
        
        self.client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=config
        )
        
        logger.info(f"Initialized DataLakeManager with bucket: {bucket_name}")
    
    async def initialize(self) -> None:
        """Initialize the data lake by creating bucket and setting up lifecycle policies."""
        try:
            await self._create_bucket_if_not_exists()
            await self._setup_lifecycle_policies()
            logger.info("Data lake initialization completed successfully")
        except Exception as e:
            logger.error(f"Failed to initialize data lake: {e}")
            raise
    
    async def _create_bucket_if_not_exists(self) -> None:
        """Create the bucket if it doesn't exist."""
        def _create_bucket() -> None:
            try:
                self.client.head_bucket(Bucket=self.bucket_name)
                logger.info(f"Bucket {self.bucket_name} already exists")
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == '404':
                    self.client.create_bucket(Bucket=self.bucket_name)
                    logger.info(f"Created bucket: {self.bucket_name}")
                else:
                    raise
        
        await asyncio.get_event_loop().run_in_executor(self.executor, _create_bucket)
    
    async def _setup_lifecycle_policies(self) -> None:
        """Set up lifecycle policies for automatic data cleanup."""
        lifecycle_config = {
            'Rules': [
                {
                    'ID': 'raw-posts-cleanup',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': 'raw/posts/'},
                    'Expiration': {'Days': 90},  # Keep raw posts for 90 days
                },
                {
                    'ID': 'processed-trends-cleanup',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': 'processed/trends/'},
                    'Expiration': {'Days': 365},  # Keep processed trends for 1 year
                },
                {
                    'ID': 'transition-to-ia',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': 'processed/'},
                    'Transitions': [
                        {
                            'Days': 30,
                            'StorageClass': 'STANDARD_IA'  # Transition to infrequent access
                        }
                    ]
                }
            ]
        }
        
        def _put_lifecycle() -> None:
            try:
                self.client.put_bucket_lifecycle_configuration(
                    Bucket=self.bucket_name,
                    LifecycleConfiguration=lifecycle_config
                )
                logger.info("Lifecycle policies configured successfully")
            except ClientError as e:
                # MinIO might not support all lifecycle features
                logger.warning(f"Could not set lifecycle policies: {e}")
        
        await asyncio.get_event_loop().run_in_executor(self.executor, _put_lifecycle)
    
    def get_partition_path(self, date: datetime, data_type: str) -> str:
        """Generate partition path based on date and data type.
        
        Args:
            date: Date for partitioning
            data_type: Type of data ('raw/posts' or 'processed/trends')
            
        Returns:
            Partition path in format: data_type/year=YYYY/month=MM/day=DD/hour=HH/
        """
        return (
            f"{data_type}/"
            f"year={date.year:04d}/"
            f"month={date.month:02d}/"
            f"day={date.day:02d}/"
            f"hour={date.hour:02d}/"
        )
    
    async def store_raw_posts(
        self, 
        posts: List[PostData], 
        partition_date: Optional[datetime] = None
    ) -> None:
        """Store raw posts in partitioned object storage.
        
        Args:
            posts: List of PostData objects to store
            partition_date: Date for partitioning (defaults to current time)
        """
        if not posts:
            logger.warning("No posts to store")
            return
        
        if partition_date is None:
            partition_date = datetime.utcnow()
        
        partition_path = self.get_partition_path(partition_date, "raw/posts")
        timestamp = int(partition_date.timestamp())
        object_key = f"{partition_path}posts_{timestamp}.json"
        
        # Convert posts to JSON-serializable format
        posts_data = []
        for post in posts:
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
            posts_data.append(post_dict)
        
        def _upload_posts() -> None:
            try:
                json_data = json.dumps(posts_data, ensure_ascii=False, indent=None)
                self.client.put_object(
                    Bucket=self.bucket_name,
                    Key=object_key,
                    Body=json_data.encode('utf-8'),
                    ContentType='application/json',
                    Metadata={
                        'post_count': str(len(posts)),
                        'partition_date': partition_date.isoformat(),
                        'data_type': 'raw_posts'
                    }
                )
                logger.info(f"Stored {len(posts)} posts to {object_key}")
            except Exception as e:
                logger.error(f"Failed to store posts: {e}")
                raise
        
        await asyncio.get_event_loop().run_in_executor(self.executor, _upload_posts)
    
    async def store_trend_aggregations(
        self, 
        trends: List[TrendAlert], 
        window_time: Optional[datetime] = None
    ) -> None:
        """Store processed trend aggregations in partitioned object storage.
        
        Args:
            trends: List of TrendAlert objects to store
            window_time: Window time for partitioning (defaults to current time)
        """
        if not trends:
            logger.warning("No trends to store")
            return
        
        if window_time is None:
            window_time = datetime.utcnow()
        
        partition_path = self.get_partition_path(window_time, "processed/trends")
        timestamp = int(window_time.timestamp())
        object_key = f"{partition_path}trends_{timestamp}.json"
        
        # Convert trends to JSON-serializable format
        trends_data = []
        for trend in trends:
            trend_dict = {
                'keyword': trend.keyword,
                'frequency': trend.frequency,
                'growth_rate': trend.growth_rate,
                'confidence_score': trend.confidence_score,
                'window_start': trend.window_start.isoformat(),
                'window_end': trend.window_end.isoformat(),
                'sample_posts': trend.sample_posts,
                'unique_authors': trend.unique_authors,
                'rank': trend.rank
            }
            trends_data.append(trend_dict)
        
        def _upload_trends() -> None:
            try:
                json_data = json.dumps(trends_data, ensure_ascii=False, indent=None)
                self.client.put_object(
                    Bucket=self.bucket_name,
                    Key=object_key,
                    Body=json_data.encode('utf-8'),
                    ContentType='application/json',
                    Metadata={
                        'trend_count': str(len(trends)),
                        'window_time': window_time.isoformat(),
                        'data_type': 'processed_trends'
                    }
                )
                logger.info(f"Stored {len(trends)} trends to {object_key}")
            except Exception as e:
                logger.error(f"Failed to store trends: {e}")
                raise
        
        await asyncio.get_event_loop().run_in_executor(self.executor, _upload_trends)
    
    async def query_historical_trends(
        self, 
        start_date: datetime, 
        end_date: datetime,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Query historical trend data from the data lake.
        
        Args:
            start_date: Start date for the query range
            end_date: End date for the query range
            limit: Maximum number of trend records to return
            
        Returns:
            List of trend dictionaries matching the query criteria
        """
        def _query_trends() -> List[Dict[str, Any]]:
            trends = []
            
            # Generate all possible partition paths in the date range
            current_date = start_date.replace(minute=0, second=0, microsecond=0)
            while current_date <= end_date:
                partition_path = self.get_partition_path(current_date, "processed/trends")
                
                try:
                    # List objects in this partition
                    response = self.client.list_objects_v2(
                        Bucket=self.bucket_name,
                        Prefix=partition_path
                    )
                    
                    if 'Contents' in response:
                        for obj in response['Contents']:
                            # Download and parse each trend file
                            try:
                                obj_response = self.client.get_object(
                                    Bucket=self.bucket_name,
                                    Key=obj['Key']
                                )
                                content = obj_response['Body'].read().decode('utf-8')
                                trend_data = json.loads(content)
                                trends.extend(trend_data)
                                
                                if limit and len(trends) >= limit:
                                    return trends[:limit]
                                    
                            except Exception as e:
                                logger.warning(f"Failed to read trend file {obj['Key']}: {e}")
                                continue
                                
                except ClientError as e:
                    if e.response['Error']['Code'] != 'NoSuchKey':
                        logger.warning(f"Error querying partition {partition_path}: {e}")
                
                current_date += timedelta(hours=1)
            
            return trends
        
        return await asyncio.get_event_loop().run_in_executor(self.executor, _query_trends)
    
    async def query_raw_posts(
        self,
        start_date: datetime,
        end_date: datetime,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Query raw post data from the data lake.
        
        Args:
            start_date: Start date for the query range
            end_date: End date for the query range
            limit: Maximum number of post records to return
            
        Returns:
            List of post dictionaries matching the query criteria
        """
        def _query_posts() -> List[Dict[str, Any]]:
            posts = []
            
            # Generate all possible partition paths in the date range
            current_date = start_date.replace(minute=0, second=0, microsecond=0)
            while current_date <= end_date:
                partition_path = self.get_partition_path(current_date, "raw/posts")
                
                try:
                    # List objects in this partition
                    response = self.client.list_objects_v2(
                        Bucket=self.bucket_name,
                        Prefix=partition_path
                    )
                    
                    if 'Contents' in response:
                        for obj in response['Contents']:
                            # Download and parse each post file
                            try:
                                obj_response = self.client.get_object(
                                    Bucket=self.bucket_name,
                                    Key=obj['Key']
                                )
                                content = obj_response['Body'].read().decode('utf-8')
                                post_data = json.loads(content)
                                posts.extend(post_data)
                                
                                if limit and len(posts) >= limit:
                                    return posts[:limit]
                                    
                            except Exception as e:
                                logger.warning(f"Failed to read post file {obj['Key']}: {e}")
                                continue
                                
                except ClientError as e:
                    if e.response['Error']['Code'] != 'NoSuchKey':
                        logger.warning(f"Error querying partition {partition_path}: {e}")
                
                current_date += timedelta(hours=1)
            
            return posts
        
        return await asyncio.get_event_loop().run_in_executor(self.executor, _query_posts)
    
    async def cleanup_old_data(self, retention_days: int = 90) -> None:
        """Clean up old data based on retention policy.
        
        Args:
            retention_days: Number of days to retain data
        """
        cutoff_date = datetime.utcnow() - timedelta(days=retention_days)
        
        def _cleanup() -> None:
            try:
                # List all objects older than cutoff date
                paginator = self.client.get_paginator('list_objects_v2')
                page_iterator = paginator.paginate(Bucket=self.bucket_name)
                
                objects_to_delete = []
                for page in page_iterator:
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            if obj['LastModified'].replace(tzinfo=None) < cutoff_date:
                                objects_to_delete.append({'Key': obj['Key']})
                
                # Delete objects in batches
                if objects_to_delete:
                    for i in range(0, len(objects_to_delete), 1000):  # S3 delete limit
                        batch = objects_to_delete[i:i+1000]
                        self.client.delete_objects(
                            Bucket=self.bucket_name,
                            Delete={'Objects': batch}
                        )
                    
                    logger.info(f"Cleaned up {len(objects_to_delete)} old objects")
                else:
                    logger.info("No old objects to clean up")
                    
            except Exception as e:
                logger.error(f"Failed to cleanup old data: {e}")
                raise
        
        await asyncio.get_event_loop().run_in_executor(self.executor, _cleanup)
    
    async def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics for the data lake.
        
        Returns:
            Dictionary containing storage statistics
        """
        def _get_stats() -> Dict[str, Any]:
            try:
                paginator = self.client.get_paginator('list_objects_v2')
                page_iterator = paginator.paginate(Bucket=self.bucket_name)
                
                stats = {
                    'total_objects': 0,
                    'total_size_bytes': 0,
                    'raw_posts_count': 0,
                    'raw_posts_size': 0,
                    'processed_trends_count': 0,
                    'processed_trends_size': 0,
                    'oldest_object': None,
                    'newest_object': None
                }
                
                for page in page_iterator:
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            stats['total_objects'] += 1
                            stats['total_size_bytes'] += obj['Size']
                            
                            # Track oldest and newest objects
                            obj_date = obj['LastModified'].replace(tzinfo=None)
                            if stats['oldest_object'] is None or obj_date < stats['oldest_object']:
                                stats['oldest_object'] = obj_date
                            if stats['newest_object'] is None or obj_date > stats['newest_object']:
                                stats['newest_object'] = obj_date
                            
                            # Categorize by data type
                            if obj['Key'].startswith('raw/posts/'):
                                stats['raw_posts_count'] += 1
                                stats['raw_posts_size'] += obj['Size']
                            elif obj['Key'].startswith('processed/trends/'):
                                stats['processed_trends_count'] += 1
                                stats['processed_trends_size'] += obj['Size']
                
                return stats
                
            except Exception as e:
                logger.error(f"Failed to get storage stats: {e}")
                raise
        
        return await asyncio.get_event_loop().run_in_executor(self.executor, _get_stats)
    
    async def close(self) -> None:
        """Clean up resources."""
        self.executor.shutdown(wait=True)
        logger.info("DataLakeManager closed")