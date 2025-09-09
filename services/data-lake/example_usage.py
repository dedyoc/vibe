"""Example usage of DataLakeManager.

This script demonstrates how to use the DataLakeManager for storing
and retrieving data from MinIO object storage.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List

from data_lake_manager import DataLakeManager
from shared.models import PostData, TrendAlert


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def create_sample_data() -> tuple[List[PostData], List[TrendAlert]]:
    """Create sample data for demonstration."""
    # Sample posts
    posts = [
        PostData(
            uri="at://did:plc:example1/app.bsky.feed.post/123",
            author_did="did:plc:example1",
            text="This is a sample post about #AI and #MachineLearning",
            created_at=datetime.utcnow() - timedelta(minutes=5),
            language="en",
            hashtags=["AI", "MachineLearning"]
        ),
        PostData(
            uri="at://did:plc:example2/app.bsky.feed.post/456",
            author_did="did:plc:example2",
            text="Another post discussing @someone about trending topics",
            created_at=datetime.utcnow() - timedelta(minutes=3),
            mentions=["did:plc:someone"]
        ),
        PostData(
            uri="at://did:plc:example3/app.bsky.feed.post/789",
            author_did="did:plc:example3",
            text="AI is revolutionizing everything! #AI #Tech #Future",
            created_at=datetime.utcnow() - timedelta(minutes=1),
            language="en",
            hashtags=["AI", "Tech", "Future"]
        )
    ]
    
    # Sample trends
    trends = [
        TrendAlert(
            keyword="AI",
            frequency=250,
            growth_rate=45.2,
            confidence_score=0.92,
            window_start=datetime.utcnow() - timedelta(minutes=10),
            window_end=datetime.utcnow(),
            sample_posts=[
                "at://did:plc:example1/app.bsky.feed.post/123",
                "at://did:plc:example3/app.bsky.feed.post/789"
            ],
            unique_authors=180,
            rank=1
        ),
        TrendAlert(
            keyword="MachineLearning",
            frequency=120,
            growth_rate=22.8,
            confidence_score=0.78,
            window_start=datetime.utcnow() - timedelta(minutes=10),
            window_end=datetime.utcnow(),
            sample_posts=["at://did:plc:example1/app.bsky.feed.post/123"],
            unique_authors=95,
            rank=2
        )
    ]
    
    return posts, trends


async def demonstrate_data_lake_operations():
    """Demonstrate DataLakeManager operations."""
    # Initialize DataLakeManager
    data_lake = DataLakeManager(
        endpoint_url="http://localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin123",
        bucket_name="bluesky-demo-lake"
    )
    
    try:
        logger.info("Initializing data lake...")
        await data_lake.initialize()
        
        # Create sample data
        logger.info("Creating sample data...")
        posts, trends = await create_sample_data()
        
        # Store raw posts
        logger.info(f"Storing {len(posts)} raw posts...")
        await data_lake.store_raw_posts(posts)
        
        # Store trend aggregations
        logger.info(f"Storing {len(trends)} trend aggregations...")
        await data_lake.store_trend_aggregations(trends)
        
        # Query historical data
        logger.info("Querying historical data...")
        start_date = datetime.utcnow() - timedelta(hours=1)
        end_date = datetime.utcnow() + timedelta(hours=1)
        
        retrieved_posts = await data_lake.query_raw_posts(start_date, end_date)
        retrieved_trends = await data_lake.query_historical_trends(start_date, end_date)
        
        logger.info(f"Retrieved {len(retrieved_posts)} posts")
        logger.info(f"Retrieved {len(retrieved_trends)} trends")
        
        # Display some retrieved data
        if retrieved_posts:
            logger.info("Sample retrieved post:")
            post = retrieved_posts[0]
            logger.info(f"  URI: {post['uri']}")
            logger.info(f"  Author: {post['author_did']}")
            logger.info(f"  Text: {post['text'][:50]}...")
        
        if retrieved_trends:
            logger.info("Sample retrieved trend:")
            trend = retrieved_trends[0]
            logger.info(f"  Keyword: {trend['keyword']}")
            logger.info(f"  Frequency: {trend['frequency']}")
            logger.info(f"  Growth Rate: {trend['growth_rate']}%")
        
        # Get storage statistics
        logger.info("Getting storage statistics...")
        stats = await data_lake.get_storage_stats()
        logger.info(f"Storage stats:")
        logger.info(f"  Total objects: {stats['total_objects']}")
        logger.info(f"  Total size: {stats['total_size_bytes']} bytes")
        logger.info(f"  Raw posts: {stats['raw_posts_count']} objects, {stats['raw_posts_size']} bytes")
        logger.info(f"  Processed trends: {stats['processed_trends_count']} objects, {stats['processed_trends_size']} bytes")
        
        # Demonstrate partition path generation
        logger.info("Partition path examples:")
        test_date = datetime(2024, 1, 15, 14, 30, 0)
        raw_path = data_lake.get_partition_path(test_date, "raw/posts")
        trends_path = data_lake.get_partition_path(test_date, "processed/trends")
        logger.info(f"  Raw posts path: {raw_path}")
        logger.info(f"  Trends path: {trends_path}")
        
        logger.info("Data lake operations completed successfully!")
        
    except Exception as e:
        logger.error(f"Error during data lake operations: {e}")
        raise
    finally:
        await data_lake.close()


async def demonstrate_cleanup_operations():
    """Demonstrate data cleanup operations."""
    data_lake = DataLakeManager(
        endpoint_url="http://localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin123",
        bucket_name="bluesky-demo-lake"
    )
    
    try:
        await data_lake.initialize()
        
        logger.info("Demonstrating cleanup operations...")
        
        # Note: This would clean up data older than 30 days
        # For demo purposes, we'll use a very short retention period
        logger.info("Running cleanup for data older than 30 days...")
        await data_lake.cleanup_old_data(retention_days=30)
        
        logger.info("Cleanup operations completed!")
        
    except Exception as e:
        logger.error(f"Error during cleanup operations: {e}")
        raise
    finally:
        await data_lake.close()


if __name__ == "__main__":
    print("DataLakeManager Example Usage")
    print("=" * 40)
    print()
    print("This example demonstrates:")
    print("1. Initializing the data lake")
    print("2. Storing raw posts and trend data")
    print("3. Querying historical data")
    print("4. Getting storage statistics")
    print("5. Cleanup operations")
    print()
    print("Prerequisites:")
    print("- MinIO running on localhost:9000")
    print("- Default credentials: minioadmin/minioadmin123")
    print()
    
    try:
        # Run the main demonstration
        asyncio.run(demonstrate_data_lake_operations())
        
        print("\n" + "=" * 40)
        print("Running cleanup demonstration...")
        asyncio.run(demonstrate_cleanup_operations())
        
    except KeyboardInterrupt:
        print("\nDemo interrupted by user")
    except Exception as e:
        print(f"\nDemo failed: {e}")
        print("Make sure MinIO is running on localhost:9000")