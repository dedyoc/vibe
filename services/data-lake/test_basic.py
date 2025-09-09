#!/usr/bin/env python3
"""Basic test to verify DataLakeManager functionality."""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from data_lake_manager import DataLakeManager
from shared.models import PostData, TrendAlert
from datetime import datetime


def test_basic_functionality():
    """Test basic DataLakeManager functionality."""
    print("Testing DataLakeManager basic functionality...")
    
    # Test instantiation
    manager = DataLakeManager(
        endpoint_url='http://localhost:9000',
        access_key='test',
        secret_key='test'
    )
    print("✓ DataLakeManager instantiated successfully")
    
    # Test partition path generation
    test_date = datetime(2024, 1, 15, 14, 30, 0)
    raw_path = manager.get_partition_path(test_date, 'raw/posts')
    trends_path = manager.get_partition_path(test_date, 'processed/trends')
    
    expected_raw = "raw/posts/year=2024/month=01/day=15/hour=14/"
    expected_trends = "processed/trends/year=2024/month=01/day=15/hour=14/"
    
    assert raw_path == expected_raw, f"Expected {expected_raw}, got {raw_path}"
    assert trends_path == expected_trends, f"Expected {expected_trends}, got {trends_path}"
    print("✓ Partition path generation working correctly")
    
    # Test data model creation
    post = PostData(
        uri='at://test/post/123',
        author_did='did:test',
        text='Test post',
        created_at=datetime.utcnow()
    )
    
    trend = TrendAlert(
        keyword='test',
        frequency=100,
        growth_rate=10.0,
        confidence_score=0.8,
        window_start=datetime.utcnow(),
        window_end=datetime.utcnow(),
        sample_posts=['at://test/post/123'],
        unique_authors=50,
        rank=1
    )
    
    print("✓ Data models created successfully")
    print("✓ All basic functionality working!")
    
    return True


if __name__ == "__main__":
    try:
        test_basic_functionality()
        print("\n🎉 All tests passed!")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)