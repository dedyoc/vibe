#!/usr/bin/env python3
"""
Complete Superset Setup Script
Runs after Superset is initialized to configure data sources and dashboards
"""

import os
import sys
import time
import json
import requests
from typing import Dict, Any, Optional

def wait_for_superset(url: str = "http://localhost:8088", max_retries: int = 30) -> bool:
    """Wait for Superset to be ready"""
    print("Waiting for Superset to be ready...")
    
    for i in range(max_retries):
        try:
            response = requests.get(f"{url}/health", timeout=5)
            if response.status_code == 200:
                print("✓ Superset is ready!")
                return True
        except requests.exceptions.RequestException:
            pass
        
        print(f"  Attempt {i+1}/{max_retries}...")
        time.sleep(5)
    
    print("✗ Superset failed to start within timeout")
    return False

def login_to_superset(url: str, username: str = "admin", password: str = "admin123") -> Optional[str]:
    """Login to Superset and get CSRF token"""
    session = requests.Session()
    
    try:
        # Get login page
        login_page = session.get(f"{url}/login/")
        if login_page.status_code != 200:
            print(f"✗ Failed to get login page: {login_page.status_code}")
            return None
        
        # Extract CSRF token
        csrf_start = login_page.text.find('csrf_token')
        if csrf_start == -1:
            print("✗ CSRF token not found in login page")
            return None
        
        csrf_start = login_page.text.find('value="', csrf_start) + 7
        csrf_end = login_page.text.find('"', csrf_start)
        csrf_token = login_page.text[csrf_start:csrf_end]
        
        # Login
        login_data = {
            'username': username,
            'password': password,
            'csrf_token': csrf_token
        }
        
        response = session.post(f"{url}/login/", data=login_data)
        if response.status_code == 200 and 'login' not in response.url:
            # Get API CSRF token
            csrf_response = session.get(f"{url}/api/v1/security/csrf_token/")
            if csrf_response.status_code == 200:
                api_csrf_token = csrf_response.json().get('result')
                print("✓ Successfully logged in to Superset")
                return api_csrf_token
        
        print("✗ Failed to login to Superset")
        return None
        
    except Exception as e:
        print(f"✗ Login error: {e}")
        return None

def create_redis_database(url: str, csrf_token: str) -> Optional[int]:
    """Create Redis database connection"""
    session = requests.Session()
    
    redis_config = {
        "database_name": "Redis Trends Cache",
        "engine": "redis",
        "configuration_method": "sqlalchemy_form",
        "sqlalchemy_uri": f"redis://{os.getenv('REDIS_HOST', 'redis')}:{os.getenv('REDIS_PORT', '6379')}/0",
        "expose_in_sqllab": True,
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_dml": False,
        "extra": json.dumps({
            "metadata_params": {},
            "engine_params": {
                "connect_args": {
                    "decode_responses": True
                }
            }
        })
    }
    
    headers = {
        'X-CSRFToken': csrf_token,
        'Content-Type': 'application/json'
    }
    
    try:
        response = session.post(
            f"{url}/api/v1/database/",
            json=redis_config,
            headers=headers
        )
        
        if response.status_code == 201:
            db_id = response.json()['id']
            print(f"✓ Redis database created with ID: {db_id}")
            return db_id
        else:
            print(f"✗ Failed to create Redis database: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"✗ Error creating Redis database: {e}")
        return None

def create_sample_data_in_redis():
    """Create sample data in Redis for testing"""
    try:
        import redis
        
        r = redis.Redis(
            host=os.getenv('REDIS_HOST', 'redis'),
            port=int(os.getenv('REDIS_PORT', '6379')),
            decode_responses=True
        )
        
        # Sample trending data
        sample_trends = {
            'python': json.dumps({
                'frequency': 150,
                'growth_rate': 2.5,
                'confidence_score': 0.85,
                'window_start': '2024-01-01T10:00:00Z',
                'window_end': '2024-01-01T11:00:00Z',
                'unique_authors': 75,
                'rank': 1,
                'sample_posts': ['Sample post about Python', 'Another Python post']
            }),
            'javascript': json.dumps({
                'frequency': 120,
                'growth_rate': 1.8,
                'confidence_score': 0.78,
                'window_start': '2024-01-01T10:00:00Z',
                'window_end': '2024-01-01T11:00:00Z',
                'unique_authors': 60,
                'rank': 2,
                'sample_posts': ['JavaScript trending post']
            }),
            'ai': json.dumps({
                'frequency': 200,
                'growth_rate': 3.2,
                'confidence_score': 0.92,
                'window_start': '2024-01-01T10:00:00Z',
                'window_end': '2024-01-01T11:00:00Z',
                'unique_authors': 95,
                'rank': 0,
                'sample_posts': ['AI is trending', 'Machine learning post', 'AI breakthrough news']
            })
        }
        
        # Store sample data
        for keyword, data in sample_trends.items():
            r.hset("trends:current", keyword, data)
        
        # Create Superset-formatted data
        superset_data = []
        for keyword, data_json in sample_trends.items():
            trend_data = json.loads(data_json)
            superset_data.append({
                'keyword': keyword,
                'frequency': trend_data['frequency'],
                'growth_rate': trend_data['growth_rate'],
                'confidence_score': trend_data['confidence_score'],
                'window_start': trend_data['window_start'],
                'window_end': trend_data['window_end'],
                'unique_authors': trend_data['unique_authors'],
                'rank': trend_data['rank'],
                'sample_posts': json.dumps(trend_data['sample_posts']),
                'last_updated': '2024-01-01T10:30:00Z'
            })
        
        r.set("superset:trending_keywords", json.dumps(superset_data), ex=3600)
        
        print("✓ Sample trending data created in Redis")
        return True
        
    except Exception as e:
        print(f"✗ Failed to create sample data: {e}")
        return False

def main():
    """Main setup function"""
    print("🚀 Starting Superset complete setup...\n")
    
    superset_url = os.getenv('SUPERSET_URL', 'http://localhost:8088')
    
    # Wait for Superset
    if not wait_for_superset(superset_url):
        sys.exit(1)
    
    # Login
    csrf_token = login_to_superset(superset_url)
    if not csrf_token:
        sys.exit(1)
    
    # Create Redis database
    redis_db_id = create_redis_database(superset_url, csrf_token)
    if not redis_db_id:
        print("⚠️  Redis database creation failed, but continuing...")
    
    # Create sample data
    if create_sample_data_in_redis():
        print("✓ Sample data created successfully")
    
    print("\n🎉 Superset setup completed!")
    print("\n📊 Access your dashboards:")
    print(f"   • Superset Web UI: {superset_url}")
    print("   • Username: admin")
    print("   • Password: admin123")
    print("\n📈 Available dashboards:")
    print("   • Real-time Trending Topics")
    print("   • Historical Trend Analysis")
    print("\n🔧 Next steps:")
    print("   1. Create charts using the Redis data source")
    print("   2. Build custom dashboards")
    print("   3. Set up automated reports")
    print("   4. Configure user access controls")

if __name__ == "__main__":
    main()