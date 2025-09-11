#!/usr/bin/env python3
"""
Superset Data Sources and Dashboard Setup Script
Configures Redis and MinIO connections and creates initial dashboards
"""

import os
import json
import time
import requests
from typing import Dict, Any, List, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SupersetSetup:
    """Handles Superset configuration and dashboard setup"""
    
    def __init__(self, base_url: str = "http://localhost:8088"):
        self.base_url = base_url
        self.session = requests.Session()
        self.csrf_token: Optional[str] = None
        
    def wait_for_superset(self, max_retries: int = 30) -> bool:
        """Wait for Superset to be ready"""
        for i in range(max_retries):
            try:
                response = self.session.get(f"{self.base_url}/health")
                if response.status_code == 200:
                    logger.info("Superset is ready!")
                    return True
            except requests.exceptions.RequestException:
                pass
            
            logger.info(f"Waiting for Superset... ({i+1}/{max_retries})")
            time.sleep(5)
        
        logger.error("Superset failed to start within timeout")
        return False
    
    def login(self, username: str = "admin", password: str = "admin123") -> bool:
        """Login to Superset and get CSRF token"""
        try:
            # Get login page to extract CSRF token
            login_page = self.session.get(f"{self.base_url}/login/")
            if login_page.status_code != 200:
                logger.error(f"Failed to get login page: {login_page.status_code}")
                return False
            
            # Extract CSRF token from response
            csrf_start = login_page.text.find('csrf_token')
            if csrf_start == -1:
                logger.error("CSRF token not found in login page")
                return False
            
            csrf_start = login_page.text.find('value="', csrf_start) + 7
            csrf_end = login_page.text.find('"', csrf_start)
            self.csrf_token = login_page.text[csrf_start:csrf_end]
            
            # Login
            login_data = {
                'username': username,
                'password': password,
                'csrf_token': self.csrf_token
            }
            
            response = self.session.post(f"{self.base_url}/login/", data=login_data)
            if response.status_code == 200 and 'login' not in response.url:
                logger.info("Successfully logged in to Superset")
                return True
            else:
                logger.error(f"Login failed: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Login error: {e}")
            return False
    
    def get_csrf_token(self) -> Optional[str]:
        """Get fresh CSRF token for API calls"""
        try:
            response = self.session.get(f"{self.base_url}/api/v1/security/csrf_token/")
            if response.status_code == 200:
                return response.json().get('result')
        except Exception as e:
            logger.error(f"Failed to get CSRF token: {e}")
        return None
    
    def create_redis_database(self) -> Optional[int]:
        """Create Redis database connection"""
        try:
            csrf_token = self.get_csrf_token()
            if not csrf_token:
                logger.error("Failed to get CSRF token for Redis database creation")
                return None
            
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
            
            response = self.session.post(
                f"{self.base_url}/api/v1/database/",
                json=redis_config,
                headers=headers
            )
            
            if response.status_code == 201:
                db_id = response.json()['id']
                logger.info(f"Redis database created with ID: {db_id}")
                return db_id
            else:
                logger.error(f"Failed to create Redis database: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error creating Redis database: {e}")
            return None
    
    def create_minio_database(self) -> Optional[int]:
        """Create MinIO S3 database connection"""
        try:
            csrf_token = self.get_csrf_token()
            if not csrf_token:
                logger.error("Failed to get CSRF token for MinIO database creation")
                return None
            
            # Note: For MinIO/S3, we'll use a SQLite connection that can query S3 data
            # In production, you might use Trino/Presto or similar for S3 querying
            minio_config = {
                "database_name": "MinIO Data Lake",
                "engine": "sqlite",
                "configuration_method": "sqlalchemy_form",
                "sqlalchemy_uri": "sqlite:////tmp/minio_metadata.db",
                "expose_in_sqllab": True,
                "allow_ctas": True,
                "allow_cvas": True,
                "allow_dml": False,
                "extra": json.dumps({
                    "metadata_params": {},
                    "engine_params": {},
                    "schemas_allowed_for_csv_upload": ["main"]
                })
            }
            
            headers = {
                'X-CSRFToken': csrf_token,
                'Content-Type': 'application/json'
            }
            
            response = self.session.post(
                f"{self.base_url}/api/v1/database/",
                json=minio_config,
                headers=headers
            )
            
            if response.status_code == 201:
                db_id = response.json()['id']
                logger.info(f"MinIO database created with ID: {db_id}")
                return db_id
            else:
                logger.error(f"Failed to create MinIO database: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error creating MinIO database: {e}")
            return None
    
    def create_dataset(self, database_id: int, table_name: str, sql: str) -> Optional[int]:
        """Create a dataset from SQL query"""
        try:
            csrf_token = self.get_csrf_token()
            if not csrf_token:
                return None
            
            dataset_config = {
                "database": database_id,
                "table_name": table_name,
                "sql": sql,
                "is_sqllab_view": True
            }
            
            headers = {
                'X-CSRFToken': csrf_token,
                'Content-Type': 'application/json'
            }
            
            response = self.session.post(
                f"{self.base_url}/api/v1/dataset/",
                json=dataset_config,
                headers=headers
            )
            
            if response.status_code == 201:
                dataset_id = response.json()['id']
                logger.info(f"Dataset '{table_name}' created with ID: {dataset_id}")
                return dataset_id
            else:
                logger.error(f"Failed to create dataset '{table_name}': {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error creating dataset '{table_name}': {e}")
            return None
    
    def create_dashboard(self, dashboard_config: Dict[str, Any]) -> Optional[int]:
        """Create a dashboard"""
        try:
            csrf_token = self.get_csrf_token()
            if not csrf_token:
                return None
            
            headers = {
                'X-CSRFToken': csrf_token,
                'Content-Type': 'application/json'
            }
            
            response = self.session.post(
                f"{self.base_url}/api/v1/dashboard/",
                json=dashboard_config,
                headers=headers
            )
            
            if response.status_code == 201:
                dashboard_id = response.json()['id']
                logger.info(f"Dashboard '{dashboard_config['dashboard_title']}' created with ID: {dashboard_id}")
                return dashboard_id
            else:
                logger.error(f"Failed to create dashboard: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error creating dashboard: {e}")
            return None
    
    def setup_trend_dashboards(self, redis_db_id: int, minio_db_id: int) -> None:
        """Set up trend analysis dashboards"""
        
        # Create datasets for trend analysis
        trending_keywords_sql = """
        SELECT 
            keyword,
            frequency,
            growth_rate,
            confidence_score,
            window_start,
            window_end,
            unique_authors,
            rank
        FROM trending_keywords 
        WHERE window_end >= datetime('now', '-1 hour')
        ORDER BY rank ASC
        LIMIT 50
        """
        
        historical_trends_sql = """
        SELECT 
            DATE(window_start) as trend_date,
            keyword,
            AVG(frequency) as avg_frequency,
            MAX(growth_rate) as max_growth_rate,
            COUNT(*) as trend_occurrences
        FROM trending_keywords 
        WHERE window_start >= datetime('now', '-7 days')
        GROUP BY DATE(window_start), keyword
        ORDER BY trend_date DESC, avg_frequency DESC
        """
        
        # Create datasets
        trending_dataset_id = self.create_dataset(
            redis_db_id, 
            "current_trending_keywords", 
            trending_keywords_sql
        )
        
        historical_dataset_id = self.create_dataset(
            minio_db_id,
            "historical_trend_patterns",
            historical_trends_sql
        )
        
        if trending_dataset_id and historical_dataset_id:
            # Create real-time trends dashboard
            realtime_dashboard = {
                "dashboard_title": "Real-time Trending Topics",
                "slug": "realtime-trending-topics",
                "published": True,
                "json_metadata": json.dumps({
                    "refresh_frequency": 30,
                    "timed_refresh_immune_slices": [],
                    "expanded_slices": {},
                    "label_colors": {},
                    "color_scheme": "supersetColors",
                    "positions": {}
                })
            }
            
            # Create historical analysis dashboard
            historical_dashboard = {
                "dashboard_title": "Historical Trend Analysis",
                "slug": "historical-trend-analysis", 
                "published": True,
                "json_metadata": json.dumps({
                    "refresh_frequency": 300,
                    "timed_refresh_immune_slices": [],
                    "expanded_slices": {},
                    "label_colors": {},
                    "color_scheme": "supersetColors",
                    "positions": {}
                })
            }
            
            self.create_dashboard(realtime_dashboard)
            self.create_dashboard(historical_dashboard)
    
    def setup_all(self) -> bool:
        """Set up all data sources and dashboards"""
        try:
            if not self.wait_for_superset():
                return False
            
            if not self.login():
                return False
            
            # Create database connections
            redis_db_id = self.create_redis_database()
            minio_db_id = self.create_minio_database()
            
            if redis_db_id and minio_db_id:
                # Set up dashboards
                self.setup_trend_dashboards(redis_db_id, minio_db_id)
                logger.info("Superset setup completed successfully!")
                return True
            else:
                logger.error("Failed to create required database connections")
                return False
                
        except Exception as e:
            logger.error(f"Setup failed: {e}")
            return False

def main():
    """Main setup function"""
    setup = SupersetSetup()
    success = setup.setup_all()
    
    if success:
        logger.info("Superset configuration completed successfully!")
        print("Access Superset at: http://localhost:8088")
        print("Username: admin")
        print("Password: admin123")
    else:
        logger.error("Superset setup failed!")
        exit(1)

if __name__ == "__main__":
    main()