"""
Superset Integration Manager for Bluesky Streaming Pipeline
Handles data source configuration, dashboard management, and automated reporting
"""

import os
import json
import asyncio
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass
import aioredis
import boto3
from botocore.exceptions import ClientError
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class DashboardConfig:
    """Configuration for Superset dashboard"""
    title: str
    slug: str
    description: str
    refresh_frequency: int  # seconds
    charts: List[Dict[str, Any]]
    filters: Dict[str, Any]

@dataclass
class ReportConfig:
    """Configuration for automated reports"""
    name: str
    dashboard_id: int
    recipients: List[str]
    schedule: str  # cron expression
    format: str  # pdf, png, csv
    enabled: bool = True

class SupersetManager:
    """Manages Superset integration for the streaming pipeline"""
    
    def __init__(
        self,
        superset_url: str = "http://superset:8088",
        redis_host: str = "redis",
        redis_port: int = 6379,
        minio_endpoint: str = "minio:9000",
        minio_access_key: str = "minioadmin",
        minio_secret_key: str = "minioadmin123"
    ):
        self.superset_url = superset_url
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        
        # Initialize HTTP session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        self.redis_client: Optional[aioredis.Redis] = None
        self.s3_client: Optional[boto3.client] = None
        self.csrf_token: Optional[str] = None
        
    async def initialize(self) -> None:
        """Initialize connections to Redis and MinIO"""
        try:
            # Initialize Redis connection
            self.redis_client = aioredis.from_url(
                f"redis://{self.redis_host}:{self.redis_port}",
                decode_responses=True
            )
            await self.redis_client.ping()
            logger.info("Connected to Redis successfully")
            
            # Initialize MinIO S3 client
            self.s3_client = boto3.client(
                's3',
                endpoint_url=f"http://{self.minio_endpoint}",
                aws_access_key_id=self.minio_access_key,
                aws_secret_access_key=self.minio_secret_key,
                region_name='us-east-1'
            )
            
            # Test S3 connection
            self.s3_client.list_buckets()
            logger.info("Connected to MinIO successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize connections: {e}")
            raise
    
    async def login_to_superset(self, username: str = "admin", password: str = "admin123") -> bool:
        """Login to Superset and obtain CSRF token"""
        try:
            # Get login page
            login_response = self.session.get(f"{self.superset_url}/login/")
            if login_response.status_code != 200:
                logger.error(f"Failed to access login page: {login_response.status_code}")
                return False
            
            # Extract CSRF token
            csrf_start = login_response.text.find('csrf_token')
            if csrf_start == -1:
                logger.error("CSRF token not found in login page")
                return False
            
            csrf_start = login_response.text.find('value="', csrf_start) + 7
            csrf_end = login_response.text.find('"', csrf_start)
            csrf_token = login_response.text[csrf_start:csrf_end]
            
            # Perform login
            login_data = {
                'username': username,
                'password': password,
                'csrf_token': csrf_token
            }
            
            response = self.session.post(f"{self.superset_url}/login/", data=login_data)
            
            if response.status_code == 200 and 'login' not in response.url:
                # Get API CSRF token
                csrf_response = self.session.get(f"{self.superset_url}/api/v1/security/csrf_token/")
                if csrf_response.status_code == 200:
                    self.csrf_token = csrf_response.json().get('result')
                    logger.info("Successfully logged in to Superset")
                    return True
            
            logger.error("Failed to login to Superset")
            return False
            
        except Exception as e:
            logger.error(f"Login error: {e}")
            return False
    
    async def sync_trend_data_to_superset(self) -> None:
        """Sync current trend data from Redis to Superset-accessible format"""
        try:
            if not self.redis_client:
                await self.initialize()
            
            # Get current trending keywords from Redis
            trending_data = await self.redis_client.hgetall("trends:current")
            
            if not trending_data:
                logger.warning("No trending data found in Redis")
                return
            
            # Transform data for Superset consumption
            superset_data = []
            for keyword, data_json in trending_data.items():
                try:
                    trend_data = json.loads(data_json)
                    superset_data.append({
                        'keyword': keyword,
                        'frequency': trend_data.get('frequency', 0),
                        'growth_rate': trend_data.get('growth_rate', 0.0),
                        'confidence_score': trend_data.get('confidence_score', 0.0),
                        'window_start': trend_data.get('window_start'),
                        'window_end': trend_data.get('window_end'),
                        'unique_authors': trend_data.get('unique_authors', 0),
                        'rank': trend_data.get('rank', 999),
                        'sample_posts': json.dumps(trend_data.get('sample_posts', [])),
                        'last_updated': datetime.utcnow().isoformat()
                    })
                except json.JSONDecodeError:
                    logger.warning(f"Failed to parse trend data for keyword: {keyword}")
                    continue
            
            # Store in Redis with a key that Superset can query
            if superset_data:
                await self.redis_client.set(
                    "superset:trending_keywords",
                    json.dumps(superset_data),
                    ex=300  # 5 minute expiry
                )
                logger.info(f"Synced {len(superset_data)} trending keywords to Superset format")
            
        except Exception as e:
            logger.error(f"Failed to sync trend data: {e}")
    
    async def sync_historical_data_to_superset(self) -> None:
        """Sync historical data from MinIO to Superset-accessible format"""
        try:
            if not self.s3_client:
                await self.initialize()
            
            # List recent trend files from MinIO
            bucket_name = "data-lake"
            prefix = "processed/trends/"
            
            # Get files from the last 7 days
            cutoff_date = datetime.utcnow() - timedelta(days=7)
            
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=prefix,
                    MaxKeys=1000
                )
                
                historical_data = []
                
                for obj in response.get('Contents', []):
                    # Parse date from object key
                    key_parts = obj['Key'].split('/')
                    if len(key_parts) >= 6:  # processed/trends/year=2024/month=12/day=09/hour=14/
                        try:
                            year = int(key_parts[2].split('=')[1])
                            month = int(key_parts[3].split('=')[1])
                            day = int(key_parts[4].split('=')[1])
                            hour = int(key_parts[5].split('=')[1])
                            
                            file_date = datetime(year, month, day, hour)
                            
                            if file_date >= cutoff_date:
                                # Download and process the file
                                file_obj = self.s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
                                file_content = file_obj['Body'].read().decode('utf-8')
                                
                                # Parse trend data (assuming JSON format)
                                trend_records = json.loads(file_content)
                                
                                for record in trend_records:
                                    historical_data.append({
                                        'trend_date': file_date.date().isoformat(),
                                        'keyword': record.get('keyword'),
                                        'avg_frequency': record.get('frequency', 0),
                                        'max_growth_rate': record.get('growth_rate', 0.0),
                                        'trend_occurrences': 1,
                                        'confidence_score': record.get('confidence_score', 0.0),
                                        'unique_authors': record.get('unique_authors', 0)
                                    })
                                    
                        except (ValueError, IndexError, json.JSONDecodeError) as e:
                            logger.warning(f"Failed to process file {obj['Key']}: {e}")
                            continue
                
                # Aggregate data by date and keyword
                aggregated_data = {}
                for record in historical_data:
                    key = (record['trend_date'], record['keyword'])
                    if key not in aggregated_data:
                        aggregated_data[key] = record.copy()
                    else:
                        # Aggregate metrics
                        existing = aggregated_data[key]
                        existing['avg_frequency'] = (existing['avg_frequency'] + record['avg_frequency']) / 2
                        existing['max_growth_rate'] = max(existing['max_growth_rate'], record['max_growth_rate'])
                        existing['trend_occurrences'] += record['trend_occurrences']
                        existing['unique_authors'] += record['unique_authors']
                
                # Store aggregated data in Redis for Superset access
                if aggregated_data:
                    final_data = list(aggregated_data.values())
                    await self.redis_client.set(
                        "superset:historical_trends",
                        json.dumps(final_data),
                        ex=3600  # 1 hour expiry
                    )
                    logger.info(f"Synced {len(final_data)} historical trend records to Superset format")
                
            except ClientError as e:
                logger.error(f"Failed to access MinIO bucket: {e}")
                
        except Exception as e:
            logger.error(f"Failed to sync historical data: {e}")
    
    def create_dashboard(self, config: DashboardConfig) -> Optional[int]:
        """Create a new dashboard in Superset"""
        try:
            if not self.csrf_token:
                logger.error("Not logged in to Superset")
                return None
            
            dashboard_data = {
                "dashboard_title": config.title,
                "slug": config.slug,
                "description": config.description,
                "published": True,
                "json_metadata": json.dumps({
                    "refresh_frequency": config.refresh_frequency,
                    "default_filters": config.filters,
                    "timed_refresh_immune_slices": [],
                    "expanded_slices": {},
                    "label_colors": {},
                    "color_scheme": "supersetColors"
                })
            }
            
            headers = {
                'X-CSRFToken': self.csrf_token,
                'Content-Type': 'application/json'
            }
            
            response = self.session.post(
                f"{self.superset_url}/api/v1/dashboard/",
                json=dashboard_data,
                headers=headers
            )
            
            if response.status_code == 201:
                dashboard_id = response.json()['id']
                logger.info(f"Created dashboard '{config.title}' with ID: {dashboard_id}")
                return dashboard_id
            else:
                logger.error(f"Failed to create dashboard: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error creating dashboard: {e}")
            return None
    
    def setup_automated_reports(self, reports: List[ReportConfig]) -> None:
        """Set up automated report generation"""
        try:
            for report in reports:
                if not report.enabled:
                    continue
                
                report_data = {
                    "type": "Dashboard",
                    "name": report.name,
                    "description": f"Automated report for dashboard {report.dashboard_id}",
                    "dashboard": report.dashboard_id,
                    "recipients": [{"recipient_config_json": json.dumps({"target": email})} for email in report.recipients],
                    "report_format": report.format.upper(),
                    "crontab": report.schedule,
                    "timezone": "UTC",
                    "active": True
                }
                
                headers = {
                    'X-CSRFToken': self.csrf_token,
                    'Content-Type': 'application/json'
                }
                
                response = self.session.post(
                    f"{self.superset_url}/api/v1/report/",
                    json=report_data,
                    headers=headers
                )
                
                if response.status_code == 201:
                    logger.info(f"Created automated report: {report.name}")
                else:
                    logger.error(f"Failed to create report {report.name}: {response.status_code}")
                    
        except Exception as e:
            logger.error(f"Error setting up automated reports: {e}")
    
    async def run_data_sync_loop(self, interval: int = 300) -> None:
        """Run continuous data synchronization loop"""
        logger.info(f"Starting data sync loop with {interval}s interval")
        
        while True:
            try:
                await self.sync_trend_data_to_superset()
                await self.sync_historical_data_to_superset()
                logger.info("Data sync completed successfully")
                
            except Exception as e:
                logger.error(f"Data sync failed: {e}")
            
            await asyncio.sleep(interval)
    
    async def cleanup(self) -> None:
        """Clean up connections"""
        if self.redis_client:
            await self.redis_client.close()
        logger.info("Superset manager cleanup completed")

async def main():
    """Main function for running the Superset manager"""
    from health_server import HealthServer
    
    # Start health server
    health_server = HealthServer()
    await health_server.start_server()
    
    manager = SupersetManager()
    
    try:
        await manager.initialize()
        
        # Wait for Superset to be ready
        logger.info("Waiting for Superset to be ready...")
        await asyncio.sleep(30)  # Give Superset time to start
        
        # Login to Superset
        login_attempts = 0
        max_attempts = 10
        while login_attempts < max_attempts:
            if await manager.login_to_superset():
                break
            login_attempts += 1
            logger.info(f"Login attempt {login_attempts}/{max_attempts} failed, retrying in 10s...")
            await asyncio.sleep(10)
        
        if login_attempts >= max_attempts:
            logger.error("Failed to login to Superset after maximum attempts")
            return
        
        # Set up initial dashboards
        realtime_config = DashboardConfig(
            title="Real-time Trending Topics",
            slug="realtime-trending-topics",
            description="Live view of trending keywords from Bluesky firehose",
            refresh_frequency=30,
            charts=[],
            filters={}
        )
        
        historical_config = DashboardConfig(
            title="Historical Trend Analysis", 
            slug="historical-trend-analysis",
            description="Historical analysis of trending patterns",
            refresh_frequency=300,
            charts=[],
            filters={"time_range": "Last 7 days"}
        )
        
        realtime_id = manager.create_dashboard(realtime_config)
        historical_id = manager.create_dashboard(historical_config)
        
        # Set up automated reports
        if realtime_id and historical_id:
            reports = [
                ReportConfig(
                    name="Daily Trend Summary",
                    dashboard_id=realtime_id,
                    recipients=["admin@bluesky-pipeline.local"],
                    schedule="0 9 * * *",  # Daily at 9 AM
                    format="pdf"
                ),
                ReportConfig(
                    name="Weekly Trend Analysis",
                    dashboard_id=historical_id,
                    recipients=["admin@bluesky-pipeline.local"],
                    schedule="0 9 * * 1",  # Weekly on Monday at 9 AM
                    format="pdf"
                )
            ]
            
            manager.setup_automated_reports(reports)
        
        # Start data sync loop
        await manager.run_data_sync_loop()
        
    except KeyboardInterrupt:
        logger.info("Shutting down Superset manager...")
    finally:
        await manager.cleanup()

if __name__ == "__main__":
    asyncio.run(main())