"""
Tests for Superset Integration Manager
"""

import pytest
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timedelta

from superset_manager import SupersetManager, DashboardConfig, ReportConfig


class TestSupersetManager:
    """Test cases for SupersetManager"""
    
    @pytest.fixture
    def manager(self):
        """Create a SupersetManager instance for testing"""
        return SupersetManager(
            superset_url="http://test-superset:8088",
            redis_host="test-redis",
            redis_port=6379,
            minio_endpoint="test-minio:9000"
        )
    
    @pytest.mark.asyncio
    async def test_initialize_connections(self, manager):
        """Test initialization of Redis and MinIO connections"""
        with patch('aioredis.from_url') as mock_redis, \
             patch('boto3.client') as mock_s3:
            
            # Mock Redis connection
            mock_redis_client = AsyncMock()
            mock_redis_client.ping = AsyncMock()
            mock_redis.return_value = mock_redis_client
            
            # Mock S3 client
            mock_s3_client = MagicMock()
            mock_s3_client.list_buckets.return_value = {'Buckets': []}
            mock_s3.return_value = mock_s3_client
            
            await manager.initialize()
            
            # Verify connections were established
            mock_redis.assert_called_once()
            mock_s3.assert_called_once()
            mock_redis_client.ping.assert_called_once()
            mock_s3_client.list_buckets.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_login_to_superset(self, manager):
        """Test Superset login functionality"""
        with patch.object(manager.session, 'get') as mock_get, \
             patch.object(manager.session, 'post') as mock_post:
            
            # Mock login page response
            login_page_response = MagicMock()
            login_page_response.status_code = 200
            login_page_response.text = 'csrf_token" value="test-csrf-token"'
            mock_get.side_effect = [login_page_response]
            
            # Mock login response
            login_response = MagicMock()
            login_response.status_code = 200
            login_response.url = "http://test-superset:8088/dashboard/"
            mock_post.return_value = login_response
            
            # Mock CSRF token API response
            csrf_response = MagicMock()
            csrf_response.status_code = 200
            csrf_response.json.return_value = {'result': 'api-csrf-token'}
            mock_get.side_effect = [login_page_response, csrf_response]
            
            result = await manager.login_to_superset()
            
            assert result is True
            assert manager.csrf_token == 'api-csrf-token'
    
    @pytest.mark.asyncio
    async def test_sync_trend_data_to_superset(self, manager):
        """Test syncing trend data from Redis to Superset format"""
        # Mock Redis client
        mock_redis_client = AsyncMock()
        trend_data = {
            'python': json.dumps({
                'frequency': 150,
                'growth_rate': 2.5,
                'confidence_score': 0.85,
                'window_start': '2024-01-01T10:00:00',
                'window_end': '2024-01-01T11:00:00',
                'unique_authors': 75,
                'rank': 1,
                'sample_posts': ['Post 1', 'Post 2']
            }),
            'javascript': json.dumps({
                'frequency': 120,
                'growth_rate': 1.8,
                'confidence_score': 0.78,
                'window_start': '2024-01-01T10:00:00',
                'window_end': '2024-01-01T11:00:00',
                'unique_authors': 60,
                'rank': 2,
                'sample_posts': ['JS Post 1']
            })
        }
        
        mock_redis_client.hgetall.return_value = trend_data
        mock_redis_client.set = AsyncMock()
        manager.redis_client = mock_redis_client
        
        await manager.sync_trend_data_to_superset()
        
        # Verify Redis operations
        mock_redis_client.hgetall.assert_called_once_with("trends:current")
        mock_redis_client.set.assert_called_once()
        
        # Check the data format
        call_args = mock_redis_client.set.call_args
        assert call_args[0][0] == "superset:trending_keywords"
        synced_data = json.loads(call_args[0][1])
        assert len(synced_data) == 2
        assert synced_data[0]['keyword'] == 'python'
        assert synced_data[0]['frequency'] == 150
    
    @pytest.mark.asyncio
    async def test_sync_historical_data_to_superset(self, manager):
        """Test syncing historical data from MinIO to Superset format"""
        # Mock S3 client
        mock_s3_client = MagicMock()
        
        # Mock S3 list_objects_v2 response
        mock_s3_client.list_objects_v2.return_value = {
            'Contents': [
                {
                    'Key': 'processed/trends/year=2024/month=01/day=01/hour=10/trends.json',
                    'LastModified': datetime.utcnow()
                }
            ]
        }
        
        # Mock S3 get_object response
        trend_records = [
            {
                'keyword': 'python',
                'frequency': 100,
                'growth_rate': 2.0,
                'confidence_score': 0.8,
                'unique_authors': 50
            }
        ]
        
        mock_file_obj = MagicMock()
        mock_file_obj['Body'].read.return_value = json.dumps(trend_records).encode('utf-8')
        mock_s3_client.get_object.return_value = mock_file_obj
        
        manager.s3_client = mock_s3_client
        
        # Mock Redis client
        mock_redis_client = AsyncMock()
        manager.redis_client = mock_redis_client
        
        await manager.sync_historical_data_to_superset()
        
        # Verify S3 operations
        mock_s3_client.list_objects_v2.assert_called_once()
        mock_s3_client.get_object.assert_called_once()
        
        # Verify Redis set operation
        mock_redis_client.set.assert_called_once()
        call_args = mock_redis_client.set.call_args
        assert call_args[0][0] == "superset:historical_trends"
    
    def test_create_dashboard(self, manager):
        """Test dashboard creation"""
        manager.csrf_token = "test-csrf-token"
        
        with patch.object(manager.session, 'post') as mock_post:
            # Mock successful dashboard creation
            response = MagicMock()
            response.status_code = 201
            response.json.return_value = {'id': 123}
            mock_post.return_value = response
            
            config = DashboardConfig(
                title="Test Dashboard",
                slug="test-dashboard",
                description="Test description",
                refresh_frequency=60,
                charts=[],
                filters={}
            )
            
            dashboard_id = manager.create_dashboard(config)
            
            assert dashboard_id == 123
            mock_post.assert_called_once()
            
            # Verify request data
            call_args = mock_post.call_args
            request_data = call_args[1]['json']
            assert request_data['dashboard_title'] == "Test Dashboard"
            assert request_data['slug'] == "test-dashboard"
    
    def test_setup_automated_reports(self, manager):
        """Test automated report setup"""
        manager.csrf_token = "test-csrf-token"
        
        with patch.object(manager.session, 'post') as mock_post:
            # Mock successful report creation
            response = MagicMock()
            response.status_code = 201
            mock_post.return_value = response
            
            reports = [
                ReportConfig(
                    name="Test Report",
                    dashboard_id=123,
                    recipients=["test@example.com"],
                    schedule="0 9 * * *",
                    format="pdf"
                )
            ]
            
            manager.setup_automated_reports(reports)
            
            mock_post.assert_called_once()
            
            # Verify request data
            call_args = mock_post.call_args
            request_data = call_args[1]['json']
            assert request_data['name'] == "Test Report"
            assert request_data['dashboard'] == 123
    
    @pytest.mark.asyncio
    async def test_cleanup(self, manager):
        """Test cleanup functionality"""
        mock_redis_client = AsyncMock()
        manager.redis_client = mock_redis_client
        
        await manager.cleanup()
        
        mock_redis_client.close.assert_called_once()


class TestDashboardConfig:
    """Test cases for DashboardConfig"""
    
    def test_dashboard_config_creation(self):
        """Test DashboardConfig creation"""
        config = DashboardConfig(
            title="Test Dashboard",
            slug="test-dashboard",
            description="Test description",
            refresh_frequency=30,
            charts=[{"type": "line", "data": "test"}],
            filters={"date_range": "last_7_days"}
        )
        
        assert config.title == "Test Dashboard"
        assert config.slug == "test-dashboard"
        assert config.refresh_frequency == 30
        assert len(config.charts) == 1
        assert "date_range" in config.filters


class TestReportConfig:
    """Test cases for ReportConfig"""
    
    def test_report_config_creation(self):
        """Test ReportConfig creation"""
        config = ReportConfig(
            name="Weekly Report",
            dashboard_id=123,
            recipients=["admin@example.com", "user@example.com"],
            schedule="0 9 * * 1",
            format="pdf",
            enabled=True
        )
        
        assert config.name == "Weekly Report"
        assert config.dashboard_id == 123
        assert len(config.recipients) == 2
        assert config.schedule == "0 9 * * 1"
        assert config.format == "pdf"
        assert config.enabled is True
    
    def test_report_config_default_enabled(self):
        """Test ReportConfig default enabled value"""
        config = ReportConfig(
            name="Test Report",
            dashboard_id=456,
            recipients=["test@example.com"],
            schedule="0 12 * * *",
            format="png"
        )
        
        assert config.enabled is True


if __name__ == "__main__":
    pytest.main([__file__])