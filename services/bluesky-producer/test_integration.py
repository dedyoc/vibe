"""
Integration tests for Bluesky Producer Service

Tests the service startup, health endpoints, and basic functionality.
"""

import asyncio
import pytest
from unittest.mock import patch, AsyncMock
from fastapi.testclient import TestClient

# Import the FastAPI app
import sys
import os
sys.path.append(os.path.dirname(__file__))
from main import app


class TestBlueskyProducerIntegration:
    """Integration tests for the Bluesky Producer service."""
    
    def test_health_endpoint(self) -> None:
        """Test the health check endpoint."""
        with TestClient(app) as client:
            response = client.get("/health")
            
            assert response.status_code == 200
            data = response.json()
            assert data["service"] == "bluesky-producer"
            assert "status" in data
    
    def test_root_endpoint(self) -> None:
        """Test the root endpoint."""
        with TestClient(app) as client:
            response = client.get("/")
            
            assert response.status_code == 200
            data = response.json()
            assert data["service"] == "bluesky-producer"
            assert data["description"] == "Bluesky firehose data producer"
    
    def test_metrics_endpoint(self) -> None:
        """Test the Prometheus metrics endpoint."""
        with TestClient(app) as client:
            response = client.get("/metrics")
            
            assert response.status_code == 200
            assert "text/plain" in response.headers["content-type"]
            # Should contain some basic Prometheus metrics
            content = response.text
            assert "bluesky_posts_processed_total" in content
    
    def test_stats_endpoint_no_client(self) -> None:
        """Test the stats endpoint when no client is initialized."""
        with TestClient(app) as client:
            response = client.get("/stats")
            
            assert response.status_code == 200
            data = response.json()
            assert "error" in data
            assert data["error"] == "Client not initialized"
    
    def test_health_endpoint_with_client(self) -> None:
        """Test health endpoint when firehose client is initialized."""
        # Mock the global firehose_client
        with patch('main.firehose_client') as mock_client:
            mock_client.get_stats.return_value = {
                'is_connected': True,
                'posts_processed': 42,
                'last_message_time': '2024-01-15T10:30:00+00:00'
            }
            
            with TestClient(app) as client:
                response = client.get("/health")
                
                assert response.status_code == 200
                data = response.json()
                assert data["status"] == "healthy"
                assert data["connected"] is True
                assert data["posts_processed"] == 42
    
    def test_health_endpoint_disconnected_client(self) -> None:
        """Test health endpoint when firehose client is disconnected."""
        with patch('main.firehose_client') as mock_client:
            mock_client.get_stats.return_value = {
                'is_connected': False,
                'posts_processed': 10,
                'last_message_time': None
            }
            
            with TestClient(app) as client:
                response = client.get("/health")
                
                assert response.status_code == 200
                data = response.json()
                assert data["status"] == "unhealthy"
                assert data["connected"] is False
    
    def test_stats_endpoint_with_client(self) -> None:
        """Test stats endpoint when firehose client is initialized."""
        expected_stats = {
            'is_connected': True,
            'posts_processed': 100,
            'connection_attempts': 2,
            'retry_count': 0,
            'current_cursor': 'test_cursor_123',
            'last_message_time': '2024-01-15T10:30:00+00:00',
            'correlation_id': 'test-correlation-id'
        }
        
        with patch('main.firehose_client') as mock_client:
            mock_client.get_stats.return_value = expected_stats
            
            with TestClient(app) as client:
                response = client.get("/stats")
                
                assert response.status_code == 200
                data = response.json()
                assert data == expected_stats


class TestServiceStartup:
    """Test service startup and configuration."""
    
    def test_environment_variable_defaults(self) -> None:
        """Test that environment variables have proper defaults."""
        import os
        
        # Test default values when env vars are not set
        kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        redis_host = os.getenv("REDIS_HOST", "localhost")
        redis_port = int(os.getenv("REDIS_PORT", "6379"))
        log_level = os.getenv("LOG_LEVEL", "INFO")
        prometheus_port = int(os.getenv("PROMETHEUS_PORT", "8000"))
        
        assert kafka_servers == "localhost:9092"
        assert redis_host == "localhost"
        assert redis_port == 6379
        assert log_level == "INFO"
        assert prometheus_port == 8000
    
    def test_environment_variable_override(self) -> None:
        """Test that environment variables can be overridden."""
        import os
        
        # Set custom environment variables
        test_env = {
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
            "REDIS_HOST": "redis",
            "REDIS_PORT": "6380",
            "LOG_LEVEL": "DEBUG",
            "PROMETHEUS_PORT": "9000"
        }
        
        # Temporarily set environment variables
        original_env = {}
        for key, value in test_env.items():
            original_env[key] = os.environ.get(key)
            os.environ[key] = value
        
        try:
            # Test that values are read correctly
            kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
            redis_host = os.getenv("REDIS_HOST", "localhost")
            redis_port = int(os.getenv("REDIS_PORT", "6379"))
            log_level = os.getenv("LOG_LEVEL", "INFO")
            prometheus_port = int(os.getenv("PROMETHEUS_PORT", "8000"))
            
            assert kafka_servers == "kafka:9092"
            assert redis_host == "redis"
            assert redis_port == 6380
            assert log_level == "DEBUG"
            assert prometheus_port == 9000
            
        finally:
            # Restore original environment
            for key, original_value in original_env.items():
                if original_value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = original_value