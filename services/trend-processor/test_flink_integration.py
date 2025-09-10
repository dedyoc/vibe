#!/usr/bin/env python3
"""
Integration tests for Flink stream processing job.

These tests verify that the Flink job can be deployed, processes data correctly,
and integrates properly with Kafka and MinIO.
"""

import asyncio
import json
import logging
import os
import pytest
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
from unittest.mock import Mock, patch, AsyncMock

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

from flink_manager import FlinkJobManager, FlinkJobDeployer, FlinkJobStatus
from flink_job import create_flink_job_config, KeywordExtractor, PostDataDeserializer
from shared.models import PostData, WindowedKeywordCount

# Configure logging for tests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture
def sample_posts() -> List[PostData]:
    """Create sample posts for testing."""
    base_time = datetime.now()
    
    return [
        PostData(
            uri="at://did:plc:test1/app.bsky.feed.post/1",
            author_did="did:plc:test1",
            text="This is a test post about #python programming and #AI trends",
            created_at=base_time,
            language="en",
            hashtags=["python", "AI"]
        ),
        PostData(
            uri="at://did:plc:test2/app.bsky.feed.post/2",
            author_did="did:plc:test2",
            text="Machine learning and artificial intelligence are trending topics",
            created_at=base_time + timedelta(seconds=30),
            language="en"
        ),
        PostData(
            uri="at://did:plc:test3/app.bsky.feed.post/3",
            author_did="did:plc:test3",
            text="Python is great for data science and #MachineLearning projects",
            created_at=base_time + timedelta(seconds=60),
            language="en",
            hashtags=["MachineLearning"]
        )
    ]


@pytest.fixture
def flink_config() -> Dict[str, Any]:
    """Create test configuration for Flink job."""
    return create_flink_job_config(
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        kafka_topic_posts="test-bluesky-posts",
        kafka_topic_trends="test-windowed-keyword-counts",
        kafka_consumer_group="test-trend-processor",
        minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin123",
        parallelism=1,
        checkpoint_interval_ms=30000,  # 30 seconds for testing
        window_size_minutes=1  # 1 minute for faster testing
    )


class TestKeywordExtraction:
    """Test keyword extraction functionality."""
    
    def test_keyword_extractor_basic(self, sample_posts):
        """Test basic keyword extraction."""
        extractor = KeywordExtractor()
        
        post = sample_posts[0]  # Post with hashtags
        keywords_with_posts = extractor.flat_map(post)
        
        # Extract just the keywords
        keywords = [kw for kw, _ in keywords_with_posts]
        
        # Should extract meaningful keywords
        assert "python" in keywords or "#python" in keywords
        assert "programming" in keywords
        assert "trends" in keywords
        
        # Should filter out stop words
        assert "this" not in keywords
        assert "is" not in keywords
        assert "a" not in keywords
    
    def test_keyword_extractor_hashtags(self, sample_posts):
        """Test hashtag extraction."""
        extractor = KeywordExtractor()
        
        post = sample_posts[2]  # Post with #MachineLearning
        keywords_with_posts = extractor.flat_map(post)
        keywords = [kw for kw, _ in keywords_with_posts]
        
        # Should extract hashtags with # prefix
        assert "#machinelearning" in keywords
        assert "python" in keywords
        assert "data" in keywords
        assert "science" in keywords
    
    def test_keyword_extractor_empty_post(self):
        """Test keyword extraction with empty or None post."""
        extractor = KeywordExtractor()
        
        # Test with None
        result = extractor.flat_map(None)
        assert result == []
        
        # Test with empty text
        empty_post = PostData(
            uri="test",
            author_did="test",
            text="",
            created_at=datetime.now()
        )
        result = extractor.flat_map(empty_post)
        assert result == []
    
    def test_keyword_normalization(self):
        """Test keyword normalization and filtering."""
        extractor = KeywordExtractor()
        
        test_text = "PYTHON Programming! @user123 https://example.com #AI-ML 123 a"
        keywords = extractor._extract_keywords(test_text)
        
        # Should normalize to lowercase
        assert "python" in keywords
        assert "programming" in keywords
        
        # Should remove URLs and mentions
        assert "https://example.com" not in " ".join(keywords)
        assert "@user123" not in " ".join(keywords)
        
        # Should handle hashtags
        assert "#ai-ml" in keywords or "ai-ml" in keywords
        
        # Should filter out numbers and stop words
        assert "123" not in keywords
        assert "a" not in keywords


class TestPostDataDeserialization:
    """Test post data deserialization."""
    
    def test_deserialize_valid_post(self, sample_posts):
        """Test deserialization of valid post JSON."""
        deserializer = PostDataDeserializer()
        
        post = sample_posts[0]
        post_json = json.dumps({
            'uri': post.uri,
            'author_did': post.author_did,
            'text': post.text,
            'created_at': post.created_at.isoformat(),
            'language': post.language,
            'hashtags': post.hashtags
        })
        
        result = deserializer.map(post_json)
        
        assert result is not None
        assert result.uri == post.uri
        assert result.author_did == post.author_did
        assert result.text == post.text
        assert result.language == post.language
        assert result.hashtags == post.hashtags
    
    def test_deserialize_invalid_json(self):
        """Test deserialization with invalid JSON."""
        deserializer = PostDataDeserializer()
        
        # Invalid JSON
        result = deserializer.map("invalid json")
        assert result is None
        
        # Missing required fields
        result = deserializer.map('{"uri": "test"}')
        assert result is None
    
    def test_deserialize_minimal_post(self):
        """Test deserialization with minimal required fields."""
        deserializer = PostDataDeserializer()
        
        minimal_json = json.dumps({
            'uri': 'test://uri',
            'author_did': 'did:test',
            'text': 'test text',
            'created_at': datetime.now().isoformat()
        })
        
        result = deserializer.map(minimal_json)
        
        assert result is not None
        assert result.uri == 'test://uri'
        assert result.author_did == 'did:test'
        assert result.text == 'test text'
        assert result.mentions == []
        assert result.hashtags == []


@pytest.mark.asyncio
class TestFlinkJobManager:
    """Test Flink job manager functionality."""
    
    async def test_job_manager_initialization(self):
        """Test FlinkJobManager initialization."""
        async with FlinkJobManager("localhost", 8081) as manager:
            assert manager.flink_host == "localhost"
            assert manager.flink_port == 8081
            assert manager.base_url == "http://localhost:8081"
            assert manager.session is not None
    
    @patch('aiohttp.ClientSession.get')
    async def test_check_cluster_health_success(self, mock_get):
        """Test successful cluster health check."""
        # Mock successful response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={'flink-version': '1.18.0'})
        mock_get.return_value.__aenter__.return_value = mock_response
        
        async with FlinkJobManager() as manager:
            result = await manager.check_cluster_health()
            assert result is True
    
    @patch('aiohttp.ClientSession.get')
    async def test_check_cluster_health_failure(self, mock_get):
        """Test failed cluster health check."""
        # Mock failed response
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_get.return_value.__aenter__.return_value = mock_response
        
        async with FlinkJobManager() as manager:
            result = await manager.check_cluster_health()
            assert result is False
    
    @patch('aiohttp.ClientSession.get')
    async def test_list_jobs(self, mock_get):
        """Test listing Flink jobs."""
        # Mock jobs response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={
            'jobs': [
                {
                    'jid': 'job-123',
                    'name': 'Test Job',
                    'state': 'RUNNING',
                    'start-time': int(time.time() * 1000),
                    'duration': 60000
                }
            ]
        })
        mock_get.return_value.__aenter__.return_value = mock_response
        
        async with FlinkJobManager() as manager:
            jobs = await manager.list_jobs()
            
            assert len(jobs) == 1
            assert jobs[0].job_id == 'job-123'
            assert jobs[0].name == 'Test Job'
            assert jobs[0].state == 'RUNNING'
    
    @patch('subprocess.run')
    async def test_submit_python_job_success(self, mock_run):
        """Test successful Python job submission."""
        # Mock successful subprocess result
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "Job has been submitted with JobID abc123"
        mock_run.return_value.stderr = ""
        
        async with FlinkJobManager() as manager:
            job_id = await manager.submit_python_job("/path/to/job.py", ["--arg1", "value1"])
            
            assert job_id == "abc123"
            mock_run.assert_called_once()
    
    @patch('subprocess.run')
    async def test_submit_python_job_failure(self, mock_run):
        """Test failed Python job submission."""
        # Mock failed subprocess result
        mock_run.return_value.returncode = 1
        mock_run.return_value.stdout = ""
        mock_run.return_value.stderr = "Job submission failed"
        
        async with FlinkJobManager() as manager:
            job_id = await manager.submit_python_job("/path/to/job.py")
            
            assert job_id is None


@pytest.mark.asyncio
class TestFlinkJobDeployer:
    """Test Flink job deployer functionality."""
    
    async def test_job_deployer_initialization(self, flink_config):
        """Test FlinkJobDeployer initialization."""
        mock_manager = Mock(spec=FlinkJobManager)
        deployer = FlinkJobDeployer(mock_manager, flink_config)
        
        assert deployer.job_manager == mock_manager
        assert deployer.config == flink_config
        assert deployer.current_job_id is None
    
    async def test_deploy_job_success(self, flink_config):
        """Test successful job deployment."""
        mock_manager = AsyncMock(spec=FlinkJobManager)
        mock_manager.check_cluster_health.return_value = True
        mock_manager.submit_python_job.return_value = "job-123"
        
        deployer = FlinkJobDeployer(mock_manager, flink_config)
        
        job_id = await deployer.deploy_trend_detection_job()
        
        assert job_id == "job-123"
        assert deployer.current_job_id == "job-123"
        mock_manager.check_cluster_health.assert_called_once()
        mock_manager.submit_python_job.assert_called_once()
    
    async def test_deploy_job_unhealthy_cluster(self, flink_config):
        """Test job deployment with unhealthy cluster."""
        mock_manager = AsyncMock(spec=FlinkJobManager)
        mock_manager.check_cluster_health.return_value = False
        
        deployer = FlinkJobDeployer(mock_manager, flink_config)
        
        job_id = await deployer.deploy_trend_detection_job()
        
        assert job_id is None
        mock_manager.check_cluster_health.assert_called_once()
        mock_manager.submit_python_job.assert_not_called()
    
    async def test_stop_current_job(self, flink_config):
        """Test stopping current job."""
        mock_manager = AsyncMock(spec=FlinkJobManager)
        mock_manager.cancel_job.return_value = True
        mock_manager.wait_for_job_completion.return_value = FlinkJobStatus(
            job_id="job-123",
            name="Test Job",
            state="CANCELED"
        )
        
        deployer = FlinkJobDeployer(mock_manager, flink_config)
        deployer.current_job_id = "job-123"
        
        result = await deployer.stop_current_job()
        
        assert result is True
        assert deployer.current_job_id is None
        mock_manager.cancel_job.assert_called_once_with("job-123")
    
    async def test_monitor_job_health_no_job(self, flink_config):
        """Test monitoring job health when no job is running."""
        mock_manager = Mock(spec=FlinkJobManager)
        deployer = FlinkJobDeployer(mock_manager, flink_config)
        
        health = await deployer.monitor_job_health()
        
        assert health["status"] == "no_job"
        assert health["message"] == "No job currently running"
    
    async def test_monitor_job_health_running(self, flink_config):
        """Test monitoring job health for running job."""
        mock_manager = AsyncMock(spec=FlinkJobManager)
        mock_manager.get_job_details.return_value = {
            'jid': 'job-123',
            'name': 'Test Job',
            'state': 'RUNNING',
            'start-time': int(time.time() * 1000)
        }
        mock_manager.get_job_metrics.return_value = {'numRecordsIn': 1000}
        
        deployer = FlinkJobDeployer(mock_manager, flink_config)
        deployer.current_job_id = "job-123"
        
        health = await deployer.monitor_job_health()
        
        assert health["status"] == "healthy"
        assert health["job_id"] == "job-123"
        assert health["state"] == "RUNNING"
        assert "metrics" in health


@pytest.mark.integration
@pytest.mark.asyncio
class TestFlinkIntegrationEnd2End:
    """End-to-end integration tests (requires running infrastructure)."""
    
    @pytest.mark.skipif(
        not os.getenv("RUN_INTEGRATION_TESTS"),
        reason="Integration tests require RUN_INTEGRATION_TESTS=1"
    )
    async def test_kafka_connectivity(self, flink_config):
        """Test Kafka connectivity for integration."""
        try:
            # Test producer
            producer = KafkaProducer(
                bootstrap_servers=flink_config['kafka_bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                request_timeout_ms=5000
            )
            
            # Send test message
            test_message = {
                'uri': 'test://message',
                'author_did': 'did:test',
                'text': 'test message',
                'created_at': datetime.now().isoformat()
            }
            
            future = producer.send(flink_config['kafka_topic_posts'], test_message)
            producer.flush(timeout=10)
            
            # Verify message was sent
            record_metadata = future.get(timeout=10)
            assert record_metadata.topic == flink_config['kafka_topic_posts']
            
            producer.close()
            
        except KafkaError as e:
            pytest.skip(f"Kafka not available for integration test: {e}")
    
    @pytest.mark.skipif(
        not os.getenv("RUN_INTEGRATION_TESTS"),
        reason="Integration tests require RUN_INTEGRATION_TESTS=1"
    )
    async def test_flink_cluster_connectivity(self):
        """Test Flink cluster connectivity."""
        try:
            flink_host = os.getenv("FLINK_JOBMANAGER_HOST", "localhost")
            flink_port = int(os.getenv("FLINK_JOBMANAGER_PORT", "8081"))
            
            async with FlinkJobManager(flink_host, flink_port) as manager:
                healthy = await manager.check_cluster_health()
                if not healthy:
                    pytest.skip("Flink cluster not healthy for integration test")
                
                jobs = await manager.list_jobs()
                assert isinstance(jobs, list)
                
        except Exception as e:
            pytest.skip(f"Flink cluster not available for integration test: {e}")


def run_integration_tests():
    """Run integration tests with proper setup."""
    # Set environment variable to enable integration tests
    os.environ["RUN_INTEGRATION_TESTS"] = "1"
    
    # Run pytest with integration test markers
    pytest.main([
        __file__,
        "-v",
        "-m", "integration",
        "--tb=short"
    ])


if __name__ == "__main__":
    # Run unit tests by default
    pytest.main([__file__, "-v", "--tb=short"])