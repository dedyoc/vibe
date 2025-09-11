#!/usr/bin/env python3
"""
Flink Job Manager

This module provides utilities for managing Flink jobs, including deployment,
monitoring, and integration with the trend processor service.
"""

import asyncio
import json
import logging
import os
import subprocess
import time
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from datetime import datetime

import aiohttp
from prometheus_client import Counter, Gauge, Histogram

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics
FLINK_JOB_SUBMISSIONS = Counter('flink_job_submissions_total', 'Total Flink job submissions')
FLINK_ACTIVE_JOBS = Gauge('flink_active_jobs', 'Number of active Flink jobs')
FLINK_JOB_DURATION = Histogram('flink_job_duration_seconds', 'Flink job execution duration')


@dataclass
class FlinkJobStatus:
    """Represents the status of a Flink job."""
    
    job_id: str
    name: str
    state: str  # CREATED, RUNNING, FINISHED, CANCELED, FAILED
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration: Optional[int] = None  # in milliseconds
    
    @classmethod
    def from_api_response(cls, data: Dict[str, Any]) -> 'FlinkJobStatus':
        """Create FlinkJobStatus from Flink REST API response."""
        start_time = None
        end_time = None
        
        if data.get('start-time'):
            start_time = datetime.fromtimestamp(data['start-time'] / 1000)
        
        if data.get('end-time'):
            end_time = datetime.fromtimestamp(data['end-time'] / 1000)
        
        return cls(
            job_id=data['jid'],
            name=data['name'],
            state=data['state'],
            start_time=start_time,
            end_time=end_time,
            duration=data.get('duration')
        )


class FlinkJobManager:
    """Manages Flink job lifecycle and monitoring."""
    
    def __init__(
        self,
        flink_host: str = "flink-jobmanager",
        flink_port: int = 8081,
        job_jar_path: Optional[str] = None
    ):
        """
        Initialize Flink job manager.
        
        Args:
            flink_host: Flink JobManager hostname
            flink_port: Flink JobManager REST API port
            job_jar_path: Path to the Flink job JAR file (for Java jobs)
        """
        self.flink_host = flink_host
        self.flink_port = flink_port
        self.job_jar_path = job_jar_path
        self.base_url = f"http://{flink_host}:{flink_port}"
        self.session = None
        
    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    async def check_cluster_health(self) -> bool:
        """
        Check if Flink cluster is healthy and accessible.
        
        Returns:
            True if cluster is healthy, False otherwise
        """
        try:
            async with self.session.get(f"{self.base_url}/overview") as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Flink cluster healthy: {data.get('flink-version', 'unknown version')}")
                    return True
                else:
                    logger.warning(f"Flink cluster health check failed: HTTP {response.status}")
                    return False
        except Exception as e:
            logger.error(f"Failed to check Flink cluster health: {e}")
            return False
    
    async def list_jobs(self) -> List[FlinkJobStatus]:
        """
        List all jobs in the Flink cluster.
        
        Returns:
            List of FlinkJobStatus objects
        """
        try:
            async with self.session.get(f"{self.base_url}/jobs") as response:
                if response.status == 200:
                    data = await response.json()
                    jobs = []
                    for job_data in data.get('jobs', []):
                        jobs.append(FlinkJobStatus.from_api_response(job_data))
                    
                    # Update metrics
                    FLINK_ACTIVE_JOBS.set(len([j for j in jobs if j.state == 'RUNNING']))
                    
                    return jobs
                else:
                    logger.error(f"Failed to list jobs: HTTP {response.status}")
                    return []
        except Exception as e:
            logger.error(f"Failed to list Flink jobs: {e}")
            return []
    
    async def get_job_details(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a specific job.
        
        Args:
            job_id: Flink job ID
            
        Returns:
            Job details dictionary or None if not found
        """
        try:
            async with self.session.get(f"{self.base_url}/jobs/{job_id}") as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.warning(f"Job {job_id} not found: HTTP {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Failed to get job details for {job_id}: {e}")
            return None
    
    async def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a running Flink job.
        
        Args:
            job_id: Flink job ID to cancel
            
        Returns:
            True if cancellation was successful, False otherwise
        """
        try:
            async with self.session.patch(f"{self.base_url}/jobs/{job_id}") as response:
                if response.status == 202:
                    logger.info(f"Job {job_id} cancellation requested")
                    return True
                else:
                    logger.error(f"Failed to cancel job {job_id}: HTTP {response.status}")
                    return False
        except Exception as e:
            logger.error(f"Failed to cancel job {job_id}: {e}")
            return False
    
    async def submit_python_job(
        self,
        python_file: str,
        job_args: Optional[List[str]] = None,
        parallelism: Optional[int] = None
    ) -> Optional[str]:
        """
        Submit a Python Flink job using the Flink CLI.
        
        Args:
            python_file: Path to the Python file containing the Flink job
            job_args: Optional arguments to pass to the job
            parallelism: Optional parallelism setting
            
        Returns:
            Job ID if submission was successful, None otherwise
        """
        try:
            # Build the flink run command
            cmd = ["flink", "run", "-py", python_file]
            
            if parallelism:
                cmd.extend(["-p", str(parallelism)])
            
            if job_args:
                cmd.extend(job_args)
            
            logger.info(f"Submitting Python job: {' '.join(cmd)}")
            
            # Execute the command
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=60  # 1 minute timeout
            )
            
            if result.returncode == 0:
                # Parse job ID from output
                output_lines = result.stdout.split('\n')
                for line in output_lines:
                    if 'Job has been submitted with JobID' in line:
                        job_id = line.split('JobID')[-1].strip()
                        logger.info(f"Python job submitted successfully: {job_id}")
                        FLINK_JOB_SUBMISSIONS.inc()
                        return job_id
                
                logger.warning("Job submitted but couldn't parse job ID")
                FLINK_JOB_SUBMISSIONS.inc()
                return "unknown"
            else:
                logger.error(f"Failed to submit Python job: {result.stderr}")
                return None
                
        except subprocess.TimeoutExpired:
            logger.error("Job submission timed out")
            return None
        except Exception as e:
            logger.error(f"Failed to submit Python job: {e}")
            return None
    
    async def wait_for_job_completion(
        self,
        job_id: str,
        timeout_seconds: int = 300
    ) -> Optional[FlinkJobStatus]:
        """
        Wait for a job to complete (finish, cancel, or fail).
        
        Args:
            job_id: Job ID to wait for
            timeout_seconds: Maximum time to wait in seconds
            
        Returns:
            Final job status or None if timeout
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout_seconds:
            job_details = await self.get_job_details(job_id)
            if job_details:
                status = FlinkJobStatus.from_api_response(job_details)
                
                if status.state in ['FINISHED', 'CANCELED', 'FAILED']:
                    logger.info(f"Job {job_id} completed with state: {status.state}")
                    return status
            
            await asyncio.sleep(5)  # Check every 5 seconds
        
        logger.warning(f"Timeout waiting for job {job_id} to complete")
        return None
    
    async def get_job_metrics(self, job_id: str) -> Dict[str, Any]:
        """
        Get metrics for a specific job.
        
        Args:
            job_id: Job ID to get metrics for
            
        Returns:
            Dictionary containing job metrics
        """
        try:
            async with self.session.get(f"{self.base_url}/jobs/{job_id}/metrics") as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.warning(f"Failed to get metrics for job {job_id}: HTTP {response.status}")
                    return {}
        except Exception as e:
            logger.error(f"Failed to get job metrics for {job_id}: {e}")
            return {}


class FlinkJobDeployer:
    """Handles deployment of the trend detection Flink job."""
    
    def __init__(self, job_manager: FlinkJobManager, config: Dict[str, Any]):
        """
        Initialize job deployer.
        
        Args:
            job_manager: FlinkJobManager instance
            config: Job configuration dictionary
        """
        self.job_manager = job_manager
        self.config = config
        self.current_job_id = None
    
    async def deploy_trend_detection_job(self) -> Optional[str]:
        """
        Deploy the trend detection Flink job.
        
        Returns:
            Job ID if deployment was successful, None otherwise
        """
        try:
            # Check if cluster is healthy
            if not await self.job_manager.check_cluster_health():
                logger.error("Flink cluster is not healthy, cannot deploy job")
                return None
            
            # Cancel existing job if running
            if self.current_job_id:
                await self.stop_current_job()
            
            # Prepare job arguments from config
            job_args = [
                "--kafka-bootstrap-servers", self.config.get('kafka_bootstrap_servers', 'kafka:29092'),
                "--kafka-topic-posts", self.config.get('kafka_topic_posts', 'bluesky-posts'),
                "--kafka-topic-trends", self.config.get('kafka_topic_trends', 'windowed-keyword-counts'),
                "--minio-endpoint", self.config.get('minio_endpoint', 'http://minio:9000'),
                "--minio-access-key", self.config.get('minio_access_key', 'minioadmin'),
                "--minio-secret-key", self.config.get('minio_secret_key', 'minioadmin123'),
                "--window-size-minutes", str(self.config.get('window_size_minutes', 10)),
                "--slide-interval-minutes", str(self.config.get('slide_interval_minutes', 1))
            ]
            
            # Submit the job
            job_id = await self.job_manager.submit_python_job(
                python_file="/app/flink_job.py",
                job_args=job_args,
                parallelism=self.config.get('parallelism', 2)
            )
            
            if job_id:
                self.current_job_id = job_id
                logger.info(f"Trend detection job deployed successfully: {job_id}")
                return job_id
            else:
                logger.error("Failed to deploy trend detection job")
                return None
                
        except Exception as e:
            logger.error(f"Failed to deploy trend detection job: {e}")
            return None
    
    async def stop_current_job(self) -> bool:
        """
        Stop the currently running job.
        
        Returns:
            True if job was stopped successfully, False otherwise
        """
        if not self.current_job_id:
            logger.info("No current job to stop")
            return True
        
        try:
            success = await self.job_manager.cancel_job(self.current_job_id)
            if success:
                # Wait for job to actually stop
                final_status = await self.job_manager.wait_for_job_completion(
                    self.current_job_id, timeout_seconds=60
                )
                if final_status and final_status.state in ['CANCELED', 'FAILED']:
                    logger.info(f"Job {self.current_job_id} stopped successfully")
                    self.current_job_id = None
                    return True
            
            logger.error(f"Failed to stop job {self.current_job_id}")
            return False
            
        except Exception as e:
            logger.error(f"Failed to stop current job: {e}")
            return False
    
    async def monitor_job_health(self) -> Dict[str, Any]:
        """
        Monitor the health of the current job.
        
        Returns:
            Dictionary containing job health information
        """
        if not self.current_job_id:
            return {"status": "no_job", "message": "No job currently running"}
        
        try:
            job_details = await self.job_manager.get_job_details(self.current_job_id)
            if not job_details:
                return {"status": "error", "message": "Job not found"}
            
            job_status = FlinkJobStatus.from_api_response(job_details)
            metrics = await self.job_manager.get_job_metrics(self.current_job_id)
            
            return {
                "status": "healthy" if job_status.state == "RUNNING" else "unhealthy",
                "job_id": self.current_job_id,
                "state": job_status.state,
                "start_time": job_status.start_time.isoformat() if job_status.start_time else None,
                "duration": job_status.duration,
                "metrics": metrics
            }
            
        except Exception as e:
            logger.error(f"Failed to monitor job health: {e}")
            return {"status": "error", "message": str(e)}


async def main():
    """Example usage of FlinkJobManager."""
    config = {
        'kafka_bootstrap_servers': 'kafka:29092',
        'kafka_topic_posts': 'bluesky-posts',
        'kafka_topic_trends': 'windowed-keyword-counts',
        'minio_endpoint': 'http://minio:9000',
        'minio_access_key': 'minioadmin',
        'minio_secret_key': 'minioadmin123',
        'parallelism': 2,
        'window_size_minutes': 10,
        'slide_interval_minutes': 1
    }
    
    async with FlinkJobManager() as job_manager:
        deployer = FlinkJobDeployer(job_manager, config)
        
        # Deploy job
        job_id = await deployer.deploy_trend_detection_job()
        if job_id:
            print(f"Job deployed: {job_id}")
            
            # Monitor for a while
            for _ in range(5):
                health = await deployer.monitor_job_health()
                print(f"Job health: {health}")
                await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(main())