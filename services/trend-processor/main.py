#!/usr/bin/env python3
"""
Trend Processor Service - Flink Stream Processing Integration

This service manages the Flink stream processing job for trend detection,
provides health checks, metrics, and job management endpoints.
"""

import asyncio
import logging
import os
from typing import Dict, Any, Optional

from fastapi import FastAPI, HTTPException
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response
import uvicorn

import sys
import os
sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from flink_manager import FlinkJobManager, FlinkJobDeployer
from flink_job import create_flink_job_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics
TRENDS_DETECTED = Counter('trends_detected_total', 'Total trends detected')
PROCESSING_TIME = Histogram('trend_processing_seconds', 'Time spent processing trends')
FLINK_JOB_RESTARTS = Counter('flink_job_restarts_total', 'Total Flink job restarts')

# FastAPI app for health checks and metrics
app = FastAPI(title="Trend Processor", version="1.0.0")

# Global variables for job management
job_manager: Optional[FlinkJobManager] = None
job_deployer: Optional[FlinkJobDeployer] = None


@app.get("/health")
async def health_check() -> Dict[str, str]:
    """Health check endpoint for Docker health checks."""
    global job_deployer
    
    if not job_deployer:
        return {"status": "starting", "service": "trend-processor"}
    
    # Check Flink job health
    job_health = await job_deployer.monitor_job_health()
    
    if job_health.get("status") == "healthy":
        return {"status": "healthy", "service": "trend-processor", "flink_job": "running"}
    elif job_health.get("status") == "no_job":
        return {"status": "healthy", "service": "trend-processor", "flink_job": "not_deployed"}
    else:
        return {"status": "degraded", "service": "trend-processor", "flink_job": job_health.get("status")}


@app.get("/metrics")
async def metrics() -> Response:
    """Prometheus metrics endpoint."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/")
async def root() -> Dict[str, str]:
    """Root endpoint with service information."""
    return {
        "service": "trend-processor",
        "status": "running",
        "description": "Real-time trend detection processor with Flink integration"
    }


@app.get("/flink/status")
async def flink_status() -> Dict[str, Any]:
    """Get Flink cluster and job status."""
    global job_manager, job_deployer
    
    if not job_manager:
        raise HTTPException(status_code=503, detail="Flink manager not initialized")
    
    try:
        # Check cluster health
        cluster_healthy = await job_manager.check_cluster_health()
        
        # List all jobs
        jobs = await job_manager.list_jobs()
        
        # Get current job health if deployer exists
        job_health = {}
        if job_deployer:
            job_health = await job_deployer.monitor_job_health()
        
        return {
            "cluster_healthy": cluster_healthy,
            "total_jobs": len(jobs),
            "running_jobs": len([j for j in jobs if j.state == "RUNNING"]),
            "current_job": job_health,
            "all_jobs": [
                {
                    "job_id": job.job_id,
                    "name": job.name,
                    "state": job.state,
                    "start_time": job.start_time.isoformat() if job.start_time else None
                }
                for job in jobs
            ]
        }
    except Exception as e:
        logger.error(f"Failed to get Flink status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/flink/deploy")
async def deploy_flink_job() -> Dict[str, Any]:
    """Deploy the trend detection Flink job."""
    global job_deployer
    
    if not job_deployer:
        raise HTTPException(status_code=503, detail="Job deployer not initialized")
    
    try:
        job_id = await job_deployer.deploy_trend_detection_job()
        if job_id:
            return {"status": "success", "job_id": job_id, "message": "Job deployed successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to deploy job")
    except Exception as e:
        logger.error(f"Failed to deploy Flink job: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/flink/stop")
async def stop_flink_job() -> Dict[str, Any]:
    """Stop the current Flink job."""
    global job_deployer
    
    if not job_deployer:
        raise HTTPException(status_code=503, detail="Job deployer not initialized")
    
    try:
        success = await job_deployer.stop_current_job()
        if success:
            return {"status": "success", "message": "Job stopped successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to stop job")
    except Exception as e:
        logger.error(f"Failed to stop Flink job: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def initialize_flink_integration() -> None:
    """Initialize Flink job manager and deployer."""
    global job_manager, job_deployer
    
    try:
        # Get configuration from environment
        flink_host = os.getenv("FLINK_JOBMANAGER_HOST", "flink-jobmanager")
        flink_port = int(os.getenv("FLINK_JOBMANAGER_PORT", "8081"))
        
        # Create Flink job configuration
        flink_config = create_flink_job_config(
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
            kafka_topic_posts=os.getenv("KAFKA_TOPIC_POSTS", "bluesky-posts"),
            kafka_topic_windowed_counts=os.getenv("KAFKA_TOPIC_WINDOWED_COUNTS", "windowed-keyword-counts"),
            kafka_topic_trend_alerts=os.getenv("KAFKA_TOPIC_TREND_ALERTS", "trend-alerts"),
            minio_endpoint=f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}",
            minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
            parallelism=int(os.getenv("FLINK_PARALLELISM", "2")),
            window_size_minutes=int(os.getenv("WINDOW_SIZE_MINUTES", "10"))
        )
        
        # Initialize job manager
        job_manager = FlinkJobManager(flink_host, flink_port)
        await job_manager.__aenter__()
        
        # Initialize job deployer
        job_deployer = FlinkJobDeployer(job_manager, flink_config)
        
        logger.info("Flink integration initialized successfully")
        
        # Wait for Flink cluster to be ready
        max_retries = 30
        for attempt in range(max_retries):
            if await job_manager.check_cluster_health():
                logger.info("Flink cluster is healthy and ready")
                break
            else:
                logger.info(f"Waiting for Flink cluster... (attempt {attempt + 1}/{max_retries})")
                await asyncio.sleep(10)
        else:
            logger.warning("Flink cluster health check timed out, but continuing...")
        
    except Exception as e:
        logger.error(f"Failed to initialize Flink integration: {e}")
        raise


async def cleanup_flink_integration() -> None:
    """Cleanup Flink integration resources."""
    global job_manager, job_deployer
    
    try:
        if job_deployer:
            await job_deployer.stop_current_job()
        
        if job_manager:
            await job_manager.__aexit__(None, None, None)
        
        logger.info("Flink integration cleaned up successfully")
    except Exception as e:
        logger.error(f"Failed to cleanup Flink integration: {e}")


async def main() -> None:
    """Main application entry point."""
    logger.info("Starting Trend Processor service...")
    
    # Get configuration from environment
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    flink_host = os.getenv("FLINK_JOBMANAGER_HOST", "flink-jobmanager")
    flink_port = int(os.getenv("FLINK_JOBMANAGER_PORT", "8081"))
    log_level = os.getenv("LOG_LEVEL", "INFO")
    prometheus_port = int(os.getenv("PROMETHEUS_PORT", "8001"))
    
    logger.info(f"Configuration: Kafka={kafka_servers}, Redis={redis_host}:{redis_port}")
    logger.info(f"MinIO={minio_endpoint}, Flink={flink_host}:{flink_port}")
    
    try:
        # Initialize Flink integration
        await initialize_flink_integration()
        
        logger.info("Trend Processor service is ready with Flink integration")
        
        # Start the FastAPI server for health checks and metrics
        config = uvicorn.Config(
            app,
            host="0.0.0.0",
            port=prometheus_port,
            log_level=log_level.lower()
        )
        server = uvicorn.Server(config)
        await server.serve()
        
    except Exception as e:
        logger.error(f"Failed to start Trend Processor service: {e}")
        raise
    finally:
        # Cleanup resources
        await cleanup_flink_integration()


if __name__ == "__main__":
    asyncio.run(main())