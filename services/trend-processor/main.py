#!/usr/bin/env python3
"""
Trend Processor Service - Kafka-based Stream Processing

This service processes Bluesky posts from Kafka and performs real-time trend detection
using sliding windows, providing health checks, metrics, and trend management endpoints.
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

from kafka_trend_processor import KafkaTrendProcessor

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

# Global variables for trend processing
trend_processor: Optional[KafkaTrendProcessor] = None
processing_task: Optional[asyncio.Task] = None


@app.get("/health")
async def health_check() -> Dict[str, str]:
    """Health check endpoint for Docker health checks."""
    global trend_processor, processing_task
    
    if not trend_processor:
        return {"status": "starting", "service": "trend-processor"}
    
    # Check processing task health
    if processing_task and not processing_task.done():
        return {"status": "healthy", "service": "trend-processor", "processor": "running"}
    elif processing_task and processing_task.done():
        return {"status": "degraded", "service": "trend-processor", "processor": "stopped"}
    else:
        return {"status": "healthy", "service": "trend-processor", "processor": "not_started"}


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
        "description": "Real-time trend detection processor with Kafka streaming"
    }


@app.get("/stats")
async def get_stats() -> Dict[str, Any]:
    """Get trend processor statistics."""
    global trend_processor, processing_task
    
    if not trend_processor:
        raise HTTPException(status_code=503, detail="Trend processor not initialized")
    
    try:
        stats = trend_processor.get_stats()
        stats["processing_task_running"] = processing_task and not processing_task.done()
        return stats
    except Exception as e:
        logger.error(f"Failed to get stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/processor/start")
async def start_processor() -> Dict[str, Any]:
    """Start the trend processor."""
    global trend_processor, processing_task
    
    if not trend_processor:
        raise HTTPException(status_code=503, detail="Trend processor not initialized")
    
    if processing_task and not processing_task.done():
        return {"status": "already_running", "message": "Processor is already running"}
    
    try:
        processing_task = asyncio.create_task(trend_processor.start_processing())
        return {"status": "success", "message": "Trend processor started successfully"}
    except Exception as e:
        logger.error(f"Failed to start processor: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/processor/stop")
async def stop_processor() -> Dict[str, Any]:
    """Stop the trend processor."""
    global processing_task
    
    if not processing_task or processing_task.done():
        return {"status": "not_running", "message": "Processor is not running"}
    
    try:
        processing_task.cancel()
        try:
            await processing_task
        except asyncio.CancelledError:
            pass
        return {"status": "success", "message": "Trend processor stopped successfully"}
    except Exception as e:
        logger.error(f"Failed to stop processor: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def initialize_trend_processor() -> None:
    """Initialize the Kafka-based trend processor."""
    global trend_processor
    
    try:
        # Get configuration from environment
        kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
        kafka_topic_posts = os.getenv("KAFKA_TOPIC_POSTS", "bluesky-posts")
        kafka_topic_trends = os.getenv("KAFKA_TOPIC_TRENDS", "trend-alerts")
        redis_host = os.getenv("REDIS_HOST", "redis")
        redis_port = int(os.getenv("REDIS_PORT", "6379"))
        window_size_minutes = int(os.getenv("WINDOW_SIZE_MINUTES", "10"))
        
        # Initialize trend processor
        trend_processor = KafkaTrendProcessor(
            kafka_bootstrap_servers=kafka_servers,
            kafka_topic_posts=kafka_topic_posts,
            kafka_topic_trends=kafka_topic_trends,
            redis_host=redis_host,
            redis_port=redis_port,
            window_size_minutes=window_size_minutes
        )
        
        logger.info("Trend processor initialized successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize trend processor: {e}")
        raise


async def cleanup_trend_processor() -> None:
    """Cleanup trend processor resources."""
    global processing_task
    
    try:
        if processing_task and not processing_task.done():
            processing_task.cancel()
            try:
                await processing_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Trend processor cleaned up successfully")
    except Exception as e:
        logger.error(f"Failed to cleanup trend processor: {e}")


async def main() -> None:
    """Main application entry point."""
    logger.info("Starting Trend Processor service...")
    
    # Get configuration from environment
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    log_level = os.getenv("LOG_LEVEL", "INFO")
    prometheus_port = int(os.getenv("PROMETHEUS_PORT", "8001"))
    
    logger.info(f"Configuration: Kafka={kafka_servers}, Redis={redis_host}:{redis_port}")
    
    try:
        # Initialize trend processor
        await initialize_trend_processor()
        
        logger.info("Trend Processor service is ready with Kafka streaming")
        
        # Start the trend processor automatically in background
        global processing_task
        processing_task = asyncio.create_task(trend_processor.start_processing())
        
        # Start the FastAPI server for health checks and metrics
        config = uvicorn.Config(
            app,
            host="0.0.0.0",
            port=prometheus_port,
            log_level=log_level.lower()
        )
        server = uvicorn.Server(config)
        
        # Run both the server and processor concurrently
        await asyncio.gather(
            server.serve(),
            processing_task,
            return_exceptions=True
        )
        
    except Exception as e:
        logger.error(f"Failed to start Trend Processor service: {e}")
        raise
    finally:
        # Cleanup resources
        await cleanup_trend_processor()


if __name__ == "__main__":
    asyncio.run(main())