#!/usr/bin/env python3
"""
Bluesky Producer Service

Connects to Bluesky firehose and processes posts for the streaming pipeline.
Provides health check endpoints and metrics collection.
"""

import asyncio
import logging
import os
import signal
from typing import Dict, Any, Optional

from fastapi import FastAPI
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response
import uvicorn
import structlog

from bluesky_client import BlueskyFirehoseClient
from kafka_producer import KafkaProducerManager

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)

# Prometheus metrics
POSTS_PROCESSED = Counter('bluesky_posts_processed_total', 'Total posts processed')
PROCESSING_TIME = Histogram('bluesky_processing_seconds', 'Time spent processing posts')
CONNECTION_ATTEMPTS = Counter('bluesky_connection_attempts_total', 'Total connection attempts')
RECONNECTIONS = Counter('bluesky_reconnections_total', 'Total reconnection attempts')

# Global client instances
firehose_client: Optional[BlueskyFirehoseClient] = None
kafka_manager: Optional[KafkaProducerManager] = None
shutdown_event = asyncio.Event()

# FastAPI app for health checks and metrics
app = FastAPI(title="Bluesky Producer", version="1.0.0")


@app.get("/health")
async def health_check() -> Dict[str, Any]:
    """Health check endpoint for Docker health checks."""
    global firehose_client, kafka_manager
    
    if firehose_client is None or kafka_manager is None:
        return {"status": "starting", "service": "bluesky-producer"}
    
    firehose_stats = firehose_client.get_stats()
    kafka_stats = kafka_manager.get_all_stats()
    
    # Check if both firehose and kafka are healthy
    firehose_healthy = firehose_stats['is_connected']
    kafka_healthy = any(stats['is_connected'] for stats in kafka_stats.values()) if kafka_stats else False
    
    return {
        "status": "healthy" if (firehose_healthy and kafka_healthy) else "unhealthy",
        "service": "bluesky-producer",
        "firehose_connected": firehose_healthy,
        "kafka_connected": kafka_healthy,
        "posts_processed": firehose_stats['posts_processed'],
        "last_message_time": firehose_stats['last_message_time'],
        "kafka_topics": list(kafka_stats.keys())
    }


@app.get("/metrics")
async def metrics() -> Response:
    """Prometheus metrics endpoint."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/")
async def root() -> Dict[str, Any]:
    """Root endpoint with service information."""
    global firehose_client, kafka_manager
    
    base_info = {
        "service": "bluesky-producer",
        "status": "running",
        "description": "Bluesky firehose data producer with Kafka publishing"
    }
    
    if firehose_client:
        base_info.update(firehose_client.get_stats())
    
    if kafka_manager:
        base_info["kafka_stats"] = kafka_manager.get_all_stats()
    
    return base_info


@app.get("/stats")
async def get_stats() -> Dict[str, Any]:
    """Get detailed client statistics."""
    global firehose_client, kafka_manager
    
    stats = {}
    
    if firehose_client is None:
        stats["firehose"] = {"error": "Firehose client not initialized"}
    else:
        stats["firehose"] = firehose_client.get_stats()
    
    if kafka_manager is None:
        stats["kafka"] = {"error": "Kafka manager not initialized"}
    else:
        stats["kafka"] = kafka_manager.get_all_stats()
    
    return stats


async def process_firehose_data() -> None:
    """Process data from Bluesky firehose and publish to Kafka."""
    global firehose_client, kafka_manager
    
    log = logger.bind(component="firehose_processor")
    
    # Get configuration
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC_POSTS", "bluesky-posts")
    
    while not shutdown_event.is_set():
        try:
            # Initialize clients if needed
            if firehose_client is None:
                firehose_client = BlueskyFirehoseClient()
            
            if kafka_manager is None:
                kafka_manager = KafkaProducerManager(kafka_servers)
                log.info("Initialized Kafka producer manager", 
                        servers=kafka_servers, topic=kafka_topic)
            
            # Connect to firehose
            CONNECTION_ATTEMPTS.inc()
            await firehose_client.connect_to_firehose()
            log.info("Connected to Bluesky firehose")
            
            # Process messages
            async for post_data in firehose_client.process_firehose_stream():
                if shutdown_event.is_set():
                    break
                
                POSTS_PROCESSED.inc()
                
                # Publish to Kafka
                try:
                    success = await kafka_manager.publish_to_topic(kafka_topic, post_data)
                    if success:
                        log.debug("Published post to Kafka", 
                                 author=post_data.author_did,
                                 text_length=len(post_data.text),
                                 hashtags=len(post_data.hashtags),
                                 topic=kafka_topic)
                    else:
                        log.warning("Failed to publish post to Kafka",
                                   author=post_data.author_did,
                                   topic=kafka_topic)
                        
                except Exception as kafka_error:
                    log.error("Error publishing to Kafka", 
                             error=str(kafka_error),
                             author=post_data.author_did)
                
        except Exception as e:
            log.error("Error in firehose processing", error=str(e))
            RECONNECTIONS.inc()
            
            if firehose_client:
                try:
                    await firehose_client.handle_reconnection()
                    log.info("Reconnected to firehose")
                except Exception as reconnect_error:
                    log.error("Failed to reconnect", error=str(reconnect_error))
                    # Wait before retrying
                    await asyncio.sleep(30)
            else:
                await asyncio.sleep(5)


async def shutdown_handler() -> None:
    """Handle graceful shutdown."""
    global firehose_client, kafka_manager
    
    logger.info("Shutting down Bluesky Producer service...")
    shutdown_event.set()
    
    # Close Kafka producers first to flush pending messages
    if kafka_manager:
        try:
            await kafka_manager.close_all()
            logger.info("Kafka producers closed")
        except Exception as e:
            logger.error("Error closing Kafka producers", error=str(e))
    
    # Close firehose client
    if firehose_client:
        try:
            await firehose_client.disconnect()
            logger.info("Firehose client disconnected")
        except Exception as e:
            logger.error("Error disconnecting firehose client", error=str(e))
    
    logger.info("Shutdown complete")


def signal_handler(signum: int, frame) -> None:
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}, initiating shutdown...")
    asyncio.create_task(shutdown_handler())


async def main() -> None:
    """Main application entry point."""
    logger.info("Starting Bluesky Producer service...")
    
    # Get configuration from environment
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    log_level = os.getenv("LOG_LEVEL", "INFO")
    prometheus_port = int(os.getenv("PROMETHEUS_PORT", "8000"))
    
    logger.info("Configuration loaded",
               kafka_servers=kafka_servers,
               redis_host=redis_host,
               redis_port=redis_port,
               prometheus_port=prometheus_port)
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start background tasks
    firehose_task = asyncio.create_task(process_firehose_data())
    
    # Start the FastAPI server for health checks and metrics
    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=prometheus_port,
        log_level=log_level.lower()
    )
    server = uvicorn.Server(config)
    
    try:
        # Run server and firehose processing concurrently
        await asyncio.gather(
            server.serve(),
            firehose_task,
            return_exceptions=True
        )
    except Exception as e:
        logger.error("Error in main loop", error=str(e))
    finally:
        await shutdown_handler()


if __name__ == "__main__":
    asyncio.run(main())