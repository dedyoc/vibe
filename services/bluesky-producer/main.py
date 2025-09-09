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

# Global client instance
firehose_client: Optional[BlueskyFirehoseClient] = None
shutdown_event = asyncio.Event()

# FastAPI app for health checks and metrics
app = FastAPI(title="Bluesky Producer", version="1.0.0")


@app.get("/health")
async def health_check() -> Dict[str, Any]:
    """Health check endpoint for Docker health checks."""
    global firehose_client
    
    if firehose_client is None:
        return {"status": "starting", "service": "bluesky-producer"}
    
    stats = firehose_client.get_stats()
    return {
        "status": "healthy" if stats['is_connected'] else "unhealthy",
        "service": "bluesky-producer",
        "connected": stats['is_connected'],
        "posts_processed": stats['posts_processed'],
        "last_message_time": stats['last_message_time']
    }


@app.get("/metrics")
async def metrics() -> Response:
    """Prometheus metrics endpoint."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/")
async def root() -> Dict[str, Any]:
    """Root endpoint with service information."""
    global firehose_client
    
    base_info = {
        "service": "bluesky-producer",
        "status": "running",
        "description": "Bluesky firehose data producer"
    }
    
    if firehose_client:
        base_info.update(firehose_client.get_stats())
    
    return base_info


@app.get("/stats")
async def get_stats() -> Dict[str, Any]:
    """Get detailed client statistics."""
    global firehose_client
    
    if firehose_client is None:
        return {"error": "Client not initialized"}
    
    return firehose_client.get_stats()


async def process_firehose_data() -> None:
    """Process data from Bluesky firehose."""
    global firehose_client
    
    log = logger.bind(component="firehose_processor")
    
    while not shutdown_event.is_set():
        try:
            if firehose_client is None:
                firehose_client = BlueskyFirehoseClient()
            
            # Connect to firehose
            CONNECTION_ATTEMPTS.inc()
            await firehose_client.connect_to_firehose()
            log.info("Connected to Bluesky firehose")
            
            # Process messages
            async for post_data in firehose_client.process_firehose_stream():
                if shutdown_event.is_set():
                    break
                
                POSTS_PROCESSED.inc()
                log.debug("Processed post", 
                         author=post_data.author_did,
                         text_length=len(post_data.text),
                         hashtags=len(post_data.hashtags))
                
                # TODO: Send to Kafka (Task 4)
                
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
    global firehose_client
    
    logger.info("Shutting down Bluesky Producer service...")
    shutdown_event.set()
    
    if firehose_client:
        await firehose_client.disconnect()
    
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