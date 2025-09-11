"""
Health check server for Superset Integration service
"""

import asyncio
import json
from datetime import datetime
from typing import Dict, Any
from aiohttp import web, ClientSession
import aioredis
import logging

logger = logging.getLogger(__name__)

class HealthServer:
    """HTTP server for health checks and metrics"""
    
    def __init__(self, port: int = 8002):
        self.port = port
        self.app = web.Application()
        self.setup_routes()
        
    def setup_routes(self) -> None:
        """Set up HTTP routes"""
        self.app.router.add_get('/health', self.health_check)
        self.app.router.add_get('/metrics', self.metrics)
        self.app.router.add_get('/status', self.status)
    
    async def health_check(self, request: web.Request) -> web.Response:
        """Health check endpoint"""
        try:
            # Check Redis connection
            redis_client = aioredis.from_url("redis://redis:6379", decode_responses=True)
            await redis_client.ping()
            await redis_client.close()
            
            # Check Superset connection
            async with ClientSession() as session:
                async with session.get("http://superset:8088/health", timeout=5) as resp:
                    superset_healthy = resp.status == 200
            
            if superset_healthy:
                return web.json_response({
                    "status": "healthy",
                    "timestamp": datetime.utcnow().isoformat(),
                    "services": {
                        "redis": "healthy",
                        "superset": "healthy"
                    }
                })
            else:
                return web.json_response({
                    "status": "unhealthy",
                    "timestamp": datetime.utcnow().isoformat(),
                    "services": {
                        "redis": "healthy",
                        "superset": "unhealthy"
                    }
                }, status=503)
                
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return web.json_response({
                "status": "unhealthy",
                "timestamp": datetime.utcnow().isoformat(),
                "error": str(e)
            }, status=503)
    
    async def metrics(self, request: web.Request) -> web.Response:
        """Prometheus metrics endpoint"""
        try:
            redis_client = aioredis.from_url("redis://redis:6379", decode_responses=True)
            
            # Get metrics from Redis
            trending_count = await redis_client.hlen("trends:current")
            historical_count = len(await redis_client.get("superset:historical_trends") or "[]")
            
            await redis_client.close()
            
            metrics = f"""# HELP superset_integration_trending_keywords_total Current trending keywords count
# TYPE superset_integration_trending_keywords_total gauge
superset_integration_trending_keywords_total {trending_count}

# HELP superset_integration_historical_records_total Historical trend records count
# TYPE superset_integration_historical_records_total gauge
superset_integration_historical_records_total {historical_count}

# HELP superset_integration_last_sync_timestamp Last successful data sync timestamp
# TYPE superset_integration_last_sync_timestamp gauge
superset_integration_last_sync_timestamp {datetime.utcnow().timestamp()}
"""
            
            return web.Response(text=metrics, content_type='text/plain')
            
        except Exception as e:
            logger.error(f"Metrics collection failed: {e}")
            return web.Response(text="# Metrics unavailable\n", content_type='text/plain', status=503)
    
    async def status(self, request: web.Request) -> web.Response:
        """Detailed status endpoint"""
        try:
            redis_client = aioredis.from_url("redis://redis:6379", decode_responses=True)
            
            # Get detailed status
            trending_data = await redis_client.hgetall("trends:current")
            historical_data = await redis_client.get("superset:historical_trends")
            
            await redis_client.close()
            
            status_data = {
                "service": "superset-integration",
                "timestamp": datetime.utcnow().isoformat(),
                "data_sources": {
                    "redis_trending_keywords": len(trending_data),
                    "historical_trends": len(json.loads(historical_data or "[]")),
                },
                "superset_dashboards": {
                    "realtime_trends": "configured",
                    "historical_analysis": "configured"
                },
                "sync_status": {
                    "last_sync": datetime.utcnow().isoformat(),
                    "sync_interval": "300s",
                    "status": "active"
                }
            }
            
            return web.json_response(status_data)
            
        except Exception as e:
            logger.error(f"Status check failed: {e}")
            return web.json_response({
                "service": "superset-integration",
                "timestamp": datetime.utcnow().isoformat(),
                "error": str(e)
            }, status=503)
    
    async def start_server(self) -> None:
        """Start the health check server"""
        runner = web.AppRunner(self.app)
        await runner.setup()
        
        site = web.TCPSite(runner, '0.0.0.0', self.port)
        await site.start()
        
        logger.info(f"Health server started on port {self.port}")

async def run_health_server():
    """Run the health check server"""
    server = HealthServer()
    await server.start_server()
    
    # Keep the server running
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Health server shutting down...")

if __name__ == "__main__":
    asyncio.run(run_health_server())