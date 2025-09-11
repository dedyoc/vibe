#!/bin/bash
# Superset initialization script for Bluesky Streaming Pipeline

set -e

echo "Starting Superset initialization..."

# Wait for dependencies
echo "Waiting for Redis to be ready..."
while ! redis-cli -h ${REDIS_HOST:-redis} -p ${REDIS_PORT:-6379} ping > /dev/null 2>&1; do
    echo "Redis is unavailable - sleeping"
    sleep 2
done
echo "Redis is ready!"

# Initialize Superset database
echo "Initializing Superset database..."
superset db upgrade

# Create admin user if it doesn't exist
echo "Creating admin user..."
superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@bluesky-pipeline.local \
    --password admin123 || echo "Admin user already exists"

# Initialize Superset
echo "Initializing Superset..."
superset init

# Load example data and dashboards
echo "Loading example dashboards..."
superset load_examples || echo "Examples already loaded or failed to load"

echo "Superset initialization completed!"

# Start Superset web server
echo "Starting Superset web server..."
exec superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger