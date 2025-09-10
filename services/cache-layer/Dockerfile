FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared models
COPY ../../shared /app/shared

# Copy cache layer source code
COPY . .

# Create non-root user
RUN useradd --create-home --shell /bin/bash cache_user
USER cache_user

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import asyncio; from cache_layer import CacheLayer; asyncio.run(CacheLayer().health_check())" || exit 1

# Default command (can be overridden)
CMD ["python", "-c", "print('Cache layer service ready')"]