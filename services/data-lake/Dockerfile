# Data Lake service Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared models
COPY ../../shared /app/shared

# Copy service code
COPY . .

# Set Python path to include shared modules
ENV PYTHONPATH=/app

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import asyncio; from data_lake_manager import DataLakeManager; print('OK')" || exit 1

# Default command (can be overridden)
CMD ["python", "-c", "print('Data Lake service ready')"]