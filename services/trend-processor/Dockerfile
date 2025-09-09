FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies including Java for Flink
RUN apt-get update && apt-get install -y \
    curl \
    openjdk-11-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Copy requirements first for better caching
COPY services/trend-processor/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared modules
COPY shared/ ./shared/

# Copy application code
COPY services/trend-processor/ .

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash app \
    && chown -R app:app /app
USER app

# Expose metrics port
EXPOSE 8001

# Health check endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8001/health || exit 1

# Run the application
CMD ["python", "main.py"]