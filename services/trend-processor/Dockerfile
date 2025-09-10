FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies including Java for Flink
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    openjdk-11-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Install Flink CLI (for job submission)
ENV FLINK_VERSION=1.18.0
ENV SCALA_VERSION=2.12
RUN wget -q https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz \
    && tar -xzf flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz \
    && mv flink-${FLINK_VERSION} /opt/flink \
    && rm flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz

# Add Flink to PATH
ENV PATH="/opt/flink/bin:${PATH}"
ENV FLINK_HOME="/opt/flink"

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