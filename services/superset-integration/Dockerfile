FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .
COPY ../shared /app/shared

# Create non-root user
RUN useradd --create-home --shell /bin/bash superset-integration
RUN chown -R superset-integration:superset-integration /app
USER superset-integration

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8002/health || exit 1

# Expose port for health checks and metrics
EXPOSE 8002

# Run the application
CMD ["python", "superset_manager.py"]