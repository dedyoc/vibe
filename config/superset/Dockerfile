FROM apache/superset:3.0.3

# Switch to root to install additional packages
USER root

# Install additional dependencies for Redis and S3 connectivity
RUN pip install --no-cache-dir \
    redis==4.6.0 \
    boto3==1.34.0 \
    s3fs==2023.12.2 \
    sqlalchemy-redshift==0.8.14

# Install Redis CLI for health checks
RUN apt-get update && apt-get install -y redis-tools && rm -rf /var/lib/apt/lists/*

# Copy configuration files
COPY superset_config.py /app/pythonpath/superset_config.py
COPY init_superset.sh /app/init_superset.sh

# Make initialization script executable
RUN chmod +x /app/init_superset.sh

# Create superset home directory
RUN mkdir -p /app/superset_home && chown -R superset:superset /app/superset_home

# Switch back to superset user
USER superset

# Set environment variables
ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py
ENV SUPERSET_HOME=/app/superset_home
ENV PYTHONPATH=/app/pythonpath:$PYTHONPATH

# Expose port
EXPOSE 8088

# Use custom initialization script
ENTRYPOINT ["/app/init_superset.sh"]