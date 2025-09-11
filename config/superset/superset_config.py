# Superset Configuration for Bluesky Streaming Pipeline
import os
from typing import Dict, Any

# Database configuration - using SQLite for simplicity in development
SQLALCHEMY_DATABASE_URI = 'sqlite:////app/superset_home/superset.db'

# Redis configuration for caching
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_HOST': os.getenv('REDIS_HOST', 'redis'),
    'CACHE_REDIS_PORT': int(os.getenv('REDIS_PORT', '6379')),
    'CACHE_REDIS_DB': 1,
}

# Data cache configuration
DATA_CACHE_CONFIG = CACHE_CONFIG.copy()
DATA_CACHE_CONFIG['CACHE_REDIS_DB'] = 2

# Security configuration
SECRET_KEY = os.getenv('SUPERSET_SECRET_KEY', 'bluesky-streaming-pipeline-secret-key-change-in-production')
WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = None

# Feature flags
FEATURE_FLAGS = {
    'DASHBOARD_NATIVE_FILTERS': True,
    'DASHBOARD_CROSS_FILTERS': True,
    'DASHBOARD_RBAC': True,
    'ENABLE_TEMPLATE_PROCESSING': True,
    'ALERT_REPORTS': True,
    'DYNAMIC_PLUGINS': True,
}

# Row limit for SQL queries
ROW_LIMIT = 5000
VIZ_ROW_LIMIT = 10000

# Async query configuration
SUPERSET_WEBSERVER_TIMEOUT = 300

# Email configuration for reports (disabled for development)
SMTP_HOST = None
SMTP_STARTTLS = False
SMTP_SSL = False
SMTP_USER = None
SMTP_PASSWORD = None
SMTP_MAIL_FROM = None

# Custom roles and permissions
CUSTOM_SECURITY_MANAGER = None

# Dashboard and chart configuration
DASHBOARD_AUTO_REFRESH_MODE = "fetch"
DASHBOARD_AUTO_REFRESH_INTERVALS = [
    [0, "Don't refresh"],
    [10, "10 seconds"],
    [30, "30 seconds"],
    [60, "1 minute"],
    [300, "5 minutes"],
    [1800, "30 minutes"],
    [3600, "1 hour"],
]

# SQL Lab configuration
SQLLAB_CTAS_NO_LIMIT = True
SQLLAB_TIMEOUT = 300
SQLLAB_DEFAULT_DBID = None

# Logging configuration
ENABLE_TIME_ROTATE = True
TIME_ROTATE_LOG_LEVEL = 'INFO'
FILENAME = os.path.join(os.path.expanduser('~'), 'superset.log')

# Celery configuration for async tasks
class CeleryConfig:
    broker_url = f"redis://{os.getenv('REDIS_HOST', 'redis')}:{os.getenv('REDIS_PORT', '6379')}/3"
    result_backend = f"redis://{os.getenv('REDIS_HOST', 'redis')}:{os.getenv('REDIS_PORT', '6379')}/4"
    worker_prefetch_multiplier = 1
    task_acks_late = False
    task_annotations = {
        'sql_lab.get_sql_results': {
            'rate_limit': '100/s',
        },
    }

CELERY_CONFIG = CeleryConfig

# Custom CSS and branding
APP_NAME = "Bluesky Streaming Analytics"
APP_ICON = "/static/assets/images/superset-logo-horiz.png"
FAVICONS = [{"href": "/static/assets/images/favicon.png"}]

# Public role permissions
PUBLIC_ROLE_LIKE_GAMMA = True

# CORS configuration
ENABLE_CORS = True
CORS_OPTIONS = {
    'supports_credentials': True,
    'allow_headers': ['*'],
    'resources': ['*'],
    'origins': ['*']
}

# Database connection pool settings
SQLALCHEMY_ENGINE_OPTIONS = {
    'pool_pre_ping': True,
    'pool_recycle': 300,
    'connect_args': {'check_same_thread': False}
}

# Custom database connections will be added programmatically
# Redis and MinIO connections are configured in the initialization script