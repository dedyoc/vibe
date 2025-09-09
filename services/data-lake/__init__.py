"""Data Lake service for MinIO S3-compatible object storage.

This package provides data lake management functionality for storing
and retrieving raw posts and processed trend data.
"""

try:
    from .data_lake_manager import DataLakeManager
except ImportError:
    from data_lake_manager import DataLakeManager

__all__ = ['DataLakeManager']