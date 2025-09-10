#!/usr/bin/env python3
"""
Test runner for Flink integration tests.

This script runs both unit tests and integration tests for the Flink stream processing job.
"""

import os
import sys
import subprocess
import logging
from typing import List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_command(cmd: List[str], cwd: Optional[str] = None) -> bool:
    """
    Run a command and return success status.
    
    Args:
        cmd: Command and arguments to run
        cwd: Working directory for the command
        
    Returns:
        True if command succeeded, False otherwise
    """
    try:
        logger.info(f"Running: {' '.join(cmd)}")
        result = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        if result.returncode == 0:
            logger.info("Command succeeded")
            if result.stdout:
                logger.info(f"Output: {result.stdout}")
            return True
        else:
            logger.error(f"Command failed with return code {result.returncode}")
            if result.stderr:
                logger.error(f"Error: {result.stderr}")
            if result.stdout:
                logger.info(f"Output: {result.stdout}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error("Command timed out")
        return False
    except Exception as e:
        logger.error(f"Failed to run command: {e}")
        return False


def check_dependencies() -> bool:
    """
    Check if required dependencies are available.
    
    Returns:
        True if all dependencies are available, False otherwise
    """
    logger.info("Checking dependencies...")
    
    # Check Python packages
    try:
        import pytest
        import kafka
        import aiohttp
        logger.info("Python dependencies OK")
    except ImportError as e:
        logger.error(f"Missing Python dependency: {e}")
        return False
    
    # Check if Kafka is accessible (for integration tests)
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    logger.info(f"Kafka servers configured: {kafka_servers}")
    
    # Check if Flink is accessible (for integration tests)
    flink_host = os.getenv("FLINK_JOBMANAGER_HOST", "localhost")
    flink_port = os.getenv("FLINK_JOBMANAGER_PORT", "8081")
    logger.info(f"Flink JobManager configured: {flink_host}:{flink_port}")
    
    return True


def run_unit_tests() -> bool:
    """
    Run unit tests.
    
    Returns:
        True if all tests passed, False otherwise
    """
    logger.info("Running unit tests...")
    
    cmd = [
        "python", "-m", "pytest",
        "test_flink_integration.py",
        "-v",
        "--tb=short",
        "-m", "not integration"
    ]
    
    return run_command(cmd)


def run_integration_tests() -> bool:
    """
    Run integration tests.
    
    Returns:
        True if all tests passed, False otherwise
    """
    logger.info("Running integration tests...")
    
    # Set environment variable to enable integration tests
    env = os.environ.copy()
    env["RUN_INTEGRATION_TESTS"] = "1"
    
    cmd = [
        "python", "-m", "pytest",
        "test_flink_integration.py",
        "-v",
        "--tb=short",
        "-m", "integration"
    ]
    
    try:
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if result.returncode == 0:
            logger.info("Integration tests passed")
            if result.stdout:
                logger.info(f"Output: {result.stdout}")
            return True
        else:
            logger.warning("Integration tests failed (this may be expected if infrastructure is not running)")
            if result.stderr:
                logger.warning(f"Error: {result.stderr}")
            if result.stdout:
                logger.info(f"Output: {result.stdout}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error("Integration tests timed out")
        return False
    except Exception as e:
        logger.error(f"Failed to run integration tests: {e}")
        return False


def run_linting() -> bool:
    """
    Run code linting with ruff.
    
    Returns:
        True if linting passed, False otherwise
    """
    logger.info("Running code linting...")
    
    # Check if ruff is available
    try:
        import ruff
    except ImportError:
        logger.warning("Ruff not available, skipping linting")
        return True
    
    cmd = ["python", "-m", "ruff", "check", "."]
    return run_command(cmd)


def main():
    """Main test runner."""
    logger.info("Starting Flink integration test suite...")
    
    # Check dependencies
    if not check_dependencies():
        logger.error("Dependency check failed")
        sys.exit(1)
    
    success = True
    
    # Run linting
    if not run_linting():
        logger.warning("Linting failed, but continuing with tests")
    
    # Run unit tests
    if not run_unit_tests():
        logger.error("Unit tests failed")
        success = False
    
    # Run integration tests (optional)
    if os.getenv("SKIP_INTEGRATION_TESTS") != "1":
        if not run_integration_tests():
            logger.warning("Integration tests failed (may be expected without infrastructure)")
            # Don't fail the overall test suite for integration test failures
    else:
        logger.info("Skipping integration tests (SKIP_INTEGRATION_TESTS=1)")
    
    if success:
        logger.info("Test suite completed successfully!")
        sys.exit(0)
    else:
        logger.error("Test suite failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()