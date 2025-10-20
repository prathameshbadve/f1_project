"""
Unit tests for logging setup.

Basic tests to check if logging directory and message colors are configured correctly.
"""

import os
from dotenv import load_dotenv

import pytest

from config.logging import get_logger

load_dotenv()
os.environ["ENVIRONMENT"] = "testing"


@pytest.mark.unit
def test_logging_environment():
    """Check that logger reads the correct environment variables"""

    env = os.getenv("ENVIRONMENT", "development")
    log_dir = os.getenv("LOG_DIR", "testing/logs")
    log_level = os.getenv("LOG_LEVEL", "DEBUG")

    assert env == "testing"
    assert log_dir == "monitoring/logs"
    assert log_level == "INFO"


@pytest.mark.unit
def test_logging_handlers():
    """Check that logging handlers are set up correctly for testing environment"""

    test_logger = get_logger("data_ingestion")

    assert test_logger is not None, "Logger for 'data_ingestion' should be configured"
    assert test_logger.level == 20, "Test logger level should be 20 (INFO)"
    # fmt: off
    assert "data_ingestion" in [handler.name for handler in test_logger.handlers], (
        "Logger should have 'test_handler' in its handlers"
    )
    # fmt: on
