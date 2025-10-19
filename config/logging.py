"""
Logging configuration for the project
"""

# pylint: disable=line-too-long

import os
import logging
import logging.config
from pathlib import Path

from dotenv import load_dotenv

from src.utils.helpers import get_project_root, ensure_directory

load_dotenv()

ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
LOG_DIR = os.getenv("LOG_DIR", "mointoring/logs")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")


class ColoredFormatter(logging.Formatter):
    """
    Formatter that adds colors to console output for better readability.
    """

    # ANSI color codes
    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
        "RESET": "\033[0m",  # Reset
    }

    def format(self, record: logging.LogRecord) -> str:
        # Save the original levelname
        original_levelname = record.levelname

        # Add color to level name
        if record.levelname in self.COLORS:
            record.levelname = f"{self.COLORS[record.levelname]}{record.levelname}{self.COLORS['RESET']}"

        # Format the record
        result = super().format(record)

        # Restore the original levelname so other handlers aren't affected
        record.levelname = original_levelname

        return result


def setup_logging():
    """Function to setup logging"""

    project_root = get_project_root()
    log_dir = project_root / Path(LOG_DIR)
    ensure_directory(log_dir)

    # Formatters
    formatters = {
        "detailed": {
            "format": "%(asctime)s - %(name)s - %(levelname)s -"
            " %(funcName)s:%(lineno)d - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "json": {
            "format": "{"
            '"timestamp": "%(asctime)s", '
            '"logger": "%(name)s", '
            '"level": "%(levelname)s", '
            '"function": "%(funcName)s", '
            '"line": %(lineno)d, '
            '"message": "%(message)s"'
            "}",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "colored": {
            "()": "config.logging.ColoredFormatter",
            "format": "%(asctime)s - %(name)s - %(levelname)s -"
            " %(funcName)s:%(lineno)d - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    }

    # Handlers - In development, log level for all is set to DEBUG except errors.log.
    handlers = {
        # Console handler for real-time output
        "console": {
            "class": "logging.StreamHandler",
            "level": os.getenv("LOG_LEVEL", "INFO"),
            "formatter": "colored",
            "stream": "ext://sys.stdout",
        },
        # General application logs
        "file_info": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": os.getenv("LOG_LEVEL", "INFO"),
            "formatter": "detailed",
            "filename": log_dir / "app.log",
            "maxBytes": int(os.getenv("LOG_MAX_SIZE", "10485760")),
            "backupCount": int(os.getenv("LOG_BACKUP_COUNT", "5")),
        },
        # FastF1 specific logs
        "fastf1_info": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": os.getenv("FASTF1_LOG_LEVEL", "INFO"),
            "formatter": "detailed",
            "filename": log_dir / "fastf1.log",
            "maxBytes": int(os.getenv("LOG_MAX_SIZE", "10485760")),
            "backupCount": int(os.getenv("LOG_BACKUP_COUNT", "5")),
        },
        # Error logs
        "file_error": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "ERROR",
            "formatter": "detailed",
            "filename": log_dir / "error.log",
            "maxBytes": int(os.getenv("LOG_MAX_SIZE", "10485760")),
            "backupCount": int(os.getenv("LOG_BACKUP_COUNT", "5")),
        },
        # Data ingestion logs
        "data_ingestion": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": os.getenv("LOG_LEVEL", "INFO"),
            "formatter": "detailed",
            "filename": log_dir / "data_ingestion.log",
            "maxBytes": int(os.getenv("LOG_MAX_SIZE", "10485760")),
            "backupCount": int(os.getenv("LOG_BACKUP_COUNT", "5")),
        },
        # Data processing logs
        "data_processing": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": os.getenv("LOG_LEVEL", "INFO"),
            "formatter": "detailed",
            "filename": log_dir / "data_processing.log",
            "maxBytes": int(os.getenv("LOG_MAX_SIZE", "10485760")),
            "backupCount": int(os.getenv("LOG_BACKUP_COUNT", "5")),
        },
    }

    loggers = {
        # Root logger
        "": {
            "handlers": [
                "console",
                "file_info",
                "file_error",
            ],
            "level": os.getenv("LOG_LEVEL", "INFO"),
            "propagate": False,
        },
        # Specific logger for FastF1 API
        "fastf1": {
            "handlers": [
                "console",
                "fastf1_info",
                "file_error",
            ],
            "level": os.getenv("FASTF1_LOG_LEVEL", "INFO"),
            "propagate": False,
        },
        # Specific to data ingestion module
        "data_ingestion": {
            "handlers": [
                "console",
                "data_ingestion",
                "file_error",
            ],
            "level": os.getenv("LOG_LEVEL", "INFO"),
            "propagate": False,
        },
        # Specific to data processing module
        "data_processing": {
            "handlers": [
                "console",
                "data_processing",
                "file_error",
            ],
            "level": os.getenv("LOG_LEVEL", "INFO"),
            "propagate": False,
        },
    }

    if ENVIRONMENT == "production":
        # In production, log level is set to WARNING except errors.log.
        # This is done directly via .env

        # Data ingestion logs in JSON format
        handlers["data_ingestion_json"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "WARNING",
            "formatter": "json",
            "filename": log_dir / "data_ingestion_json.log",
            "maxBytes": int(os.getenv("LOG_MAX_SIZE", "10485760")),
            "backupCount": int(os.getenv("LOG_BACKUP_COUNT", "5")),
        }
        # Data processing logs in JSON format
        handlers["data_processing_json"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "WARNING",
            "formatter": "json",
            "filename": log_dir / "data_processing_json.log",
            "maxBytes": int(os.getenv("LOG_MAX_SIZE", "10485760")),
            "backupCount": int(os.getenv("LOG_BACKUP_COUNT", "5")),
        }
        # Adding the handlers to respective loggers
        loggers["data_ingestion"]["handlers"].append("data_ingestion_json")
        loggers["data_processing"]["handlers"].append("data_processing_json")

    elif ENVIRONMENT == "testing":
        # Log file for unit tests
        handlers["test_handler"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "level": os.getenv("log_level", "INFO"),
            "formatter": "detailed",
            "filename": log_dir / "test.log",
            "maxBytes": int(os.getenv("LOG_MAX_SIZE", "10485760")),
            "backupCount": int(os.getenv("LOG_BACKUP_COUNT", "5")),
        }
        # For unit tests
        loggers["test"] = {
            "handlers": [
                "console",
                "test_handler",
                "file_error",
            ],
            "level": "DEBUG",
            "propagate": False,
        }

    # Log Configuration
    log_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": formatters,
        "handlers": handlers,
        "loggers": loggers,
    }

    # Setting up logging configuration as per the above config dictionary
    logging.config.dictConfig(log_config)

    # Log startup info
    logger = logging.getLogger("logging_config")
    logger.info("Logging configured. Log directory: %s", log_dir.absolute())
    logger.info("Environment: %s", ENVIRONMENT)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance.

    Args:
        name (str): The name of the logger. If None, returns the root logger.

    Returns:
        logger (logging.Logger): The logger instance.
    """

    return logging.getLogger(name)
