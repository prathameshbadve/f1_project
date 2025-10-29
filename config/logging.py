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
LOG_DIR = os.getenv("LOG_DIR", "monitoring/logs")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")


# Global flag to ensure setup_logging() only runs once
_LOGGING_CONFIGURED = False


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
        origial_name = record.name
        original_func_name = record.funcName

        # Truncate logger name to length of 30
        if len(record.name) > 30:
            record.name = record.name[:14] + "..." + record.name[-13:]

        # Truncate function name
        if len(record.funcName) > 30:
            record.funcName = record.funcName[:14] + "..." + record.funcName[-13:]

        # Add color to level name
        if record.levelname in self.COLORS:
            record.levelname = f"{self.COLORS[record.levelname]}{record.levelname}{self.COLORS['RESET']}"

        # Format the record
        result = super().format(record)

        # Restore the original levelname so other handlers aren't affected
        record.levelname = original_levelname
        record.name = origial_name
        record.funcName = original_func_name

        return result


class DetailedFormatter(logging.Formatter):
    """
    Updates the logger and function names to a length of 30.
    Uses the first 14 and last 13 characters and joins them with ellipsis(...)
    """

    def format(self, record: logging.LogRecord) -> str:
        origial_name = record.name
        original_func_name = record.funcName

        # Truncate logger name to length of 30
        if len(record.name) > 30:
            record.name = record.name[:14] + "..." + record.name[-13:]

        # Truncate function name
        if len(record.funcName) > 30:
            record.funcName = record.funcName[:14] + "..." + record.funcName[-13:]

        # Format the record
        result = super().format(record)

        record.name = origial_name
        record.funcName = original_func_name

        return result


def setup_logging():
    """Function to setup logging (only runs once)"""

    global _LOGGING_CONFIGURED  # pylint: disable=global-statement

    # Check if already configured
    if _LOGGING_CONFIGURED:
        return  # Already configured, skip

    project_root = get_project_root()
    log_dir = project_root / Path(LOG_DIR)
    ensure_directory(log_dir)

    # Formatters
    formatters = {
        "detailed": {
            "()": "config.logging.DetailedFormatter",
            "format": "%(asctime)s - [%(levelname)-8s] - %(name)-30s -"
            " [%(funcName)-30s:%(lineno)4d] - %(message)s",
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
            "format": "%(asctime)s - [%(levelname)-8s] - %(name)-30s -"
            " [%(funcName)-30s:%(lineno)4d] - %(message)s",
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
        # Dagster sepcific logs (outside the scope of pipeline)
        "dagster": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": os.getenv("LOG_LEVEL", "INFO"),
            "formatter": "detailed",
            "filename": log_dir / "dagster.log",
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
        # Dagster logs outside the scope of pipeline
        "dagster": {
            "handlers": [
                "console",
                "dagster",
                "file_error",
            ],
            "level": os.getenv("LOG_LEVEL", "INFO"),
            "propagate": False,
        },
        # Dagster asset execution logs (context.log in assets)
        "dagster.asset": {
            "handlers": [
                "console",
                "dagster",
                "file_error",
            ],
            "level": os.getenv("LOG_LEVEL", "INFO"),
            "propagate": False,
        },
        # Dagster op execution logs
        "dagster.op": {
            "handlers": [
                "console",
                "dagster",
                "file_error",
            ],
            "level": os.getenv("LOG_LEVEL", "INFO"),
            "propagate": False,
        },
        # Dagster job execution logs
        "dagster.job": {
            "handlers": [
                "console",
                "dagster",
                "file_error",
            ],
            "level": os.getenv("LOG_LEVEL", "INFO"),
            "propagate": False,
        },
        # Dagster hooks logs
        "dagster.hooks": {
            "handlers": [
                "console",
                "dagster",
                "file_error",
            ],
            "level": os.getenv("LOG_LEVEL", "INFO"),
            "propagate": False,
        },
        # Dagster run logs
        "dagster.run": {
            "handlers": [
                "console",
                "dagster",
                "file_error",
            ],
            "level": os.getenv("LOG_LEVEL", "INFO"),
            "propagate": False,
        },
        # Dagster event logs
        "dagster.event": {
            "handlers": [
                "console",
                "dagster",
                "file_error",
            ],
            "level": os.getenv("LOG_LEVEL", "INFO"),
            "propagate": False,
        },
        # Dagster resources logs
        "dagster.resources": {
            "handlers": [
                "console",
                "dagster",
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

    # Setting up logging configuration as per the above config dictionary
    _LOGGING_CONFIGURED = True

    # Log startup info
    logger = logging.getLogger("logging_config")
    logger.info("Logging configured. Log directory: %s", log_dir.absolute())
    logger.info("Environment: %s", ENVIRONMENT)

    # return log_config


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance.

    Args:
        name (str): The name of the logger. If None, returns the root logger.

    Returns:
        logger (logging.Logger): The logger instance.
    """

    return logging.getLogger(name)
