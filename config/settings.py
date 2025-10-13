"""
FastF1 API Settings for the porject
"""

import os
from pathlib import Path

from dotenv import load_dotenv
import fastf1
from fastf1 import Cache

from src.utils.helpers import get_project_root, ensure_directory

load_dotenv()

ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
LOG_DIR = os.getenv("LOG_DIR", "mointoring/logs")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
FASTF1_LOG_LEVEL = os.getenv("FASTF1_LOG_LEVEL", "INFO")


class Config:
    """Case configuration class."""

    def __init__(self):
        self.environment = ENVIRONMENT
        self.project_root = get_project_root()


class FastF1Config(Config):
    """FastF1 specific configuration class."""

    def __init__(self):
        super().__init__()

        # General settings
        cache_dir = os.getenv("FASTF1_CACHE_DIR", "data/external/cache")
        self.cache_dir = (
            self.project_root / cache_dir
            if not Path(cache_dir).is_absolute()
            else Path(cache_dir)
        )
        self.cache_enabled = os.getenv("FASTF1_CACHE_ENABLED", "True").lower() == "true"
        self.log_level = os.getenv("FASTF1_LOG_LEVEL", "INFO")
        self.force_renew = os.getenv("FASTF1_FORCE_RENEW", "False").lower() == "true"
        self.request_timeout = int(os.getenv("FASTF1_REQUEST_TIMEOUT", "30"))
        self.max_retries = int(os.getenv("FASTF1_MAX_RETRIES", "3"))
        self.retry_delay = int(os.getenv("FASTF1_RETRY_DELAY", "5"))

        # Session settings
        self.include_testing = os.getenv("INCLUDE_TESTING", "False").lower() == "true"
        self.include_free_practice = (
            os.getenv("INCLUDE_FREE_PRACTICE", "False").lower() == "true"
        )
        self.include_qualifying = (
            os.getenv("INCLUDE_QUALIFYING", "True").lower() == "true"
        )
        self.include_sprint = os.getenv("INCLUDE_SPRINT", "True").lower() == "true"

        # Data features
        self.enable_laps = os.getenv("ENABLE_LAPS", "True").lower() == "true"
        self.enable_race_control_messages = (
            os.getenv("ENABLE_RACE_CONTROL_MSGS", "False").lower() == "true"
        )
        self.enable_weather_data = (
            os.getenv("ENABLE_WEATHER_DATA", "True").lower() == "true"
        )
        self.enable_telemetry = os.getenv("ENABLE_TELEMETRY", "False").lower() == "true"

        # Session types
        self.session_types = self._get_session_types()

    def _get_session_types(self):
        """Retrieve session types based on environment variables."""

        session_types = ["R"]

        if self.include_free_practice:
            session_types.extend(["FP1", "FP2", "FP3"])
        if self.include_qualifying:
            session_types.append("Q")
        if self.include_sprint:
            session_types.extend(["S", "SS", "SQ"])

        return session_types

    def setup_fastf1(self):
        """Initialize FastF1 with the configured settings."""

        if self.cache_enabled:
            ensure_directory(self.cache_dir)
            Cache.enable_cache(
                cache_dir=str(self.cache_dir),
                force_renew=self.force_renew,
            )
        else:
            Cache.set_disabled()

        fastf1.set_log_level(self.log_level)


fastf1_config = FastF1Config()
fastf1_config.setup_fastf1()
