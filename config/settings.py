"""
FastF1 API Settings for the porject
"""

import os
from pathlib import Path
from enum import Enum
from typing import Dict, Any, Optional

from dotenv import load_dotenv
import fastf1
from fastf1 import Cache

from src.utils.helpers import get_project_root, ensure_directory

load_dotenv()


class Environment(str, Enum):
    """Environment types"""

    DEVELOPMENT = "development"
    PRODUCTION = "production"


ENVIRONMENT = os.getenv("ENVIRONMENT", Environment.DEVELOPMENT.value)
LOG_DIR = os.getenv("LOG_DIR", "mointoring/logs")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
FASTF1_LOG_LEVEL = os.getenv("FASTF1_LOG_LEVEL", "INFO")


class Config:
    """Case configuration class."""

    def __init__(self):
        self.environment = ENVIRONMENT
        self.project_root = get_project_root()


class StorageConfig(Config):
    """Configuration for storage client (MinIO/S3)"""

    def __init__(
        self,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        secure: Optional[bool] = None,
        raw_bucket_name: Optional[str] = None,
        processed_bucket_name: Optional[str] = None,
        region: Optional[str] = None,
    ):
        """
        Initialize storage configuration.

        Args:
            endpoint: Storage endpoint URL
            access_key: Access key for authentication
            secret_key: Secret key for authentication
            secure: Whether to use secure connection (HTTPS)
            raw_bucket_name: Name of the raw data bucket
            processed_bucket_name: Name of the processed data bucket
            region: AWS region (for S3)
            environment: Deployment environment (development/production)
        """

        super().__init__()

        # Load environment-specific defaults
        if self.environment == Environment.PRODUCTION.value:
            self._load_production_config(
                endpoint,
                access_key,
                secret_key,
                secure,
                raw_bucket_name,
                processed_bucket_name,
                region,
            )
        else:
            self._load_development_config(
                endpoint,
                access_key,
                secret_key,
                secure,
                raw_bucket_name,
                processed_bucket_name,
                region,
            )

    def _load_development_config(
        self,
        endpoint: Optional[str],
        access_key: Optional[str],
        secret_key: Optional[str],
        secure: Optional[bool],
        raw_bucket_name: Optional[str],
        processed_bucket_name: Optional[str],
        region: Optional[str],
    ):
        """Load configuration for development environment (MinIO)"""

        self.endpoint = endpoint or os.getenv("MINIO_ENDPOINT", "localhost:9000")
        self.access_key = access_key or os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.secret_key = secret_key or os.getenv("MINIO_SECRET_KEY", "minioadmin")
        self.secure = secure or os.getenv("MINIO_SECURE", "False").lower() == "true"
        self.raw_bucket_name = raw_bucket_name or os.getenv(
            "MINIO_BUCKET_RAW", "f1-raw-data-dev"
        )
        self.processed_bucket_name = processed_bucket_name or os.getenv(
            "MINIO_BUCKET_PROCESSED", "f1-processed-data-dev"
        )
        self.region = region  # MinIO doesn't require region

    def _load_production_config(
        self,
        endpoint: Optional[str],
        access_key: Optional[str],
        secret_key: Optional[str],
        secure: Optional[bool],
        raw_bucket_name: Optional[str],
        processed_bucket_name: Optional[str],
        region: Optional[str],
    ):
        """Load configuration for production environment (AWS S3)"""

        self.endpoint = endpoint or "s3.amazonaws.com"
        self.access_key = access_key or os.getenv("AWS_ACCESS_KEY_ID")
        self.secret_key = secret_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        self.secure = secure or os.getenv("AWS_SECURE", "True").lower() == "true"
        self.raw_bucket_name = raw_bucket_name or os.getenv(
            "S3_BUCKET_RAW", "pb-f1-raw-data"
        )
        self.processed_bucket_name = processed_bucket_name or os.getenv(
            "S3_BUCKET_PROCESSED", "pb-f1-processed-data"
        )
        self.region = region or os.getenv("AWS_REGION", "ap-south-1")

    def is_config_valid(self) -> bool:
        """
        Validate that all required configuration values are present.

        Returns:
            bool: True if configuration is valid

        Raises:
            ValueError: If required configuration is missing
        """

        required_fields = [
            ("endpoint", self.endpoint),
            ("access_key", self.access_key),
            ("secret_key", self.secret_key),
            ("raw_bucket_name", self.raw_bucket_name),
            ("processed_bucket_name", self.processed_bucket_name),
        ]

        missing_fields = [name for name, value in required_fields if not value]

        if missing_fields:
            raise ValueError(
                f"Missing required storage configuration: {', '.join(missing_fields)}"
            )

        return True

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert configuration to dictionary.

        Returns:
            dict: Configuration as dictionary
        """

        return {
            "endpoint": self.endpoint,
            "access_key": self.access_key,
            "secret_key": "***" if self.secret_key else None,  # Masked for security
            "secure": self.secure,
            "raw_bucket_name": self.raw_bucket_name,
            "processed_bucket_name": self.processed_bucket_name,
            "region": self.region,
            "environment": self.environment,
        }

    def __repr__(self) -> str:
        """String representation of configuration"""

        config_dict = self.to_dict()
        return f"StorageConfig({', '.join(f'{k}={v}' for k, v in config_dict.items())})"


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


class DatabaseConfig(Config):
    """Database configuration"""

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[str] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        db_url: Optional[str] = None,
    ):
        super().__init__()

        if self.environment == Environment.PRODUCTION.value:
            self._load_production_database_config(
                host,
                port,
                database,
                user,
                password,
            )
        else:
            self._load_development_database_config(
                host,
                port,
                database,
                user,
                password,
            )

        # Support DATABASE_URL override
        db_url = os.getenv("DATABASE_URL")
        if db_url:
            self.db_url = db_url
        else:
            self.db_url = (
                f"postgresql://{self.user}:{self.password}@"
                f"{self.host}:{self.port}/{self.database}"
            )

    def _load_development_database_config(
        self,
        host,
        port,
        database,
        user,
        password,
    ):
        """Development database configuration"""

        self.host = host or os.getenv("DB_HOST", "localhost")
        self.port = port or int(os.getenv("DB_PORT", "5434"))
        self.database = database or os.getenv("DB_NAME", "f1_data")
        self.user = user or os.getenv("DB_USER", "f1user")
        self.password = password or os.getenv("DB_PASSWORD", "f1pass")

    def _load_production_database_config(
        self,
        host,
        port,
        database,
        user,
        password,
    ):
        """Production database configuration"""

        self.host = host or os.getenv("DB_HOST")
        self.port = port or int(os.getenv("DB_PORT", "5432"))
        self.database = database or os.getenv("DB_NAME")
        self.user = user or os.getenv("DB_USER")
        self.password = password or os.getenv("DB_PASSWORD")

    def is_config_valid(self) -> bool:
        """
        Validate that all required configuration values are present.

        Returns:
            bool: True if configuration is valid

        Raises:
            ValueError: If required configuration is missing
        """

        required_fields = [
            ("host", self.host),
            ("port", self.port),
            ("database", self.database),
            ("user", self.user),
            ("password", self.password),
        ]

        missing_fields = [name for name, value in required_fields if not value]

        if missing_fields:
            raise ValueError(
                f"Missing required database configuration: {', '.join(missing_fields)}"
            )

        return True

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert configuration to dictionary.

        Returns:
            dict: Configuration as dictionary
        """

        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": "***" if self.password else None,  # Masked for security
            "environment": self.environment,
        }

    def __repr__(self) -> str:
        """String representation of configuration"""

        config_dict = self.to_dict()
        return (
            f"DatabaseConfig({', '.join(f'{k}={v}' for k, v in config_dict.items())})"
        )


class MLFlowConfig(Config):
    """MLflow configuration"""

    def __init__(
        self,
        tracking_uri: Optional[str] = None,
        experiment_name: Optional[str] = None,
    ):
        super().__init__()

        if self.environment == Environment.PRODUCTION.value:
            self._load_development_mlflow_config(tracking_uri, experiment_name)
        else:
            self._load_production_mlflow_config(tracking_uri, experiment_name)

    def _load_development_mlflow_config(self, tracking_uri, experiment_name):
        """Load development MLFLow Config"""

        self.tracking_uri = tracking_uri or os.getenv(
            "MLFLOW_TRACKING_URI", "http://localhost:8080"
        )
        self.experiment_name = experiment_name or os.getenv(
            "MLFLOW_EXPERIMENT_NAME", "f1-race-prediction-local"
        )

    def _load_production_mlflow_config(self, tracking_uri, experiment_name):
        """Load production MLFLow Config"""

        self.tracking_uri = tracking_uri or os.getenv("MLFLOW_TRACKING_URI")
        self.experiment_name = experiment_name or os.getenv(
            "MLFLOW_EXPERIMENT_NAME", "f1-race-prediction"
        )

    def is_config_valid(self):
        """
        Validate that all required configuration values are present.

        Returns:
            bool: True if configuration is valid

        Raises:
            ValueError: If required configuration is missing
        """

        required_fields = [
            ("tracking_uri", self.tracking_uri),
            ("experiment_name", self.experiment_name),
        ]

        missing_fields = [name for name, value in required_fields if not value]

        if missing_fields:
            raise ValueError(
                f"Missing required mlflow configuration: {', '.join(missing_fields)}"
            )

        return True

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert configuration to dictionary.

        Returns:
            dict: Configuration as dictionary
        """

        return {
            "tracking_uri": self.tracking_uri,
            "experiment_name": self.experiment_name,
        }

    def __repr__(self) -> str:
        """String representation of configuration"""

        config_dict = self.to_dict()
        return f"MLFLowConfig({', '.join(f'{k}={v}' for k, v in config_dict.items())})"


# Set up FastF1 config
fastf1_config = FastF1Config()
fastf1_config.setup_fastf1()

# Set up Storage Config
storage_config = StorageConfig()

# Set up Database config
database_config = DatabaseConfig()

# Set up MLFlow config
mlflow_config = MLFlowConfig()


class DagsterConfig(Config):
    """Dagster-specific configuration"""

    def __init__(
        self,
        run_storage: Optional[Dict[str, str | Dict]] = None,
        event_log_storage: Optional[Dict[str, str | Dict]] = None,
        schedule_storage: Optional[Dict[str, str | Dict]] = None,
    ):
        super().__init__()

        self.run_storage = run_storage or {
            "module": "dagster_postgres.run_storage",
            "class": "PostgresRunStorage",
            "config": {"postgres_url": database_config.db_url},
        }

        self.event_log_storage = event_log_storage or {
            "module": "dagster_postgres.event_log",
            "class": "PostgresEventLogStorage",
            "config": {"postgres_url": database_config.db_url},
        }

        self.schedule_storage = schedule_storage or {
            "module": "dagster_postgres.schedule_storage",
            "class": "PostgresScheduleStorage",
            "config": {"postgres_url": database_config.db_url},
        }


def is_development() -> bool:
    """Check if running in development environment"""

    return ENVIRONMENT == Environment.DEVELOPMENT.value


def is_production() -> bool:
    """Check if running in production environment"""

    return ENVIRONMENT == Environment.PRODUCTION.value
