"""
Dagster resources for F1 data pipeline
Resources provide reusable connections to external systems
"""

# pylint: disable=unused-argument, redefined-builtin

import os
import shutil
from typing import Optional
from datetime import datetime

from dagster import ConfigurableResource, ResourceDependency
from pydantic import Field

from config.logging import get_logger
from config.settings import storage_config
from src.utils.helpers import get_project_root, ensure_directory
from src.clients.storage_client import StorageClient
from src.clients.redis_client import RedisClient


# ============================================================================
# Storage Resource
# ============================================================================


class StorageResource(ConfigurableResource):
    """Dagster resource wrapper for StorageClient"""

    def create_client(self) -> StorageClient:
        """Create and return StorageClient instance"""
        # Your StorageClient initialization logic
        return StorageClient()


# ============================================================================
# Redis Resource
# ============================================================================


class RedisResource(ConfigurableResource):
    """Dagster resource wrapper for RedisClient"""

    def create_client(self) -> RedisClient:
        """Create and return StorageClient instance"""
        # Your StorageClient initialization logic
        return RedisClient()


# ============================================================================
# Logging Resource
# ============================================================================


class LoggingResource(ConfigurableResource):
    """
    Resource for managing logging in Dagster runs.
    Handles log file uploads to cloud storage after each run.
    """

    # Use ResourceDependency to inject the StorageResource
    storage_resource: ResourceDependency[StorageResource]

    bucket_name: str
    log_dir: str
    upload_enabled: bool = True

    def setup(self):
        """Setup the logger"""

        self.logger = get_logger(  # pylint: disable=attribute-defined-outside-init
            "dagster.logging_resource"
        )
        self.logger.info("LoggingResource initialized for Dagster run")

    def get_run_log_prefix(
        self, run_id: str, run_timestamp: Optional[datetime] = None
    ) -> str:
        """
        Generate a prefix for log files in bucket storage.

        Args:
            run_id: Dagster run ID
            run_timestamp: Timestamp of the run (defaults to current time)

        Returns:
            Prefix path for cloud storage
            (e.g., "dagster-logs/2025/10/28/run_abc123_20251028_143022")
        """

        if run_timestamp is None:
            run_timestamp = datetime.now()

        date_path = run_timestamp.strftime("%Y/%m/%d")
        timestamp_str = run_timestamp.strftime("%Y%m%d_%H%M%S")

        return f"monitoring/logs/{date_path}/run_{run_id}_{timestamp_str}"

    def upload_logs_to_bucket(self, run_id: str, run_timestamp: datetime) -> bool:
        """Upload log files to MinIO"""

        try:
            log_dir = self.log_dir
            prefix = self.get_run_log_prefix(run_id, run_timestamp)

            uploaded_files = []
            for log_file in log_dir.glob("*.log"):
                object_key = f"{prefix}/{log_file.name}"
                self.storage_resource.upload_file(
                    log_file,
                    object_key,
                    self.bucket_name,
                    content_type="text/plain",
                )
                uploaded_files.append(object_key)
                self.logger.info(
                    "Uploaded %s to s3://%s/%s",
                    log_file.name,
                    self.bucket_name,
                    object_key,
                )

            self.logger.info(
                "Successfully uploaded %d log files to bucket storage",
                len(uploaded_files),
            )
            return True

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to upload logs to S3: %s", str(e), exc_info=True)
            return False

    def upload_logs(
        self, run_id: str, run_timestamp: Optional[datetime] = None
    ) -> bool:
        """
        Upload all log files to cloud storage.

        Args:
            run_id: Dagster run ID
            run_timestamp: Timestamp of the run

        Returns:
            bool: True if upload successful, False otherwise
        """
        if not self.upload_enabled:
            self.logger.info("Log upload disabled, skipping")
            return True

        if run_timestamp is None:
            run_timestamp = datetime.now()

        self.logger.info("Uploading logs for run %s to bucket storage.", run_id)

        self.upload_logs_to_bucket(run_id, run_timestamp)

    def archive_local_logs(
        self, run_id: str, archive_dir: Optional[str] = None
    ) -> bool:
        """
        Archive local log files after uploading to cloud storage.

        Args:
            run_id: Dagster run ID
            archive_dir: Directory to archive logs (defaults to monitoring/logs/archive)

        Returns:
            bool: True if archival successful, False otherwise
        """
        try:
            log_dir = self.log_dir

            if archive_dir is None:
                archive_dir = log_dir + "/archive"

            ensure_directory(archive_dir)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_subdir = archive_dir / f"run_{run_id}_{timestamp}"
            ensure_directory(archive_subdir)

            archived_files = []
            for log_file in log_dir.glob("*.log"):
                dest = archive_subdir / log_file.name
                shutil.copy2(log_file, dest)
                archived_files.append(log_file.name)

            self.logger.info(
                "Archived %d log files to %s", len(archived_files), archive_subdir
            )
            return True

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to archive logs: %s", str(e), exc_info=True)
            return False


# ============================================================================
# Catalog Config Resource
# ============================================================================


class CatalogConfig(ConfigurableResource):
    """Configuration for catalog building"""

    raw_bucket: str = Field(
        default="dev-f1-data-raw", description="Raw data bucket name"
    )
    processed_bucket: str = Field(
        default="dev-f1-data-processed", description="Processed data bucket name"
    )
    years: Optional[list[int]] = Field(
        default=None, description="Specific years to scan (None = all)"
    )


# ============================================================================
# RESOURCE INSTANCES
# ============================================================================

# Storage resource (MinIO/S3)
storage_resource = StorageResource()

# Redis Resource
redis_resource = RedisResource()

# Logging resource
logging_resource = LoggingResource(
    bucket_name=storage_config.raw_bucket_name,
    log_dir=str(get_project_root() / os.getenv("LOG_DIR", "monitoring/logs")),
    upload_enabled=True,
)

# Catalog Config resource
catalog_config = CatalogConfig()


all_resources = {
    "storage_resource": storage_resource,
    "logging_resource": logging_resource,
    "redis_resource": redis_resource,
    "catalog_config": catalog_config,
}
