"""
Dagster resources for F1 data pipeline
Resources provide reusable connections to external systems
"""

# pylint: disable=unused-argument, redefined-builtin

import os
import shutil
from io import BytesIO
from typing import Any, Dict, Optional
from datetime import datetime

# import mlflow
import pandas as pd
from minio import Minio
from dagster import ConfigurableResource, ResourceDependency

# from psycopg2.pool import SimpleConnectionPool
# from sqlalchemy import create_engine

from config.settings import storage_config
from config.logging import get_logger
from src.utils.helpers import get_project_root, ensure_directory


# ============================================================================
# Storage Resource
# ============================================================================


class StorageResource(ConfigurableResource):
    """Resource for MinIO/S3 storage operations"""

    endpoint: str
    access_key: str
    secret_key: str
    bucket_raw: str
    bucket_processed: str
    secure: bool
    region: Optional[str] = None

    def get_client(self) -> Minio:
        """Get MinIO/S3 client"""

        return Minio(
            endpoint=self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
            region=self.region,
        )

    def build_object_key(
        self,
        data_type: str,
        year: int,
        event_name: Optional[str],
        session_type: Optional[str],
    ) -> str:
        """
        Build a standardized object key for F1 data.

        Args:
            year: Season year
            data_type: Type of data (race_results, qualifying_results, laps, weather, schedule)
            event_name: Name of the event (required unless data_type is 'schedule')
            session_type: Q, R, S, etc. (required unless data_type is 'schedule')

        Returns:
            Object key string

        Example:
            >>> key = storage.build_object_key(2024, "race_results", "Bahrain Grand Prix", "R")
            >>> print(key)
            '2024/bahrain_grand_prix/R/race_results.parquet'

            >>> key = storage.build_object_key(2024, "schedule")
            >>> print(key)
            '2024/schedule.parquet'
        """

        # Special case for schedule data
        if data_type == "schedule":
            object_key = f"{year}/season_{data_type}.parquet"
            # logger.info(
            #     "Built object key for season schedule %d: %s", year, object_key
            # )
            return object_key

        # For all other data types, event_name and session_type are required
        if event_name is None:
            raise ValueError(f"event_name is required for data_type '{data_type}'")
        if session_type is None:
            raise ValueError(f"session_type is required for data_type '{data_type}'")

        # Clean event name (lowercase, replace spaces with underscores)
        clean_event = event_name.lower().replace(" ", "_")

        # Build path: year/round_XX_event_name/data_type.parquet
        object_key = f"{year}/{clean_event}/{session_type}/{data_type}.parquet"

        # self.logger.info(
        #     "Built object key for %s of %s %s %d: %s",
        #     data_type,
        #     event_name,
        #     session_type,
        #     year,
        #     object_key,
        # )

        return object_key

    def upload_dataframe(
        self,
        df: pd.DataFrame,
        object_name: str,
        bucket: Optional[str] = None,
        format: str = "parquet",
    ) -> None:
        """
        Upload pandas DataFrame to storage.

        Args:
            df: DataFrame to upload
            object_name: Object key/path in storage
            bucket: Bucket name (defaults to raw bucket)
            format: File format ('parquet' or 'csv')
        """

        client = self.get_client()
        bucket = bucket or self.bucket_raw

        # Convert DataFrame to bytes
        buffer = BytesIO()
        if format == "parquet":
            df.to_parquet(buffer, index=False)
            content_type = "application/octet-stream"
        elif format == "csv":
            df.to_csv(buffer, index=False)
            content_type = "text/csv"
        else:
            raise ValueError(f"Unsupported format: {format}")

        # Upload
        buffer.seek(0)
        client.put_object(
            bucket,
            object_name,
            buffer,
            length=buffer.getbuffer().nbytes,
            content_type=content_type,
        )

    def download_dataframe(
        self,
        bucket: str,
        object_name: str,
        format: str = "parquet",
    ) -> pd.DataFrame:
        """
        Download DataFrame from storage.

        Args:
            bucket: Bucket name
            object_name: Object key/path
            format: File format ('parquet' or 'csv')

        Returns:
            pandas DataFrame
        """

        client = self.get_client()
        response = client.get_object(bucket, object_name)

        if format == "parquet":
            return pd.read_parquet(BytesIO(response.read()))
        elif format == "csv":
            return pd.read_csv(BytesIO(response.read()))
        else:
            raise ValueError(f"Unsupported format: {format}")

    def list_objects(self, bucket: str, prefix: str = "") -> list:
        """List objects in bucket with optional prefix"""

        client = self.get_client()
        objects = client.list_objects(bucket, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects]

    def object_exists(self, bucket: str, object_name: str) -> bool:
        """Check if object exists in bucket"""

        try:
            client = self.get_client()
            client.stat_object(bucket, object_name)
            return True
        except Exception:  # pylint: disable=broad-exception-caught
            return False

    def delete_object(self, bucket: str, object_name: str) -> None:
        """Delete an object from storage"""

        client = self.get_client()
        client.remove_object(bucket, object_name)

    def get_object_metadata(self, bucket: str, object_name: str) -> Dict[str, Any]:
        """Get metadata about an object"""

        client = self.get_client()
        stat = client.stat_object(bucket, object_name)

        return {
            "size": stat.size,
            "last_modified": stat.last_modified,
            "etag": stat.etag,
            "content_type": stat.content_type,
        }

    def upload_file(
        self,
        local_path: str,
        object_name: str,
        bucket_name: Optional[str] = None,
        content_type: str = "application/octet-stream",
    ) -> None:
        """
        Upload a local file to MinIO storage.

        Args:
            local_path: Path to the local file
            object_name: Object key/path in storage
            bucket: Bucket name (defaults to raw bucket)
            content_type: MIME type of the file
        """

        client = self.get_client()
        bucket = bucket_name or self.bucket_raw

        with open(local_path, "rb", encoding="utf-8") as file_data:
            file_stat = os.stat(local_path)
            client.put_object(
                bucket,
                object_name,
                file_data,
                length=file_stat.st_size,
                content_type=content_type,
            )

    # def upload_json(self, bucket: str, object_name: str, data: str) -> None:
    #     """Upload JSON data to storage"""

    #     client = self.get_client()
    #     data_bytes = data.encode("utf-8")
    #     client.put_object(
    #         bucket,
    #         object_name,
    #         BytesIO(data_bytes),
    #         length=len(data_bytes),
    #         content_type="application/json",
    #     )

    # def download_json(self, bucket: str, object_name: str) -> str:
    #     """Download JSON data from storage"""

    #     client = self.get_client()
    #     response = client.get_object(bucket, object_name)
    #     return response.read().decode("utf-8")


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


# class DatabaseResource(ConfigurableResource):
#     """Resource for PostgreSQL database operations"""

#     host: str
#     port: int
#     database: str
#     user: str
#     password: str

#     _pool: Optional[SimpleConnectionPool] = None

#     def setup_for_execution(self, context: InitResourceContext) -> None:
#         """Initialize connection pool"""

#         self._pool = SimpleConnectionPool(
#             minconn=1,
#             maxconn=10,
#             host=self.host,
#             port=self.port,
#             database=self.database,
#             user=self.user,
#             password=self.password,
#         )

#     def teardown_after_execution(self, context: InitResourceContext) -> None:
#         """Close connection pool"""

#         if self._pool:
#             self._pool.closeall()

#     def get_connection(self):
#         """Get a connection from the pool"""

#         if not self._pool:
#             raise RuntimeError("Database pool not initialized")
#         return self._pool.getconn()

#     def return_connection(self, conn):
#         """Return connection to the pool"""

#         if self._pool:
#             self._pool.putconn(conn)

#     def execute_query(self, query: str, params: tuple = None) -> list:
#         """Execute a SELECT query and return results"""

#         conn = self.get_connection()
#         try:
#             with conn.cursor() as cursor:
#                 cursor.execute(query, params)
#                 return cursor.fetchall()
#         finally:
#             self.return_connection(conn)

#     def execute_command(self, command: str, params: tuple = None) -> None:
#         """Execute an INSERT/UPDATE/DELETE command"""

#         conn = self.get_connection()
#         try:
#             with conn.cursor() as cursor:
#                 cursor.execute(command, params)
#                 conn.commit()
#         finally:
#             self.return_connection(conn)

#     def execute_many(self, command: str, params_list: list) -> None:
#         """Execute a command with multiple parameter sets"""

#         conn = self.get_connection()
#         try:
#             with conn.cursor() as cursor:
#                 cursor.executemany(command, params_list)
#                 conn.commit()
#         finally:
#             self.return_connection(conn)

#     def dataframe_to_sql(
#         self, df: pd.DataFrame, table_name: str, if_exists: str = "append"
#     ) -> None:
#         """
#         Write DataFrame to PostgreSQL table.

#         Args:
#             df: DataFrame to write
#             table_name: Name of table
#             if_exists: What to do if table exists ('fail', 'replace', 'append')
#         """

#         engine = create_engine(
#             f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
#         )

#         df.to_sql(table_name, engine, if_exists=if_exists, index=False)

#     def query_to_dataframe(self, query: str) -> pd.DataFrame:
#         """Execute query and return results as DataFrame"""

#         engine = create_engine(
#             f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
#         )

#         return pd.read_sql(query, engine)


# class MLflowResource(ConfigurableResource):
#     """Resource for MLflow experiment tracking"""

#     tracking_uri: str
#     experiment_name: str

#     def setup_for_execution(self, context: InitResourceContext) -> None:
#         """Configure MLflow"""

#         mlflow.set_tracking_uri(self.tracking_uri)
#         mlflow.set_experiment(self.experiment_name)

#     def start_run(self, run_name: str = None, tags: Dict[str, str] = None):
#         """Start an MLflow run"""

#         return mlflow.start_run(run_name=run_name, tags=tags)

#     def log_params(self, params: Dict[str, Any]) -> None:
#         """Log parameters to MLflow"""

#         mlflow.log_params(params)

#     def log_metrics(self, metrics: Dict[str, float]) -> None:
#         """Log metrics to MLflow"""

#         mlflow.log_metrics(metrics)

#     def log_artifact(self, local_path: str) -> None:
#         """Log artifact to MLflow"""

#         mlflow.log_artifact(local_path)

#     def log_model(self, model, artifact_path: str) -> None:
#         """Log model to MLflow"""

#         mlflow.sklearn.log_model(model, artifact_path)

#     def end_run(self) -> None:
#         """End the current MLflow run"""

#         mlflow.end_run()


# ============================================================================
# RESOURCE INSTANCES
# ============================================================================

# Storage resource (MinIO/S3)
storage_resource = StorageResource(
    endpoint=storage_config.endpoint,
    access_key=storage_config.access_key,
    secret_key=storage_config.secret_key,
    bucket_raw=storage_config.raw_bucket_name,
    bucket_processed=storage_config.processed_bucket_name,
    secure=storage_config.secure,
    region=storage_config.region,
)

# Logging resource
logging_resource = LoggingResource(
    bucket_name=storage_resource.bucket_raw,
    log_dir=str(get_project_root() / os.getenv("LOG_DIR", "monitoring/logs")),
    upload_enabled=True,
)

# # Database resource (PostgreSQL)
# database_resource = DatabaseResource(
#     host=database_config.host,
#     port=database_config.port,
#     database=database_config.database,
#     user=database_config.user,
#     password=database_config.password,
# )

# # MLflow resource
# mlflow_resource = MLflowResource(
#     tracking_uri=mlflow_config.tracking_uri,
#     experiment_name=mlflow_config.experiment_name,
# )


all_resources = {
    "storage_resource": storage_resource,
    "logging_resource": logging_resource,
}
