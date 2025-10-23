"""
Dagster resources for F1 data pipeline
Resources provide reusable connections to external systems
"""

# pylint: disable=unused-argument, redefined-builtin

from io import BytesIO
from typing import Any, Dict, Optional

# import mlflow
import pandas as pd
from dagster import ConfigurableResource  # , InitResourceContext
from minio import Minio

# from psycopg2.pool import SimpleConnectionPool
# from sqlalchemy import create_engine

from config.settings import (
    # database_config,
    # mlflow_config,
    storage_config,
)


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
