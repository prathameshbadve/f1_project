"""
Dagster resources for F1 data pipeline
Resources provide reusable connections to external systems
"""

import time
from io import BytesIO
from typing import Any, Dict, Optional

import mlflow
import psycopg2
import requests
from dagster import ConfigurableResource, InitResourceContext
from minio import Minio
from psycopg2.pool import SimpleConnectionPool

from config.settings import (
    database_config,
    fastf1_config,
    mlflow_config,
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

    def upload_json(self, bucket: str, object_name: str, data: str) -> None:
        """Upload JSON data to storage"""

        client = self.get_client()
        data_bytes = data.encode("utf-8")
        client.put_object(
            bucket,
            object_name,
            BytesIO(data_bytes),
            length=len(data_bytes),
            content_type="application/json",
        )

    def download_json(self, bucket: str, object_name: str) -> str:
        """Download JSON data from storage"""

        client = self.get_client()
        response = client.get_object(bucket, object_name)
        return response.read().decode("utf-8")

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


class DatabaseResource(ConfigurableResource):
    """Resource for PostgreSQL database operations"""

    host: str
    port: int
    database: str
    user: str
    password: str

    _pool: Optional[SimpleConnectionPool] = None

    def setup_for_execution(self, context: InitResourceContext) -> None:
        """Initialize connection pool"""

        self._pool = SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )

    def teardown_after_execution(self, context: InitResourceContext) -> None:
        """Close connection pool"""

        if self._pool:
            self._pool.closeall()

    def get_connection(self):
        """Get a connection from the pool"""

        if not self._pool:
            raise RuntimeError("Database pool not initialized")
        return self._pool.getconn()

    def return_connection(self, conn):
        """Return connection to the pool"""

        if self._pool:
            self._pool.putconn(conn)

    def execute_query(self, query: str, params: tuple = None) -> list:
        """Execute a SELECT query and return results"""

        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
        finally:
            self.return_connection(conn)

    def execute_command(self, command: str, params: tuple = None) -> None:
        """Execute an INSERT/UPDATE/DELETE command"""

        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(command, params)
                conn.commit()
        finally:
            self.return_connection(conn)

    def execute_many(self, command: str, params_list: list) -> None:
        """Execute a command with multiple parameter sets"""

        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.executemany(command, params_list)
                conn.commit()
        finally:
            self.return_connection(conn)


class MLflowResource(ConfigurableResource):
    """Resource for MLflow experiment tracking"""

    tracking_uri: str
    experiment_name: str

    def setup_for_execution(self, context: InitResourceContext) -> None:
        """Configure MLflow"""

        mlflow.set_tracking_uri(self.tracking_uri)
        mlflow.set_experiment(self.experiment_name)

    def start_run(self, run_name: str = None, tags: Dict[str, str] = None):
        """Start an MLflow run"""

        return mlflow.start_run(run_name=run_name, tags=tags)

    def log_params(self, params: Dict[str, Any]) -> None:
        """Log parameters to MLflow"""

        mlflow.log_params(params)

    def log_metrics(self, metrics: Dict[str, float]) -> None:
        """Log metrics to MLflow"""

        mlflow.log_metrics(metrics)

    def log_artifact(self, local_path: str) -> None:
        """Log artifact to MLflow"""

        mlflow.log_artifact(local_path)

    def log_model(self, model, artifact_path: str) -> None:
        """Log model to MLflow"""

        mlflow.sklearn.log_model(model, artifact_path)

    def end_run(self) -> None:
        """End the current MLflow run"""

        mlflow.end_run()


class F1APIResource(ConfigurableResource):
    """Resource for F1 API operations (Ergast API)"""

    base_url: str
    rate_limit_delay: float
    timeout: int

    def _make_request(self, endpoint: str, params: Dict[str, Any] = None) -> Dict:
        """Make API request with rate limiting"""

        url = f"{self.base_url}/{endpoint}.json"

        # Rate limiting
        time.sleep(self.rate_limit_delay)

        response = requests.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()

        return response.json()

    def get_race_results(self, year: int, round_number: int) -> Dict:
        """Get race results for a specific year and round"""

        endpoint = f"{year}/{round_number}/results"
        return self._make_request(endpoint)

    def get_qualifying_results(self, year: int, round_number: int) -> Dict:
        """Get qualifying results for a specific year and round"""

        endpoint = f"{year}/{round_number}/qualifying"
        return self._make_request(endpoint)

    def get_driver_standings(self, year: int, round_number: int = None) -> Dict:
        """Get driver standings for a year (optionally after specific round)"""

        if round_number:
            endpoint = f"{year}/{round_number}/driverStandings"
        else:
            endpoint = f"{year}/driverStandings"
        return self._make_request(endpoint)

    def get_constructor_standings(self, year: int, round_number: int = None) -> Dict:
        """Get constructor standings for a year"""

        if round_number:
            endpoint = f"{year}/{round_number}/constructorStandings"
        else:
            endpoint = f"{year}/constructorStandings"
        return self._make_request(endpoint)

    def get_race_schedule(self, year: int) -> Dict:
        """Get race schedule for a specific year"""

        endpoint = f"{year}"
        return self._make_request(endpoint)

    def get_current_season(self) -> int:
        """Get current F1 season year"""

        endpoint = "current"
        data = self._make_request(endpoint)
        return int(data["MRData"]["RaceTable"]["season"])

    def get_circuits(self, year: int) -> Dict:
        """Get circuits for a specific year"""

        endpoint = f"{year}/circuits"
        return self._make_request(endpoint)

    def get_lap_times(self, year: int, round_number: int, lap: int) -> Dict:
        """Get lap times for a specific lap"""

        endpoint = f"{year}/{round_number}/laps/{lap}"
        return self._make_request(endpoint)

    def get_pit_stops(self, year: int, round_number: int) -> Dict:
        """Get pit stop data for a race"""

        endpoint = f"{year}/{round_number}/pitstops"
        return self._make_request(endpoint)


# Resource instances with environment-aware configuration
storage_resource = StorageResource(
    endpoint=storage_config.endpoint,
    access_key=storage_config.access_key,
    secret_key=storage_config.secret_key,
    bucket_raw=storage_config.raw_bucket_name,
    bucket_processed=storage_config.processed_bucket_name,
    secure=storage_config.secure,
    region=storage_config.region,
)

database_resource = DatabaseResource(
    host=database_config.host,
    port=database_config.port,
    database=database_config.database,
    user=database_config.user,
    password=database_config.password,
)

mlflow_resource = MLflowResource(
    tracking_uri=mlflow_config.tracking_uri,
    experiment_name=mlflow_config.experiment_name,
)

# f1_api_resource = F1APIResource(
#     base_url=f1_api_config["base_url"],
#     rate_limit_delay=f1_api_config["rate_limit_delay"],
#     timeout=f1_api_config["timeout"],
# )
