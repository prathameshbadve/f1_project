"""
Storage Client for managing data in MinIO/S3.

Provides abstraction layer for storing and retrieving data in object storage,
supporting both local (MinIO) and production (S3) environments.
"""

import io
from typing import Optional, List, Dict, Any
import pandas as pd

from minio import Minio
from minio.error import S3Error

from config.logging import get_logger
from config.settings import storage_config, StorageConfig


class StorageClient:
    """
    Client for interacting with object storage (MinIO/S3).

    Handles uploading and downloading data in Parquet format with proper
    organization by session type.
    """

    def __init__(self, storage_cfg: StorageConfig = None):
        self.config = storage_cfg or storage_config
        self.logger = get_logger("data_ingestion.storage_client")

        # Initialize MinIO client
        try:
            self.client = Minio(
                endpoint=self.config.endpoint,
                access_key=self.config.access_key,
                secret_key=self.config.secret_key,
                secure=self.config.secure,
                cert_check=False if not self.config.secure else True,
            )
            self.logger.info("Storage client initialized: %s", self.config.endpoint)

        except Exception as e:
            self.logger.error("Failed to initialize storage client: %s", str(e))
            raise

        # Ensure bucket exists
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self) -> None:
        """
        Ensure the bucket exists, create it if it doesn't.
        """

        try:
            if not self.client.bucket_exists(self.config.raw_bucket_name):
                self.client.make_bucket(self.config.raw_bucket_name)
                self.logger.info("Created bucket: %s", self.config.raw_bucket_name)
            else:
                self.logger.debug(
                    "Bucket already exists: %s", self.config.raw_bucket_name
                )

        except S3Error as e:
            self.logger.error("Error checking/creating bucket: %s", str(e))
            raise

    def upload_dataframe(
        self,
        df: pd.DataFrame,
        object_key: str,
        compression: str = "snappy",
    ) -> bool:
        """
        Upload a pandas DataFrame as a Parquet file.

        Args:
            df: DataFrame to upload
            object_key: Storage path (e.g., "2024/round_01/race_results.parquet")
            compression: Parquet compression method (snappy, gzip, brotli, none)

        Returns:
            True if successful, False otherwise

        Example:
            >>> df = pd.DataFrame({'col': [1, 2, 3]})
            >>> storage.upload_dataframe(df, "test/data.parquet")
        """

        if df is None or df.empty:
            self.logger.warning("Cannot upload empty DataFrame to %s", object_key)
            return False

        try:
            # Convert DataFrame to Parquet in memory
            buffer = io.BytesIO()
            df.to_parquet(
                buffer, engine="pyarrow", compression=compression, index=False
            )

            # Get size for logging
            size_bytes = buffer.tell()
            buffer.seek(0)

            # Upload to MinIO
            self.client.put_object(
                bucket_name=self.config.raw_bucket_name,
                object_name=object_key,
                data=buffer,
                length=size_bytes,
                content_type="application/octet-stream",
            )

            size_mb = size_bytes / (1024 * 1024)
            self.logger.info(
                "✅ Uploaded %s (%d rows, %.2f MB)",
                object_key,
                len(df),
                size_mb,
            )
            return True

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("❌ Failed to upload %s: %s", object_key, str(e))
            return False

    def download_dataframe(self, object_key: str) -> Optional[pd.DataFrame]:
        """
        Download a Parquet file as a pandas DataFrame.

        Args:
            object_key: Storage path to download

        Returns:
            DataFrame if successful, None otherwise

        Example:
            >>> df = storage.download_dataframe("2024/round_01/race_results.parquet")
        """

        try:
            # Download object
            response = self.client.get_object(
                bucket_name=self.config.raw_bucket_name, object_name=object_key
            )

            # Read Parquet from bytes
            buffer = io.BytesIO(response.read())
            df = pd.read_parquet(buffer, engine="pyarrow")

            # Automatically find all timedelta columns and replace NaT with None
            timedelta_cols = df.select_dtypes(include=["timedelta64"]).columns
            for col in timedelta_cols:
                df[col] = df[col].replace({pd.NaT: None})

            self.logger.info("✅ Downloaded %s (%d rows)", object_key, len(df))
            return df

        except S3Error as e:
            if e.code == "NoSuchKey":
                self.logger.warning("Object not found: %s", object_key)
            else:
                self.logger.error("❌ Failed to download %s: %s", object_key, e)
            return None

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("❌ Failed to download %s: %s", object_key, e)
            return None

        finally:
            if "response" in locals():
                response.close()
                response.release_conn()

    def object_exists(self, object_key: str) -> bool:
        """
        Check if an object exists in storage.

        Args:
            object_key: Storage path to check

        Returns:
            True if object exists, False otherwise
        """

        try:
            self.client.stat_object(
                bucket_name=self.config.raw_bucket_name, object_name=object_key
            )
            self.logger.info("Object %s exists.", object_key)
            return True
        except S3Error as e:
            if e.code == "NoSuchKey":
                self.logger.info("No such key exists.")
                return False
            else:
                self.logger.error("Error checking object existence: %s", str(e))
                return False

    def get_object_size_mb(self, object_key: str) -> float:
        """
        Get the size of an object in the MinIO bucket in megabytes.

        Args:
            object_key: The key/path of the object in the bucket

        Returns:
            Size of the object in megabytes (MB)

        Raises:
            Exception: If the object doesn't exist or there's an error accessing it

        Example:
            >>> size = storage.get_object_size_mb("2024/bahrain_grand_prix/R/race_results.parquet")
            >>> print(f"File size: {size:.2f} MB")
            File size: 2.45 MB
        """

        try:
            # Get object stats
            stat = self.client.stat_object(
                bucket_name=self.config.raw_bucket_name, object_name=object_key
            )

            # Convert bytes to megabytes
            size_mb = stat.size / (1024 * 1024)

            return size_mb

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error(
                "Error getting size for object '%s': %s",
                object_key,
                str(e),
            )

    def list_objects(self, prefix: str = "", recursive: bool = True) -> List[str]:
        """
        List objects in the bucket with optional prefix filter.

        Args:
            prefix: Filter objects by prefix (e.g., "2024/")
            recursive: List all objects recursively

        Returns:
            List of object keys

        Example:
            >>> objects = storage.list_objects(prefix="2024/")
            >>> print(objects)
            ['2024/round_01/race_results.parquet', '2024/round_01/laps.parquet', ...]
        """
        try:
            objects = self.client.list_objects(
                bucket_name=self.config.raw_bucket_name,
                prefix=prefix,
                recursive=recursive,
            )

            object_keys = [obj.object_name for obj in objects]
            self.logger.debug(
                "Found %d objects with prefix '%s'", len(object_keys), prefix
            )
            return object_keys

        except S3Error as e:
            self.logger.error("Failed to list objects: %s", str(e))
            return []

    def delete_object(self, object_key: str) -> bool:
        """
        Delete an object from storage.

        Args:
            object_key: Storage path to delete

        Returns:
            True if successful, False otherwise
        """

        try:
            self.client.remove_object(
                bucket_name=self.config.raw_bucket_name, object_name=object_key
            )
            self.logger.info("Deleted object: %s", object_key)
            return True
        except S3Error as e:
            self.logger.error("Failed to delete %s: %s", object_key, e)
            return False

    def get_object_metadata(self, object_key: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for an object.

        Args:
            object_key: Storage path

        Returns:
            Dictionary with metadata or None if object doesn't exist
        """
        try:
            stat = self.client.stat_object(
                bucket_name=self.config.raw_bucket_name, object_name=object_key
            )

            metadata = {
                "size_bytes": stat.size,
                "size_mb": stat.size / (1024 * 1024),
                "last_modified": stat.last_modified,
                "content_type": stat.content_type,
                "etag": stat.etag,
            }
            self.logger.info("Fetched metadata for %s", object_key)
            return metadata

        except S3Error as e:
            self.logger.error("Failed to get metadata for %s: %s", object_key, str(e))
            return None

    def build_object_key(
        self,
        data_type: str,
        year: int,
        event_name: Optional[str] = None,
        session_type: Optional[str] = None,
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
            self.logger.info(
                "Built object key for season schedule %d: %s", year, object_key
            )
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

        self.logger.info(
            "Built object key for %s of %s %s %d: %s",
            data_type,
            event_name,
            session_type,
            year,
            object_key,
        )

        return object_key

    def upload_session_data(
        self,
        year: int,
        event_name: str,
        session_type: str,
        session_data: Dict[str, Optional[pd.DataFrame]],
    ) -> Dict[str, bool]:
        """
        Upload all data for a session (results, laps, weather).

        Args:
            year: Season year
            event_name: Event name
            session_type: Q, R, S, etc.
            session_data: Dictionary with keys 'results', 'laps', 'weather'

        Returns:
            Dictionary with upload status for each data type

        Example:
            >>> status = storage.upload_session_data(
            ...     year=2024,
            ...     event_name="Bahrain Grand Prix",
            ...     session_type="R",
            ...     session_data={'results': df_results, 'laps': df_laps, 'weather': df_weather}
            ... )
        """

        upload_status = {}

        # Map data types to storage names
        data_type_mapping = {
            "session_info": "session_info",
            "results": "results",
            "laps": "laps",
            "weather": "weather",
            "race_control_messages": "race_control_messages",
            "session_status": "session_status",
            "track_status": "track_status",
        }

        for key, storage_name in data_type_mapping.items():
            df = session_data.get(key)

            if df is not None and not df.empty:
                object_key = self.build_object_key(
                    year=year,
                    data_type=storage_name,
                    event_name=event_name,
                    session_type=session_type,
                )

                success = self.upload_dataframe(df, object_key)
                upload_status[storage_name] = success
            else:
                self.logger.debug("Skipping %s - no data available", storage_name)
                upload_status[storage_name] = False

        return upload_status

    def get_ingestion_summary(self, year: Optional[int] = None) -> Dict[str, Any]:
        """
        Get a summary of ingested data.

        Args:
            year: Filter by year (optional)

        Returns:
            Dictionary with summary statistics
        """

        prefix = f"{year}/" if year else ""
        objects = self.list_objects(prefix=prefix)

        # Categorize objects
        summary = {
            "total_objects": len(objects),
            "by_type": {
                "session_info": 0,
                "laps": 0,
                "results": 0,
                "weather": 0,
                "race_control_messages": 0,
                "session_status": 0,
                "track_status": 0,
            },
            "by_year": {},
            "total_size_mb": 0.0,
        }

        for obj_key in objects:
            # Get metadata
            metadata = self.get_object_metadata(obj_key)
            if metadata:
                summary["total_size_mb"] += metadata["size_mb"]

            # Count by type
            if "results.parquet" in obj_key:
                summary["by_type"]["results"] += 1
            elif "laps.parquet" in obj_key:
                summary["by_type"]["laps"] += 1
            elif "weather.parquet" in obj_key:
                summary["by_type"]["weather"] += 1

            # Count by year
            obj_year = obj_key.split("/")[0]
            if obj_year.isdigit():
                summary["by_year"][obj_year] = summary["by_year"].get(obj_year, 0) + 1

        return summary
