"""
Base Aggregator

Abstract base class for all data aggregators with common patterns.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Generic, TypeVar
import pandas as pd

from config.logging import get_logger
from src.clients.storage_client import StorageClient


T = TypeVar("T")


@dataclass
class AggregationResult(Generic[T]):
    """
    Result of aggregation with metadata.

    Generic type T is the output data type (usually pd.DataFrame)
    """

    data: T
    items_processed: int
    items_skipped: int
    skipped_items: List[dict]
    warnings: List[str]
    errors: List[str]

    @property
    def success_rate(self) -> float:
        """Calculate success rate"""

        total = self.items_processed + self.items_skipped
        return self.items_processed / total if total > 0 else 0.0

    @property
    def failure_rate(self) -> float:
        """Calculate failure rate"""

        return 1.0 - self.success_rate


class BaseAggregator(ABC):
    """
    Abstract base class for all aggregators.

    Provides:
    - Common initialization (storage, logging)
    - Counter management
    - Progress tracking
    - Validation framework
    - Error/warning collection

    Subclasses must implement:
    - aggregate(): Main aggregation logic
    - _process_single_item(): Process one item
    - _validate_output(): Validate aggregated output
    """

    def __init__(
        self,
        storage_client: StorageClient,
        raw_bucket: str = "dev-f1-data-raw",
        processed_bucket: str = "dev-f1-data-processed",
        logger_name: str = "etl.aggregator",
    ):
        """
        Initialize aggregator.

        Args:
            storage_client: Storage client for S3/MinIO
            raw_bucket: Bucket containing raw data
            processed_bucket: Bucket for processed output
            logger_name: Logger name for this aggregator
        """

        self.storage = storage_client
        self.raw_bucket = raw_bucket
        self.processed_bucket = processed_bucket
        self.logger = get_logger(logger_name)

        # Counters
        self.items_processed = 0
        self.items_skipped = 0
        self.skipped_items: List[dict] = []
        self.warnings: List[str] = []
        self.errors: List[str] = []

    def _reset_counters(self):
        """Reset all counters before aggregation"""

        self.items_processed = 0
        self.items_skipped = 0
        self.skipped_items = []
        self.warnings = []
        self.errors = []

    def _log_progress(self, current: int, total: int, interval: int = 10):
        """
        Log progress at regular intervals.

        Args:
            current: Current item number (1-indexed)
            total: Total items to process
            interval: Log every N items
        """

        if current % interval == 0:
            self.logger.info("Progress: %d/%d items processed", current, total)

    def _log_aggregation_header(self, title: str):
        """Log formatted header for aggregation start"""

        self.logger.info("=" * 80)
        self.logger.info(title)
        self.logger.info("=" * 80)

    def _log_aggregation_summary(
        self, total_items: int, output_size: int, output_description: str = "records"
    ):
        """
        Log formatted summary of aggregation.

        Args:
            total_items: Total items attempted
            output_size: Size of output (e.g., len(df))
            output_description: Description of output units
        """

        self.logger.info("=" * 80)
        self.logger.info("Aggregation Complete")
        self.logger.info("=" * 80)
        self.logger.info("Items processed: %d/%d", self.items_processed, total_items)
        self.logger.info("Items skipped: %s", self.items_skipped)
        self.logger.info("Total %s: %s", output_description, output_size)
        self.logger.info("Warnings: %d", len(self.warnings))
        self.logger.info("Errors: %d", len(self.errors))

    def _record_skip(self, item_id: str, reason: str):
        """
        Record a skipped item.

        Args:
            item_id: Identifier for the skipped item
            reason: Reason for skipping
        """

        self.items_skipped += 1
        self.skipped_items.append({"item_id": item_id, "reason": reason})

    def _record_warning(self, message: str):
        """Record a warning"""

        self.logger.warning(message)
        self.warnings.append(message)

    def _record_error(self, message: str):
        """Record an error"""

        self.logger.error(message)
        self.errors.append(message)

    def _load_parquet_safe(
        self,
        key: str,
        bucket: Optional[str] = None,
        required_columns: Optional[List[str]] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Safely load parquet file with validation.

        Args:
            key: S3 key to load
            bucket: Bucket name (defaults to raw_bucket)
            required_columns: Columns that must be present

        Returns:
            DataFrame if successful, None otherwise
        """

        bucket = bucket or self.raw_bucket

        try:
            df = self.storage.download_dataframe(bucket_name=bucket, object_key=key)

            # Check if empty
            if df.empty:
                self.logger.warning("Empty file: %s", key)
                return None

            # Check required columns
            if required_columns:
                missing = [col for col in required_columns if col not in df.columns]
                if missing:
                    self.logger.warning("Missing columns in %s: %s", key, missing)
                    return None

            return df

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to load %s: %s", key, e)
            return None

    def _validate_record_count(
        self,
        actual: int,
        expected_min: int,
        expected_max: int,
        data_description: str = "records",
    ) -> bool:
        """
        Validate that record count is within expected range.

        Args:
            actual: Actual record count
            expected_min: Minimum expected records
            expected_max: Maximum expected records
            data_description: Description for logging

        Returns:
            True if valid, False otherwise (logs warning)
        """

        if not expected_min <= actual <= expected_max:
            warning = (
                f"Unexpected {data_description} count: {actual} "
                f"(expected {expected_min}-{expected_max})"
            )
            self._record_warning(warning)
            return False
        return True

    def _check_duplicates(
        self, df: pd.DataFrame, subset: List[str], description: str = "records"
    ) -> bool:
        """
        Check for duplicate records.

        Args:
            df: DataFrame to check
            subset: Columns to check for duplicates
            description: Description for logging

        Returns:
            True if no duplicates, False if duplicates found
        """

        duplicates = df[df.duplicated(subset=subset, keep=False)]
        if len(duplicates) > 0:
            error = f"Found {len(duplicates)} duplicate {description}!"
            self._record_error(error)
            return False
        return True

    def _check_required_columns(
        self, df: pd.DataFrame, required_columns: List[str]
    ) -> bool:
        """
        Check that all required columns are present.

        Args:
            df: DataFrame to check
            required_columns: List of required column names

        Returns:
            True if all present, False if any missing
        """

        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            error = f"Missing required columns: {missing}"
            self._record_error(error)
            return False
        return True

    def _check_null_threshold(
        self, df: pd.DataFrame, column: str, threshold: float, description: str = ""
    ) -> bool:
        """
        Check that null percentage is below threshold.

        Args:
            df: DataFrame to check
            column: Column to check for nulls
            threshold: Maximum allowed null percentage (0.0-1.0)
            description: Description for logging

        Returns:
            True if below threshold, False otherwise
        """

        null_count = df[column].isnull().sum()
        null_pct = null_count / len(df)

        if null_pct > threshold:
            warning = (
                f"High null {description or column}: "
                f"{null_count}/{len(df)} ({null_pct:.1%} > {threshold:.1%})"
            )
            self._record_warning(warning)
            return False
        return True

    @abstractmethod
    def aggregate(
        self, catalog_df: pd.DataFrame, sample_size: Optional[int] = None, **kwargs
    ) -> AggregationResult:
        """
        Main aggregation method - must be implemented by subclasses.

        Args:
            catalog_df: Validated catalog DataFrame
            sample_size: If provided, only process first N items (for testing)
            **kwargs: Additional aggregator-specific parameters

        Returns:
            AggregationResult with combined data and metadata
        """
        pass

    @abstractmethod
    def _process_single_item(
        self, item_info: pd.Series, **kwargs
    ) -> Optional[pd.DataFrame]:
        """
        Process a single item (race, session, etc.) - must be implemented by subclasses.

        Args:
            item_info: Row from catalog for this item
            **kwargs: Additional processing parameters

        Returns:
            DataFrame with processed data for this item, or None if failed
        """
        pass

    @abstractmethod
    def _validate_output(self, df: pd.DataFrame, total_items: int):
        """
        Validate aggregated output - must be implemented by subclasses.

        Args:
            df: Aggregated DataFrame
            total_items: Total items processed

        Should use helper methods like:
        - _validate_record_count()
        - _check_duplicates()
        - _check_required_columns()
        - _check_null_threshold()
        """
        pass
