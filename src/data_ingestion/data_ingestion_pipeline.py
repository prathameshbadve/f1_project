"""
Main data ingestion pipeline for F1 data.

Orchestrates fetching data from FastF1 API, validating it, and storing in MinIO/S3.
Includes progress tracking, error handling, and resumability.
"""

import json
import time
from typing import Dict, Optional, List
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path

from config.logging import get_logger

from src.data_ingestion.schedule_loader import ScheduleLoader
from src.data_ingestion.session_data_loader import SessionLoader
from src.data_ingestion.storage_client import StorageClient
from src.data_ingestion.schemas import validate_session_data


@dataclass
class SessionResult:
    """Result of processing a single session"""

    year: int
    event_name: str
    session_type: str
    success: bool
    validation_errors: int
    uploaded_files: Dict[str, bool]
    error_message: Optional[str] = None
    processing_time_seconds: float = 0.0


@dataclass
class PipelineStats:
    """Statistics for pipeline execution"""

    total_sessions: int = 0
    successful_sessions: int = 0
    failed_sessions: int = 0
    skipped_sessions: int = 0
    total_validation_errors: int = 0
    total_files_uploaded: int = 0
    total_processing_time_seconds: float = 0.0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""

        data = asdict(self)
        if self.start_time:
            data["start_time"] = self.start_time.isoformat()
        if self.end_time:
            data["end_time"] = self.end_time.isoformat()
        return data


class IngestionPipeline:
    """
    Main pipeline for ingesting F1 data.

    Handles:
    - Fetching data from FastF1 API
    - Validating data quality
    - Storing data in MinIO/S3
    - Progress tracking
    - Error handling
    - Resumability

    Example:
        >>> pipeline = IngestionPipeline()
        >>> results = pipeline.ingest_season(2024)
        >>> pipeline.print_summary()
    """

    def __init__(
        self,
        skip_existing: bool = True,
        validate_data: bool = True,
        delay_between_sessions: int = 3,
    ):
        """
        Initialize ingestion pipeline.

        Args:
            skip_existing: Skip sessions that are already ingested
            validate_data: Validate data before storing
            delay_between_sessions: Delay in seconds between API calls
        """

        self.logger = get_logger("data_ingestion.pipeline")
        self.schedule_loader = ScheduleLoader()
        self.session_loader = SessionLoader()
        self.storage_client = StorageClient()

        self.skip_existing = skip_existing
        self.validate_data = validate_data
        self.delay_between_sessions = delay_between_sessions

        self.stats = PipelineStats()
        self.session_results: List[SessionResult] = []

        self.logger.info("Ingestion pipeline initialized")
        self.logger.info("  Skip existing: %s", skip_existing)
        self.logger.info("  Validate data: %s", validate_data)
        self.logger.info("  Delay between sessions: %ds", delay_between_sessions)

    def ingest_session(self, year: int, event: str, session_type: str) -> SessionResult:
        """
        Ingest a single session.

        Args:
            year: Season year
            event: Event identifier (name or round number)
            session_type: Session type (R, Q, S, etc.)

        Returns:
            SessionResult with ingestion details
        """
        start_time = time.time()
        event_name = str(event)  # Will be updated with actual name

        self.logger.info("=" * 70)
        self.logger.info("Ingesting session: %d %s %s", year, event, session_type)
        self.logger.info("=" * 70)

        events_in_season = self.schedule_loader.get_events_for_ingestion(year)

        if event not in events_in_season:
            self.logger.warning(
                "Event not in season calendar, please check again: %s", event
            )
            error_msg = "Event not in season calendar, please check again"
            return SessionResult(
                year=year,
                event_name=event,
                session_type=session_type,
                success=False,
                validation_errors=0,
                uploaded_files={},
                error_message=error_msg,
                processing_time_seconds=time.time() - start_time,
            )

        try:
            # Get session cache summary
            session_cache_summary = self.session_loader.get_session_cache_summary(
                year,
                event,
                session_type,
            )

            # Check if cache is complete
            if session_cache_summary["cache_complete"]:
                self.logger.info("⏭️  Session already exists, skipping")
                return SessionResult(
                    year=year,
                    event_name=event,
                    session_type=session_type,
                    success=True,
                    validation_errors=0,
                    uploaded_files={},
                    error_message="Skipped (already exists)",
                    processing_time_seconds=time.time() - start_time,
                )

            # Extract data
            self.logger.info("Extracting session data...")
            session_data = self.session_loader.load_session_data(
                year,
                event,
                session_type,
            )

            # Check if we got any data
            if all(df is None for df in session_data.values()):
                error_msg = "No data extracted from session"
                self.logger.warning(error_msg)
                return SessionResult(
                    year=year,
                    event_name=event,
                    session_type=session_type,
                    success=False,
                    validation_errors=0,
                    uploaded_files={},
                    error_message=error_msg,
                    processing_time_seconds=time.time() - start_time,
                )

            validation_errors = 0

            # Validate data if enabled
            if self.validate_data:
                self.logger.info("Validating data...")
                validated = validate_session_data(session_data)

                # Replace with validated data and count errors
                for data_type, (valid_df, errors) in validated.items():
                    session_data[data_type] = valid_df
                    validation_errors += len(errors)

                    if errors:
                        self.logger.warning(
                            "%s: %d validation errors found", data_type, len(errors)
                        )
                    else:
                        self.logger.info("%s: All rows validated ✅", data_type)

                if validation_errors > 0:
                    self.logger.warning(
                        "Total validation errors: %d", validation_errors
                    )
                else:
                    self.logger.info("✅ All data validated successfully")

            # Upload to storage
            self.logger.info("Uploading to storage...")
            upload_status = self.storage_client.upload_session_data(
                year=year,
                event_name=event_name,
                session_type=session_type,
                session_data=session_data,
            )

            # Check upload results
            successful_uploads = sum(1 for success in upload_status.values() if success)
            total_uploads = len(upload_status)

            self.logger.info(
                "Upload complete: %d/%d files uploaded",
                successful_uploads,
                total_uploads,
            )

            processing_time = time.time() - start_time
            self.logger.info("✅ Session processed in %.2f seconds", processing_time)

            return SessionResult(
                year=year,
                event_name=event_name,
                session_type=session_type,
                success=True,
                validation_errors=validation_errors,
                uploaded_files=upload_status,
                processing_time_seconds=processing_time,
            )

        except Exception as e:  # pylint: disable=broad-except
            error_msg = str(e)
            self.logger.error(
                "❌ Failed to ingest session: %s", error_msg, exc_info=True
            )

            return SessionResult(
                year=year,
                event_name=event_name,
                session_type=session_type,
                success=False,
                validation_errors=0,
                uploaded_files={},
                error_message=error_msg,
                processing_time_seconds=time.time() - start_time,
            )

        finally:
            # Delay before next session
            if self.delay_between_sessions > 0:
                self.logger.debug(
                    "Waiting %ds before next session...", self.delay_between_sessions
                )
                time.sleep(self.delay_between_sessions)

    def ingest_race_weekend(
        self,
        year: int,
        event: str,
        session_types: Optional[List[str]] = None,
    ) -> List[SessionResult]:
        """
        Ingest all sessions for a race weekend.

        Args:
            year: Season year
            event: Event identifier
            session_types: List of session types to ingest (defaults to config)

        Returns:
            List of SessionResult objects
        """

        if session_types is None:
            session_types = self.session_loader.config.session_types

        self.logger.info("=" * 70)
        self.logger.info("INGESTING RACE WEEKEND: %d %s", year, event)
        self.logger.info("Sessions: %s", ", ".join(session_types))
        self.logger.info("=" * 70)

        results = []

        for session_type in session_types:
            result = self.ingest_session(year, event, session_type)
            results.append(result)

            # Update stats
            self.stats.total_sessions += 1
            if result.success:
                if result.error_message and "Skipped" in result.error_message:
                    self.stats.skipped_sessions += 1
                else:
                    self.stats.successful_sessions += 1
                    self.stats.total_validation_errors += result.validation_errors
                    self.stats.total_files_uploaded += sum(
                        1 for success in result.uploaded_files.values() if success
                    )
            else:
                self.stats.failed_sessions += 1

            self.stats.total_processing_time_seconds += result.processing_time_seconds
            self.session_results.append(result)

        return results

    def ingest_season(
        self,
        year: int,
        events: Optional[List[str]] = None,
        session_types: Optional[List[str]] = None,
    ) -> List[SessionResult]:
        """
        Ingest all sessions for a complete season.

        Args:
            year: Season year
            events: List of specific events to ingest (None = all events)
            session_types: List of session types (None = use config defaults)

        Returns:
            List of SessionResult objects
        """

        self.stats.start_time = datetime.now()

        self.logger.info("=" * 70)
        self.logger.info("INGESTING COMPLETE SEASON: %d", year)
        self.logger.info("=" * 70)

        # Get season schedule
        self.logger.info("Fetching season schedule...")
        schedule = self.schedule_loader.load_season_schedule(year)

        # Filter to specified events if provided
        if events:
            schedule = schedule[schedule["EventName"].isin(events)]
            self.logger.info("Filtered to %d specified events", len(schedule))
        else:
            self.logger.info("Processing all %d events", len(schedule))

        # Also upload season schedule
        self.logger.info("Uploading season schedule...")
        object_key = self.storage_client.build_object_key("schedule", year)
        self.storage_client.upload_dataframe(schedule, object_key)

        results = []

        # Process each event
        for idx, event_row in schedule.iterrows():
            event_name = event_row["EventName"]

            self.logger.info("Event %d/%d: %s", idx + 1, len(schedule), event_name)

            weekend_results = self.ingest_race_weekend(
                year=year, event=event_name, session_types=session_types
            )

            results.extend(weekend_results)

        self.stats.end_time = datetime.now()

        self.logger.info("=" * 70)
        self.logger.info("SEASON INGESTION COMPLETE")
        self.logger.info("=" * 70)

        self.print_summary()

        return results

    def ingest_multiple_seasons(
        self,
        years: List[int],
        events: Optional[List[str]] = None,
        session_types: Optional[List[str]] = None,
    ) -> List[SessionResult]:
        """
        Ingest multiple seasons.

        Args:
            years: List of season years
            events: List of specific events (applied to all seasons)
            session_types: List of session types

        Returns:
            List of all SessionResult objects
        """

        self.logger.info("=" * 70)
        self.logger.info("INGESTING MULTIPLE SEASONS: %s", ", ".join(map(str, years)))
        self.logger.info("=" * 70)

        all_results = []

        for year in years:
            season_results = self.ingest_season(
                year=year, events=events, session_types=session_types
            )
            all_results.extend(season_results)

        return all_results

    def print_summary(self):
        """Print pipeline execution summary"""

        self.logger.info("=" * 70)
        self.logger.info("PIPELINE EXECUTION SUMMARY")
        self.logger.info("=" * 70)
        self.logger.info("Total sessions processed:  %d", self.stats.total_sessions)
        self.logger.info(
            "  ✅ Successful:           %d", self.stats.successful_sessions
        )
        self.logger.info("  ❌ Failed:               %d", self.stats.failed_sessions)
        self.logger.info("  ⏭️  Skipped (existing):   %d", self.stats.skipped_sessions)
        self.logger.info("")
        self.logger.info(
            "Validation errors:         %d", self.stats.total_validation_errors
        )
        self.logger.info(
            "Files uploaded:            %d", self.stats.total_files_uploaded
        )
        self.logger.info(
            "Total processing time:     %.2f seconds (%.2f minutes)",
            self.stats.total_processing_time_seconds,
            self.stats.total_processing_time_seconds / 60,
        )

        if self.stats.start_time and self.stats.end_time:
            duration = (self.stats.end_time - self.stats.start_time).total_seconds()
            self.logger.info(
                "Wall clock time:           %.2f seconds (%.2f minutes)",
                duration,
                duration / 60,
            )

        self.logger.info("=" * 70)

        # Print failed sessions if any
        if self.stats.failed_sessions > 0:
            self.logger.warning("\nFailed sessions:")
            for result in self.session_results:
                if (
                    not result.success
                    and result.error_message
                    and "Skipped" not in result.error_message
                ):
                    self.logger.warning(
                        "  ❌ %d %s %s: %s",
                        result.year,
                        result.event_name,
                        result.session_type,
                        result.error_message,
                    )

    def save_results(self, filepath: str):
        """
        Save pipeline results to JSON file.

        Args:
            filepath: Path to save results
        """

        results_data = {
            "stats": self.stats.to_dict(),
            "sessions": [asdict(result) for result in self.session_results],
        }

        filepath = Path(filepath)
        filepath.parent.mkdir(parents=True, exist_ok=True)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(results_data, f, indent=2, default=str)

        self.logger.info("Results saved to: %s", filepath)

    def get_failed_sessions(self) -> List[SessionResult]:
        """Get list of failed sessions for retry"""

        return [
            result
            for result in self.session_results
            if not result.success
            and result.error_message
            and "Skipped" not in result.error_message
        ]

    def retry_failed_sessions(self) -> List[SessionResult]:
        """Retry all previously failed sessions"""

        failed = self.get_failed_sessions()

        if not failed:
            self.logger.info("No failed sessions to retry")
            return []

        self.logger.info("Retrying %d failed sessions...", len(failed))

        retry_results = []
        for failed_result in failed:
            result = self.ingest_session(
                year=failed_result.year,
                event=failed_result.event_name,
                session_type=failed_result.session_type,
            )
            retry_results.append(result)

        return retry_results


# Convenience functions
def ingest_single_session(
    year: int, event: str, session_type: str, skip_existing: bool = True
) -> SessionResult:
    """
    Convenience function to ingest a single session.

    Args:
        year: Season year
        event: Event name or identifier
        session_type: Session type (R, Q, S, etc.)
        skip_existing: Skip if already ingested

    Returns:
        SessionResult

    Example:
        >>> result = ingest_single_session(2024, "Bahrain", "R")
        >>> print(f"Success: {result.success}")
    """

    pipeline = IngestionPipeline(skip_existing=skip_existing)
    return pipeline.ingest_session(year, event, session_type)


def ingest_single_season(
    year: int, skip_existing: bool = True, validate: bool = True
) -> List[SessionResult]:
    """
    Convenience function to ingest a complete season.

    Args:
        year: Season year
        skip_existing: Skip already ingested sessions
        validate: Validate data before storing

    Returns:
        List of SessionResult objects

    Example:
        >>> results = ingest_single_season(2024)
        >>> successful = sum(1 for r in results if r.success)
        >>> print(f"Successfully ingested {successful} sessions")
    """

    pipeline = IngestionPipeline(skip_existing=skip_existing, validate_data=validate)
    return pipeline.ingest_season(year)
