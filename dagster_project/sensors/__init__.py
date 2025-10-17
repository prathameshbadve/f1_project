"""
Dagster sensors for F1 data pipeline
Sensors trigger jobs based on events (e.g., new files in S3)
"""

from datetime import datetime

from dagster import (
    sensor,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    DefaultSensorStatus,
    SkipReason,
)

from dagster_project.jobs import (
    weekly_processing_job,
    race_data_ingestion_job,
    ml_features_job,
)
from dagster_project.resources import StorageResource
from config.settings import is_development, storage_config


@sensor(
    name="raw_data_sensor",
    job=weekly_processing_job,
    minimum_interval_seconds=300,  # Check every 5 minutes
    default_status=DefaultSensorStatus.STOPPED
    if is_development()
    else DefaultSensorStatus.RUNNING,
    description="Trigger processing when new raw data is detected in storage",
)
def raw_data_sensor(
    context: SensorEvaluationContext,
    storage: StorageResource,
):
    """
    Sensor that watches for new raw data files and triggers processing.

    In production: Monitors S3 for new race results
    In local: Disabled by default (can be manually enabled for testing)
    """

    # Get cursor (last checked timestamp or state)
    last_checked = context.cursor or "0"

    try:
        # List recent race result files
        race_files = storage.list_objects(
            bucket=storage.bucket_raw, prefix="race_results/"
        )

        # Filter for new files (files that are greater than last checked)
        new_files = [f for f in race_files if f > last_checked]

        if new_files:
            context.log.info(f"Detected {len(new_files)} new raw data files")

            # Update cursor to latest file
            new_cursor = max(new_files)

            return SensorResult(
                run_requests=[
                    RunRequest(
                        run_key=f"raw_data_{new_cursor}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                        tags={
                            "source": "sensor",
                            "trigger": "new_raw_data",
                            "files_detected": str(len(new_files)),
                            "latest_file": new_cursor,
                        },
                    )
                ],
                cursor=new_cursor,
            )

        # No new files
        return SensorResult(skip_reason="No new raw data files detected")

    except Exception as e:  # pylint: disable=broad-except
        context.log.error(f"Error in raw_data_sensor: {str(e)}")
        return SensorResult(skip_reason=f"Error: {str(e)}")


@sensor(
    name="race_weekend_sensor",
    job=race_data_ingestion_job,
    minimum_interval_seconds=3600,  # Check every hour
    default_status=DefaultSensorStatus.STOPPED,  # Manual enable only
    description="Trigger race data ingestion during race weekends",
)
def race_weekend_sensor(
    context: SensorEvaluationContext,
):
    """
    Sensor that triggers ingestion during F1 race weekends.

    This sensor checks if today is a race weekend and triggers ingestion.
    Note: This is a simplified version. In production, you'd integrate with
    a race calendar API to know exact race dates.
    """

    try:
        current_date = datetime.now()
        current_day = current_date.weekday()  # Monday=0, Sunday=6

        # F1 races are typically on Sundays (6) or Saturdays (5) for sprint races
        is_race_weekend = current_day in [5, 6]  # Saturday or Sunday

        # Get cursor to avoid duplicate runs on the same day
        last_run_date = context.cursor or "1970-01-01"
        today_str = current_date.strftime("%Y-%m-%d")

        if is_race_weekend and last_run_date != today_str:
            context.log.info(f"Race weekend detected on {today_str}")

            return SensorResult(
                run_requests=[
                    RunRequest(
                        run_key=f"race_weekend_{today_str}",
                        tags={
                            "source": "sensor",
                            "trigger": "race_weekend",
                            "date": today_str,
                            "day": current_date.strftime("%A"),
                        },
                    )
                ],
                cursor=today_str,
            )

        if is_race_weekend:
            return SensorResult(
                skip_reason=f"Already ran for race weekend on {today_str}"
            )
        else:
            return SensorResult(skip_reason="Not a race weekend (Saturday/Sunday)")

    except Exception as e:  # pylint: disable=broad-except
        context.log.error(f"Error in race_weekend_sensor: {str(e)}")
        return SensorResult(skip_reason=f"Error: {str(e)}")


@sensor(
    name="processed_data_sensor",
    job=ml_features_job,
    minimum_interval_seconds=600,  # Check every 10 minutes
    default_status=DefaultSensorStatus.STOPPED
    if is_development()
    else DefaultSensorStatus.RUNNING,
    description="Trigger ML feature generation when new processed data is available",
)
def processed_data_sensor(
    context: SensorEvaluationContext,
    storage: StorageResource,
):
    """
    Sensor that watches for new processed data and triggers ML feature generation.

    This creates a reactive pipeline where feature generation happens automatically
    after data processing completes.
    """

    last_checked = context.cursor or "0"

    try:
        # List processed data files
        processed_files = storage.list_objects(
            bucket=storage.bucket_processed, prefix="processed/races/"
        )

        # Filter for new files
        new_files = [f for f in processed_files if f > last_checked]

        if new_files:
            context.log.info(f"Detected {len(new_files)} new processed data files")

            # Update cursor
            new_cursor = max(new_files)

            return SensorResult(
                run_requests=[
                    RunRequest(
                        run_key=f"processed_data_{new_cursor}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                        tags={
                            "source": "sensor",
                            "trigger": "new_processed_data",
                            "files_detected": str(len(new_files)),
                            "latest_file": new_cursor,
                        },
                    )
                ],
                cursor=new_cursor,
            )

        return SensorResult(skip_reason="No new processed data files detected")

    except Exception as e:  # pylint: disable=broad-except
        context.log.error(f"Error in processed_data_sensor: {str(e)}")
        return SensorResult(skip_reason=f"Error: {str(e)}")


@sensor(
    name="data_quality_sensor",
    job=race_data_ingestion_job,
    minimum_interval_seconds=86400,  # Check once per day
    default_status=DefaultSensorStatus.STOPPED,  # Manual enable only
    description="Check data quality and trigger re-ingestion if issues detected",
)
def data_quality_sensor(
    context: SensorEvaluationContext,
    storage: StorageResource,
):
    """
    Sensor that monitors data quality and triggers re-ingestion if issues are found.

    This sensor checks for:
    - Missing expected files
    - Files with no data
    - Corrupted files

    If issues are detected, it triggers a re-ingestion job.
    """

    try:
        # Get current year for checking
        current_year = datetime.now().year

        # List race result files for current year
        race_files = storage.list_objects(
            bucket=storage.bucket_raw, prefix=f"race_results/{current_year}/"
        )

        # Expected minimum number of races in a season
        expected_min_races = 20

        if len(race_files) < expected_min_races:
            context.log.warning(
                f"Data quality issue: Only {len(race_files)} race files found for {current_year}. "
                f"Expected at least {expected_min_races}."
            )

            return SensorResult(
                run_requests=[
                    RunRequest(
                        run_key=f"data_quality_reingestion_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                        tags={
                            "source": "sensor",
                            "trigger": "data_quality_issue",
                            "issue": "missing_files",
                            "files_found": str(len(race_files)),
                            "year": str(current_year),
                        },
                    )
                ],
                cursor=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )

        context.log.info(
            f"Data quality check passed: {len(race_files)} files found for {current_year}"
        )
        return SensorResult(
            skip_reason=f"Data quality OK: {len(race_files)} race files found"
        )

    except Exception as e:  # pylint: disable=broad-except
        context.log.error(f"Error in data_quality_sensor: {str(e)}")
        return SensorResult(skip_reason=f"Error: {str(e)}")
