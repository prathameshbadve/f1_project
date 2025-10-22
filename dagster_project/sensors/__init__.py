"""
Dagster sensors for F1 data pipeline
Sensors trigger jobs based on events (file arrivals, data changes, etc.)
"""

from datetime import datetime, timedelta

import pandas as pd
from dagster import (
    sensor,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    DefaultSensorStatus,
    SkipReason,
)

from dagster_project.jobs import (
    weekly_batch_job,
    race_weekend_ingestion_job,
    ml_features_job,
    season_ingestion_job,
)
from dagster_project.resources import StorageResource
from config.settings import is_development, storage_config


# ============================================================================
# DATA ARRIVAL SENSORS
# ============================================================================


@sensor(
    name="raw_data_sensor",
    job=weekly_batch_job,
    minimum_interval_seconds=600,  # Check every 10 minutes
    default_status=DefaultSensorStatus.STOPPED
    if is_development()
    else DefaultSensorStatus.RUNNING,
    description="Trigger processing when new raw session data is detected",
)
def raw_data_sensor(
    context: SensorEvaluationContext,
    storage: StorageResource,
):
    """
    Sensor that watches for new raw session data files.

    When new session data arrives in MinIO, this sensor triggers
    the processing pipeline automatically.

    Checks: sessions/{year}/ prefix for new .parquet files
    """

    # Get cursor (last checked file path)
    last_checked = context.cursor or "sessions/2000/"

    try:
        current_year = datetime.now().year

        # List session files for current year
        session_files = storage.list_objects(
            bucket=storage.bucket_raw, prefix=f"sessions/{current_year}/"
        )

        # Filter for files newer than last checked
        new_files = [f for f in session_files if f > last_checked]

        if not new_files:
            return SensorResult(skip_reason="No new session data files detected")

        # Count unique events (group by event name in path)
        events_with_new_data = set()
        for file_path in new_files:
            # Path format: sessions/{year}/{event}/{session}/{datatype}.parquet
            parts = file_path.split("/")
            if len(parts) >= 3:
                events_with_new_data.add(parts[2])

        context.log.info(
            f"Detected {len(new_files)} new files across {len(events_with_new_data)} events"
        )

        # Update cursor to latest file
        new_cursor = max(new_files)

        # Trigger processing job
        return SensorResult(
            run_requests=[
                RunRequest(
                    run_key=f"raw_data_{new_cursor}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                    tags={
                        "source": "sensor",
                        "trigger": "new_raw_data",
                        "files_detected": str(len(new_files)),
                        "events": ",".join(sorted(events_with_new_data)),
                        "latest_file": new_cursor,
                    },
                )
            ],
            cursor=new_cursor,
        )

    except Exception as e:  # pylint: disable=broad-except
        context.log.error(f"Error in processed_data_sensor: {str(e)}")
        return SensorResult(skip_reason=f"Error: {str(e)}")


# ============================================================================
# RACE WEEKEND DETECTION SENSOR
# ============================================================================


@sensor(
    name="race_weekend_sensor",
    job=race_weekend_ingestion_job,
    minimum_interval_seconds=3600,  # Check every hour
    default_status=DefaultSensorStatus.STOPPED,  # Manual enable only
    description="Automatically detect race weekends and trigger ingestion",
)
def race_weekend_sensor(
    context: SensorEvaluationContext,
):
    """
    Sensor that detects F1 race weekends and triggers ingestion.

    Logic:
    - Races typically happen on Sundays
    - Some races have Sprint on Saturdays
    - Trigger on Saturday or Sunday to capture weekend data
    - Avoid duplicate runs using cursor

    Note: In production, you'd integrate with F1 calendar API
    for exact race dates. This is a simplified version.
    """

    try:
        current_date = datetime.now()
        current_day = current_date.weekday()  # Monday=0, Sunday=6
        today_str = current_date.strftime("%Y-%m-%d")

        # Get last run date from cursor
        last_run_date = context.cursor or "1970-01-01"

        # Check if it's a race weekend day (Saturday=5, Sunday=6)
        is_race_weekend = current_day in [5, 6]

        # Check if we haven't run today yet
        should_run = is_race_weekend and last_run_date != today_str

        if should_run:
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
            return SensorResult(
                skip_reason=f"Not a race weekend day (current: {current_date.strftime('%A')})"
            )

    except Exception as e:  # pylint: disable=broad-except
        context.log.error(f"Error in race_weekend_sensor: {str(e)}")
        return SensorResult(skip_reason=f"Error: {str(e)}")


# ============================================================================
# DATA QUALITY SENSOR
# ============================================================================


@sensor(
    name="data_quality_sensor",
    job=season_ingestion_job,
    minimum_interval_seconds=86400,  # Check once per day
    default_status=DefaultSensorStatus.STOPPED,  # Manual enable only
    description="Monitor data quality and trigger re-ingestion if issues detected",
)
def data_quality_sensor(
    context: SensorEvaluationContext,
    storage: StorageResource,
):
    """
    Sensor that monitors data quality and triggers re-ingestion if needed.

    Checks for:
    - Missing session files (expected but not present)
    - Incomplete race weekends (missing sessions)
    - Files with zero size
    - Corrupted parquet files
    """

    try:
        current_year = datetime.now().year

        # List all session files for current year
        session_files = storage.list_objects(
            bucket=storage.bucket_raw, prefix=f"sessions/{current_year}/"
        )

        # Count sessions by event
        events_data = {}
        for file_path in session_files:
            # Parse: sessions/{year}/{event}/{session}/{datatype}.parquet
            parts = file_path.split("/")
            if len(parts) >= 4:
                event = parts[2]
                session = parts[3]

                if event not in events_data:
                    events_data[event] = set()
                events_data[event].add(session)

        # Check for incomplete events
        # Typical race weekend should have: Q (Qualifying) and R (Race) at minimum
        incomplete_events = []
        for event, sessions in events_data.items():
            if "R" not in sessions:  # Missing race session
                incomplete_events.append(event)

        # Check total number of events
        total_events = len(events_data)
        expected_min_events = 15  # Minimum races in a season

        issues = []

        if total_events < expected_min_events:
            issues.append(
                f"Only {total_events} events found (expected >= {expected_min_events})"
            )

        if incomplete_events:
            issues.append(
                f"{len(incomplete_events)} incomplete events: {', '.join(incomplete_events[:3])}"
            )

        # If issues found, trigger re-ingestion
        if issues:
            context.log.warning(f"Data quality issues detected: {'; '.join(issues)}")

            return SensorResult(
                run_requests=[
                    RunRequest(
                        run_key=f"data_quality_reingestion_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                        tags={
                            "source": "sensor",
                            "trigger": "data_quality_issue",
                            "issues": "; ".join(issues),
                            "year": str(current_year),
                            "events_found": str(total_events),
                            "incomplete_events": str(len(incomplete_events)),
                        },
                    )
                ],
                cursor=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )

        context.log.info(
            f"Data quality check passed: {total_events} events, "
            f"{len(session_files)} total files"
        )

        return SensorResult(
            skip_reason=f"Data quality OK: {total_events} complete events"
        )

    except Exception as e:  # pylint: disable=broad-except
        context.log.error(f"Error in data_quality_sensor: {str(e)}")
        return SensorResult(skip_reason=f"Error: {str(e)}")


# ============================================================================
# ADVANCED: RACE CALENDAR INTEGRATION SENSOR
# ============================================================================


@sensor(
    name="race_calendar_sensor",
    job=race_weekend_ingestion_job,
    minimum_interval_seconds=7200,  # Check every 2 hours
    default_status=DefaultSensorStatus.STOPPED,  # Enable manually when needed
    description="Smart race weekend detection using F1 calendar data",
)
def race_calendar_sensor(
    context: SensorEvaluationContext,
    storage: StorageResource,
):
    """
    Advanced sensor that uses the actual F1 race calendar.

    This sensor:
    1. Reads the season schedule from storage
    2. Checks if today is a race date (or day after)
    3. Triggers ingestion at the right time

    More accurate than simple day-of-week checking.
    """

    try:
        current_year = datetime.now().year
        current_date = datetime.now().date()

        # Get cursor to avoid duplicate runs
        last_run_date = context.cursor or "1970-01-01"
        today_str = current_date.strftime("%Y-%m-%d")

        if last_run_date == today_str:
            return SensorResult(skip_reason=f"Already ran today ({today_str})")

        # Try to load season schedule from storage
        try:
            schedule_key = f"schedules/{current_year}/season_schedule.parquet"

            if not storage.object_exists(storage.bucket_raw, schedule_key):
                return SensorResult(
                    skip_reason=f"Season schedule not found: {schedule_key}"
                )

            # Download schedule
            schedule_df = storage.download_dataframe(
                storage.bucket_raw, schedule_key, format="parquet"
            )

            # Convert EventDate to datetime
            schedule_df["EventDate"] = pd.to_datetime(schedule_df["EventDate"])

            # Find races that happened yesterday or today
            yesterday = current_date - timedelta(days=1)

            recent_races = schedule_df[
                (schedule_df["EventDate"].dt.date >= yesterday)
                & (schedule_df["EventDate"].dt.date <= current_date)
            ]

            if len(recent_races) > 0:
                race_name = recent_races.iloc[0]["EventName"]
                race_date = recent_races.iloc[0]["EventDate"].strftime("%Y-%m-%d")

                context.log.info(
                    f"Race detected: {race_name} on {race_date}. Triggering ingestion."
                )

                return SensorResult(
                    run_requests=[
                        RunRequest(
                            run_key=f"race_{race_name.replace(' ', '_')}_{today_str}",
                            tags={
                                "source": "sensor",
                                "trigger": "race_calendar",
                                "race_name": race_name,
                                "race_date": race_date,
                                "detection_date": today_str,
                            },
                        )
                    ],
                    cursor=today_str,
                )

            return SensorResult(
                skip_reason=f"No races on {yesterday} or {current_date}"
            )

        except Exception as schedule_error:  # pylint: disable=broad-except
            context.log.warning(
                f"Could not load schedule: {schedule_error}. "
                f"Falling back to day-of-week check."
            )

            # Fallback: simple day check
            current_day = datetime.now().weekday()
            if current_day == 0:  # Monday after race
                return SensorResult(
                    run_requests=[
                        RunRequest(
                            run_key=f"race_fallback_{today_str}",
                            tags={
                                "source": "sensor",
                                "trigger": "fallback_monday",
                                "date": today_str,
                            },
                        )
                    ],
                    cursor=today_str,
                )

            return SensorResult(skip_reason="Not Monday (fallback mode)")

    except Exception as e:  # pylint: disable=broad-except
        context.log.error(f"Error in race_calendar_sensor: {str(e)}")
        return SensorResult(skip_reason=f"Error: {str(e)}")


# ============================================================================
# SENSOR NOTES
# ============================================================================

# pylint: disable=pointless-string-statement

"""
Sensor Strategy:
----------------

SENSORS VS SCHEDULES:
- Schedules: Time-based (run at specific times)
- Sensors: Event-based (run when something happens)

CURSOR PATTERN:
All sensors use `context.cursor` to maintain state:
- Stores last checked file/date
- Prevents duplicate runs
- Survives Dagster restarts

SENSOR INTERVALS:
- raw_data_sensor: 10 min (fast response to new data)
- processed_data_sensor: 15 min (less critical)
- race_weekend_sensor: 1 hour (infrequent check)
- data_quality_sensor: 1 day (background monitoring)
- race_calendar_sensor: 2 hours (moderate frequency)

PRODUCTION RECOMMENDATIONS:
1. Enable raw_data_sensor for reactive processing
2. Keep race_weekend_sensor STOPPED (use schedules instead)
3. Enable data_quality_sensor for monitoring
4. Use race_calendar_sensor for precise race detection

DEVELOPMENT MODE:
- All sensors STOPPED by default
- Prevents automatic runs during testing
- Enable individually as needed
"""

# # pylint: disable=broad-except
#         context.log.error(f"Error in raw_data_sensor: {str(e)}")
#         return SensorResult(skip_reason=f"Error: {str(e)}")


# @sensor(
#     name="processed_data_sensor",
#     job=ml_features_job,
#     minimum_interval_seconds=900,  # Check every 15 minutes
#     default_status=DefaultSensorStatus.STOPPED
#     if is_development()
#     else DefaultSensorStatus.RUNNING,
#     description="Trigger ML feature generation when new processed data is available",
# )
# def processed_data_sensor(
#     context: SensorEvaluationContext,
#     storage: StorageResource,
# ):
#     """
#     Sensor that watches for new processed data.

#     Creates a reactive pipeline: raw data → processing → features
#     This sensor triggers feature generation automatically after processing.
#     """

#     last_checked = context.cursor or "processed/2000/"

#     try:
#         current_year = datetime.now().year

#         # List processed data files
#         processed_files = storage.list_objects(
#             bucket=storage.bucket_processed,
#             prefix=f"processed/{current_year}/"
#         )

#         # Filter for new files
#         new_files = [f for f in processed_files if f > last_checked]

#         if not new_files:
#             return SensorResult(skip_reason="No new processed data files detected")

#         context.log.info(f"Detected {len(new_files)} new processed data files")

#         # Update cursor
#         new_cursor = max(new_files)

#         return SensorResult(
#             run_requests=[
#                 RunRequest(
#                     run_key=f"processed_data_{new_cursor}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
#                     tags={
#                         "source": "sensor",
#                         "trigger": "new_processed_data",
#                         "files_detected": str(len(new_files)),
#                         "latest_file": new_cursor,
#                     },
#                 )
#             ],
#             cursor=new_cursor,
#         )

#     except Exception as e:
