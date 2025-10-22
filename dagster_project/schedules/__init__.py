"""
Dagster schedules for F1 data pipeline
Schedules trigger jobs at specific times using cron expressions
"""

from dagster import ScheduleDefinition, DefaultScheduleStatus

from dagster_project.jobs import (
    race_weekend_ingestion_job,
    weekly_batch_job,
    full_pipeline_job,
)
from config.settings import is_development


# ============================================================================
# INGESTION SCHEDULES
# ============================================================================

# Race weekend ingestion schedule
# Runs on Monday morning after race weekends to capture final data
race_weekend_schedule = ScheduleDefinition(
    name="race_weekend_schedule",
    job=race_weekend_ingestion_job,
    cron_schedule="0 6 * * 1",  # 6 AM UTC every Monday
    default_status=DefaultScheduleStatus.STOPPED
    if is_development()
    else DefaultScheduleStatus.RUNNING,
    description=(
        "Ingest F1 data after race weekends. Runs Monday at 6 AM UTC. "
        "In development: manually triggered. In production: runs automatically."
    ),
    tags={
        "team": "data-engineering",
        "priority": "high",
        "type": "ingestion",
        "frequency": "weekly",
    },
)


# ============================================================================
# PROCESSING SCHEDULES
# ============================================================================

# Weekly processing schedule
# Runs mid-week to process accumulated race data
weekly_processing_schedule = ScheduleDefinition(
    name="weekly_processing_schedule",
    job=weekly_batch_job,
    cron_schedule="0 3 * * 3",  # 3 AM UTC every Wednesday
    default_status=DefaultScheduleStatus.STOPPED
    if is_development()
    else DefaultScheduleStatus.RUNNING,
    description=(
        "Process raw data and generate ML features weekly. "
        "Runs Wednesday at 3 AM UTC to allow time for data collection."
    ),
    tags={
        "team": "data-engineering",
        "priority": "medium",
        "type": "processing",
        "frequency": "weekly",
    },
)


# ============================================================================
# MAINTENANCE SCHEDULES
# ============================================================================

# Monthly full pipeline refresh
# Complete end-to-end refresh including re-ingestion
monthly_full_pipeline_schedule = ScheduleDefinition(
    name="monthly_full_pipeline_schedule",
    job=full_pipeline_job,
    cron_schedule="0 4 1 * *",  # 4 AM UTC on 1st of every month
    default_status=DefaultScheduleStatus.STOPPED,  # Always manual in all environments
    description=(
        "Full pipeline refresh: re-ingest, process, and generate features. "
        "Runs monthly on the 1st at 4 AM UTC. Manual trigger recommended to avoid costs."
    ),
    tags={
        "team": "data-engineering",
        "priority": "low",
        "type": "maintenance",
        "frequency": "monthly",
    },
)


# ============================================================================
# OPTIONAL: DAILY INCREMENTAL SCHEDULE
# ============================================================================

# Commented out by default - enable if you want daily ingestion during season
#
# daily_incremental_schedule = ScheduleDefinition(
#     name="daily_incremental_schedule",
#     job=season_ingestion_job,
#     cron_schedule="0 2 * * *",  # 2 AM UTC every day
#     default_status=DefaultScheduleStatus.STOPPED,
#     description=(
#         "Daily incremental ingestion during race season. "
#         "Captures practice sessions and qualifying data. "
#         "Enable manually during active race season only."
#     ),
#     tags={
#         "team": "data-engineering",
#         "priority": "medium",
#         "type": "ingestion",
#         "frequency": "daily",
#     },
# )


# ============================================================================
# SCHEDULE NOTES
# ============================================================================

"""
Schedule Strategy:
------------------

RACE WEEKEND FLOW:
  Friday-Sunday:    Race weekend (FP1, FP2, FP3, Qualifying, Race)
  Monday 6 AM:      Ingestion runs (race_weekend_schedule)
  Wednesday 3 AM:   Processing runs (weekly_processing_schedule)

DEVELOPMENT MODE:
  - All schedules STOPPED by default
  - Trigger jobs manually via Dagster UI
  - Prevents automatic API calls and costs

PRODUCTION MODE:
  - race_weekend_schedule: RUNNING (automatic)
  - weekly_processing_schedule: RUNNING (automatic)
  - monthly_full_pipeline_schedule: STOPPED (manual only)

CRON EXPRESSIONS:
  - "0 6 * * 1" = 6:00 AM UTC every Monday
  - "0 3 * * 3" = 3:00 AM UTC every Wednesday
  - "0 4 1 * *" = 4:00 AM UTC on 1st of month
  
  Format: minute hour day month day_of_week
  - minute: 0-59
  - hour: 0-23 (UTC)
  - day: 1-31
  - month: 1-12
  - day_of_week: 0-6 (0 = Sunday)

TIMEZONE:
  All schedules use UTC to avoid DST issues.
  Adjust based on your race schedule needs.
"""
