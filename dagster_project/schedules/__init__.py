"""
Dagster schedules for F1 data pipeline
Schedules trigger jobs at specific times
"""

from dagster import ScheduleDefinition, DefaultScheduleStatus

from dagster_project.jobs import (
    daily_ingestion_job,
    weekly_processing_job,
    full_refresh_job,
)
from config.settings import is_development


# Daily ingestion schedule
# In local: disabled by default (run manually)
# In production: runs daily at 2 AM UTC
daily_ingestion_schedule = ScheduleDefinition(
    name="daily_ingestion_schedule",
    job=daily_ingestion_job,
    cron_schedule="0 2 * * *",  # 2 AM UTC every day
    default_status=DefaultScheduleStatus.STOPPED
    if is_development()
    else DefaultScheduleStatus.RUNNING,
    description="Ingest latest F1 data daily after races. Runs at 2 AM UTC.",
    tags={
        "team": "data-engineering",
        "priority": "high",
        "type": "ingestion",
    },
)


# Weekly processing schedule
# In local: disabled by default (run manually)
# In production: runs weekly on Monday at 3 AM UTC
weekly_processing_schedule = ScheduleDefinition(
    name="weekly_processing_schedule",
    job=weekly_processing_job,
    cron_schedule="0 3 * * 1",  # 3 AM UTC every Monday
    default_status=DefaultScheduleStatus.STOPPED
    if is_development()
    else DefaultScheduleStatus.RUNNING,
    description="Process raw data and generate ML features weekly. Runs Monday at 3 AM UTC.",
    tags={
        "team": "data-engineering",
        "priority": "medium",
        "type": "processing",
    },
)


# Monthly full refresh schedule
# In local: disabled by default (run manually only)
# In production: runs on the 1st of each month at 4 AM UTC
monthly_refresh_schedule = ScheduleDefinition(
    name="monthly_refresh_schedule",
    job=full_refresh_job,
    cron_schedule="0 4 1 * *",  # 4 AM UTC on the 1st of every month
    default_status=DefaultScheduleStatus.STOPPED,  # Always manual trigger
    description="Full data refresh including all historical data. Runs monthly on the 1st at 4 AM UTC. Manual trigger recommended.",
    tags={
        "team": "data-engineering",
        "priority": "low",
        "type": "maintenance",
    },
)
