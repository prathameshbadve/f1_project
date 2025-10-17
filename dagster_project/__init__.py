"""
F1 Race Prediction Data Pipeline
Dagster project for ingesting, processing, and preparing F1 data for ML models
"""

from dagster import Definitions

from dagster_project.resources import (
    storage_resource,
    database_resource,
    mlflow_resource,
    f1_api_resource,
)
from dagster_project.assets import (
    raw_race_results,
    raw_qualifying_results,
    raw_driver_standings,
    processed_race_data,
    ml_features,
)
from dagster_project.jobs import (
    daily_ingestion_job,
    weekly_processing_job,
    full_refresh_job,
    race_data_ingestion_job,
    ml_features_job,
    data_processing_job,
)
from dagster_project.schedules import (
    daily_ingestion_schedule,
    weekly_processing_schedule,
    monthly_refresh_schedule,
)
from dagster_project.sensors import (
    raw_data_sensor,
    race_weekend_sensor,
    processed_data_sensor,
    data_quality_sensor,
)


# Define all Dagster components
# This is the main entry point that Dagster uses to discover your code
defs = Definitions(
    assets=[
        # Ingestion assets - fetch raw data from F1 API
        raw_race_results,
        raw_qualifying_results,
        raw_driver_standings,
        # Processing assets - clean and structure data
        processed_race_data,
        # ML assets - generate features for model training
        ml_features,
    ],
    jobs=[
        # Scheduled jobs
        daily_ingestion_job,
        weekly_processing_job,
        full_refresh_job,
        # On-demand jobs
        race_data_ingestion_job,
        ml_features_job,
        data_processing_job,
    ],
    schedules=[
        # Time-based triggers
        daily_ingestion_schedule,
        weekly_processing_schedule,
        monthly_refresh_schedule,
    ],
    sensors=[
        # Event-based triggers
        raw_data_sensor,
        race_weekend_sensor,
        processed_data_sensor,
        data_quality_sensor,
    ],
    resources={
        # External system connections
        "storage": storage_resource,
        "database": database_resource,
        "mlflow": mlflow_resource,
        "f1_api": f1_api_resource,
    },
)
