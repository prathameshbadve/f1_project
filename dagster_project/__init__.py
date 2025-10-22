"""
F1 Race Prediction Data Pipeline
Dagster project for ingesting, processing, and preparing F1 data for ML models

This is the main entry point that Dagster uses to discover all pipeline components.
"""

from dagster import Definitions, load_assets_from_modules

from dagster_project import assets
from dagster_project.resources import (
    storage_resource,
    database_resource,
    mlflow_resource,
)
from dagster_project.jobs import (
    race_weekend_ingestion_job,
    season_ingestion_job,
    historical_backfill_job,
    data_processing_job,
    ml_features_job,
    full_pipeline_job,
    weekly_batch_job,
)
from dagster_project.schedules import (
    race_weekend_schedule,
    weekly_processing_schedule,
    monthly_full_pipeline_schedule,
)
from dagster_project.sensors import (
    raw_data_sensor,
    race_weekend_sensor,
    # processed_data_sensor,
    data_quality_sensor,
)


# Load all assets from the assets module
# This automatically discovers all @asset decorated functions
all_assets = load_assets_from_modules([assets])


# Define all Dagster components
# This is what Dagster reads to understand your pipeline
defs = Definitions(
    assets=all_assets,
    jobs=[
        # Ingestion jobs
        race_weekend_ingestion_job,
        season_ingestion_job,
        historical_backfill_job,
        # Processing jobs
        data_processing_job,
        ml_features_job,
        # Combined jobs
        full_pipeline_job,
        weekly_batch_job,
    ],
    schedules=[
        # Time-based triggers
        race_weekend_schedule,
        weekly_processing_schedule,
        monthly_full_pipeline_schedule,
    ],
    sensors=[
        # Event-based triggers
        raw_data_sensor,
        race_weekend_sensor,
        # processed_data_sensor,
        data_quality_sensor,
    ],
    resources={
        # External system connections
        "storage": storage_resource,
        "database": database_resource,
        "mlflow": mlflow_resource,
    },
)
