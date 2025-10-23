"""
F1 Race Prediction Data Pipeline
Dagster project for ingesting, processing, and preparing F1 data for ML models

This is the main entry point that Dagster uses to discover all pipeline components.
"""

from dagster import Definitions, load_assets_from_modules

from dagster_project import assets
from dagster_project.resources import storage_resource
from dagster_project.jobs import (
    italian_gp_2024_weekend_job,
    f1_2024_season_all_sessions_job,
    f1_configurable_session_job,
)


# Load all assets from the assets module
# This automatically discovers all @asset decorated functions
all_assets = load_assets_from_modules([assets])


# Define all Dagster components
# This is what Dagster reads to understand your pipeline
defs = Definitions(
    assets=all_assets,
    jobs=[
        # Race weekend jobs
        italian_gp_2024_weekend_job,
        # Season job (materialize all partitions or select specific ones in UI)
        f1_2024_season_all_sessions_job,
        # Configurable job (pass year/event/session at runtime)
        f1_configurable_session_job,
    ],
    resources={
        # External system connections
        "storage": storage_resource,
    },
)
