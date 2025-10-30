"""
F1 Race Prediction Data Pipeline
Dagster project for ingesting, processing, and preparing F1 data for ML models

This is the main entry point that Dagster uses to discover all pipeline components.
"""

# pylint: disable=wrong-import-position, wrong-import-order

from config.logging import setup_logging, get_logger

# Initialize logging when the module is loaded
setup_logging()

logger = get_logger("dagster_project")
logger.info("Dagster project loading...")

from dagster import Definitions  # noqa: E402

from dagster_project.assets import all_assets  # noqa: E402
from dagster_project.jobs import all_jobs  # noqa: E402
from dagster_project.resources import all_resources  # noqa: E402
from dagster_project.sensors import all_sensors  # noqa: E402

# Define all Dagster components
# This is what Dagster reads to understand your pipeline
defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    resources=all_resources,
    sensors=all_sensors,
)
