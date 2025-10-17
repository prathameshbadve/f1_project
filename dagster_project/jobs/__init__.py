"""
Dagster jobs for F1 data pipeline
Jobs group assets together for execution
"""

from dagster import define_asset_job, AssetSelection


# Job to ingest raw data from F1 API
# This job runs all ingestion assets that fetch data from the F1 API
daily_ingestion_job = define_asset_job(
    name="daily_ingestion",
    description="Daily job to ingest latest F1 race data from API. Fetches race results, qualifying results, and driver standings.",
    selection=AssetSelection.groups("ingestion"),
    tags={
        "type": "ingestion",
        "frequency": "daily",
        "team": "data-engineering",
        "priority": "high",
    },
)


# Job to process raw data and generate features
# This job runs all processing and ML feature generation assets
weekly_processing_job = define_asset_job(
    name="weekly_processing",
    description="Weekly job to process raw data and generate ML features. Cleans race data and creates feature sets for model training.",
    selection=AssetSelection.groups("processing", "ml"),
    tags={
        "type": "processing",
        "frequency": "weekly",
        "team": "data-engineering",
        "priority": "medium",
    },
)


# Job for full data refresh (useful for development and backfills)
# This job runs ALL assets in the correct dependency order
full_refresh_job = define_asset_job(
    name="full_refresh",
    description="Full refresh of all data assets (ingestion + processing + ML features). Use this for backfills or complete data refresh.",
    selection=AssetSelection.all(),
    tags={
        "type": "refresh",
        "frequency": "on-demand",
        "team": "data-engineering",
        "priority": "low",
    },
)


# Job for ingestion only (specific assets)
# Runs only race results and qualifying ingestion
race_data_ingestion_job = define_asset_job(
    name="race_data_ingestion",
    description="Ingest only race results and qualifying data. Useful for quick race weekend updates.",
    selection=AssetSelection.assets(
        "raw_race_results",
        "raw_qualifying_results",
    ),
    tags={
        "type": "ingestion",
        "frequency": "on-demand",
        "team": "data-engineering",
        "priority": "high",
        "scope": "race-only",
    },
)


# Job for ML feature generation only
# Runs only the ML feature assets (assumes raw data already exists)
ml_features_job = define_asset_job(
    name="ml_features_generation",
    description="Generate ML features from existing processed data. Skips ingestion and processing steps.",
    selection=AssetSelection.groups("ml"),
    tags={
        "type": "ml",
        "frequency": "on-demand",
        "team": "ml-engineering",
        "priority": "medium",
    },
)


# Job for processing only
# Runs only the processing assets (assumes raw data already exists)
data_processing_job = define_asset_job(
    name="data_processing",
    description="Process raw data into structured format. Assumes raw data already exists in storage.",
    selection=AssetSelection.groups("processing"),
    tags={
        "type": "processing",
        "frequency": "on-demand",
        "team": "data-engineering",
        "priority": "medium",
    },
)
