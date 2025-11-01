"""
Dagster assets for F1 data pipeline.

Assets are organized into modules:
- raw_ingestion: Individual race weekend assets (Italian GP 2024)
- season_ingestion: Season-level partitioned assets (2024 season) + configurable asset
- processed: Processed and cleaned data
- features: ML-ready features
"""

from dagster import load_assets_from_modules

from dagster_project.assets import (
    full_ingestion,
    circuits_ingestion,
    catalog_assets,
    results_asset,
    weather_assets,
    lap_features_asset,
    master_features_asset,
)


all_assets = load_assets_from_modules(
    modules=[
        full_ingestion,
        circuits_ingestion,
        catalog_assets,
        results_asset,
        weather_assets,
        lap_features_asset,
        master_features_asset,
    ]
)
