"""
Master Features Asset

Dagster asset that creates the master ML-ready dataset by combining:
- Race results
- Qualifying data
- Lap features
- Weather features
- Circuit characteristics
- Historical rolling features
"""

import pandas as pd

from dagster import (
    asset,
    AssetExecutionContext,
    Output,
    MetadataValue,
    AssetIn,
    AssetKey,
)

from src.etl.aggregators.master_features_aggreagator import MasterFeaturesAggregator
from dagster_project.resources import CatalogConfig, StorageResource


@asset(
    name="master_features_dataset",
    description=(
        "Master ML-ready dataset combining all feature sources. "
        "Target variable: top_10_finish (binary classification). "
        "Includes qualifying, lap, weather, circuit, team, and historical features."
    ),
    group_name="ml_features",
    compute_kind="python",
    ins={
        "validated_catalog": AssetIn(key=AssetKey(["catalog", "validated_catalog"])),
    },
)
def master_features_dataset(
    context: AssetExecutionContext,
    validated_catalog: pd.DataFrame,
    storage_resource: StorageResource,
    catalog_config: CatalogConfig,
) -> Output[pd.DataFrame]:
    """
    Create master features dataset for ML modeling.

    Combines:
    1. Race results (base + target)
    2. Qualifying features (Q times, gaps, grid position)
    3. Lap features (pace, consistency, tire management)
    4. Weather features (track conditions)
    5. Circuit features (track type, characteristics)
    6. Team features (teammate comparisons)
    7. Historical features (rolling averages over last 5 races)

    Output:
        - ml/master_features.parquet: Complete ML dataset
        - ml/samples/master_features_sample.csv: Sample for inspection
        - ml/feature_list.txt: List of all features
    """

    context.log.info("=" * 80)
    context.log.info("Starting Master Features Dataset Creation")
    context.log.info("=" * 80)

    # Create storage client
    storage_client = storage_resource.create_client()

    # Create aggregator
    aggregator = MasterFeaturesAggregator(
        storage_client=storage_client,
        raw_bucket=catalog_config.raw_bucket,
        processed_bucket=catalog_config.processed_bucket,
    )

    # Run aggregation
    context.log.info("Running master features aggregation...")
    result = aggregator.aggregate(
        catalog_df=validated_catalog,
        sample_size=None,  # Process all races
    )

    # Get the aggregated data
    master_df = result.data

    if master_df.empty:
        raise ValueError(
            f"Master features aggregation produced no output! Errors: {result.errors}"
        )

    context.log.info(f"Generated master dataset with {len(master_df)} records")

    # Save to processed bucket
    output_key = "ml/master_features.parquet"

    context.log.info(f"Saving to: s3://{catalog_config.processed_bucket}/{output_key}")

    storage_client.upload_dataframe(
        df=master_df,
        bucket_name=catalog_config.processed_bucket,
        object_key=output_key,
    )

    context.log.info("✅ Saved master features file")

    # Save sample CSV for inspection
    sample_key = "ml/samples/master_features_sample.csv"
    sample_df = master_df.head(100)

    try:
        storage_client.upload_csv(
            df=sample_df,
            bucket_name=catalog_config.processed_bucket,
            object_key=sample_key,
        )
        context.log.info(f"✅ Saved sample CSV: {sample_key}")

    except Exception as e:  # pylint: disable=broad-except
        context.log.warning(f"Failed to save sample CSV: {e}")

    # Save feature list
    feature_list_key = "ml/feature_list.txt"
    feature_list_content = "\n".join(
        [
            "=" * 80,
            "MASTER FEATURES LIST",
            "=" * 80,
            "",
            "IDENTIFIERS:",
            *[
                f"  - {col}"
                for col in master_df.columns
                if col
                in [
                    "race_id",
                    "year",
                    "round",
                    "event_name",
                    "circuit",
                    "race_date",
                    "driver_number",
                    "driver_id",
                    "driver_name",
                    "team_id",
                    "team_name",
                ]
            ],
            "",
            "TARGET VARIABLE:",
            "  - top_10_finish (0/1)",
            "",
            "RACE OUTCOME FEATURES:",
            *[
                f"  - {col}"
                for col in master_df.columns
                if col
                in ["position", "grid_position", "points", "status", "position_gain"]
            ],
            "",
            "QUALIFYING FEATURES:",
            *[
                f"  - {col}"
                for col in master_df.columns
                if "q_" in col.lower() or col in ["Q1", "Q2", "Q3"]
            ],
            "",
            "LAP PERFORMANCE FEATURES:",
            *[
                f"  - {col}"
                for col in master_df.columns
                if any(
                    x in col
                    for x in ["lap_time", "pace", "consistency", "tire", "stint", "pit"]
                )
            ],
            "",
            "WEATHER FEATURES:",
            *[
                f"  - {col}"
                for col in master_df.columns
                if any(
                    x in col
                    for x in ["temp", "humidity", "rainfall", "weather", "grip"]
                )
            ],
            "",
            "CIRCUIT FEATURES:",
            *[
                f"  - {col}"
                for col in master_df.columns
                if "circuit" in col or "technical" in col
            ],
            "",
            "TEAM FEATURES:",
            *[
                f"  - {col}"
                for col in master_df.columns
                if "team" in col or "teammate" in col
            ],
            "",
            "HISTORICAL FEATURES (Rolling 5 races):",
            *[
                f"  - {col}"
                for col in master_df.columns
                if "last_5" in col or "prev_" in col
            ],
            "",
            f"TOTAL FEATURES: {len(master_df.columns)}",
            "=" * 80,
        ]
    )

    try:
        storage_client.client.put_object(
            bucket_name=catalog_config.processed_bucket,
            object_name=feature_list_key,
            data=feature_list_content.encode("utf-8"),
            length=len(feature_list_content.encode("utf-8")),
        )
        context.log.info(f"✅ Saved feature list: {feature_list_key}")
    except Exception as e:  # pylint: disable=broad-except
        context.log.warning(f"Failed to save feature list: {e}")

    # Compute statistics for metadata
    total_records = len(master_df)
    total_features = len(master_df.columns)

    # Target variable distribution
    top_10_count = master_df["top_10_finish"].sum()
    top_10_rate = top_10_count / total_records if total_records > 0 else 0

    # Date range
    min_date = master_df["race_date"].min()
    max_date = master_df["race_date"].max()

    # Number of unique races and drivers
    n_races = master_df["race_id"].nunique()
    n_drivers = master_df["driver_number"].nunique()

    # Missing value summary
    missing_pct = (master_df.isnull().sum() / len(master_df) * 100).round(2)
    high_missing = missing_pct[missing_pct > 30].sort_values(ascending=False)

    # Prepare metadata for Dagster UI
    metadata = {
        "total_records": total_records,
        "total_features": total_features,
        "unique_races": n_races,
        "unique_drivers": n_drivers,
        "date_range": f"{min_date.date()} to {max_date.date()}",
        "top_10_finish_rate": f"{top_10_rate:.1%}",
        "top_10_count": int(top_10_count),
        "output_file": f"s3://{catalog_config.processed_bucket}/{output_key}",
        # Preview table
        "preview": MetadataValue.md(sample_df.head(10).to_markdown(index=False)),
    }

    # Add feature categories to metadata
    feature_categories = {
        "identifiers": len(
            [
                c
                for c in master_df.columns
                if c
                in [
                    "race_id",
                    "year",
                    "round",
                    "event_name",
                    "circuit",
                    "race_date",
                    "driver_number",
                    "driver_id",
                    "driver_name",
                    "team_id",
                    "team_name",
                ]
            ]
        ),
        "qualifying_features": len(
            [
                c
                for c in master_df.columns
                if "q_" in c.lower() or c in ["Q1", "Q2", "Q3"]
            ]
        ),
        "lap_features": len(
            [
                c
                for c in master_df.columns
                if any(
                    x in c
                    for x in ["lap_time", "pace", "consistency", "tire", "stint", "pit"]
                )
            ]
        ),
        "weather_features": len(
            [
                c
                for c in master_df.columns
                if any(
                    x in c for x in ["temp", "humidity", "rainfall", "weather", "grip"]
                )
            ]
        ),
        "circuit_features": len(
            [c for c in master_df.columns if "circuit" in c or "technical" in c]
        ),
        "team_features": len(
            [c for c in master_df.columns if "team" in c or "teammate" in c]
        ),
        "historical_features": len(
            [c for c in master_df.columns if "last_5" in c or "prev_" in c]
        ),
    }

    metadata["feature_breakdown"] = MetadataValue.md(
        "### Feature Categories\n"
        + "\n".join(
            [
                f"- **{k.replace('_', ' ').title()}**: {v}"
                for k, v in feature_categories.items()
            ]
        )
    )

    # Add high missing value columns to metadata if any
    if len(high_missing) > 0:
        metadata["high_missing_features"] = MetadataValue.md(
            "### Features with >30% Missing Values\n"
            + "\n".join([f"- {col}: {pct:.1f}%" for col, pct in high_missing.items()])
        )

    # Add warnings and errors if any
    if result.warnings:
        metadata["warnings"] = MetadataValue.md(
            "### Warnings\n" + "\n".join([f"- {w}" for w in result.warnings[:10]])
        )

    if result.errors:
        metadata["errors"] = MetadataValue.md(
            "### Errors\n" + "\n".join([f"- {e}" for e in result.errors[:10]])
        )

    context.log.info("=" * 80)
    context.log.info("Master Features Dataset Creation Complete")
    context.log.info(f"  Total Records: {total_records:,}")
    context.log.info(f"  Total Features: {total_features}")
    context.log.info("  Target Variable: top_10_finish")
    context.log.info(f"  Class Distribution: {top_10_rate:.1%} top 10 finishes")
    context.log.info("=" * 80)

    return Output(
        value=master_df,
        metadata=metadata,
    )
