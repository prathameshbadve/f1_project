"""
Lap Features Aggregation Asset

Dagster asset that creates aggregated lap-level features for ML modeling.
"""

import pandas as pd

from dagster import asset, AssetExecutionContext, Output, MetadataValue

from src.etl.aggregators.lap_aggregator import LapFeaturesAggregator
from dagster_project.resources import CatalogConfig, StorageResource


@asset(
    name="aggregated_lap_features",
    description=(
        "Aggregated lap-level features for each driver in each race. "
        "Includes pace metrics, consistency, tire management, and position changes."
    ),
    group_name="aggregation",
    compute_kind="python",
)
def aggregated_lap_features(
    context: AssetExecutionContext,
    validated_catalog: pd.DataFrame,
    storage_resource: StorageResource,
    catalog_config: CatalogConfig,
) -> Output[pd.DataFrame]:
    """
    Create aggregated lap features dataset.

    Input:
        - validated_catalog: Catalog of all available race sessions

    Output:
        - aggregated/lap_features_all.parquet: All lap features (~1,320 records)
        - aggregated/samples/lap_features_sample.csv: Sample for inspection

    Features per driver per race:
        - Pace: avg/median/best lap times
        - Consistency: std, CV, IQR of lap times
        - Degradation: tire wear rate, stint analysis
        - Sectors: sector times and consistency
        - Position: overtakes, position changes
        - Tires: pit stops, stint lengths
    """

    context.log.info("=" * 80)
    context.log.info("Starting Lap Features Aggregation")
    context.log.info("=" * 80)

    # Create storage client
    storage_client = storage_resource.create_client()

    # Create aggregator
    aggregator = LapFeaturesAggregator(
        storage_client=storage_client,
        raw_bucket=catalog_config.raw_bucket,
        processed_bucket=catalog_config.processed_bucket,
    )

    # Run aggregation
    context.log.info("Running lap features aggregation...")
    result = aggregator.aggregate(
        catalog_df=validated_catalog,
        sample_size=None,  # Process all races
    )

    # Check for failures
    failure_rate = result.failure_rate
    context.log.info(f"Aggregation complete. Failure rate: {failure_rate:.1%}")

    if failure_rate > 0.20:  # Allow higher failure rate for lap features (20%)
        context.log.warning(
            f"High failure rate: {failure_rate:.1%} "
            f"({result.items_skipped}/{result.items_processed + result.items_skipped} races)"
        )
        # Don't fail - lap features are optional for some use cases

    # Get the aggregated data
    lap_features_df = result.data

    if lap_features_df.empty:
        raise ValueError(
            "Lap features aggregation produced no output! "
            f"All {result.items_skipped} races failed."
        )

    context.log.info(
        f"Generated {len(lap_features_df)} driver-race lap feature records"
    )

    # Save to processed bucket
    output_key = f"{catalog_config.aggregated_prefix}lap_features_all.parquet"

    context.log.info(f"Saving to: s3://{catalog_config.processed_bucket}/{output_key}")

    storage_client.upload_dataframe(
        df=lap_features_df,
        bucket_name=catalog_config.processed_bucket,
        object_key=output_key,
    )

    context.log.info("✅ Saved lap features file")

    # Also save a sample CSV for easy inspection
    sample_key = f"{catalog_config.aggregated_prefix}samples/lap_features_sample.csv"
    sample_df = lap_features_df.head(100)

    try:
        storage_client.upload_csv(
            df=sample_df,
            bucket_name=catalog_config.processed_bucket,
            object_key=sample_key,
        )
        context.log.info(f"✅ Saved sample CSV: {sample_key}")

    except Exception as e:  # pylint: disable=broad-except
        context.log.warning(f"Failed to save sample CSV: {e}")

    # Prepare metadata for Dagster UI
    metadata = {
        "races_processed": result.items_processed,
        "races_skipped": result.items_skipped,
        "total_records": len(lap_features_df),
        "success_rate": result.success_rate,
        "failure_rate_pct": f"{failure_rate:.1%}",
        "output_file": f"s3://{catalog_config.processed_bucket}/{output_key}",
        # Feature statistics
        "avg_laps_per_driver": lap_features_df["total_laps"].mean(),
        "avg_valid_lap_pct": (
            lap_features_df["valid_laps"] / lap_features_df["total_laps"]
        ).mean(),
        # Lap time statistics
        "avg_lap_time_mean": lap_features_df["avg_lap_time"].mean(),
        "avg_lap_time_std": lap_features_df["avg_lap_time"].std(),
        # Preview table
        "preview": MetadataValue.md(sample_df.head(10).to_markdown(index=False)),
    }

    # Add warnings and errors if any
    if result.warnings:
        metadata["warnings"] = MetadataValue.md(
            "### Warnings\n" + "\n".join([f"- {w}" for w in result.warnings[:10]])
        )

    if result.errors:
        metadata["errors"] = MetadataValue.md(
            "### Errors\n" + "\n".join([f"- {e}" for e in result.errors[:10]])
        )

    if result.skipped_items:
        # Show which races were skipped
        skipped_races = [item["item_id"] for item in result.skipped_items[:20]]
        metadata["skipped_races"] = MetadataValue.md(
            "### Skipped Races\n" + "\n".join([f"- {race}" for race in skipped_races])
        )

    context.log.info("=" * 80)
    context.log.info("Lap Features Aggregation Complete")
    context.log.info("=" * 80)

    return Output(
        value=lap_features_df,
        metadata=metadata,
    )
