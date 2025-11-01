"""
Race Results Aggregation Asset

Dagster asset that creates aggregated race and qualifying results features for ML modeling.
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

from src.etl.aggregators.results_aggregator import RaceResultsAggregator
from dagster_project.resources import CatalogConfig, StorageResource


@asset(
    name="aggregated_results",
    key_prefix=["aggregation"],
    group_name="aggregation",
    compute_kind="python",
    description=(
        "Aggregated race and qualifying results features for each driver in each race. "
        "Includes grid positions, finishing positions, points, valid & invalid laps, etc."
    ),
    ins={
        "validated_catalog": AssetIn(
            key=AssetKey(["catalog", "validated_catalog"]),
        )
    },
)
def aggregated_results_features(
    context: AssetExecutionContext,
    validated_catalog: pd.DataFrame,
    storage_resource: StorageResource,
    catalog_config: CatalogConfig,
) -> Output[pd.DataFrame]:
    """
    Aggregate all race results with qualifying data into single dataset.

    This asset:
    - Processes all complete race sessions from catalog
    - Loads race results and qualifying results for each race
    - Merges race and qualifying data on driver
    - Adds derived features (position_gain, finished_top10)
    - Validates data quality
    - Saves to processed bucket
    - Creates sample CSV for inspection

    Output: DataFrame with ~1,320 rows (66 races × 20 drivers)

    Fails if: >10% of races fail to process
    """

    context.log.info("=" * 80)
    context.log.info("Aggregating Race Results")
    context.log.info("=" * 80)

    # Initialize aggregator
    storage_client = storage_resource.create_client()

    aggregator = RaceResultsAggregator(
        storage_client=storage_client,
        raw_bucket=catalog_config.raw_bucket,
        logger_name="dagster.race_results_aggregator",
    )

    # Run aggregation
    result = aggregator.aggregate(
        catalog_df=validated_catalog,
        sample_size=None,  # Process all races
    )

    # Check failure threshold
    total_races = result.items_processed + result.items_skipped
    failure_rate = result.items_skipped / total_races if total_races > 0 else 0

    context.log.info("Aggregation Summary:")
    context.log.info(f"  Races processed: {result.items_processed}/{total_races}")
    context.log.info(f"  Races skipped: {result.items_skipped}")
    context.log.info(f"  Failure rate: {failure_rate:.1%}")
    context.log.info(f"  Output records: {len(result.data)}")
    context.log.info(f"  Warnings: {len(result.warnings)}")
    context.log.info(f"  Errors: {len(result.errors)}")

    # Fail if >10% races failed
    if failure_rate > 0.10:
        error_msg = (
            f"FAILED: {failure_rate:.1%} of races failed to process "
            f"(threshold: 10%). Skipped races: {result.skipped_items}"
        )
        context.log.error(error_msg)
        raise ValueError(error_msg)

    if result.data.empty:
        raise ValueError("No data produced - all races failed to process!")

    # Save to processed bucket
    output_key = "aggregated/race_results_all.parquet"

    try:
        storage_client.upload_dataframe(
            df=result.data,
            bucket_name=catalog_config.processed_bucket,
            object_key=output_key,
        )
        context.log.info(
            f"✅ Saved parquet: s3://{catalog_config.processed_bucket}/{output_key}"
        )

    except Exception as e:
        context.log.error(f"Failed to save parquet: {e}")
        raise

    # Save sample CSV for inspection (first 100 rows)
    try:
        sample_df = result.data.head(100)
        csv_key = "aggregated/samples/race_results_sample.csv"

        storage_client.upload_csv(
            df=sample_df,
            bucket_name=catalog_config.processed_bucket,
            object_key=csv_key,
        )
        context.log.info(
            f"✅ Saved sample CSV: s3://{catalog_config.processed_bucket}/{csv_key}"
        )

    except Exception as e:  # pylint: disable=broad-except
        context.log.warning(f"Failed to save sample CSV: {e}")

    # Calculate statistics for metadata
    stats = _calculate_statistics(result.data)

    # Create data preview
    preview_df = result.data[
        [
            "race_id",
            "driver_abbreviation",
            "team_name",
            "grid_position",
            "finish_position",
            "position_gain",
            "points",
            "finished_top10",
        ]
    ].head(20)

    # Prepare metadata
    metadata = {
        # Processing stats
        "races_processed": result.items_processed,
        "races_skipped": result.items_skipped,
        "failure_rate_pct": round(failure_rate * 100, 2),
        # Output stats
        "total_records": len(result.data),
        "total_drivers": result.data["driver_id"].nunique(),
        "total_teams": result.data["team_id"].nunique(),
        "years": sorted(result.data["year"].unique().tolist()),
        # Data quality
        "num_warnings": len(result.warnings),
        "num_errors": len(result.errors),
        "null_finish_positions": int(result.data["finish_position"].isnull().sum()),
        "null_grid_positions": int(result.data["grid_position"].isnull().sum()),
        "records_with_quali": int((~result.data["q1"].isnull()).sum()),
        # Statistics
        "avg_grid_position": round(stats["avg_grid"], 2),
        "avg_finish_position": round(stats["avg_finish"], 2),
        "avg_points_per_race": round(stats["avg_points"], 2),
        "dnf_rate_pct": round(stats["dnf_rate"] * 100, 2),
        "top10_rate_pct": round(stats["top10_rate"] * 100, 2),
        # Files
        "output_path": f"s3://{catalog_config.processed_bucket}/{output_key}",
        "sample_csv_path": f"s3://{catalog_config.processed_bucket}/{csv_key}",
        # Preview
        "preview": MetadataValue.md(preview_df.to_markdown(index=False)),
    }

    # Add warnings and errors to metadata
    if result.warnings:
        metadata["warnings"] = MetadataValue.json(result.warnings[:10])  # First 10

    if result.errors:
        metadata["errors"] = MetadataValue.json(result.errors[:10])

    if result.skipped_items:
        metadata["skipped_races"] = MetadataValue.json(result.skipped_items)

    # Log summary
    context.log.info("\n" + "=" * 80)
    context.log.info("✅ Race Results Aggregation Complete")
    context.log.info("=" * 80)
    context.log.info(f"Total records: {len(result.data)}")
    context.log.info(f"Output saved to: {output_key}")

    if result.warnings:
        context.log.warning(f"\n⚠️  {len(result.warnings)} warnings (see metadata)")

    return Output(value=result.data, metadata=metadata)


def _calculate_statistics(df: pd.DataFrame) -> dict:
    """Calculate summary statistics for metadata"""

    stats = {
        "avg_grid": df["grid_position"].mean(),
        "avg_finish": df["finish_position"].mean(),
        "avg_points": df["points"].mean(),
        "dnf_rate": df["dnf"].mean() if "dnf" in df.columns else 0,
        "top10_rate": df["finished_top10"].mean()
        if "finished_top10" in df.columns
        else 0,
    }

    # Handle NaN values
    for key, value in stats.items():
        if pd.isna(value):
            stats[key] = 0.0

    return stats


# Future assets (placeholders for now)


# @asset(
#     name="aggregated_race_metadata",
#     key_prefix=["aggregated"],
#     group_name="aggregation",
#     description="Race-level metadata (weather, incidents, circuit info)",
#     deps=[AssetKey(["catalog", "validated_catalog"])],
# )
# def aggregated_race_metadata(...):
#     """TODO: Implement race metadata aggregation"""
#     pass
