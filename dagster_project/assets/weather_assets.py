"""
Weather Aggregation Asset

Dagster asset that creates aggregated weather features for each race session.
"""

import pandas as pd

from dagster import asset, AssetExecutionContext, Output, MetadataValue

from src.etl.aggregators.weather_aggregator import WeatherAggregator
from dagster_project.resources import CatalogConfig, StorageResource


@asset(
    name="aggregated_weather",
    description=(
        "Aggregated weather conditions for each race session. "
        "Includes temperature, humidity, rainfall, wind, and weather stability metrics."
    ),
    group_name="aggregation",
    compute_kind="python",
)
def aggregated_weather(
    context: AssetExecutionContext,
    validated_catalog: pd.DataFrame,
    storage_resource: StorageResource,
    catalog_config: CatalogConfig,
) -> Output[pd.DataFrame]:
    """
    Create aggregated weather dataset.

    Input:
        - validated_catalog: Catalog of all available sessions

    Output:
        - aggregated/weather_all.parquet: All weather features (~130-200 records)
        - aggregated/samples/weather_sample.csv: Sample for inspection

    Features per session:
        - Temperature: air/track temp avg, min, max, trends
        - Humidity: avg, min, max, stability
        - Pressure: avg, min, max
        - Rain: any rain, rain percentage, timing
        - Wind: speed, direction
        - Conditions: wet/dry, temp category, stability
    """

    context.log.info("=" * 80)
    context.log.info("Starting Weather Aggregation")
    context.log.info("=" * 80)

    # Create storage client
    storage_client = storage_resource.create_client()

    # Create aggregator
    aggregator = WeatherAggregator(
        storage_client=storage_client,
        raw_bucket=catalog_config.raw_bucket,
        processed_bucket=catalog_config.processed_bucket,
    )

    # Run aggregation (race and qualifying sessions)
    context.log.info("Running weather aggregation...")
    result = aggregator.aggregate(
        catalog_df=validated_catalog,
        sample_size=None,  # Process all sessions
        session_types=["Race"],
    )

    # Check for failures
    failure_rate = result.failure_rate
    context.log.info(f"Aggregation complete. Failure rate: {failure_rate:.1%}")

    if (
        failure_rate > 0.30
    ):  # Allow 30% failure rate (some sessions may lack weather data)
        context.log.warning(
            f"High failure rate: {failure_rate:.1%} "
            f"({result.items_skipped}/{result.items_processed + result.items_skipped} sessions)"
        )
        # Don't fail - weather data is optional for some use cases

    # Get the aggregated data
    weather_df = result.data

    if weather_df.empty:
        raise ValueError(
            "Weather aggregation produced no output! "
            f"All {result.items_skipped} sessions failed."
        )

    context.log.info(f"Generated {len(weather_df)} session weather records")

    # Save to processed bucket
    output_key = f"{catalog_config.aggregated_prefix}weather_all.parquet"

    context.log.info(f"Saving to: s3://{catalog_config.processed_bucket}/{output_key}")

    storage_client.upload_dataframe(
        df=weather_df,
        bucket_name=catalog_config.processed_bucket,
        object_key=output_key,
    )

    context.log.info("✅ Saved weather file")

    # Also save a sample CSV for easy inspection
    sample_key = f"{catalog_config.aggregated_prefix}samples/weather_sample.csv"
    sample_df = weather_df.head(100)

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
        "sessions_processed": result.items_processed,
        "sessions_skipped": result.items_skipped,
        "total_records": len(weather_df),
        "success_rate": result.success_rate,
        "failure_rate_pct": f"{failure_rate:.1%}",
        "output_file": f"s3://{catalog_config.processed_bucket}/{output_key}",
        # Weather statistics
        "avg_air_temp": weather_df["air_temp_avg"].mean(),
        "avg_track_temp": weather_df["track_temp_avg"].mean(),
        "avg_humidity": weather_df["humidity_avg"].mean(),
        "wet_sessions_pct": f"{(weather_df['is_wet_session'].sum() / len(weather_df) * 100):.1f}%",
        # Preview table
        "preview": MetadataValue.md(
            sample_df[
                [
                    "race_id",
                    "session_name",
                    "air_temp_avg",
                    "track_temp_avg",
                    "humidity_avg",
                    "any_rain",
                    "temp_category",
                ]
            ]
            .head(10)
            .to_markdown(index=False)
        ),
    }

    # Add session type breakdown
    session_counts = weather_df["session_name"].value_counts().to_dict()
    metadata["session_breakdown"] = MetadataValue.md(
        "### Sessions by Type\n"
        + "\n".join(
            [f"- {session}: {count}" for session, count in session_counts.items()]
        )
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

    if result.skipped_items:
        # Show which sessions were skipped
        skipped_sessions = [item["item_id"] for item in result.skipped_items[:20]]
        metadata["skipped_sessions"] = MetadataValue.md(
            "### Skipped Sessions\n"
            + "\n".join([f"- {session}" for session in skipped_sessions])
        )

    context.log.info("=" * 80)
    context.log.info("Weather Aggregation Complete")
    context.log.info("=" * 80)

    return Output(
        value=weather_df,
        metadata=metadata,
    )
