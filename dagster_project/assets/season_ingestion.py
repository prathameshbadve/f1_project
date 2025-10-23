"""
Season-level ingestion assets using partitions.

This module uses Dagster partitions to handle entire seasons efficiently.
Instead of creating 100+ individual assets, we create one partitioned asset
that can handle any session based on the partition key.
"""

from typing import Dict, Any

from dagster import asset, AssetExecutionContext, Output, MetadataValue
import pandas as pd

from dagster_project.partitions import f1_2024_sessions_partitions
from src.data_ingestion.session_data_loader import SessionLoader
from src.data_ingestion.storage_client import StorageClient

from config.logging import setup_logging, get_logger


# ============================================================================
# LOGGING SETUP
# ============================================================================

setup_logging()
dagster_logger = get_logger("data_ingestion.dagster")


@asset(
    partitions_def=f1_2024_sessions_partitions,
    group_name="raw_2024_season",
    compute_kind="fastf1",
    description="Raw F1 session data for 2024 season - partitioned by event and session type",
)
def f1_2024_session_raw(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Ingest a single F1 session for 2024 season.

    This asset is partitioned, meaning Dagster can materialize many sessions
    in parallel or individually based on the partition key.

    Partition key format: "event_name|session_type"
    Example: "Bahrain Grand Prix|R" or "Monaco Grand Prix|Q"
    """

    # Parse partition key
    partition_key = context.partition_key
    event_name, session_type = partition_key.split("|")
    year = 2024

    context.log.info(f"Starting ingestion: {year} {event_name} {session_type}")
    context.log.info(f"Partition key: {partition_key}")

    dagster_logger.info("Starting ingestion: %d %s %s", year, event_name, session_type)
    dagster_logger.info("Partition key: %s", partition_key)

    # Initialize loader
    session_loader = SessionLoader()

    try:
        # Load session data
        context.log.info("Fetching session data from FastF1...")
        dagster_logger.info("Fetching session data from FastF1...")

        session_data = session_loader.load_session_data(
            year=year,
            event_name=event_name,
            session_type=session_type,
            save_to_storage=True,
            force_refresh=False,
        )

        context.log.info("Session data loaded successfully!")
        dagster_logger.info("Session data loaded successfully!")

        # Log which data types were loaded
        for data_type, data in session_data.items():
            if data is not None:
                if isinstance(data, pd.DataFrame):
                    context.log.info(f"  ✅ {data_type}: {len(data)} rows")
                    dagster_logger.info("  ✅ %s: %d rows", data_type, len(data))
                else:
                    context.log.info(f"  ✅ {data_type}: loaded")
                    dagster_logger.info("  ✅ %s: loaded", data_type)
            else:
                context.log.warning(f"  ❌ {data_type}: NOT loaded")
                dagster_logger.warning("  ❌ %s: NOT loaded", data_type)

        # Extract metadata
        session_info = session_data.get("session_info")
        results = session_data.get("results")
        laps = session_data.get("laps")
        weather = session_data.get("weather")
        race_control_messages = session_data.get("race_control_messages")

        # Session date
        session_date = None
        if session_info is not None and len(session_info) > 0:
            session_date = (
                str(session_info["session_date"].iloc[0])
                if "session_date" in session_info.columns
                else "Unknown"
            )

        # Get total laps
        total_laps = 0
        if (
            session_info is not None
            and len(session_info) > 0
            and "total_laps" in session_info.columns
            and session_info["total_laps"].iloc[0] is not None
        ):
            total_laps = int(session_info["total_laps"].iloc[0])

        # Winner (for Race) or Pole Position (for Qualifying)
        winner_or_pole = None
        if results is not None and len(results) > 0:
            winner_or_pole = (
                results["Abbreviation"].iloc[0]
                if "Abbreviation" in results.columns
                else "Unknown"
            )

        # Build metadata
        metadata = {
            "partition_key": partition_key,
            "year": year,
            "event": event_name,
            "session_type": session_type,
            "session_date": session_date or "Unknown",
            "winner_or_pole": winner_or_pole or "Unknown",
            "num_drivers": len(results) if results is not None else 0,
            "total_laps": total_laps,
            "num_lap_records": len(laps) if laps is not None else 0,
            "num_weather_records": len(weather) if weather is not None else 0,
            "num_race_control_messages": len(race_control_messages)
            if race_control_messages is not None
            else 0,
            "data_types_loaded": [k for k, v in session_data.items() if v is not None],
            "data_types_failed": [k for k, v in session_data.items() if v is None],
        }

        # Add results preview for Race and Qualifying
        if results is not None and len(results) > 0 and session_type in ["R", "Q"]:
            preview_cols = ["Position", "Abbreviation", "TeamName"]
            if "Time" in results.columns:
                preview_cols.append("Time")
            results_preview = results[preview_cols].head(5)
            metadata["results_preview"] = MetadataValue.md(
                results_preview.to_markdown(index=False)
            )

        context.log.info(f"✅ Ingestion complete: {year} {event_name} {session_type}")
        dagster_logger.info(
            "✅ Ingestion complete: %d %s %s", year, event_name, session_type
        )

        return Output(
            value={
                "year": year,
                "event": event_name,
                "session_type": session_type,
                "partition_key": partition_key,
                "success": True,
                "num_drivers": len(results) if results is not None else 0,
                "total_laps": total_laps,
                "data_types_loaded": metadata["data_types_loaded"],
            },
            metadata=metadata,
        )

    except Exception as e:  # pylint: disable=broad-except
        context.log.error(
            f"❌ Failed to ingest {year} {event_name} {session_type}: {str(e)}"
        )
        dagster_logger.error(
            "❌ Failed to ingest %d %s %s: %s", year, event_name, session_type, str(e)
        )

        return Output(
            value={
                "year": year,
                "event": event_name,
                "session_type": session_type,
                "partition_key": partition_key,
                "success": False,
                "error": str(e),
            },
            metadata={
                "partition_key": partition_key,
                "year": year,
                "event": event_name,
                "session_type": session_type,
                "status": "failed",
                "error_message": str(e),
            },
        )


# ============================================================================
# SEASON SUMMARY ASSET (depends on all session partitions)
# ============================================================================


@asset(
    group_name="raw_2024_season",
    compute_kind="python",
    description="Summary statistics for 2024 season ingestion",
    deps=[f1_2024_session_raw],
)
def f1_2024_season_summary(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Generate a summary of the 2024 season ingestion.

    This asset depends on all partitions of f1_2024_session_raw,
    so it only runs after all sessions are materialized.
    """

    context.log.info("Generating 2024 season summary...")
    dagster_logger.info("Generating 2024 season summary...")

    storage_client = StorageClient()
    year = 2024

    # Count files in storage
    try:
        # List all objects in the 2024/ prefix
        objects = storage_client.client.list_objects(
            storage_client.config.raw_bucket_name,
            prefix=f"{year}/",
            recursive=True,
        )

        total_files = 0
        events_with_data = set()
        session_types = set()

        for obj in objects:
            total_files += 1
            # Parse path: 2024/event_name/session_type/data_type.parquet
            parts = obj.object_name.split("/")
            if len(parts) >= 3:
                events_with_data.add(parts[1])
                session_types.add(parts[2])

        metadata = {
            "year": year,
            "total_files": total_files,
            "num_events": len(events_with_data),
            "num_session_types": len(session_types),
            "events": ", ".join(sorted(events_with_data)),
            "session_types": ", ".join(sorted(session_types)),
        }

        context.log.info("✅ Season summary complete:")
        context.log.info(f"   Total files: {total_files}")
        context.log.info(f"   Events with data: {len(events_with_data)}")
        context.log.info(f"   Session types: {len(session_types)}")

        dagster_logger.info("✅ Season summary complete:")
        dagster_logger.info("   Total files: %d", total_files)
        dagster_logger.info("   Events with data: %d", len(events_with_data))
        dagster_logger.info("   Session types: %d", len(session_types))

        return Output(
            value={
                "year": year,
                "total_files": total_files,
                "num_events": len(events_with_data),
                "events_with_data": sorted(events_with_data),
            },
            metadata=metadata,
        )

    except Exception as e:  # pylint: disable=broad-except
        context.log.error(f"Failed to generate summary: {str(e)}")
        dagster_logger.error("Failed to generate summary: %s", str(e))

        return Output(
            value={
                "year": year,
                "success": False,
                "error": str(e),
            },
            metadata={
                "year": year,
                "status": "failed",
                "error_message": str(e),
            },
        )
