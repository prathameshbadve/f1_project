"""
Raw data ingestion assets for F1 sessions.

Each asset represents one F1 session (e.g., Italian GP 2024 Race).
These assets fetch data from FastF1 API and store it in MinIO/S3.
"""

from typing import Dict, Any

from dagster import asset, AssetExecutionContext, Output, MetadataValue
import pandas as pd

from src.data_ingestion.session_data_loader import SessionLoader
# from src.data_ingestion.storage_client import StorageClient
# from src.data_ingestion.schedule_loader import ScheduleLoader

from config.logging import setup_logging, get_logger


# ============================================================================
# LOGGING SETUP
# ============================================================================

setup_logging()
dagster_logger = get_logger("data_ingestion.dagster")


# ============================================================================
# HELPER FUNCTION
# ============================================================================

"""
Ingest Italian Grand Prix 2024 Race session.

Fetches from FastF1 API:
- Session info (event details, date, etc.)
- Race results (finishing positions, times, etc.)
- Lap data (lap times, sectors, pit stops)
- Weather data (track temp, air temp, etc.)
- Race control messages
- Session status
- Track status

Stores as Parquet files in MinIO/S3 at:
- 2024/italian_grand_prix/R/session_info.parquet
- 2024/italian_grand_prix/R/results.parquet
- 2024/italian_grand_prix/R/laps.parquet
- 2024/italian_grand_prix/R/weather.parquet
- 2024/italian_grand_prix/R/race_control_messages.parquet
- 2024/italian_grand_prix/R/session_status.parquet
- 2024/italian_grand_prix/R/track_status.parquet
"""


def ingest_session_asset(
    context: AssetExecutionContext,
    year: int,
    event_name: str,
    session_type: str,
) -> Output[Dict[str, Any]]:
    """
    Generic function to ingest a single F1 session.

    This is reused by all session assets to avoid code duplication.
    """

    context.log.info(f"Starting ingestion: {year} {event_name} {session_type}")
    dagster_logger.info("Starting ingestion: %d %s %s", year, event_name, session_type)

    # Initialize loaders
    session_loader = SessionLoader()

    try:
        # Load session data (this fetches from FastF1 API or cache)
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
        context.log.info("Data types loaded:")
        dagster_logger.info("Data types loaded:")
        for data_type, data in session_data.items():
            if data is not None:
                if isinstance(data, pd.DataFrame):
                    context.log.info(f"  ✅ {data_type}: {len(data)} rows")
                    dagster_logger.info("  ✅ %s: %d rows", data_type, len(data))
                else:
                    context.log.info(f"  ✅ {data_type}: loaded")
                    dagster_logger.info("  ✅ %s: loaded", data_type)
            else:
                context.log.warning(f"  ❌ {data_type}: NOT loaded (None)")
                dagster_logger.warning("  ❌ %s: NOT loaded (None)", data_type)

        # Extract metadata
        session_info = session_data.get("session_info")
        results = session_data.get("results")
        laps = session_data.get("laps")

        # Get session date
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

        # Build metadata
        metadata = {
            "event": f"{year} {event_name}",
            "session_type": session_type,
            "session_date": session_date or "Unknown",
            "num_drivers": len(results) if results is not None else 0,
            "total_laps": total_laps,
            "num_lap_records": len(laps) if laps is not None else 0,
            "data_types_loaded": [k for k, v in session_data.items() if v is not None],
            "data_types_failed": [k for k, v in session_data.items() if v is None],
        }

        # Add results preview for Race and Qualifying
        if results is not None and len(results) > 0 and session_type in ["R", "Q"]:
            preview_cols = (
                ["Position", "Abbreviation", "TeamName", "Time"]
                if "Time" in results.columns
                else ["Position", "Abbreviation", "TeamName"]
            )
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
                "success": True,
                "num_drivers": len(results) if results is not None else 0,
                "total_laps": total_laps,
                "data_types_loaded": metadata["data_types_loaded"],
                "data_types_failed": metadata["data_types_failed"],
            },
            metadata=metadata,
        )

    except Exception as e:  # pylint: disable=broad-except
        context.log.error(
            f"❌ Failed to ingest {year} {event_name} {session_type}: {str(e)}"
        )
        dagster_logger.error(
            "❌ Failed to ingest %d %s %s: %s",
            year,
            event_name,
            session_type,
            str(e),
        )

        return Output(
            value={
                "year": year,
                "event": event_name,
                "session_type": session_type,
                "success": False,
                "error": str(e),
            },
            metadata={
                "event": f"{year} {event_name}",
                "session_type": session_type,
                "status": "failed",
                "error_message": str(e),
            },
        )


# ============================================================================
# ITALIAN GP 2024 - ALL SESSION ASSETS
# ============================================================================


@asset(
    group_name="raw_italian_gp_2024",
    compute_kind="fastf1",
    description="Italian Grand Prix 2024 - Free Practice 1",
)
def italian_gp_2024_fp1(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """Ingest Italian GP 2024 - Free Practice 1"""

    return ingest_session_asset(context, 2024, "Italian Grand Prix", "FP1")


@asset(
    group_name="raw_italian_gp_2024",
    compute_kind="fastf1",
    description="Italian Grand Prix 2024 - Free Practice 2",
)
def italian_gp_2024_fp2(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """Ingest Italian GP 2024 - Free Practice 2"""

    return ingest_session_asset(context, 2024, "Italian Grand Prix", "FP2")


@asset(
    group_name="raw_italian_gp_2024",
    compute_kind="fastf1",
    description="Italian Grand Prix 2024 - Free Practice 3",
)
def italian_gp_2024_fp3(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """Ingest Italian GP 2024 - Free Practice 3"""

    return ingest_session_asset(context, 2024, "Italian Grand Prix", "FP3")


@asset(
    group_name="raw_italian_gp_2024",
    compute_kind="fastf1",
    description="Italian Grand Prix 2024 - Qualifying",
)
def italian_gp_2024_qualifying(
    context: AssetExecutionContext,
) -> Output[Dict[str, Any]]:
    """Ingest Italian GP 2024 - Qualifying"""

    return ingest_session_asset(context, 2024, "Italian Grand Prix", "Q")


@asset(
    group_name="raw_italian_gp_2024",
    compute_kind="fastf1",
    description="Italian Grand Prix 2024 - Race",
)
def italian_gp_2024_race(
    context: AssetExecutionContext,
) -> Output[Dict[str, Any]]:
    """Ingest Italian GP 2024 - Race"""

    return ingest_session_asset(context, 2024, "Italian Grand Prix", "R")
