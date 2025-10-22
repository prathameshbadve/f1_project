"""
Raw data ingestion assets for F1 sessions.

Each asset represents one F1 session (e.g., Italian GP 2024 Race).
These assets fetch data from FastF1 API and store it in MinIO/S3.
"""

from typing import Dict, Any

from dagster import asset, AssetExecutionContext, Output, MetadataValue
# import pandas as pd

from config.logging import get_logger, setup_logging

from src.data_ingestion.session_data_loader import SessionLoader
# from src.data_ingestion.storage_client import StorageClient


# ============================================================================
# LOGGING SETUP
# ============================================================================

setup_logging()
dagster_logger = get_logger("data_ingestion.dagster")


# ============================================================================
# SINGLE SESSION ASSETS
# ============================================================================


@asset(
    group_name="raw_sessions_2024",
    compute_kind="fastf1",
    description=(
        "Italian Grand Prix 2024 - Race session info, results, laps, weather,"
        "session_status, track_status and race_control_messages data"
    ),
)
def italian_gp_2024_race(
    context: AssetExecutionContext,
) -> Output[Dict[str, Any]]:
    """
    Ingest Italian Grand Prix 2024 Race session.

    Fetches from FastF1 API:
    - Session info (event details, date, etc.)
    - Race results (finishing positions, times, etc.)
    - Lap data (lap times, sectors, pit stops)
    - Weather data (track temp, air temp, etc.)
    - Race control messages
    - Session Status
    - Track Status

    Stores as Parquet files in MinIO/S3 at:
    - 2024/italian_grand_prix/R/session_info.parquet
    - 2024/italian_grand_prix/R/results.parquet
    - 2024/italian_grand_prix/R/laps.parquet
    - 2024/italian_grand_prix/R/weather.parquet
    - 2024/italian_grand_prix/R/race_control_messages.parquet
    - 2024/italian_grand_prix/R/session_status.parquet
    - 2024/italian_grand_prix/R/track_status.parquet
    """

    # Configuration
    year = 2024
    event_name = "Italian Grand Prix"
    session_type = "R"  # R = Race

    context.log.info(f"Starting ingestion: {year} {event_name} {session_type}")
    dagster_logger.info("Starting ingestion: %d %s %s", year, event_name, session_type)

    # Initialize loaders
    session_loader = SessionLoader()
    # storage_client = StorageClient()

    try:
        # Load session data (this fetches from FastF1 API or cache)
        context.log.info("Fetching session data from FastF1...")
        dagster_logger.info("Fetching session data from FastF1...")
        session_data = session_loader.load_session_data(
            year=year,
            event_name=event_name,
            session_type=session_type,
            save_to_storage=True,  # Automatically saves to MinIO
            force_refresh=False,  # Use cached data if available
        )

        context.log.info("Session data loaded successfully!")
        dagster_logger.info("Session data loaded successfully!")

        # Extract metadata for Dagster UI
        session_info = session_data.get("session_info")
        results = session_data.get("results")
        # laps = session_data.get("laps")
        # weather = session_data.get("weather")
        # race_control_messages = session_data.get("race_control_messages")
        # session_status = session_data.get("session_status")
        # track_status = session_data.get("track_status")

        # Get winner info
        winner = None
        winner_time = None
        if results is not None and len(results) > 0:
            winner = (
                results.iloc[0]["Abbreviation"]
                if "Abbreviation" in results.columns
                else "Unknown"
            )
            winner_time = (
                str(results.iloc[0]["Time"]) if "Time" in results.columns else "N/A"
            )

        # Get session date
        session_date = None
        if session_info is not None and len(session_info) > 0:
            session_date = (
                str(session_info.iloc[0]["session_date"])
                if "session_date" in session_info.columns
                else "Unknown"
            )

        # Build metadata for display in Dagster UI
        metadata = {
            "event": f"{year} {event_name}",
            "session_type": "Race",
            "session_date": session_date or "Unknown",
            "winner": winner or "Unknown",
            "winner_time": winner_time or "N/A",
            "num_drivers": len(results) if results is not None else 0,
            "num_laps": int(session_data["session_info"]["total_laps"].iloc[0])
            if session_data["session_info"] is not None
            else 0,
            "data_types": list(session_data.keys()),
        }

        # Add preview of results data to metadata (shows in Dagster UI)
        if results is not None and len(results) > 0:
            # Show top 5 finishers
            results_preview = results[
                ["Position", "Abbreviation", "TeamName", "Time"]
            ].head(5)
            metadata["results_preview"] = MetadataValue.md(
                results_preview.to_markdown(index=False)
            )

        context.log.info(f"✅ Ingestion complete: {year} {event_name} {session_type}")
        context.log.info(f"   Winner: {winner}")
        context.log.info(f"   Drivers: {len(results) if results is not None else 0}")
        context.log.info(
            f"   Laps: {int(session_data['session_info']['total_laps'].iloc[0]) if session_data['session_info'] is not None else 0}"
        )

        dagster_logger.info(
            "✅ Ingestion complete: %d %s %s", year, event_name, session_type
        )
        dagster_logger.info("   Winner: %s", winner)
        dagster_logger.info(
            "   Drivers: %d", len(results) if results is not None else 0
        )
        dagster_logger.info(
            "   Laps: %d",
            int(session_data["session_info"]["total_laps"].iloc[0])
            if session_data["session_info"] is not None
            else 0,
        )

        # Return output with metadata
        return Output(
            value={
                "year": year,
                "event": event_name,
                "session_type": session_type,
                "success": True,
                "num_drivers": len(results) if results is not None else 0,
                "num_laps": int(session_data["session_info"]["total_laps"].iloc[0])
                if session_data["session_info"] is not None
                else 0,
                "winner": winner,
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
                "success": False,
                "error": str(e),
            },
            metadata={
                "event": f"{year} {event_name}",
                "session_type": "Race",
                "status": "failed",
                "error_message": str(e),
            },
        )
