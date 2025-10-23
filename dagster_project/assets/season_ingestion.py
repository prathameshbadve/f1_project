"""
Season-level ingestion assets using partitions.

This module uses Dagster partitions to handle entire seasons efficiently.
Instead of creating 100+ individual assets, we create one partitioned asset
that can handle any session based on the partition key.
"""

from typing import Dict, Any, Optional, List

from dagster import asset, AssetExecutionContext, Output, MetadataValue, Config
import pandas as pd

from dagster_project.partitions import f1_2024_sessions_partitions
from src.data_ingestion.session_data_loader import SessionLoader
from src.data_ingestion.schedule_loader import ScheduleLoader
from src.data_ingestion.storage_client import StorageClient

from config.logging import get_logger


# ============================================================================
# LOGGING SETUP
# ============================================================================

dagster_logger = get_logger("data_ingestion.dagster")


# ============================================================================
# PARTITIONED ASSET (2024 Season)
# ============================================================================


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
            "num_race_control_messages": (
                len(race_control_messages) if race_control_messages is not None else 0
            ),
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
# CONFIGURABLE ASSET (Any Year/Event/Session)
# ============================================================================


class F1SessionConfig(Config):
    """
    Configuration for ingesting F1 sessions with flexible scope.

    Supports three modes:
    1. Single session: Provide year + event_name + session_type
    2. Whole event: Provide year + event_name (ingests all sessions)
    3. Whole season: Provide only year (ingests all events and sessions)
    """

    year: int
    event_name: Optional[List[str]] = None
    session_type: Optional[List[str]] = None


@asset(
    group_name="raw_configurable",
    compute_kind="fastf1",
    description="Ingest any F1 session by providing year, event_name, and session_type as config",
)
def f1_session_configurable(
    context: AssetExecutionContext,
    config: F1SessionConfig,
) -> Output[Dict[str, Any]]:
    """
    Ingest F1 sessions with flexible configuration.

    Three usage modes:

    1. Single Session:
       config = {"year": 2023, "event_name": "Monaco Grand Prix", "session_type": "R"}

    2. Whole Event (all sessions):
       config = {"year": 2023, "event_name": "Monaco Grand Prix"}

    3. Whole Season (all events and sessions):
       config = {"year": 2023}

    4. Custom session types for event:
       config = {"year": 2023, "event_name": "Monaco Grand Prix", "session_types": ["Q", "R"]}

    5. Custom session types for season:
       config = {"year": 2023, "session_types": ["R"]}  # Only races
    """

    year = config.year
    event_name = config.event_name
    session_type = config.session_type

    context.log.info(f"Starting ingestion: {year} {event_name} {session_type}")
    context.log.info(
        f"Config: year={year}, event(s)={event_name}, session(s)={session_type}"
    )

    dagster_logger.info("Starting ingestion: %d %s %s", year, event_name, session_type)
    dagster_logger.info(
        "Config: year=%d, event=%s session=%s", year, event_name, session_type
    )

    # Initialize loaders
    session_loader = SessionLoader()
    schedule_loader = ScheduleLoader()

    # Determine scope

    # One Session Type is provided (eg. ["R"])
    if session_type and len(session_type) == 1:
        # One event name is also provided (eg. ["Italian Grand Prix"])
        if event_name and len(event_name) == 1:
            # Mode 1: Single session from one grand prix event
            scope = "one_event_one_session"
            sessions_to_ingest = [f"{event_name}|{session_type}"]
            context.log.info(
                f"Mode: One Event One Session - {year} {event_name} {session_type}"
            )
            dagster_logger.info(
                "Mode: One Event One Session - %d %s %s", year, event_name, session_type
            )

        elif event_name and len(event_name) > 1:
            # Mode 2: Same session type from multiple grand prix event
            scope = "multiple_events_one_session"
            sessions_to_ingest = [f"{e}|{session_type}" for e in event_name]
            context.log.info(
                f"Mode: Multiple Events, One Session - {year} {event_name} {session_type} (Total: {len(sessions_to_ingest)})"
            )
            dagster_logger.info(
                "Mode: Multiple Events, One Session - %d %s %s  (Total: %d)",
                year,
                event_name,
                session_type,
                len(sessions_to_ingest),
            )

        else:
            # Mode 3: Same session type from all grand prix events of the season
            scope = "all_events_one_session"

            # Get all events for the year
            events = schedule_loader.get_events_for_ingestion(year)
            sessions_to_ingest = []

            for event in events:
                sessions_to_ingest.append(f"{event}|{session_type}")

            context.log.info(
                f"Mode: All Events, One Session - {session_type} files from all GPs of {year} (Total: {len(sessions_to_ingest)})"
            )
            dagster_logger.info(
                "Mode: All Events, One Session - %s files from all GPs of %d (Total: %d)",
                session_type,
                year,
                len(sessions_to_ingest),
            )

    elif session_type and len(session_type) > 1:
        # One event name is also provided (eg. ["Italian Grand Prix"])
        if event_name and len(event_name) == 1:
            # Mode 4: Multiple sessions from one grand prix event
            scope = "one_event_multiple_sessions"
            sessions_to_ingest = [f"{event_name}|{s}" for s in session_type]
            context.log.info(
                f"Mode: One Event, Multiple Sessions - {year} {event_name} {session_type}"
            )
            dagster_logger.info(
                "Mode: One Event, Multiple Sessions - %d %s %s",
                year,
                event_name,
                session_type,
            )

        elif event_name and len(event_name) > 1:
            # Mode 5: Multiple session type from multiple grand prix event
            scope = "multiple_events_multiple_sessions"
            sessions_to_ingest = [f"{e}|{s}" for e in event_name for s in session_type]
            context.log.info(
                f"Mode: Multiple Events, Multiple Sessions - {year} {event_name} {session_type} (Total: {len(sessions_to_ingest)})"
            )
            dagster_logger.info(
                "Mode: Multiple Events, Multiple Sessions - %d %s %s (Total: %d)",
                year,
                event_name,
                session_type,
                len(sessions_to_ingest),
            )

        else:
            # Mode 6: Multiple session type from all grand prix events of the season
            scope = "all_events_multiple_sessions"

            # Get all events for the year
            events = schedule_loader.get_events_for_ingestion(year)
            sessions_to_ingest = []

            for event in events:
                sessions_to_ingest.extend([f"{event}|{s}" for s in session_type])

            context.log.info(
                f"Mode: All Events, Multiple Sessions - {session_type} files from all GPs of {year} (Total: {len(sessions_to_ingest)})"
            )
            dagster_logger.info(
                "Mode: All Events, Multiple Sessions - %s files from all GPs of %d (Total: %d)",
                session_type,
                year,
                len(sessions_to_ingest),
            )

    else:
        # One event name is also provided (eg. ["Italian Grand Prix"])
        if event_name and len(event_name) == 1:
            # Mode 7: All sessions from one grand prix event
            scope = "one_event_all_sessions"

            # Get all sessions for this event from schedule loader
            sessions = schedule_loader.get_sessions_to_load(year, event_name)

            sessions_to_ingest = [f"{event_name}|{s}" for s in sessions]
            num_of_sessions = len(sessions_to_ingest)

            context.log.info(
                f"Mode: One Event, All Sessions for - {year} {event_name} (Total: {num_of_sessions})"
            )
            dagster_logger.info(
                "Mode: One Event, All Sessions for - %d %s (Total: %d)",
                year,
                event_name,
                num_of_sessions,
            )

        elif event_name and len(event_name) > 1:
            # Mode 8: All session type from multiple grand prix event
            scope = "multiple_events_all_sessions"

            sessions_to_ingest = []

            for event in event_name:
                # Get the session to ingest for the event
                sessions = schedule_loader.get_sessions_to_load(year, event)
                sessions_to_ingest.extend([f"{event}|{s}" for s in sessions])

            context.log.info(
                f"Mode: Multiple Events, All Sessions for - {year} {event_name} (Total: {len(sessions_to_ingest)})"
            )
            dagster_logger.info(
                "Mode: Multiple Events, All Sessions for - %d %s (Total: %d)",
                year,
                event_name,
                len(sessions_to_ingest),
            )

        else:
            # Mode 9: Multiple session type from all grand prix events of the season
            scope = "whole_season"

            # Get all events for the year
            events = schedule_loader.get_events_for_ingestion(year)

            sessions_to_ingest = []

            for event in events:
                # Get the session for every event iteratively
                sessions = schedule_loader.get_sessions_to_load(year, event)

                for session in sessions:
                    sessions_to_ingest.append(f"{event}|{session}")

            context.log.info(f"Mode: Whole Season - {year}")
            context.log.info(f"  Events: {len(events)}")
            context.log.info(f"  Total sessions: {len(sessions_to_ingest)}")

            dagster_logger.info(
                "Mode: Whole Season - %d",
                year,
            )
            dagster_logger.info("  Events: %d", len(events))
            dagster_logger.info("  Total sessions: %d", len(sessions_to_ingest))

    # Ingest all sessions
    results = []
    successful = 0
    failed = 0

    context.log.info("=" * 70)
    context.log.info(f"Starting ingestion: {len(sessions_to_ingest)} sessions")
    context.log.info("=" * 70)

    dagster_logger.info("=" * 70)
    dagster_logger.info("Starting ingestion: %d sessions", len(sessions_to_ingest))
    dagster_logger.info("=" * 70)

    for idx, partition_key in enumerate(sessions_to_ingest, 1):
        event, session = partition_key.split("|")

        context.log.info(f"[{idx}/{len(sessions_to_ingest)}] {year} {event} {session}")
        dagster_logger.info(
            "[%d/%d] %d %s %s", idx, len(sessions_to_ingest), year, event, session
        )

        try:
            # Load session data
            session_data = session_loader.load_session_data(
                year=year,
                event_name=event,
                session_type=session,
                save_to_storage=True,
                force_refresh=False,
            )

            # Log data types loaded
            loaded_types = [k for k, v in session_data.items() if v is not None]
            context.log.info(f"  ✅ Success - Loaded: {', '.join(loaded_types)}")
            dagster_logger.info("  ✅ Success - Loaded: %s", ", ".join(loaded_types))

            results.append(
                {
                    "event": event,
                    "session": session,
                    "success": True,
                    "data_types": loaded_types,
                }
            )
            successful += 1

        except Exception as e:  # pylint: disable=broad-except
            context.log.error(f"  ❌ Failed: {str(e)}")
            dagster_logger.error("  ❌ Failed: %s", str(e))
            results.append(
                {
                    "event": event,
                    "session": session,
                    "success": False,
                    "error": str(e),
                }
            )
            failed += 1

    context.log.info("=" * 70)
    context.log.info("Ingestion complete!")
    context.log.info(f"  Successful: {successful}/{len(sessions_to_ingest)}")
    context.log.info(f"  Failed: {failed}/{len(sessions_to_ingest)}")
    context.log.info("=" * 70)

    dagster_logger.info("=" * 70)
    dagster_logger.info("Ingestion complete!")
    dagster_logger.info("  Successful: %d/%d", successful, len(sessions_to_ingest))
    dagster_logger.info("  Failed: %d/%d", failed, len(sessions_to_ingest))
    dagster_logger.info("=" * 70)

    # Build metadata
    metadata = {
        "year": year,
        "scope": scope,
        "total_sessions": len(sessions_to_ingest),
        "successful": successful,
        "failed": failed,
        "success_rate": f"{(successful / len(sessions_to_ingest) * 100):.1f}%",
    }

    metadata["event"] = event_name if event_name else "All events"

    metadata["session_type"] = session_type if session_type else "All sessions"

    # Create summary table
    summary_rows = []
    for r in results:
        status = "✅" if r["success"] else "❌"
        summary_rows.append(
            {
                "Status": status,
                "Event": r["event"],
                "Session": r["session"],
                "Result": "Success" if r["success"] else r.get("error", "Failed")[:50],
            }
        )

    if len(summary_rows) <= 20:
        # Show full table for small ingestions
        summary_df = pd.DataFrame(summary_rows)
        metadata["ingestion_summary"] = MetadataValue.md(
            summary_df.to_markdown(index=False)
        )
    else:
        # Show just stats for large ingestions
        metadata["events_ingested"] = len(
            set(r["event"] for r in results if r["success"])
        )
        metadata["sample_results"] = MetadataValue.md(
            pd.DataFrame(summary_rows[:10]).to_markdown(index=False)
            + f"\n\n... and {len(summary_rows) - 10} more sessions"
        )

    return Output(
        value={
            "year": year,
            "scope": scope,
            "total_sessions": len(sessions_to_ingest),
            "successful": successful,
            "failed": failed,
            "results": results,
        },
        metadata=metadata,
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
