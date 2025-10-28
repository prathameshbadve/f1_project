"""
Season-level ingestion assets using partitions.

This module uses Dagster partitions to handle entire seasons efficiently.
Instead of creating 100+ individual assets, we create one partitioned asset
that can handle any session based on the partition key.
"""

import time
from typing import Optional, List, Dict, Any

import pandas as pd
from dagster import Config, asset, AssetExecutionContext, Output, MetadataValue

from config.logging import get_logger
from src.data_ingestion.schedule_loader import ScheduleLoader
from src.data_ingestion.session_data_loader import SessionLoader

# ============================================================================
# LOGGING SETUP
# ============================================================================

logger = get_logger("data_ingestion.dagster")

# ============================================================================
# CONFIGURABLE ASSET (Any Year/Event/Session)
# ============================================================================


class F1SessionConfig(Config):
    """
    Configuration for ingesting F1 sessions with flexible scope.
    Supports multiple modes:
    1. Single session: year + single event + single session
    2. Multiple sessions from one event: year + single event + multiple sessions
    3. Multiple events with one session type: year + multiple events + single session
    4. Multiple events with multiple session types: year + multiple events + multiple sessions
    5. All sessions from one event: year + single event (no session type)
    6. All sessions from multiple events: year + multiple events (no session type)
    7. One session type from all events: year + single session type (no event)
    8. Multiple session types from all events: year + multiple session types (no event)
    9. Whole season: year only (no events, no sessions)
    """

    year: int
    events: Optional[List[str]] = None
    sessions: Optional[List[str]] = None


@asset(
    group_name="raw_configurable",
    compute_kind="fastf1",
    description="Ingest any F1 session by providing year, events and sessions",
)
def f1_session_configurable(
    context: AssetExecutionContext,
    config: F1SessionConfig,
) -> Output[Dict[str, Any]]:
    """
    Ingest F1 sessions with flexible configuration.

    Nine usage modes as mentioned in F1SessionConfig
    """

    year = config.year
    events = config.events
    sessions = config.sessions

    context.log.info("Gathering scope for ingestion...")
    context.log.info(
        "Config: year=%d, event=%s session=%s",
        year,
        events if events else "All Events",
        sessions if sessions else "All Sessions",
    )

    logger.info("Gathering scope for ingestion...")
    logger.info(
        "Config: year=%d, event=%s session=%s",
        year,
        events if events else "All Events",
        sessions if sessions else "All Sessions",
    )

    # Initialize loaders
    schedule_loader = ScheduleLoader()
    session_loader = SessionLoader()

    # Determin scope
    # One Session Type is provided (eg. ["R"])
    if sessions and len(sessions) == 1:
        # One event name is also provided (eg. ["Italian Grand Prix"])
        if events and len(events) == 1:
            # Mode 1: Single session from one grand prix event
            scope = "one_event_one_session"
            sessions_to_ingest = [(events[0], sessions[0])]
            context.log.info("Mode: One Event, One Session")
            logger.info("Mode: One Event, One Session")

        elif events and len(events) > 1:
            # Mode 2: Same session type from multiple grand prix event
            scope = "multiple_events_one_session"
            sessions_to_ingest = [(e, sessions[0]) for e in events]
            context.log.info("Mode: Multiple Events, One Session")
            logger.info("Mode: Multiple Events, One Session")

        else:
            # Mode 3: Same session type from all grand prix events of the season
            scope = "all_events_one_session"

            # Get all events for the year
            events = schedule_loader.get_events_for_ingestion(year)
            sessions_to_ingest = []

            for event in events:
                sessions_to_ingest.append((event, sessions[0]))

            context.log.info("Mode: All Events, One Session")
            logger.info("Mode: All Events, One Session")

    # Multiple sessions are provided
    elif sessions and len(sessions) > 1:
        # One event name is also provided (eg. ["Italian Grand Prix"])
        if events and len(events) == 1:
            # Mode 4: Multiple sessions from one grand prix event
            scope = "one_event_multiple_sessions"
            sessions_to_ingest = [(events[0], s) for s in sessions]
            context.log.info("Mode: One Event, Multiple Sessions")
            logger.info("Mode: One Event, Multiple Sessions")

        elif events and len(events) > 1:
            # Mode 5: Multiple session type from multiple grand prix event
            scope = "multiple_events_multiple_sessions"
            sessions_to_ingest = [(e, s) for e in events for s in sessions]
            context.log.info("Mode: Multiple Events, Multiple Sessions")
            logger.info("Mode: Multiple Events, Multiple Sessions")

        else:
            # Mode 6: Multiple session type from all grand prix events of the season
            scope = "all_events_multiple_sessions"

            # Get all events for the year
            events = schedule_loader.get_events_for_ingestion(year)
            sessions_to_ingest = []

            for event in events:
                sessions_to_ingest.extend([(event, s) for s in sessions])

            context.log.info("Mode: All Events, Multiple Sessions")
            logger.info("Mode: All Events, Multiple Sessions")

    # Empty sessions - All sessions to ingest
    else:
        # One event name is also provided (eg. ["Italian Grand Prix"])
        if events and len(events) == 1:
            # Mode 7: All sessions from one grand prix event
            scope = "one_event_all_sessions"

            # Get all sessions for this event from schedule loader
            sessions = schedule_loader.get_sessions_to_load(year, events[0])

            sessions_to_ingest = [(events[0], s) for s in sessions]

            context.log.info("Mode: One Event, All Sessions")
            logger.info("Mode: One Event, All Sessions")

        elif events and len(events) > 1:
            # Mode 8: All session type from multiple grand prix event
            scope = "multiple_events_all_sessions"

            sessions_to_ingest = []

            for event in events:
                # Get the session to ingest for the event
                sessions = schedule_loader.get_sessions_to_load(year, event)
                sessions_to_ingest.extend([(event, s) for s in sessions])

            context.log.info("Mode: Multiple Events, All Sessions")
            logger.info("Mode: Multiple Events, All Sessions")

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
                    sessions_to_ingest.append((event, session))

            context.log.info("Mode: Whole Season")
            logger.info("Mode: Whole Season")

    context.log.info("Scope: %s", scope)
    logger.info("Scope: %s", scope)

    # Ingest all sessions
    results = []
    successful = 0
    failed = 0

    context.log.info("=" * 70)
    context.log.info(f"Starting ingestion: {len(sessions_to_ingest)} sessions")
    context.log.info("=" * 70)

    logger.info("=" * 70)
    logger.info("Starting ingestion: %d sessions", len(sessions_to_ingest))
    logger.info("=" * 70)

    for idx, partition_key in enumerate(sessions_to_ingest, 1):
        event, session = partition_key

        context.log.info(
            "Processing partition [%d/%d]: %d %s %s",
            idx,
            len(sessions_to_ingest),
            year,
            event,
            session,
        )
        logger.info(
            "Processing partition [%d/%d]: %d %s %s",
            idx,
            len(sessions_to_ingest),
            year,
            event,
            session,
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
            context.log.info("| ✅ Success - Loaded: %s", ", ".join(loaded_types))
            logger.info("| ✅ Success - Loaded: %s", ", ".join(loaded_types))

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
            context.log.error("| ❌ Failed: %s", str(e))
            logger.error("| ❌ Failed: %s", str(e))
            results.append(
                {
                    "event": event,
                    "session": session,
                    "success": False,
                    "error": str(e),
                }
            )
            failed += 1

        # Sleep for 3 seconds before moving to the next sesssion.
        time.sleep(3)

    context.log.info("=" * 70)
    context.log.info("Ingestion complete!")
    context.log.info("| Successful: %d/%d", successful, len(sessions_to_ingest))
    context.log.info("| Failed: %d/%d", failed, len(sessions_to_ingest))
    context.log.info("=" * 70)

    logger.info("=" * 70)
    logger.info("Ingestion complete!")
    logger.info("| Successful: %d/%d", successful, len(sessions_to_ingest))
    logger.info("| Failed: %d/%d", failed, len(sessions_to_ingest))
    logger.info("=" * 70)

    # Build metadata
    metadata = {
        "year": year,
        "scope": scope,
        "total_sessions": len(sessions_to_ingest),
        "successful": successful,
        "failed": failed,
        "success_rate": f"{(successful / len(sessions_to_ingest) * 100):.1f}%",
        "events": events if events else "All Events",
        "session_type": sessions if sessions else "All Sessions",
    }

    # Create summary table
    summary_rows = []
    for r in results:
        summary_rows.append(
            {
                "Status": "✅" if r["success"] else "❌",
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
