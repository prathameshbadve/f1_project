"""
Script to run F1 data ingestion jobs.
Can be used locally or in CI/CD environments.

Usage:
    # Single session
    python scripts/data_ingestion/run_ingestion.py --year 2024 --event "Italian Grand Prix" --session R

    # Multiple events with single session type
    python scripts/data_ingestion/run_ingestion.py --year 2024 --event "Italian Grand Prix" "Monaco Grand Prix" --session R

    # Single event with multiple session types
    python scripts/data_ingestion/run_ingestion.py --year 2024 --event "Monaco Grand Prix" --session R Q

    # Multiple events with multiple session types
    python scripts/data_ingestion/run_ingestion.py --year 2024 --event "Italian Grand Prix" "Monaco Grand Prix" --session R Q S

    # Full event (all sessions)
    python scripts/data_ingestion/run_ingestion.py --year 2024 --event "Italian Grand Prix"

    # Only specific session type for all events in season
    python scripts/data_ingestion/run_ingestion.py --year 2024 --session R

    # Full season
    python scripts/data_ingestion/run_ingestion.py --year 2024

    # Dry run
    python scripts/data_ingestion/run_ingestion.py --year 2024 --event "Monaco Grand Prix" --dry-run
"""

import argparse
import sys
import traceback
from typing import Optional, List

from dagster import DagsterInstance, materialize

# Import the asset directly
from dagster_project.assets.season_ingestion import f1_session_configurable

from config.logging import setup_logging, get_logger

# Setup logging
setup_logging()
logger = get_logger("data_ingestion.pipeline")


def run_ingestion(
    year: int,
    event_name: Optional[List[str]] = None,
    session_type: Optional[List[str]] = None,
    dry_run: bool = False,
) -> bool:
    """
    Run F1 data ingestion with the given parameters.

    Supports 9 modes of ingestion:
    1. Single session: year + single event + single session
    2. Multiple sessions from one event: year + single event + multiple sessions
    3. Multiple events with one session type: year + multiple events + single session
    4. Multiple events with multiple session types: year + multiple events + multiple sessions
    5. All sessions from one event: year + single event (no session type)
    6. All sessions from multiple events: year + multiple events (no session type)
    7. One session type from all events: year + single session type (no event)
    8. Multiple session types from all events: year + multiple session types (no event)
    9. Whole season: year only (no events, no sessions)

    Args:
        year: Season year (e.g., 2024)
        event_name: List of event names (e.g., ["Italian Grand Prix"]), None for all events
        session_type: List of session types (e.g., ["R", "Q"]), None for all sessions
        dry_run: If True, validate only without ingesting data

    Returns:
        True if successful, False otherwise
    """

    logger.info("%s", "\n" + "=" * 70)
    logger.info("F1 Data Ingestion")
    logger.info("=" * 70)
    logger.info("Year: %d", year)
    logger.info("Event(s): %s", event_name if event_name else "All Events")
    logger.info("Session(s): %s", session_type if session_type else "All Sessions")
    logger.info("Dry Run: %s", dry_run)
    logger.info("%s", "=" * 70 + "\n")

    # Determine mode for dry run logging
    if dry_run:
        logger.info("🔍 DRY RUN MODE - Validation Only")
        logger.info("This would ingest:")

        if event_name and session_type:
            if len(event_name) == 1 and len(session_type) == 1:
                logger.info(
                    "  ✓ Mode 1: Single session - %d %s %s",
                    year,
                    event_name[0],
                    session_type[0],
                )
            elif len(event_name) == 1 and len(session_type) > 1:
                logger.info(
                    "  ✓ Mode 2: Multiple sessions from one event - %d %s %s",
                    year,
                    event_name[0],
                    session_type,
                )
            elif len(event_name) > 1 and len(session_type) == 1:
                logger.info(
                    "  ✓ Mode 3: One session type from multiple events - %d %s %s",
                    year,
                    event_name,
                    session_type[0],
                )
            else:
                logger.info(
                    "  ✓ Mode 4: Multiple session types from multiple events - %d %s %s",
                    year,
                    event_name,
                    session_type,
                )

        elif event_name and not session_type:
            if len(event_name) == 1:
                logger.info(
                    "  ✓ Mode 5: All sessions from one event - %d %s",
                    year,
                    event_name[0],
                )
            else:
                logger.info(
                    "  ✓ Mode 6: All sessions from multiple events - %d %s",
                    year,
                    event_name,
                )

        elif session_type and not event_name:
            if len(session_type) == 1:
                logger.info(
                    "  ✓ Mode 7: One session type from all events - %d all events %s",
                    year,
                    session_type[0],
                )
            else:
                logger.info(
                    "  ✓ Mode 8: Multiple session types from all events - %d all events %s",
                    year,
                    session_type,
                )

        else:
            logger.info(
                "  ✓ Mode 9: Whole season - All events and sessions from %d", year
            )

        logger.info("✅ Dry run validation complete - no data ingested")
        return True

    # Build run config for the asset
    run_config = {
        "ops": {
            "f1_session_configurable": {
                "config": {
                    "year": year,
                }
            }
        }
    }

    # Add optional parameters as lists
    if event_name:
        run_config["ops"]["f1_session_configurable"]["config"]["event_name"] = (
            event_name
        )
    if session_type:
        run_config["ops"]["f1_session_configurable"]["config"]["session_type"] = (
            session_type
        )

    logger.info("Materializing asset with config:")
    logger.info("  Year: %d", year)
    if event_name:
        logger.info("  Event(s): %s", event_name)
    if session_type:
        logger.info("  Session(s): %s", session_type)

    # Materialize the asset
    try:
        logger.info("Starting asset materialization...")

        # Materialize using the imported asset
        result = materialize(
            [f1_session_configurable],
            instance=DagsterInstance.get(),
            run_config=run_config,
        )

        if result.success:
            logger.info("✅ Asset materialization completed successfully!")
            return True

        logger.warning("❌ Asset materialization failed!")
        return False

    except Exception as e:  # pylint: disable=broad-except
        logger.error("\n❌ Error materializing asset: %s", str(e))
        traceback.print_exc()
        return False


def main():
    """Parse command line arguments and run ingestion."""

    parser = argparse.ArgumentParser(
        description="Run F1 data ingestion with flexible scope",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Mode 1: Single session
  %(prog)s --year 2024 --event "Italian Grand Prix" --session R
  
  # Mode 2: Multiple sessions from one event
  %(prog)s --year 2024 --event "Monaco Grand Prix" --session R Q S
  
  # Mode 3: One session type from multiple events
  %(prog)s --year 2024 --event "Italian Grand Prix" "Monaco Grand Prix" --session R
  
  # Mode 4: Multiple session types from multiple events
  %(prog)s --year 2024 --event "Italian Grand Prix" "Monaco Grand Prix" --session R Q
  
  # Mode 5: All sessions from one event
  %(prog)s --year 2024 --event "Italian Grand Prix"
  
  # Mode 6: All sessions from multiple events
  %(prog)s --year 2024 --event "Italian Grand Prix" "Monaco Grand Prix"
  
  # Mode 7: One session type from all events
  %(prog)s --year 2024 --session R
  
  # Mode 8: Multiple session types from all events
  %(prog)s --year 2024 --session R Q
  
  # Mode 9: Whole season (all events and all sessions)
  %(prog)s --year 2024
  
  # Dry run
  %(prog)s --year 2024 --event "Monaco Grand Prix" --dry-run
        """,
    )

    parser.add_argument(
        "--year", type=int, required=True, help="Season year (e.g., 2024, 2023)"
    )

    parser.add_argument(
        "--event",
        type=str,
        nargs="+",  # Accept multiple values
        default=None,
        help='Event name(s). Examples: "Italian Grand Prix" or "Italian Grand Prix" "Monaco Grand Prix". Leave empty for all events.',
    )

    parser.add_argument(
        "--session",
        type=str,
        nargs="+",  # Accept multiple values
        default=None,
        choices=["R", "Q", "S", "SS", "SQ", "FP1", "FP2", "FP3"],
        help="Session type(s). Examples: R or R Q S. Leave empty for all sessions.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run mode - validate only without ingesting data",
    )

    args = parser.parse_args()

    # Validate year
    if args.year < 2018 or args.year > 2025:
        logger.warning("❌ Invalid year: %d. Must be between 2018 and 2025.", args.year)
        sys.exit(1)

    # Convert args to lists (they're already lists from nargs="+", or None)
    event_name = args.event if args.event else None
    session_type = args.session if args.session else None

    # Run ingestion
    success = run_ingestion(
        year=args.year,
        event_name=event_name,
        session_type=session_type,
        dry_run=args.dry_run,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
