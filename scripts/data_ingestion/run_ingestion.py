"""
Script to run F1 data ingestion jobs.
Can be used locally or in CI/CD environments.

Usage:
    # Single session
    python scripts/run_ingestion.py --year 2024 --event "Italian Grand Prix" --session R

    # Full event (all sessions)
    python scripts/run_ingestion.py --year 2024 --event "Italian Grand Prix"

    # Full season
    python scripts/run_ingestion.py --year 2024

    # Dry run
    python scripts/run_ingestion.py --year 2024 --event "Monaco Grand Prix" --dry-run
"""

import argparse
import sys
import traceback
from typing import Optional

from dagster import DagsterInstance, execute_job
from dagster_project import defs

from config.logging import setup_logging, get_logger

# Setup logging
setup_logging()
logger = get_logger("data_ingestion.pipeline")


def run_ingestion(
    year: int,
    event_name: Optional[str] = None,
    session_type: Optional[str] = None,
    dry_run: bool = False,
) -> bool:
    """
    Run F1 data ingestion with the given parameters.

    Args:
        year: Season year (e.g., 2024)
        event_name: Event name (e.g., ["Italian Grand Prix"], ["Italian Grand Prix", "Monaco Grand Prix"]), None for full season
        session_type: Session type (["R"], ["R", "Q"], etc.), None for all sessions
        dry_run: If True, validate only without ingesting data

    Returns:
        True if successful, False otherwise
    """

    logger.info("%s", "\n" + "=" * 60)
    logger.info("F1 Data Ingestion Job")
    logger.info("=" * 60)
    logger.info("Year: %d", year)
    logger.info("Event: %s", event_name or "All Events")
    logger.info("Session: %s", session_type or "All Sessions")
    logger.info("Dry Run: %s", dry_run)
    logger.info("%s", "=" * 60 + "\n")

    if dry_run:
        logger.info("🔍 DRY RUN MODE - Validation Only")
        logger.info("This would ingest:")
        if session_type:
            logger.info("  ✓ Single session: %d %s %s", year, event_name, session_type)
        elif event_name:
            logger.info("  ✓ All sessions from: %d %s", year, event_name)
        else:
            logger.info("  ✓ Full season: All events from %d", year)
        logger.info("✅ Dry run validation complete - no data ingested")
        return True

    # Get the configurable job
    try:
        job = defs.get_job_def("f1_configurable_session_job")
    except Exception as e:  # pylint: disable=broad-except
        logger.error("❌ Error loading job definition: %s", str(e))
        return False

    # Build config
    config = {
        "ops": {
            "f1_session_configurable": {
                "config": {
                    "year": year,
                }
            }
        }
    }

    # Add optional parameters
    if event_name:
        config["ops"]["f1_session_configurable"]["config"]["event_name"] = event_name
    if session_type:
        config["ops"]["f1_session_configurable"]["config"]["session_type"] = (
            session_type
        )

    logger.info("Executing job with config:")
    logger.info("  Year: %d", year)
    if event_name:
        logger.info("  Event: %s", event_name)
    if session_type:
        logger.info("  Session: %s", session_type)

    # Execute the job
    try:
        result = execute_job(job, instance=DagsterInstance.get(), run_config=config)

        if result.success:
            logger.info("✅ Job completed successfully!")
            return True

        logger.warning("❌ Job failed!")
        return False

    except Exception as e:  # pylint: disable=broad-except
        logger.error("\n❌ Error executing job: %s", str(e))

        traceback.print_exc()
        return False


def main():
    """Parse command line arguments and run ingestion."""

    parser = argparse.ArgumentParser(
        description="Run F1 data ingestion job",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""

Examples:
  # Single session
  %(prog)s --year 2024 --event "Italian Grand Prix" --session R
  
  # Full event (all sessions)
  %(prog)s --year 2024 --event "Italian Grand Prix"
  
  # Full season (all events)
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
        default=None,
        help='Event name (e.g., "Italian Grand Prix"). Leave empty for full season.',
    )

    parser.add_argument(
        "--session",
        type=str,
        default=None,
        choices=["R", "Q", "S", "SS", "SQ", "FP1", "FP2", "FP3"],
        help="Session type. Leave empty for all sessions.",
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

    # Run ingestion
    success = run_ingestion(
        year=args.year,
        event_name=args.event,
        session_type=args.session,
        dry_run=args.dry_run,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
