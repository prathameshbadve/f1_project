#!/usr/bin/env python3
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
        event_name: Event name (e.g., "Italian Grand Prix"), None for full season
        session_type: Session type (R, Q, S, etc.), None for all sessions
        dry_run: If True, validate only without ingesting data

    Returns:
        True if successful, False otherwise
    """

    print("\n" + "=" * 60)
    print("F1 Data Ingestion Job")
    print("=" * 60)
    print(f"Year: {year}")
    print(f"Event: {event_name or 'All Events'}")
    print(f"Session: {session_type or 'All Sessions'}")
    print(f"Dry Run: {dry_run}")
    print("=" * 60 + "\n")

    if dry_run:
        print("🔍 DRY RUN MODE - Validation Only\n")
        print("This would ingest:")
        if session_type:
            print(f"  ✓ Single session: {year} {event_name} {session_type}")
        elif event_name:
            print(f"  ✓ All sessions from: {year} {event_name}")
        else:
            print(f"  ✓ Full season: All events from {year}")
        print("\n✅ Dry run validation complete - no data ingested")
        return True

    # Get the configurable job
    try:
        job = defs.get_job_def("f1_configurable_session_job")
    except Exception as e:  # pylint: disable=broad-except
        print(f"❌ Error loading job definition: {e}")
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

    print("Executing job with config:")
    print(f"  Year: {year}")
    if event_name:
        print(f"  Event: {event_name}")
    if session_type:
        print(f"  Session: {session_type}")
    print()

    # Execute the job
    try:
        result = execute_job(job, instance=DagsterInstance.get(), run_config=config)

        if result.success:
            print("\n✅ Job completed successfully!")
            return True

        print("\n❌ Job failed!")
        return False

    except Exception as e:  # pylint: disable=broad-except
        print(f"\n❌ Error executing job: {e}")

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
        print(f"❌ Invalid year: {args.year}. Must be between 2018 and 2025.")
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
