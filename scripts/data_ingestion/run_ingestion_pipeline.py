"""
Script to ingest F1 data using the ingestion pipeline.

Usage:
    # Ingest a single session
    python scripts/ingest_data.py --year 2024 --event "Bahrain" --session R

    # Ingest a full race weekend
    python scripts/ingest_data.py --year 2024 --event "Bahrain"

    # Ingest a complete season
    python scripts/ingest_data.py --year 2024

    # Ingest multiple seasons (2022-2024)
    python scripts/ingest_data.py --years 2022 2023 2024

    # Ingest specific events only
    python scripts/ingest_data.py --year 2024 --events "Bahrain" "Saudi Arabia" "Australia"

    # Force re-ingestion (skip existing = False)
    python scripts/ingest_data.py --year 2024 --force

    # Skip validation (faster but riskier)
    python scripts/ingest_data.py --year 2024 --no-validate
"""

import argparse
import sys
from datetime import datetime

from config.logging import setup_logging, get_logger
from src.data_ingestion.data_ingestion_pipeline import IngestionPipeline


# Setup logging
setup_logging()
logger = get_logger("scripts.ingest_data")


def parse_args():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(
        description="Ingest F1 data from FastF1 API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Year(s) to ingest
    year_group = parser.add_mutually_exclusive_group(required=True)
    year_group.add_argument(
        "--year",
        type=int,
        help="Single year to ingest (e.g., 2024)",
    )
    year_group.add_argument(
        "--years",
        type=int,
        nargs="+",
        help="Multiple years to ingest (e.g., 2022 2023 2024)",
    )

    # Event(s) to ingest (optional)
    parser.add_argument(
        "--event",
        type=str,
        help="Single event to ingest (e.g., 'Bahrain')",
    )
    parser.add_argument(
        "--events",
        type=str,
        nargs="+",
        help="Multiple events to ingest (e.g., 'Bahrain' 'Saudi Arabia')",
    )

    # Session type (optional)
    parser.add_argument(
        "--session",
        type=str,
        choices=["R", "Q", "S", "SS", "SQ", "FP1", "FP2", "FP3"],
        help="Single session type to ingest (e.g., 'R' for race)",
    )

    # Options
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-ingestion (don't skip existing data)",
    )
    parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Skip data validation (faster but riskier)",
    )
    parser.add_argument(
        "--delay",
        type=int,
        default=3,
        help="Delay between API calls in seconds (default: 3)",
    )
    parser.add_argument(
        "--save-results",
        type=str,
        metavar="PATH",
        help="Save results to JSON file (e.g., results.json)",
    )

    return parser.parse_args()


def main():
    """Main ingestion script"""

    args = parse_args()

    # Determine years to process
    if args.year:
        years = [args.year]
    else:
        years = args.years

    # Determine events to process
    events = None
    if args.event:
        events = [args.event]
    elif args.events:
        events = args.events

    # Determine session types
    session_types = None
    if args.session:
        session_types = [args.session]

    # Log configuration
    logger.info("=" * 70)
    logger.info("F1 DATA INGESTION SCRIPT")
    logger.info("=" * 70)
    logger.info("Years:              %s", ", ".join(map(str, years)))
    logger.info("Events:             %s", events if events else "All events")
    logger.info(
        "Session types:      %s", session_types if session_types else "From config"
    )
    logger.info("Skip existing:      %s", not args.force)
    logger.info("Validate data:      %s", not args.no_validate)
    logger.info("Delay (seconds):    %d", args.delay)
    logger.info("=" * 70)

    # Initialize pipeline
    pipeline = IngestionPipeline(
        skip_existing=not args.force,
        validate_data=not args.no_validate,
        delay_between_sessions=args.delay,
    )

    try:
        # Case 1: Single session
        if args.event and args.session:
            logger.info("Mode: Single session ingestion")
            result = pipeline.ingest_session(
                year=years[0], event=args.event, session_type=args.session
            )

            if result.success:
                logger.info("\n✅ Session ingested successfully!")
            else:
                logger.error("\n❌ Session ingestion failed: %s", result.error_message)
                sys.exit(1)

        # Case 2: Single race weekend
        elif args.event and not args.session:
            logger.info("Mode: Race weekend ingestion")
            _ = pipeline.ingest_race_weekend(
                year=years[0], event=args.event, session_types=session_types
            )

            pipeline.print_summary()

        # Case 3: Single season
        elif len(years) == 1 and not args.event:
            logger.info("Mode: Single season ingestion")
            _ = pipeline.ingest_season(
                year=years[0], events=events, session_types=session_types
            )

        # Case 4: Multiple seasons
        else:
            logger.info("Mode: Multiple seasons ingestion")
            _ = pipeline.ingest_multiple_seasons(
                years=years, events=events, session_types=session_types
            )

            pipeline.print_summary()

        # Save results if requested
        if args.save_results:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filepath = args.save_results.replace(".json", f"_{timestamp}.json")
            pipeline.save_results(filepath)

        # Check if there were any failures
        failed = pipeline.get_failed_sessions()
        if failed:
            logger.warning(
                "\n⚠️  %d sessions failed. Review the logs for details.", len(failed)
            )

            # Ask if user wants to retry
            retry = input("\nRetry failed sessions? (y/n): ").strip().lower()
            if retry == "y":
                logger.info("Retrying failed sessions...")
                retry_results = pipeline.retry_failed_sessions()

                still_failed = [r for r in retry_results if not r.success]
                if still_failed:
                    logger.error(
                        "❌ %d sessions still failed after retry", len(still_failed)
                    )
                else:
                    logger.info("✅ All failed sessions successfully retried!")

        logger.info("=" * 70)
        logger.info("INGESTION COMPLETE")
        logger.info("=" * 70)

    except KeyboardInterrupt:
        logger.warning("\n\n⚠️  Ingestion interrupted by user")
        logger.info("Progress has been saved. You can resume later.")
        pipeline.print_summary()
        sys.exit(130)

    except Exception as e:  # pylint: disable=broad-except
        logger.error("\n❌ Ingestion failed with error: %s", e, exc_info=True)
        pipeline.print_summary()
        sys.exit(1)


if __name__ == "__main__":
    main()
