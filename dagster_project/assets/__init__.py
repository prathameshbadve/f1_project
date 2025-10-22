# """
# Dagster assets for F1 data pipeline
# Assets represent data products that are created by the pipeline
# """

# from datetime import datetime
# from typing import Dict, Any

# from dagster import asset, AssetExecutionContext, Output, MetadataValue

# from dagster_project.resources import StorageResource
# from config.settings import is_development
# from src.data_ingestion.schedule_loader import ScheduleLoader
# from src.data_ingestion.data_ingestion_pipeline import IngestionPipeline

# # ============================================================================
# # RAW DATA INGESTION ASSETS (using the data_ingestion_pipeline)
# # ============================================================================


# @asset(
#     group_name="ingestion",
#     compute_kind="fastf1",
#     description="Raw race results data from FastF1 API stored in S3/MinIO",
# )
# def raw_session_data(
#     context: AssetExecutionContext,
#     storage: StorageResource,
# ) -> Output[Dict[str, Any]]:
#     """
#     Ingest F1 session data using the existing IngestionPipeline.

#     In development: Ingests only the most recent race weekend
#     In production: Ingests the current season
#     """

#     # Determine what to load based on environment
#     schedule_loader = ScheduleLoader()
#     current_year = datetime.now().year

#     if is_development():
#         # Local: Get only the most recent event
#         context.log.info(
#             f"DEVELOPMENT MODE: Ingesting most recent event for {current_year}"
#         )

#         # Get schedule and find most recent completed event
#         schedule = schedule_loader.load_season_schedule(current_year)

#         # Filter to events that have already happened
#         now = datetime.now()
#         past_events = schedule[schedule["EventDate"] < now]

#         if len(past_events) == 0:
#             context.log.warning("No completed events found for current season")
#             return Output(
#                 value={"year": current_year, "events_ingested": 0},
#                 metadata={"status": "no_events_available"},
#             )

#         # Get the most recent event
#         most_recent = past_events.iloc[-1]
#         events_to_ingest = [most_recent["EventName"]]
#         context.log.info(f"Will ingest: {events_to_ingest[0]}")

#     else:
#         # Production: Ingest full season
#         context.log.info(f"PRODUCTION MODE: Ingesting full season {current_year}")
#         events_to_ingest = None  # None means all events

#         # Initialize your existing pipeline
#         pipeline = IngestionPipeline(
#             skip_existing=True,
#             validate_data=True,
#         )

#         # Run ingestion
#         context.log.info("Starting ingestion pipeline...")
#         results = pipeline.ingest_season(
#             year=current_year,
#             events=events_to_ingest,
#             session_types=None,  # Use config defaults
#         )

#         # Collect statistics
#         successful = sum(1 for r in results if r.success)
#         failed = sum(1 for r in results if not r.success)
#         skipped = sum(
#             1
#             for r in results
#             if r.success and r.error_message and "Skipped" in r.error_message
#         )

#         # Log failures for visibility
#         if failed > 0:
#             context.log.warning(f"Failed sessions: {failed}")
#             for result in results:
#                 if not result.success:
#                     context.log.warning(
#                         f"  ❌ {result.year} {result.event_name} {result.session_type}: "
#                         f"{result.error_message}"
#                     )

#         # Return results with rich metadata
#         return Output(
#             value={
#                 "year": current_year,
#                 "events_ingested": len(events_to_ingest)
#                 if events_to_ingest
#                 else successful,
#                 "total_sessions": len(results),
#                 "successful": successful,
#                 "failed": failed,
#                 "skipped": skipped,
#             },
#             metadata={
#                 "year": current_year,
#                 "total_sessions": len(results),
#                 "successful_sessions": successful,
#                 "failed_sessions": failed,
#                 "skipped_sessions": skipped,
#                 "validation_errors": pipeline.stats.total_validation_errors,
#                 "files_uploaded": pipeline.stats.total_files_uploaded,
#                 "processing_time_minutes": MetadataValue.float(
#                     pipeline.stats.total_processing_time_seconds / 60
#                 ),
#                 "storage_bucket": storage.bucket_raw,
#                 # Add a preview of first successful result
#                 "sample_result": MetadataValue.json(
#                     {
#                         "event": results[0].event_name if results else None,
#                         "session": results[0].session_type if results else None,
#                         "validation_errors": results[0].validation_errors
#                         if results
#                         else 0,
#                     }
#                 ),
#             },
#         )


# @asset(
#     group_name="ingestion",
#     compute_kind="fastf1",
#     description="Season schedule metadata from FastF1",
# )
# def season_schedule(
#     context: AssetExecutionContext,
#     storage: StorageResource,
# ) -> Output[Dict[str, Any]]:
#     """
#     Load and store the F1 season schedule.
#     This is metadata about when races occur.
#     """

#     schedule_loader = ScheduleLoader()
#     current_year = datetime.now().year

#     context.log.info(f"Loading season schedule for {current_year}")

#     # Load schedule
#     schedule = schedule_loader.load_season_schedule(current_year)

#     # Upload to storage
#     object_key = f"schedules/{current_year}/season_schedule.parquet"
#     storage.upload_dataframe(schedule, object_key)

#     context.log.info(f"Schedule uploaded: {len(schedule)} events")

#     return Output(
#         value={
#             "year": current_year,
#             "total_events": len(schedule),
#         },
#         metadata={
#             "year": current_year,
#             "total_events": len(schedule),
#             "first_event": schedule.iloc[0]["EventName"] if len(schedule) > 0 else None,
#             "last_event": schedule.iloc[-1]["EventName"] if len(schedule) > 0 else None,
#             "storage_bucket": storage.bucket_raw,
#             "schedule_preview": MetadataValue.md(
#                 schedule.head(5).to_markdown() if len(schedule) > 0 else "No events"
#             ),
#         },
#     )


# # ============================================================================
# # PROCESSED DATA ASSETS
# # ============================================================================


# @asset(
#     group_name="processing",
#     compute_kind="python",
#     description="Processed and cleaned F1 data ready for analysis",
#     deps=[raw_session_data],
# )
# def processed_race_data(
#     context: AssetExecutionContext,
#     storage: StorageResource,
# ) -> Output[Dict[str, Any]]:
#     """
#     Process raw session data into structured format.

#     This combines data from multiple sessions (Practice, Qualifying, Race)
#     into race weekend summaries.
#     """

#     context.log.info("Processing raw session data...")

#     # List all raw data files
#     current_year = datetime.now().year
#     raw_files = storage.list_objects(
#         bucket=storage.bucket_raw, prefix=f"sessions/{current_year}/"
#     )

#     context.log.info(f"Found {len(raw_files)} raw session files")

#     # Group by event (you'll need to implement this logic)
#     # For now, just count what we have

#     events_processed = set()
#     files_by_type = {}

#     for file_path in raw_files:
#         # Parse file path: sessions/{year}/{event}/{session_type}/{data_type}.parquet
#         parts = file_path.split("/")
#         if len(parts) >= 4:
#             event = parts[2]
#             events_processed.add(event)

#             # Count by data type
#             data_type = parts[-1].replace(".parquet", "")
#             files_by_type[data_type] = files_by_type.get(data_type, 0) + 1

#     context.log.info(f"Processing complete: {len(events_processed)} events")

#     return Output(
#         value={
#             "year": current_year,
#             "events_processed": len(events_processed),
#             "files_processed": len(raw_files),
#         },
#         metadata={
#             "year": current_year,
#             "events_processed": len(events_processed),
#             "total_files": len(raw_files),
#             "files_by_type": MetadataValue.json(files_by_type),
#             "storage_bucket": storage.bucket_processed,
#         },
#     )


# @asset(
#     group_name="ml",
#     compute_kind="python",
#     description="ML-ready features derived from processed race data",
#     deps=[processed_race_data],
# )
# def ml_features(
#     context: AssetExecutionContext,
#     storage: StorageResource,
# ) -> Output[Dict[str, Any]]:
#     """
#     Generate ML features from processed data.

#     Features include:
#     - Driver performance metrics
#     - Lap time statistics
#     - Position changes
#     - Tire strategy features
#     - Weather conditions
#     """

#     context.log.info("Generating ML features...")

#     # This is where you'd implement feature engineering
#     # For now, placeholder

#     features_generated = 0

#     context.log.info(f"Generated {features_generated} feature sets")

#     return Output(
#         value={
#             "features_generated": features_generated,
#         },
#         metadata={
#             "features_generated": features_generated,
#             "storage_bucket": storage.bucket_processed,
#             "timestamp": datetime.now().isoformat(),
#         },
#     )


# # ============================================================================
# # BACKFILL ASSETS (for historical data)
# # ============================================================================


# @asset(
#     group_name="backfill",
#     compute_kind="fastf1",
#     description="Historical F1 data backfill for multiple seasons",
# )
# def historical_session_data(
#     context: AssetExecutionContext,
#     storage: StorageResource,
# ) -> Output[Dict[str, Any]]:
#     """
#     Backfill historical F1 data for multiple seasons.

#     This asset is typically run manually to populate historical data
#     for training ML models.

#     Configure years to backfill via run configuration.
#     """

#     # Get years from run config (you can pass this when triggering the asset)
#     years_to_backfill = context.run.tags.get("backfill_years", "2022,2023,2024")
#     years = [int(y.strip()) for y in years_to_backfill.split(",")]

#     context.log.info(f"Starting backfill for years: {years}")

#     # Initialize pipeline
#     pipeline = IngestionPipeline(
#         skip_existing=True,
#         validate_data=True,
#         delay_between_sessions=5,  # Slower for historical data
#     )

#     # Run backfill
#     all_results = pipeline.ingest_multiple_seasons(years=years)

#     successful = sum(1 for r in all_results if r.success)
#     failed = sum(1 for r in all_results if not r.success)

#     context.log.info(f"Backfill complete: {successful} successful, {failed} failed")

#     return Output(
#         value={
#             "years": years,
#             "total_sessions": len(all_results),
#             "successful": successful,
#             "failed": failed,
#         },
#         metadata={
#             "years_backfilled": ", ".join(map(str, years)),
#             "total_sessions": len(all_results),
#             "successful_sessions": successful,
#             "failed_sessions": failed,
#             "storage_bucket": storage.bucket_raw,
#         },
#     )


"""
Dagster assets for F1 data pipeline.

Assets are organized into modules:
- raw_ingestion: Raw session data from FastF1 API
- processed: Processed and cleaned data
- features: ML-ready features
"""

# Import all assets from submodules
from dagster_project.assets.raw_ingestion import (
    italian_gp_2024_race,
)

# Export all assets so Dagster can discover them
__all__ = [
    # Raw ingestion assets
    "italian_gp_2024_race",
]
