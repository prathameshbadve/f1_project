# """
# Dagster jobs for F1 data pipeline
# Jobs group assets together for execution
# """

# from dagster import define_asset_job, AssetSelection, RunRequest

# # ============================================================================
# # INGESTION JOBS
# # ============================================================================

# # pylint: disable=assignment-from-no-return

# # Job to ingest current race weekend data
# race_weekend_ingestion_job = define_asset_job(
#     name="race_weekend_ingestion",
#     description="Ingest data for the most recent race weekend. Runs after each race.",
#     selection=AssetSelection.keys("raw_session_data", "season_schedule"),
#     tags={
#         "type": "ingestion",
#         "frequency": "race-weekend",
#         "team": "data-engineering",
#         "priority": "high",
#     },
# )

# # Job to ingest full season (incremental)
# season_ingestion_job = define_asset_job(
#     name="season_ingestion",
#     description="Ingest all available data for current season. Skips already ingested sessions.",
#     selection=AssetSelection.keys("raw_session_data", "season_schedule"),
#     tags={
#         "type": "ingestion",
#         "frequency": "daily",
#         "team": "data-engineering",
#         "priority": "high",
#     },
# )

# # Job to backfill historical data
# historical_backfill_job = define_asset_job(
#     name="historical_backfill",
#     description="Backfill historical F1 data for multiple seasons. Configure years via tags.",
#     selection=AssetSelection.keys("historical_session_data"),
#     tags={
#         "type": "backfill",
#         "frequency": "on-demand",
#         "team": "data-engineering",
#         "priority": "low",
#     },
# )

# # ============================================================================
# # PROCESSING JOBS
# # ============================================================================

# # Job to process raw data
# data_processing_job = define_asset_job(
#     name="data_processing",
#     description="Process raw session data into structured format for analysis.",
#     selection=AssetSelection.keys("processed_race_data"),
#     tags={
#         "type": "processing",
#         "frequency": "weekly",
#         "team": "data-engineering",
#         "priority": "medium",
#     },
# )


# # Job to generate ML features
# ml_features_job = define_asset_job(
#     name="ml_features_generation",
#     description="Generate ML features from processed data for model training.",
#     selection=AssetSelection.keys("ml_features"),
#     tags={
#         "type": "ml",
#         "frequency": "weekly",
#         "team": "ml-engineering",
#         "priority": "medium",
#     },
# )


# # ============================================================================
# # COMBINED JOBS
# # ============================================================================

# # Full pipeline: Ingest -> Process -> Features
# full_pipeline_job = define_asset_job(
#     name="full_pipeline",
#     description="Run complete pipeline: ingestion, processing, and feature generation.",
#     selection=AssetSelection.keys(
#         "raw_session_data", "season_schedule", "processed_race_data", "ml_features"
#     ),
#     tags={
#         "type": "full-pipeline",
#         "frequency": "on-demand",
#         "team": "data-engineering",
#         "priority": "medium",
#     },
# )


# # Weekly batch job
# weekly_batch_job = define_asset_job(
#     name="weekly_batch",
#     description="Weekly job to process and generate features from already-ingested data.",
#     selection=AssetSelection.keys("processed_race_data", "ml_features"),
#     tags={
#         "type": "batch",
#         "frequency": "weekly",
#         "team": "data-engineering",
#         "priority": "medium",
#     },
# )


# # ============================================================================
# # HELPER FUNCTIONS FOR DYNAMIC JOB CREATION
# # ============================================================================


# def create_backfill_request(years: list[int]) -> RunRequest:
#     """
#     Create a run request for historical backfill with specific years.

#     Usage:
#         from dagster import RunRequest
#         request = create_backfill_request([2020, 2021, 2022])
#         # Then submit this request via Dagster UI or API
#     """
#     return RunRequest(
#         run_key=f"backfill_{'_'.join(map(str, years))}",
#         tags={
#             "backfill_years": ",".join(map(str, years)),
#             "type": "backfill",
#         },
#     )


# def create_specific_event_request(year: int, event_name: str) -> RunRequest:
#     """
#     Create a run request for a specific event.

#     Usage:
#         request = create_specific_event_request(2024, "Monaco")
#     """
#     return RunRequest(
#         run_key=f"event_{year}_{event_name}",
#         tags={
#             "year": str(year),
#             "event": event_name,
#             "type": "specific-event",
#         },
#     )


"""
Dagster jobs for F1 data pipeline.

Jobs group related assets together for orchestration.
"""

from dagster import define_asset_job, AssetSelection


# ============================================================================
# RACE WEEKEND JOBS
# ============================================================================


italian_gp_2024_weekend_job = define_asset_job(
    name="italian_gp_2024_weekend",
    description="Ingest all sessions from Italian Grand Prix 2024 weekend",
    selection=AssetSelection.groups("raw_italian_gp_2024"),
    tags={
        "event": "Italian Grand Prix",
        "year": "2024",
        "type": "race_weekend",
    },
)
