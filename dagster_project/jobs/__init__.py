"""
Dagster jobs for F1 data pipeline.

Jobs group related assets together for orchestration.
"""

# pylint: disable=assignment-from-no-return

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


# ============================================================================
# SEASON JOBS
# ============================================================================


f1_2024_season_all_sessions_job = define_asset_job(
    name="f1_2024_season_all_sessions",
    description="Ingest ALL sessions for 2024 season (100+ sessions)",
    selection=AssetSelection.groups("raw_2024_season"),
    tags={
        "year": "2024",
        "type": "full_season",
        "scope": "all_sessions",
    },
)


# ============================================================================
# CONFIGURABLE JOB (Any Year/Event/Session)
# ============================================================================


f1_configurable_session_job = define_asset_job(
    name="f1_configurable_session",
    description="Ingest any F1 session by providing year, event_name, and session_type as config",
    selection=AssetSelection.groups("raw_configurable"),
    tags={
        "type": "configurable",
        "scope": "as_per_config",
    },
)


# Note: To use the configurable job:
# 1. In Dagster UI: Jobs → f1_configurable_session → Launch Run
# 2. Provide config:
#    {
#      "ops": {
#        "f1_session_configurable": {
#          "config": {
#            "year": 2023,
#            "event_name": "Monaco Grand Prix",
#            "session_type": "R"
#          }
#        }
#      }
#    }
#
# OR directly materialize the asset with config in the Assets tab
