"""
Dagster assets for F1 data pipeline.

Assets are organized into modules:
- raw_ingestion: Individual race weekend assets (Italian GP 2024)
- season_ingestion: Season-level partitioned assets (2024 season) + configurable asset
- processed: Processed and cleaned data
- features: ML-ready features
"""

# Import race weekend assets
# from dagster_project.assets.raw_ingestion import (
#     italian_gp_2024_fp1,
#     italian_gp_2024_fp2,
#     italian_gp_2024_fp3,
#     italian_gp_2024_qualifying,
#     italian_gp_2024_race,
# )

# # Import season-level assets
# from dagster_project.assets.season_ingestion import (
#     f1_2024_session_raw,
#     f1_2024_season_summary,
# )

from dagster_project.assets.full_ingestion import (
    f1_session_configurable,  # NEW: Runtime configurable asset
)


# Export all assets so Dagster can discover them
__all__ = [
    # Italian GP 2024 - Individual weekend assets
    # "italian_gp_2024_fp1",
    # "italian_gp_2024_fp2",
    # "italian_gp_2024_fp3",
    # "italian_gp_2024_qualifying",
    # "italian_gp_2024_race",
    # # 2024 Season - Partitioned assets
    # "f1_2024_session_raw",
    # "f1_2024_season_summary",
    # Configurable asset (any year/event/session)
    "f1_session_configurable",
]
