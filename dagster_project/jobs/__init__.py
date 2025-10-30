"""
Dagster jobs for F1 data pipeline.

Jobs group related assets together for orchestration.
"""

# pylint: disable=assignment-from-no-return

from dagster import define_asset_job, AssetSelection

# Import hooks
from dagster_project.hooks import upload_logs_on_success, upload_logs_on_failure

# Example: Job for specific assets
# specific_job = define_asset_job(
#     name="specific_job",
#     selection=["asset1", "asset2"],  # Specific asset selection
#     description="Specific pipeline",
# ).with_hooks({upload_logs_on_success, upload_logs_on_failure})


# ============================================================================
# CONFIGURABLE JOBS (Any Year/Event/Session)
# ============================================================================


f1_configurable_session_job = define_asset_job(
    name="f1_configurable_session",
    description="Ingest any F1 session by providing year, event_name, and session_type as config",
    selection=AssetSelection.groups("raw_configurable"),
    tags={
        "type": "configurable",
        "scope": "as_per_config",
    },
    hooks={upload_logs_on_success, upload_logs_on_failure},
)

f1_configurable_circuits_job = define_asset_job(
    name="f1_configurable_circuits",
    description="Ingest circuits information for any F1 season by giving the year",
    selection=AssetSelection.groups("raw_configurable_circuits"),
    tags={"type": "configurable", "scope": "year"},
    hooks={upload_logs_on_success, upload_logs_on_failure},
)


# Job to build complete catalog
build_catalog_job = define_asset_job(
    name="build_catalog_job",
    description="Build complete F1 race data catalog with validation",
    selection=AssetSelection.groups("catalog"),
    tags={"team": "data-engineering", "priority": "high"},
)


# Job to refresh just the report (faster)
refresh_catalog_report_job = define_asset_job(
    name="refresh_catalog_report_job",
    description="Regenerate catalog report without rebuilding catalog",
    selection=AssetSelection.keys(["catalog", "catalog_summary_report"]),
    tags={"team": "data-engineering", "priority": "low"},
)


# Export all jobs
all_jobs = [
    f1_configurable_session_job,
    f1_configurable_circuits_job,
    build_catalog_job,
    refresh_catalog_report_job,
]
