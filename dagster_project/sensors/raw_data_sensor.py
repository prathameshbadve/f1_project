"""
Sensor to detect new files in raw bucket and trigger catalog rebuild
"""

import json

import pandas as pd
from dagster import (
    sensor,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    DefaultSensorStatus,
)

from dagster_project.resources import StorageResource, CatalogConfig


@sensor(
    name="raw_data_change_sensor",
    job_name="build_catalog_job",  # Will define this next
    default_status=DefaultSensorStatus.STOPPED,  # Start disabled, enable manually
    minimum_interval_seconds=300,  # Check every 5 minutes
    description="Detects new files in raw bucket and triggers catalog rebuild",
)
def raw_data_change_sensor(
    context: SensorEvaluationContext,
    storage_resource: StorageResource,
    catalog_config: CatalogConfig,
) -> SensorResult:
    """
    Monitor raw bucket for changes and trigger catalog rebuild when detected.

    Logic:
    1. List all files in raw bucket
    2. Calculate hash/fingerprint of bucket state
    3. Compare to last known state (stored in cursor)
    4. If changed, yield RunRequest to rebuild catalog
    5. Update cursor with new state
    """

    storage_client = storage_resource.create_client()

    try:
        # Get current bucket state
        current_state = get_bucket_state(storage_client, catalog_config.raw_bucket)

        # Get last known state from cursor
        last_state = context.cursor or None

        # Compare states
        if last_state is None:
            # First run - initialize cursor
            context.log.info("First run - initializing sensor state")
            return SensorResult(
                skip_reason="Initializing sensor - no action taken",
                cursor=current_state,
            )

        if current_state != last_state:
            # Changes detected!
            changes = detect_changes(last_state, current_state)

            context.log.info("Changes detected in raw bucket:")
            context.log.info(f"  New files: {changes.get('new_files', 0)}")
            context.log.info(f"  Modified files: {changes.get('modified_files', 0)}")
            context.log.info(f"  Deleted files: {changes.get('deleted_files', 0)}")

            # Yield run request to rebuild catalog
            return SensorResult(
                run_requests=[
                    RunRequest(
                        run_key=f"catalog_rebuild_{context.cursor_updated()}",
                        run_config={},
                        tags={
                            "trigger": "raw_data_change",
                            "new_files": str(changes.get("new_files", 0)),
                        },
                    )
                ],
                cursor=current_state,
            )
        else:
            # No changes
            return SensorResult(
                skip_reason="No changes detected in raw bucket", cursor=current_state
            )

    except Exception as e:  # pylint: disable=broad-except
        context.log.error(f"Sensor evaluation failed: {e}")
        # Don't update cursor on error - will retry next interval
        return SensorResult(
            skip_reason=f"Sensor error: {str(e)}", cursor=context.cursor
        )


def get_bucket_state(storage_client, bucket: str) -> str:
    """
    Get fingerprint of current bucket state.

    Returns JSON string with:
    - File count per year
    - Total file count
    - Last modified timestamp

    This is lightweight - doesn't hash every file, just counts
    """

    try:
        # List all files
        all_files = storage_client.list_objects(bucket=bucket, prefix="")

        # Group by year
        files_by_year = {}
        for file_path in all_files:
            parts = file_path.split("/")
            if len(parts) > 0 and parts[0].isdigit():
                year = parts[0]
                files_by_year[year] = files_by_year.get(year, 0) + 1

        state = {
            "total_files": len(all_files),
            "files_by_year": files_by_year,
            "last_checked": pd.Timestamp.now().isoformat(),
        }

        return json.dumps(state, sort_keys=True)

    except Exception as e:  # pylint: disable=broad-except
        # Return empty state on error
        return json.dumps({"error": str(e)})


def detect_changes(old_state: str, new_state: str) -> dict:
    """
    Compare two bucket states and return summary of changes.

    Returns dict with:
    - new_files: Number of new files added
    - deleted_files: Number of files deleted
    - modified_files: Estimate of modified files
    """

    try:
        old = json.loads(old_state)
        new = json.loads(new_state)

        old_total = old.get("total_files", 0)
        new_total = new.get("total_files", 0)

        changes = {
            "new_files": max(0, new_total - old_total),
            "deleted_files": max(0, old_total - new_total),
            "modified_files": 0,  # Can't easily detect without file hashes
        }

        return changes

    except Exception:  # pylint: disable=broad-exception-caught
        return {"new_files": 0, "deleted_files": 0, "modified_files": 0}
