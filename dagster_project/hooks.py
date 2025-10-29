"""
Dagster hooks for automatic log management.
These hooks handle log uploads at the end of each Dagster run.
"""

from datetime import datetime
from dagster import (
    HookContext,
    failure_hook,
    success_hook,
)

from config.logging import get_logger

logger = get_logger("dagster.hooks")


@success_hook(required_resource_keys={"logging_resource"})
def upload_logs_on_success(context: HookContext):
    """
    Hook that uploads logs to cloud storage after successful run completion.
    """

    run_id = context.run_id
    run_timestamp = datetime.now()

    logger.info("Run %s completed successfully. Uploading logs...", run_id)

    logging_resource = context.resources.logging_resource

    # Upload logs to cloud storage
    upload_success = logging_resource.upload_logs(run_id, run_timestamp)

    if upload_success:
        logger.info("Logs uploaded successfully for run %s", run_id)

        # Optionally archive local logs
        logging_resource.archive_local_logs(run_id)
    else:
        logger.warning("Failed to upload logs for run %s", run_id)


@failure_hook(required_resource_keys={"logging_resource"})
def upload_logs_on_failure(context: HookContext):
    """
    Hook that uploads logs to cloud storage after run failure.
    This ensures logs are preserved even when runs fail.
    """

    run_id = context.run_id
    run_timestamp = datetime.now()

    logger.error("Run %s failed. Uploading logs for debugging...", run_id)

    logging_resource = context.resources.logging_resource

    # Upload logs to cloud storage (especially important for failed runs)
    upload_success = logging_resource.upload_logs(run_id, run_timestamp)

    if upload_success:
        logger.info("Logs uploaded successfully for failed run %s", run_id)

        # Archive local logs
        logging_resource.archive_local_logs(run_id)
    else:
        logger.error("Failed to upload logs for failed run %s", run_id)
