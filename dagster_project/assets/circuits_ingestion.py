"""
Ingest circuits information for seasons
"""

from typing import Dict, Any

from dagster import Config, asset, AssetExecutionContext, Output

from config.logging import get_logger
from src.data_ingestion.circuit_loader import CircuitLoader

# ============================================================================
# LOGGING SETUP
# ============================================================================

logger = get_logger("data_ingestion.dagster")


# ============================================================================
# CONFIGURABLE ASSET (Any season)
# ============================================================================


class F1SeasonConfig(Config):
    """
    Configuration for ingesting F1 season circuits
    """

    year: int


@asset(
    group_name="raw_configurable_circuits",
    compute_kind="ergast",
    description="Ingest any F1 season circuits by providing the year.",
)
def season_circuits_configurable(
    context: AssetExecutionContext, config: F1SeasonConfig
) -> Output[Dict[str, Any]]:
    """Ingest season circuits"""

    year = config.year

    circuit_loader = CircuitLoader()

    context.log.info("=" * 70)
    context.log.info("Ingesting circuits information for season %d", year)
    context.log.info("=" * 70)

    logger.info("=" * 70)
    logger.info("Ingesting circuits information for season %d", year)
    logger.info("=" * 70)

    try:
        circuits = circuit_loader.load_season_circuits(
            year=year,
            save_to_storage=True,
            force_refresh=False,
        )
        context.log.info(
            "| ✅ Success - Loaded circuits for season %d: Total %d circuits",
            year,
            len(circuits),
        )
        logger.info(
            "| ✅ Success - Loaded circuits for season %d: Total %d circuits",
            year,
            len(circuits),
        )

        results = {"year": year, "success": True}

    except Exception as e:  # pylint: disable=broad-except
        context.log.error("| ❌ Failed: %s", str(e))
        logger.error("| ❌ Failed: %s", str(e))
        results = {"year": year, "success": False}

    context.log.info("=" * 70)
    context.log.info("Ingesting complete!")
    context.log.info("Status: %s", "✅" if results["success"] else "❌")
    context.log.info("=" * 70)

    logger.info("=" * 70)
    logger.info("Ingesting complete!")
    logger.info("Status: %s", "✅" if results["success"] else "❌")
    logger.info("=" * 70)

    metadata = {
        "year": year,
        "status": "✅" if results["success"] else "❌",
    }

    return Output(value=results, metadata=metadata)
