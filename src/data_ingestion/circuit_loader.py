"""
Circuit information loader built on top of the Ergast Client
"""

from typing import Optional

import pandas as pd

from config.logging import get_logger

from src.clients.ergast_client import ErgastClient
from src.clients.storage_client import StorageClient


class CircuitLoader:
    """Loads circuit information"""

    def __init__(
        self,
        client: Optional[ErgastClient] = None,
        storage_client: Optional[StorageClient] = None,
    ):
        self.client = client or ErgastClient()
        self.storage_client = storage_client or StorageClient()
        self.logger = get_logger("data_ingestion.circuit_loader")

    def _is_circuits_file_valid(
        self, object_key: str
    ) -> tuple[bool, Optional[pd.DataFrame]]:
        """Checks if the existing circuits file is valid"""

        if not self.storage_client.object_exists(object_key):
            self.logger.warning("| | | Circuits file does not exist: %s", object_key)
            return False, None

        try:
            circuits = self.storage_client.download_dataframe(object_key)

            if circuits.empty:
                self.logger.warning(
                    "| | | Circuit file exists but is empty/invalid: %s", object_key
                )
                return False, None

            required_columns = [
                "circuitId",
                "circuitName",
                "locality",
                "country",
            ]

            missing_columns = [
                col for col in required_columns if col not in circuits.columns
            ]

            if missing_columns:
                self.logger.warning(
                    "| | | Circuits file missing required columns %s: %s",
                    missing_columns,
                    object_key,
                )
                return False, None

            self.logger.debug("| | | Schedule file valid: %s", object_key)
            return True, circuits

        except Exception as e:  # pylint: disable=broad-except
            self.logger.warning(
                "| | | Error validating circuits file %s: %s", object_key, str(e)
            )
            return False, None

    def _load_circuits_from_storage(self, year: int) -> Optional[pd.DataFrame]:
        """Loads schedule from storage if it exists"""

        self.logger.info("| | | Attempting to load from bucket storage.")
        object_key = self.storage_client.build_object_key("circuits", year)

        # Check if the schedule file is valid
        valid_file, cached_circuits = self._is_circuits_file_valid(object_key)
        if not valid_file:
            return None

        self.logger.debug(
            "| | | Loaded circuits for %d from local file: %s (%s events)",
            year,
            object_key,
            len(cached_circuits),
        )
        return cached_circuits

    def _enhance_circuits_data(
        self, circuits_df: pd.DataFrame, season: int
    ) -> pd.DataFrame:
        """Enhance the circuits data with season column"""

        self.logger.debug("| | | Enhancing circuit data")

        enhanced_circuit = circuits_df.copy()
        enhanced_circuit["Season"] = season  # Add year column

        return enhanced_circuit

    def load_season_circuits(
        self,
        year: int,
        save_to_storage: bool = False,
        force_refresh: bool = False,
    ) -> Optional[pd.DataFrame]:
        """
        Loads season circuits information
        """

        self.logger.info(
            "| | | Loading season circuits for %d (force_refresh=%s)",
            year,
            force_refresh,
        )

        if not force_refresh:
            storage_circuits = self._load_circuits_from_storage(year)
            if storage_circuits is not None:
                return storage_circuits

        # Step 3: Load from API if no valid local file or force_refresh is True
        self.logger.info("| | | Loading schedule from API for %d", year)

        try:
            circuits = self.client.get_season_circuits(year)

            # Add computed columns
            circuits = self._enhance_circuits_data(circuits, year)

            if save_to_storage:
                object_key = self.storage_client.build_object_key("circuits", year)
                self.storage_client.upload_dataframe(circuits, object_key)

            return circuits

        except Exception as e:
            self.logger.error(
                "| | | Failed to load season circuits for %d: %s", year, str(e)
            )
            raise

    def refresh_circuits(self, year: int):
        """
        Force refresh circuits from API and update local cache

        Args:
            year: Season year

        Returns:
            Fresh circuits data
        """

        self.logger.info("| | | Force refreshing circuits for %d", year)

        # Load fresh data (this will repopulate caches)
        return self.load_season_circuits(year, save_to_storage=True, force_refresh=True)
