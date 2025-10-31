"""
Ergast client based on the FastF1 Client.
Current use: Get Circuit Information
"""

import pandas as pd
from fastf1.ergast import Ergast

from config.logging import get_logger
from config.settings import fastf1_config


class ErgastClient:
    """Creating a Ergast Client for downloading the circuit informaiton"""

    def __init__(self):
        self.fastf1_config = fastf1_config
        self.client = Ergast()
        self.logger = get_logger("data_ingestion.ergast_client")

    def get_season_circuits(self, season: int) -> pd.DataFrame:
        """Downloads the circuits information for all circuits in the season"""

        try:
            circuits = self.client.get_circuits(season=season)
            self.logger.info(
                "| | | | | | Successfully loaded the circuit information for season %d",
                season,
            )
            return circuits
        except Exception as e:
            self.logger.error(
                "| | | | | | Error loading circuits for %d: %s", season, e
            )
            raise
