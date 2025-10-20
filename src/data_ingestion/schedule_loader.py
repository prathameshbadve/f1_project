"""
Schedule loader built on top of the FastF1 Client
"""

from typing import Optional, List

import pandas as pd

from config.settings import fastf1_config, storage_config
from config.logging import get_logger

from src.data_ingestion.fastf1_client import FastF1Client
from src.data_ingestion.storage_client import StorageClient


class ScheduleLoader:
    """Loads F1 season schedules"""

    def __init__(
        self,
        client: Optional[FastF1Client] = None,
        storage_client: Optional[StorageClient] = None,
    ):
        self.client = client or FastF1Client()
        self.storage_client = storage_client or StorageClient()
        self.config = fastf1_config
        self.storage_config = storage_config
        self.data_type = "schedule"
        self.logger = get_logger("data_ingestion.schedule_loader")

        # Storage Client has methods that create the object key and
        # check if the file already exists in the bucket.

        # To create object_key use StorageClient.build_object_key(data_type, year, event, session)
        # To check if object exitst use StorageClient.object_exists(object_key) -> bool

    def _is_schedule_file_valid(self, object_key: str) -> bool:
        """Checks if the existing file in the bucket is valid"""

        if not self.storage_client.object_exists(object_key):
            self.logger.warning("Schedule file does not exist: %s", object_key)
            return False

        try:
            schedule_data = self.storage_client.download_dataframe(object_key)

            if schedule_data.empty:
                self.logger.warning(
                    "Schedule file exists but is empty/invalid: %s", object_key
                )
                return False

            # Validate required columns
            required_columns = [
                "EventName",
                "Location",
                "Country",
                "EventFormat",
                "Season",
                "RoundNumber",
                "EventDate",
            ]
            missing_columns = [
                col for col in required_columns if col not in schedule_data.columns
            ]

            if missing_columns:
                self.logger.warning(
                    "Schedule file missing required columns %s: %s",
                    missing_columns,
                    object_key,
                )
                return False

            self.logger.debug("Schedule file valid: %s", object_key)
            return True

        except Exception as e:  # pylint: disable=broad-except
            self.logger.warning(
                "Error validating schedule file %s: %s", object_key, str(e)
            )
            return False

    def _load_schedule_from_storage(self, year: int) -> Optional[pd.DataFrame]:
        """Loads schdeule file from storage if it exists"""

        object_key = self.storage_client.build_object_key(self.data_type, year)

        # Check if the schedule file is valid
        if not self._is_schedule_file_valid(object_key):
            return None

        try:
            schedule = self.storage_client.download_dataframe(object_key)
            if schedule is not None:
                self.logger.info(
                    "Loaded schedule for %d from local file: %s (%s events)",
                    year,
                    object_key,
                    len(schedule),
                )
            return schedule

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error(
                "Failed to load schedule from file %s: %s", object_key, str(e)
            )
            return None

    def load_season_schedule(
        self,
        year: int,
        save_to_file: bool = False,
        force_refresh: bool = False,
    ) -> Optional[pd.DataFrame]:
        """Downloads the season schedule from storage if exists or from API"""

        self.logger.info(
            "Loading season schedule for %d (force_refresh=%s)", year, force_refresh
        )

        if not force_refresh:
            cached_schedule = self._load_schedule_from_storage(year)
            if cached_schedule is not None:
                return cached_schedule

        # Load from API if no valid local file or force_refresh is True
        self.logger.info("Loading schedule from API for %d", year)

        try:
            schedule = self.client.get_season_schedule(year)

            # Add computed columns
            schedule = self._enhance_schedule_data(schedule, year)

            if save_to_file:
                object_key = self.storage_client.build_object_key(self.data_type, year)
                self.storage_client.upload_dataframe(schedule, object_key)

            return schedule

        except Exception as e:
            self.logger.error("Failed to load season schedule for %d: %s", year, str(e))
            raise

    def _enhance_schedule_data(
        self, schedule_df: pd.DataFrame, year: int
    ) -> pd.DataFrame:
        """Enhance the schedule data with the season column"""

        self.logger.debug("Enhancing schedule data")

        enhanced_schedule = schedule_df.copy()
        enhanced_schedule["Season"] = year  # Add year column

        return enhanced_schedule

    def get_events_for_ingestion(
        self, year: int, force_refresh: bool = False
    ) -> List[str]:
        """
        Get list of events that should be ingested based on configuration

        Args:
            year: Season year
            force_refresh: Force refresh schedule from API

        Returns:
            List of event names to ingest
        """

        self.logger.info(
            "Getting events for ingestion: %d (force_refresh=%s)", year, force_refresh
        )

        try:
            schedule = self.load_season_schedule(
                year, save_to_file=False, force_refresh=force_refresh
            )

            # Filter events based on configuration
            events_to_ingest = []

            for _, event in schedule.iterrows():
                event_name = event.get("EventName")

                if not event_name or pd.isna(event_name):
                    self.logger.debug("Skipping event with missing name: %s", event)
                    continue

                # Skip testing events if not configured to include them
                if not self.config.include_testing and "Test" in str(event_name):
                    self.logger.debug("Skipping testing event: %s", event_name)
                    continue

                events_to_ingest.append(event_name)

            self.logger.info("Found %s events for ingestion", len(events_to_ingest))

            # Log sample events for verification
            if events_to_ingest:
                sample_events = events_to_ingest[:3]
                self.logger.debug("Sample events: %s", sample_events)

            return events_to_ingest

        except Exception as e:
            self.logger.error("Failed to get events for ingestion %d: %s", year, str(e))
            raise

    def refresh_schedule(self, year: int):
        """
        Force refresh schedule from API and update local cache

        Args:
            year: Season year

        Returns:
            Fresh schedule data
        """

        self.logger.info("Force refreshing schedule for %d", year)
        return self.load_season_schedule(year, save_to_file=True, force_refresh=True)

    def get_sessions_to_load(
        self,
        year: int,
        event_name: str,
    ) -> List[str]:
        """Get list of session types to load for an event based on its format"""

        self.logger.info("Loading sessions to load for '%s' %d", event_name, year)

        season_schedule = self.load_season_schedule(
            year, save_to_file=False, force_refresh=False
        )

        event_info_df = season_schedule[season_schedule["EventName"] == event_name]

        try:
            sessions_to_load = [
                event_info_df["Session1"].iloc[0],
                event_info_df["Session2"].iloc[0],
                event_info_df["Session3"].iloc[0],
                event_info_df["Session4"].iloc[0],
                event_info_df["Session5"].iloc[0],
            ]

            return sessions_to_load

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error(
                "Failed to get sessions to load for '%s' %d: %s",
                event_name,
                year,
                str(e),
            )
            raise
