"""
Schedule loader built on top of the FastF1 Client
"""

from typing import Optional, List

import pandas as pd

from config.settings import fastf1_config, storage_config
from config.logging import get_logger

from src.clients.fastf1_client import FastF1Client
from src.clients.storage_client import StorageClient
from src.clients.redis_client import RedisClient


class ScheduleLoader:
    """Loads F1 season schedules"""

    def __init__(
        self,
        client: Optional[FastF1Client] = None,
        storage_client: Optional[StorageClient] = None,
        redis_client: Optional[RedisClient] = None,
    ):
        self.client = client or FastF1Client()
        self.storage_client = storage_client or StorageClient()
        self.redis_client = redis_client or RedisClient()
        self.config = fastf1_config
        self.storage_config = storage_config
        self.data_type = "schedule"
        self.logger = get_logger("data_ingestion.schedule_loader")

        # Storage Client has methods that create the object key and
        # check if the file already exists in the bucket.

        # To create object_key use StorageClient.build_object_key(data_type, year, event, session)
        # To check if object exitst use StorageClient.object_exists(object_key) -> bool

    def _is_schedule_file_valid(
        self, object_key: str
    ) -> tuple[bool, Optional[pd.DataFrame]]:
        """Checks if the existing file in the bucket is valid"""

        if not self.storage_client.object_exists(object_key):
            self.logger.warning("| | | Schedule file does not exist: %s", object_key)
            return False, None

        try:
            schedule_data = self.storage_client.download_dataframe(object_key)

            if schedule_data.empty:
                self.logger.warning(
                    "| | | Schedule file exists but is empty/invalid: %s", object_key
                )
                return False, None

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
                    "| | | Schedule file missing required columns %s: %s",
                    missing_columns,
                    object_key,
                )
                return False, None

            self.logger.debug("| | | Schedule file valid: %s", object_key)
            return True, schedule_data

        except Exception as e:  # pylint: disable=broad-except
            self.logger.warning(
                "| | | Error validating schedule file %s: %s", object_key, str(e)
            )
            return False, None

    def _load_schedule_from_storage(self, year: int) -> Optional[pd.DataFrame]:
        """Loads schdeule file from storage if it exists"""

        self.logger.info("| | | Attempting to load from bucket storage.")
        object_key = self.storage_client.build_object_key(self.data_type, year)

        # Check if the schedule file is valid
        valid_file, cached_schedule = self._is_schedule_file_valid(object_key)
        if not valid_file:
            return None

        self.logger.debug(
            "| | | Loaded schedule for %d from local file: %s (%s events)",
            year,
            object_key,
            len(cached_schedule),
        )
        return cached_schedule

    def _load_schedule_from_redis(self, year: int) -> Optional[pd.DataFrame]:
        """Load dataframe to redis cache if exists, None otherwise"""

        if not self.redis_client.is_available():
            self.logger.debug("Redis not available, skipping cache check")
            return None

        try:
            # Get schedule from redis
            cached_schedule = self.redis_client.get_schedule(year)

            if cached_schedule is None or cached_schedule.empty:
                self.logger.warning(
                    "| | | Redis cache contains null or empty schedule for %d", year
                )
                self._clear_schedule_from_redis(year)
                return None

            # Check if the loaded schedule is valid
            required_columns = [
                "RoundNumber",
                "EventName",
                "Session1",
                "Session2",
                "Session3",
                "Session4",
                "Session5",
            ]
            if not all(col in cached_schedule.columns for col in required_columns):
                self.logger.warning(
                    "| | | Schedule columns is missing required columns."
                )
                self._clear_schedule_from_redis(year)
                return None

            # Cached schedule passed all validations
            self.logger.info(
                "| | | Redis cache HIT for %d schedule (%d events)",
                year,
                len(cached_schedule),
            )
            return cached_schedule

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error(
                "| | | Error while loading schedule from redis for %d: %s", year, str(e)
            )
            return None

    def _save_schedule_to_redis(self, year: int, df: pd.DataFrame) -> bool:
        """Save schedule dataframe to redis cache"""

        if not self.redis_client.is_available():
            self.logger.debug("| | | Redis not available, skipping cache save.")
            return False

        try:
            success = self.redis_client.set_schedule(year=year, df=df)

            if success:
                self.logger.info("| | | ✅ Saved schedule to Redis: %d", year)
            else:
                self.logger.warning(
                    "| | | Failed to save schedule to Redis for %d", year
                )

            return success

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error(
                "| | | Error while saving schedule for %d to cache: %s", year, str(e)
            )
            return False

    def _clear_schedule_from_redis(self, year: int) -> bool:
        """
        Clear schedule from Redis cache.

        Args:
            year: Season year

        Returns:
            True if cleared successfully, False otherwise
        """

        if not self.redis_client.is_available():
            return False

        try:
            success = self.redis_client.delete_schedule(year)
            if success:
                self.logger.info("| | | Cleared Redis cache for schedule: %d", year)
            return success

        except Exception as e:  # pylint: disable=broad-except
            self.logger.warning(
                "| | | Error clearing schedule from Redis for %d: %s", year, str(e)
            )
            return False

    def _enhance_schedule_data(
        self, schedule_df: pd.DataFrame, year: int
    ) -> pd.DataFrame:
        """Enhance the schedule data with the season column"""

        self.logger.debug("| | | Enhancing schedule data")

        enhanced_schedule = schedule_df.copy()
        enhanced_schedule["Season"] = year  # Add year column

        return enhanced_schedule

    def load_season_schedule(
        self,
        year: int,
        save_to_file: bool = False,
        force_refresh: bool = False,
    ) -> Optional[pd.DataFrame]:
        """
        Loads season schedule with Redis caching

        Cache hierarchy:
        1. Redis
        2. Bucket storage - MinIO / S3
        3. FastF1 API

        Args:
            - year: int
            - save_to_file: bool
            - force_refresh: bool

        Returns:
            Season schedule DataFrame
        """

        self.logger.info(
            "| | | Loading season schedule for %d (force_refresh=%s)",
            year,
            force_refresh,
        )

        # Step 0: If force_refresh, clear Redis cache
        if force_refresh:
            self._clear_schedule_from_redis(year)

        if not force_refresh:
            # Step 1: Load schedule from redis cache
            redis_schedule = self._load_schedule_from_redis(year)
            if redis_schedule is not None:
                return redis_schedule

            # Step 2: Load schedule from bucket storage
            storage_schedule = self._load_schedule_from_storage(year)
            if storage_schedule is not None:
                # Populate Redis cache for next time
                self._save_schedule_to_redis(year, storage_schedule)
                return storage_schedule

        # Step 3: Load from API if no valid local file or force_refresh is True
        self.logger.info("| | | Loading schedule from API for %d", year)

        try:
            schedule = self.client.get_season_schedule(year)

            # Add computed columns
            schedule = self._enhance_schedule_data(schedule, year)

            # Add enhanced schedule to redis cache
            self._save_schedule_to_redis(year=year, df=schedule)

            if save_to_file:
                object_key = self.storage_client.build_object_key(self.data_type, year)
                self.storage_client.upload_dataframe(schedule, object_key)

            return schedule

        except Exception as e:
            self.logger.error(
                "| | | Failed to load season schedule for %d: %s", year, str(e)
            )
            raise

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
            "| | | Getting events for ingestion: %d (force_refresh=%s)",
            year,
            force_refresh,
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
                    self.logger.debug(
                        "| | | Skipping event with missing name: %s", event
                    )
                    continue

                # Skip testing events if not configured to include them
                if not self.config.include_testing and "Test" in str(event_name):
                    self.logger.debug("| | | Skipping testing event: %s", event_name)
                    continue

                events_to_ingest.append(event_name)

            self.logger.info(
                "| | | Found %s events for ingestion", len(events_to_ingest)
            )

            # Log sample events for verification
            if events_to_ingest:
                sample_events = events_to_ingest[:3]
                self.logger.debug("| | | Sample events: %s", sample_events)

            return events_to_ingest

        except Exception as e:
            self.logger.error(
                "| | | Failed to get events for ingestion %d: %s", year, str(e)
            )
            raise

    def get_sessions_to_load(
        self,
        year: int,
        event_name: str,
    ) -> List[str]:
        """Get list of session types to load for an event based on its format"""

        self.logger.info("| | | Loading sessions to load for '%s' %d", event_name, year)

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
                "| | | Failed to get sessions to load for '%s' %d: %s",
                event_name,
                year,
                str(e),
            )
            raise

    def refresh_schedule(self, year: int):
        """
        Force refresh schedule from API and update local cache

        Args:
            year: Season year

        Returns:
            Fresh schedule data
        """

        self.logger.info("| | | Force refreshing schedule for %d", year)

        # Clear Redis cache
        self._clear_schedule_from_redis(year)

        # Load fresh data (this will repopulate caches)
        return self.load_season_schedule(year, save_to_file=True, force_refresh=True)
