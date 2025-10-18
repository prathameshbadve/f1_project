"""
FastF1 client that uses environment configuration
"""

from typing import Optional, List

import pandas as pd
import fastf1

from config.settings import fastf1_config
from config.logging import get_logger


class FastF1Client:
    """Extending the FastF1Client with data ingestion methods"""

    def __init__(self):
        self.config = fastf1_config
        self.logger = get_logger("data_ingestion.fastf1_client")

    def get_season_schedule(self, year: int) -> pd.DataFrame:
        """Extracts the season schedule from FastF1 API"""

        try:
            schedule = fastf1.get_event_schedule(
                year=year, include_testing=self.config.include_testing
            )
            self.logger.info(
                "Loaded schedule for season %d: %d total events. Testing included = %s",
                year,
                len(schedule),
                self.config.include_testing,
            )
            return schedule

        except Exception as e:
            self.logger.error("Error loading schedule for %d: %s", year, e)
            raise

    def get_session(self, year: int, event: str, session: str) -> fastf1.core.Session:
        """Get a session from the FastF1 API"""

        try:
            # Use FastF1's get_session function
            session_obj = fastf1.get_session(year, event, session)

            # Configure session based on environment settings
            if hasattr(session_obj, "load"):
                # Load with telemetry based on configuration
                load_laps = self.config.enable_laps
                load_telemetry = self.config.enable_telemetry
                load_weather = self.config.enable_weather_data
                load_race_control_messages = self.config.enable_race_control_messages

                self.logger.info("Loading session %s %s %d", session, event, year)
                self.logger.info(
                    "Laps: %s, Telemetry: %s, Weather: %s, Race Control Messages: %s",
                    load_laps,
                    load_telemetry,
                    load_weather,
                    load_race_control_messages,
                )

                session_obj.load(
                    laps=load_laps,
                    telemetry=load_telemetry,
                    weather=load_weather,
                    messages=load_race_control_messages,
                )

                self.logger.info(
                    "Successfully loaded session object for %s %s %s",
                    session,
                    event,
                    year,
                )

            return session_obj

        except Exception as e:
            self.logger.error(
                "Error loading session %d %s %s: %s", year, event, session, e
            )
            raise

    def get_multiple_sessions(
        self, year: int, events: List[str], sessions: Optional[List[str]]
    ):
        """Extracts session objects for multiple events"""

        if sessions is None:
            sessions = self.config.session_types

        session_objs = {}

        for event in events:
            session_objs[event] = {}
            for session in sessions:
                try:
                    session_obj = self.get_session(year, event, session)
                    session_objs[event][session] = session_obj
                    self.logger.info("Loaded %d %s %s", year, event, session)
                except Exception as e:  # pylint: disable=broad-except
                    self.logger.warning(
                        "Could not load %d %s %s: %s", year, event, session, e
                    )
                    session_objs[event][session] = None

        return session_objs

    def cache_info(self) -> tuple:
        """Get cache information"""

        return fastf1.Cache.get_cache_info()

    def clear_cache(self, deep: bool = False):
        """Clear FastF1 cache"""

        fastf1.Cache.clear_cache(deep=deep)
        self.logger.info("Cache cleared (deep=%s)", deep)
