"""
Session loader object loads data on laps,
weather, race control messages.
"""

from typing import Optional, Dict, Any, List

import pandas as pd

from config.settings import fastf1_config, storage_config
from config.logging import get_logger

from src.data_ingestion.fastf1_client import FastF1Client
from src.data_ingestion.schedule_loader import ScheduleLoader
from src.data_ingestion.storage_client import StorageClient


class SessionLoader:
    """Loads and stores raw F1 session data"""

    def __init__(
        self,
        client: Optional[FastF1Client] = None,
        storage_client: Optional[StorageClient] = None,
    ):
        self.client = client or FastF1Client()
        self.schedule_loader = ScheduleLoader()
        self.storage_client = storage_client or StorageClient()
        self.config = fastf1_config
        self.storage_config = storage_config
        self.logger = get_logger("data_ingestion.session_loader")

        self.file_formats = {
            "session_info": {
                "format": "parquet",
                "required_columns": [
                    "event_name",
                    "location",
                    "country",
                    "session_name",
                    "session_date",
                    "total_laps",
                    "event_format",
                    "round_number",
                    "official_event_name",
                ],
            },
            "laps": {
                "format": "parquet",
                "required_columns": [
                    "Time",
                    "Driver",
                    "DriverNumber",
                    "LapTime",
                    "LapNumber",
                    "Stint",
                    "PitOutTime",
                    "PitInTime",
                    "Sector1Time",
                    "Sector2Time",
                    "Sector3Time",
                    "Sector1SessionTime",
                    "Sector2SessionTime",
                    "Sector3SessionTime",
                    "SpeedI1",
                    "SpeedI2",
                    "SpeedFL",
                    "SpeedST",
                    "IsPersonalBest",
                    "Compound",
                    "TyreLife",
                    "FreshTyre",
                    "Team",
                    "LapStartTime",
                    "LapStartDate",
                    "TrackStatus",
                    "Position",
                    "Deleted",
                    "DeletedReason",
                    "FastF1Generated",
                    "IsAccurate",
                ],
            },
            "results": {
                "format": "parquet",
                "required_columns": [
                    "DriverNumber",
                    "BroadcastName",
                    "Abbreviation",
                    "DriverId",
                    "TeamName",
                    "TeamColor",
                    "TeamId",
                    "FirstName",
                    "LastName",
                    "FullName",
                    "HeadshotUrl",
                    "CountryCode",
                    "Position",
                    "ClassifiedPosition",
                    "GridPosition",
                    "Q1",
                    "Q2",
                    "Q3",
                    "Time",
                    "Status",
                    "Points",
                    "Laps",
                ],
            },
            "weather": {
                "format": "parquet",
                "required_columns": [
                    "Time",
                    "AirTemp",
                    "Humidity",
                    "Pressure",
                    "Rainfall",
                    "TrackTemp",
                    "WindDirection",
                    "WindSpeed",
                ],
            },
            "race_control_messages": {
                "format": "parquet",
                "required_columns": [
                    "Time",
                    "Category",
                    "Message",
                    "Status",
                    "Flag",
                    "Scope",
                    "Sector",
                    "RacingNumber",
                    "Lap",
                ],
            },
            "session_status": {
                "format": "parquet",
                "required_columns": [
                    "Time",
                    "Status",
                ],
            },
            "track_status": {
                "format": "parquet",
                "required_columns": [
                    "Time",
                    "Status",
                    "Message",
                ],
            },
        }

    def _is_session_file_valid(
        self,
        file_name: str,
        object_key: str,
    ) -> bool:
        """Checks if the existing file in the bucket is valid"""

        if not self.storage_client.object_exists(object_key):
            self.logger.info("Session file does not exist: %s", object_key)
            return False

        try:
            data = self.storage_client.download_dataframe(object_key)

            if data.empty:
                self.logger.info(
                    "%s file exists but is empty: %s", file_name, object_key
                )
                return False

            required_cols = self.file_formats[file_name]["required_columns"]

            missing_columns = [col for col in required_cols if col not in data.columns]

            if missing_columns:
                self.logger.warning(
                    "%s file missing required columns %s: %s",
                    file_name,
                    missing_columns,
                    object_key,
                )
                return False

            self.logger.debug("%s file valid: %s", file_name, object_key)
            return True

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error(
                "Error validating %s file %s: %s", file_name, object_key, str(e)
            )
            return False

    def _load_session_file_from_storage(
        self,
        file_name: str,
        object_key: str,
    ) -> Optional[pd.DataFrame]:
        """Loads session files from storage"""

        if not self._is_session_file_valid(file_name, object_key):
            return None

        try:
            data = self.storage_client.download_dataframe(object_key)
            if data is not None:
                self.logger.info(
                    "Loaded %s data from storage: %s", file_name, object_key
                )
            return data
        except Exception as e:  # pylint: disable=broad-except
            self.logger.error(
                "Failed to load %s file from storage %s: %s",
                file_name,
                object_key,
                str(e),
            )
            return None

    def load_session_data(
        self,
        year: int,
        event_name: str,
        session_type: str,
        force_refresh: bool = False,
        save_to_storage: bool = False,
    ) -> Dict[str, Any]:
        """
        Load comprehensive session data with intelligent caching

        Args:
            year: Season year
            event_name: Grand Prix name
            session_type: Session identifier (Q, R, FP1, etc.)
            force_refresh: Force refresh from API even if cached data exists

        Returns:
            Dictionary containing all session data types
        """

        self.logger.info(
            "Loading session data: %d %s %s (force_refresh=%s)",
            year,
            event_name,
            session_type,
            force_refresh,
        )

        # Step 1: Check cache first (unless force_refresh)
        if not force_refresh:
            cached_data, missing_file_names = self._check_complete_session_cache(
                year, event_name, session_type
            )

            # If we have complete cache, return it
            if not missing_file_names:
                self.logger.info(
                    "Using complete cached data for %d %s %s",
                    year,
                    event_name,
                    session_type,
                )
                return cached_data

            # If we have partial cache, we'll need to load missing data from API
            if missing_file_names:
                self.logger.info(
                    "Loading missing data from API: %s", missing_file_names
                )
                return self._load_partial_session_data(
                    year,
                    event_name,
                    session_type,
                    cached_data,
                    missing_file_names,
                    save_to_storage,
                )

        # Step 2: Load complete session from API
        self.logger.info(
            "Loading complete session from API: %d %s %s",
            year,
            event_name,
            session_type,
        )
        return self._load_complete_session_from_api(
            year,
            event_name,
            session_type,
            save_to_storage,
        )

    def _check_complete_session_cache(
        self,
        year: int,
        event_name: str,
        session_type: int,
    ) -> Dict[str, Any]:
        """Check if complete session data is available in cache"""

        self.logger.debug(
            "Checking cache for complete session: %d %s %s",
            year,
            event_name,
            session_type,
        )

        cached_data = {}
        missing_file_names = []

        # Check each data type
        for file_name in self.file_formats:
            object_key = self.storage_client.build_object_key(
                file_name, year, event_name, session_type
            )
            cached_item = self._load_session_file_from_storage(
                file_name,
                object_key,
            )

            if cached_item is not None:
                cached_data[file_name] = cached_item
                self.logger.debug(
                    "✅ Found cached %d %s %s %s",
                    year,
                    event_name,
                    session_type,
                    file_name,
                )
            else:
                cached_data[file_name] = None
                missing_file_names.append(file_name)
                self.logger.debug(
                    "❌ Missing cached %s for %d %s %s",
                    file_name,
                    year,
                    event_name,
                    session_type,
                )

        if len(missing_file_names) == len(self.file_formats.keys()):
            self.logger.info(
                "No cached files for %d %s %s",
                year,
                event_name,
                session_type,
            )

        elif (
            len(missing_file_names) < len(self.file_formats.keys())
            and len(missing_file_names) > 0
        ):
            self.logger.info(
                "Partial cache for %d %s %s. Missing: %s",
                year,
                event_name,
                session_type,
                missing_file_names,
            )
        else:
            self.logger.info(
                "✅ Complete cache found for %d %s %s", year, event_name, session_type
            )

        return cached_data, missing_file_names

    def _load_partial_session_data(
        self,
        year: int,
        event_name: str,
        session_type: str,
        cached_data: Dict[str, Any],
        missing_file_names: List[str],
        save_to_storage: bool,
    ):
        """Load only missing data types from API and merge with cached data"""

        try:
            # Get session object from FastF1
            session_obj = self.client.get_session(year, event_name, session_type)

            # Mapping of data types to extraction methods
            extraction_methods = {
                "session_info": self._extract_session_info,
                "laps": self._extract_lap_data,
                "results": self._extract_session_results,
                "weather": self._extract_weather_data,
                "race_control_messages": self._extract_race_control_messages,
                "session_status": self._extract_session_status,
                "track_status": self._extract_track_status,
            }

            # Load only missing data
            newly_loaded_data = {}
            for file_name in missing_file_names:
                if file_name in extraction_methods:
                    self.logger.info(
                        "Loading %s from API for %d %s %s",
                        file_name,
                        year,
                        event_name,
                        session_type,
                    )
                    newly_loaded_data[file_name] = extraction_methods[file_name](
                        session_obj
                    )
                else:
                    self.logger.warning("Unknown data requested: %s", file_name)
                    newly_loaded_data[file_name] = None

            # Save newly loaded data
            if newly_loaded_data and save_to_storage:
                _ = self.storage_client.upload_session_data(
                    year,
                    event_name,
                    session_type,
                    newly_loaded_data,
                )

            # Merge cached and newly loaded data
            final_data = cached_data.copy()
            final_data.update(newly_loaded_data)

            self.logger.info(
                "Successfully merged cached and API data for %d %s %s",
                year,
                event_name,
                session_type,
            )
            return final_data

        except Exception as e:
            self.logger.error(
                "Failed to load partial session data %d %s %s: %s",
                year,
                event_name,
                session_type,
                str(e),
            )
            raise

    def _load_complete_session_from_api(
        self,
        year: int,
        event_name: str,
        session_type: str,
        save_to_storage: bool,
    ) -> Dict[str, Any]:
        """Load complete session data from API"""

        try:
            # Get session object from FastF1
            session_obj = self.client.get_session(year, event_name, session_type)

            # Extract all data types
            session_data = {
                "session_info": self._extract_session_info(session_obj),
                "laps": self._extract_lap_data(session_obj),
                "results": self._extract_session_results(session_obj),
                "weather": self._extract_weather_data(session_obj),
                "race_control_messages": self._extract_race_control_messages(
                    session_obj
                ),
                "session_status": self._extract_session_status(session_obj),
                "track_status": self._extract_track_status(session_obj),
            }

            # Save all data
            if save_to_storage:
                self.storage_client.upload_session_data(
                    year,
                    event_name,
                    session_type,
                    session_data,
                )

            self.logger.info(
                "Successfully loaded complete session data: %d %s %s",
                year,
                event_name,
                session_type,
            )
            return session_data

        except Exception as e:
            self.logger.error(
                "Failed to load complete session data %d %s %s: %s",
                year,
                event_name,
                session_type,
                str(e),
            )
            raise

    def _extract_session_info(self, session_obj) -> Optional[pd.DataFrame]:
        """Extract basic session information"""

        self.logger.debug("Extracting session info")

        session_info = {
            "event_name": [
                session_obj.event.EventName if hasattr(session_obj, "event") else None
            ],
            "location": [
                session_obj.event.Location if hasattr(session_obj, "event") else None
            ],
            "country": [
                session_obj.event.Country if hasattr(session_obj, "event") else None
            ],
            "session_name": [
                session_obj.name if hasattr(session_obj, "name") else None
            ],
            "session_date": [
                session_obj.date if hasattr(session_obj, "date") else None
            ],
            "total_laps": [
                session_obj.total_laps if hasattr(session_obj, "total_laps") else None
            ],
        }

        # Add event metadata if available
        if hasattr(session_obj, "event"):
            event = session_obj.event
            session_info["event_format"] = getattr(event, "EventFormat", None)
            session_info["round_number"] = getattr(event, "RoundNumber", None)
            session_info["official_event_name"] = getattr(
                event, "OfficialEventName", None
            )
            session_info["session_1"] = getattr(event, "Session1", None)
            session_info["session_2"] = getattr(event, "Session2", None)
            session_info["session_3"] = getattr(event, "Session3", None)
            session_info["session_4"] = getattr(event, "Session4", None)
            session_info["session_5"] = getattr(event, "Session5", None)

        session_info_df = pd.DataFrame(session_info)
        self.logger.debug("Extracted session info: %s", session_info)
        return session_info_df

    def _extract_lap_data(self, session_obj) -> Optional[pd.DataFrame]:
        """Extract lap timing data"""

        self.logger.debug("Extracting lap data")

        if not hasattr(session_obj, "laps") or session_obj.laps.empty:
            self.logger.warning("No lap data available")
            return None

        laps = session_obj.laps.copy()

        # Add computed columns
        if "LapTime" in laps.columns:
            laps["LapTimeSeconds"] = laps["LapTime"].dt.total_seconds()

        # Replace NaT with None in all timedelta columns
        timedelta_cols = [
            "Time",
            "LapTime",
            "PitOutTime",
            "PitInTime",
            "Sector1Time",
            "Sector2Time",
            "Sector3Time",
            "Sector1SessionTime",
            "Sector2SessionTime",
            "Sector3SessionTime",
            "LapStartTime",
            "LapStartDate",
        ]
        for col in timedelta_cols:
            laps[col] = laps[col].replace({pd.NaT: None})

        # Add session metadata to each lap
        laps["EventName"] = (
            session_obj.event.EventName if hasattr(session_obj, "event") else None
        )
        laps["SessionName"] = session_obj.name if hasattr(session_obj, "name") else None
        laps["SessionDate"] = session_obj.date if hasattr(session_obj, "date") else None

        return laps

    def _extract_session_results(self, session_obj) -> Optional[pd.DataFrame]:
        """Extract session results"""

        self.logger.debug("Extracting session results")

        if not hasattr(session_obj, "results") or session_obj.results.empty:
            self.logger.warning("No results data available")
            return None

        results = session_obj.results.copy()

        # Replace NaT with None in all timedelta columns
        timedelta_cols = ["Time", "Q1", "Q2", "Q3"]
        for col in timedelta_cols:
            results[col] = results[col].replace({pd.NaT: None})

        # Add session metadata
        results["EventName"] = (
            session_obj.event.EventName if hasattr(session_obj, "event") else None
        )
        results["SessionName"] = (
            session_obj.name if hasattr(session_obj, "name") else None
        )
        results["SessionDate"] = (
            session_obj.date if hasattr(session_obj, "date") else None
        )

        return results

    def _extract_weather_data(self, session_obj) -> Optional[pd.DataFrame]:
        """Extract weather data"""

        if not self.config.enable_weather_data:
            self.logger.debug("Weather data disabled by configuration")
            return None

        self.logger.debug("Extracting weather data")

        if not hasattr(session_obj, "weather_data") or session_obj.weather_data.empty:
            self.logger.warning("No weather data available")
            return None

        weather = session_obj.weather_data.copy()

        # Add session metadata
        weather["EventName"] = (
            session_obj.event.EventName if hasattr(session_obj, "event") else None
        )
        weather["SessionName"] = (
            session_obj.name if hasattr(session_obj, "name") else None
        )
        weather["SessionDate"] = (
            session_obj.date if hasattr(session_obj, "date") else None
        )

        return weather

    def _extract_race_control_messages(self, session_obj) -> Optional[pd.DataFrame]:
        """Extract race control messages"""

        if not self.config.enable_race_control_messages:
            self.logger.debug("Race control messages disabled by configuration")
            return None

        self.logger.debug("Extracting race control messages")

        if (
            not hasattr(session_obj, "race_control_messages")
            or session_obj.race_control_messages.empty
        ):
            self.logger.warning("No data available on race control messages.")
            return None

        race_control_messages = session_obj.race_control_messages.copy()

        # Add session metadata
        race_control_messages["EventName"] = (
            session_obj.event.EventName if hasattr(session_obj, "event") else None
        )
        race_control_messages["SessionName"] = (
            session_obj.name if hasattr(session_obj, "name") else None
        )
        race_control_messages["SessionDate"] = (
            session_obj.date if hasattr(session_obj, "date") else None
        )

        return race_control_messages

    def _extract_session_status(self, session_obj) -> Optional[pd.DataFrame]:
        """Extract session status data"""

        # Log strating message
        self.logger.debug("Extracting session status")

        if (
            not hasattr(session_obj, "session_status")
            or session_obj.session_status.empty
        ):
            self.logger.warning("No session status data available")
            return None

        session_status = session_obj.session_status.copy()

        # Add session metadata
        session_status["EventName"] = (
            session_obj.event.EventName if hasattr(session_obj, "event") else None
        )
        session_status["SessionName"] = (
            session_obj.name if hasattr(session_obj, "name") else None
        )
        session_status["SessionDate"] = (
            session_obj.date if hasattr(session_obj, "date") else None
        )

        return session_status

    def _extract_track_status(self, session_obj) -> Optional[pd.DataFrame]:
        """Extract track status data"""

        self.logger.debug("Extracting track status")

        if not hasattr(session_obj, "track_status") or session_obj.track_status.empty:
            self.logger.warning("No track status data available")
            return None

        track_status = session_obj.track_status.copy()

        # Add session metadata
        track_status["EventName"] = (
            session_obj.event.EventName if hasattr(session_obj, "event") else None
        )
        track_status["SessionName"] = (
            session_obj.name if hasattr(session_obj, "name") else None
        )
        track_status["SessionDate"] = (
            session_obj.date if hasattr(session_obj, "date") else None
        )

        return track_status

    def is_session_cached(
        self,
        year: int,
        event_name: str,
        session_type: str,
    ) -> Dict[str, bool]:
        """Check which data types are available in cache for a session"""

        cache_status = {}

        for file_name in self.file_formats:
            object_key = self.storage_client.build_object_key(
                file_name,
                year,
                event_name,
                session_type,
            )
            cache_status[file_name] = self._is_session_file_valid(
                file_name,
                object_key,
            )

        return cache_status

    def get_session_cache_summary(
        self,
        year: int,
        event_name: str,
        session_type: str,
    ) -> Dict[str, Any]:
        """Get detailed cache status for a session"""

        cache_status = self.is_session_cached(year, event_name, session_type)

        # Clean event name (lowercase, replace spaces with underscores)
        clean_event_name = event_name.lower().replace(" ", "_")

        summary = {
            "session_id": f"{year}_{clean_event_name}_{session_type}",
            "session_objects_prefix": f"{year}/{clean_event_name}/{session_type}",
            "cache_status": cache_status,
            "cached_data_types": [dt for dt, cached in cache_status.items() if cached],
            "missing_data_types": [
                dt for dt, cached in cache_status.items() if not cached
            ],
            "cache_complete": all(cache_status.values()),
            "cache_partial": any(cache_status.values())
            and not all(cache_status.values()),
            "file_details": {},
        }

        # Get file size information
        for data_type, is_cached in cache_status.items():
            if is_cached:
                object_key = self.storage_client.build_object_key(
                    data_type,
                    year,
                    event_name,
                    session_type,
                )
                if self.storage_client.object_exists(object_key):
                    summary["file_details"][data_type] = {
                        "object_key": str(object_key),
                        "size_mb": self.storage_client.get_object_size_mb(object_key),
                    }

        return summary

    # Load multiple sessions
    def load_multiple_sessions(
        self,
        year: int,
        events: List[str],
        session_types: Optional[List[str]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """Load multiple sessions efficiently"""

        self.logger.info(
            "Loading multiple sessions: %d, %d events, %d session types",
            year,
            len(events),
            len(session_types),
        )

        all_data = {}

        for event in events:
            self.logger.info("Processing event: %s", event)
            all_data[event] = {}

            if session_types is None:
                session_types = self.config.session_types
            elif session_types == ["all"]:
                session_types = self.schedule_loader.get_sessions_to_load(year, event)

            for session_type in session_types:
                try:
                    session_data = self.load_session_data(year, event, session_type)
                    all_data[event][session_type] = session_data
                    self.logger.debug("✅ Loaded %d %s %s", year, event, session_type)

                except Exception as e:  # pylint: disable=broad-except
                    self.logger.warning(
                        "Failed to load %d %s %s: %s", year, event, session_type, str(e)
                    )
                    all_data[event][session_type] = None

        self.logger.info("Completed loading multiple sessions for %d", year)
        return all_data

    # Get session summary
    def get_session_summary(
        self, year: int, event: str, session: str
    ) -> Dict[str, Any]:
        """Get a summary of session without loading full data"""

        self.logger.debug("Getting session summary: %d %s %s", year, event, session)

        try:
            session_obj = self.client.get_session(year, event, session)

            summary = {
                "event_name": session_obj.event.EventName
                if hasattr(session_obj, "event")
                else None,
                "session_name": session_obj.name
                if hasattr(session_obj, "name")
                else None,
                "session_date": session_obj.date
                if hasattr(session_obj, "date")
                else None,
                "has_laps": hasattr(session_obj, "laps") and not session_obj.laps.empty,
                "has_results": hasattr(session_obj, "results")
                and not session_obj.results.empty,
                "has_weather": hasattr(session_obj, "weather_data")
                and not session_obj.weather_data.empty,
                "has_race_control_msgs": hasattr(session_obj, "race_control_messages")
                and not session_obj.race_control_messages.empty,
                "lap_count": getattr(session_obj, "total_laps"),
                "driver_count": len(session_obj.results)
                if hasattr(session_obj, "results")
                else 0,
            }

            # Add cache information
            summary["cache_info"] = self.get_session_cache_summary(year, event, session)

            return summary

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error(
                "Failed to get session summary %d %s %s: %s",
                year,
                event,
                session,
                str(e),
            )
            return {"error": str(e)}
