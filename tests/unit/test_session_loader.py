"""
Unit tests for Schedule Loader.

Tests with mocked FastF1 responses (no API calls).
"""

# pylint: disable=protected-access

from unittest.mock import patch

import pytest

from src.data_ingestion.schedule_loader import ScheduleLoader


@pytest.mark.unit
class TestSessionLoaderConfig:
    """Test that session loader respects configuration"""

    def test_session_loader_init(self, session_loader):
        """Test that session types come from config"""

        # Check for some basic attributes
        assert session_loader is not None
        assert hasattr(session_loader, "client")
        assert hasattr(session_loader, "storage_client")
        assert hasattr(session_loader, "schedule_loader")
        assert hasattr(session_loader, "config")
        assert hasattr(session_loader, "storage_config")
        assert hasattr(session_loader, "logger")

        # Check for the values of some attributes
        assert session_loader.logger.name == "data_ingestion.session_loader"
        assert "session_info" in session_loader.file_formats
        assert "laps" in session_loader.file_formats
        assert "results" in session_loader.file_formats
        assert "weather" in session_loader.file_formats
        assert "race_control_messages" in session_loader.file_formats
        assert session_loader.file_formats["laps"]["format"] == "parquet"


@pytest.mark.unit
class TestLoadSessionData:
    """Tests for loading session data"""

    def test_load_session_data_uncached(
        self,
        session_loader,
        sample_session_info,
        sample_lap_data,
        sample_race_result,
        sample_weather_data,
        sample_race_control_messages_data,
        sample_session_status_data,
        sample_track_status_data,
        test_storage_client,
    ):
        """Test loading session data when not cached"""

        year = 2024
        event_name = "Italian Grand Prix"
        session_type = "R"

        # Configure mock to return False for all data types
        with patch.object(
            session_loader,
            "_check_complete_session_cache",
            return_value=(
                {},
                [
                    "session_info",
                    "laps",
                    "results",
                    "weather",
                    "race_control_messages",
                    "session_status",
                    "track_status",
                ],
            ),
        ) as mock_check_cache:
            with patch.object(
                session_loader, "_load_partial_session_data"
            ) as mock_load_partial_data:
                with patch.object(
                    session_loader,
                    "_extract_session_info",
                    return_value=sample_session_info,
                ) as mock_session_info:
                    with patch.object(
                        session_loader,
                        "_extract_lap_data",
                        return_value=sample_lap_data,
                    ) as mock_lap_data:
                        with patch.object(
                            session_loader,
                            "_extract_session_results",
                            return_value=sample_race_result,
                        ) as mock_session_results:
                            with patch.object(
                                session_loader,
                                "_extract_weather_data",
                                return_value=sample_weather_data,
                            ) as mock_weather_data:
                                with patch.object(
                                    session_loader,
                                    "_extract_race_control_messages",
                                    return_value=sample_race_control_messages_data,
                                ) as mock_race_control_messages:
                                    with patch.object(
                                        session_loader,
                                        "_extract_session_status",
                                        return_value=sample_session_status_data,
                                    ) as mock_session_status:
                                        with patch.object(
                                            session_loader,
                                            "_extract_track_status",
                                            return_value=sample_track_status_data,
                                        ) as mock_track_status:
                                            with patch.object(
                                                test_storage_client,
                                                "upload_session_data",
                                            ) as mock_upload_data:
                                                # Call the method under test
                                                session_data = (
                                                    session_loader.load_session_data(
                                                        year,
                                                        event_name,
                                                        session_type,
                                                        force_refresh=False,
                                                        save_to_storage=False,
                                                    )
                                                )

                                                # Check that methods were called
                                                mock_check_cache.assert_called_once_with(
                                                    year, event_name, session_type
                                                )
                                                mock_load_partial_data.assert_not_called()
                                                mock_upload_data.assert_not_called()

                                                mock_session_info.assert_called_once()
                                                mock_lap_data.assert_called_once()
                                                mock_session_results.assert_called_once()
                                                mock_weather_data.assert_called_once()
                                                mock_race_control_messages.assert_called_once()
                                                mock_session_status.assert_called_once()
                                                mock_track_status.assert_called_once()

                                                # Value checks
                                                assert (
                                                    session_data["session_info"]
                                                    == sample_session_info
                                                )
                                                assert (
                                                    session_data["laps"]
                                                    == sample_lap_data
                                                )
                                                assert (
                                                    session_data["results"]
                                                    == sample_race_result
                                                )
                                                assert (
                                                    session_data["weather"]
                                                    == sample_weather_data
                                                )
                                                assert (
                                                    session_data[
                                                        "race_control_messages"
                                                    ]
                                                    == sample_race_control_messages_data
                                                )
                                                assert (
                                                    session_data["session_status"]
                                                    == sample_session_status_data
                                                )
                                                assert (
                                                    session_data["track_status"]
                                                    == sample_track_status_data
                                                )

    def test_load_session_data_partially_cached(
        self,
        session_loader,
        sample_session_info,
        sample_lap_data,
        sample_race_result,
        sample_weather_data,
        sample_race_control_messages_data,
        sample_session_status_data,
        sample_track_status_data,
    ):
        """Test loading session data when partially cached"""

        year = 2024
        event_name = "Italian Grand Prix"
        session_type = "R"

        # Configure mock to return True for some data types
        with patch.object(
            session_loader,
            "_check_complete_session_cache",
            return_value=(
                {
                    "session_info": sample_session_info,
                    "laps": sample_lap_data,
                    "results": None,
                    "weather": None,
                    "race_control_messages": None,
                    "session_status": sample_session_status_data,
                    "track_status": sample_track_status_data,
                },
                [
                    "results",
                    "weather",
                    "race_control_messages",
                ],
            ),
        ) as mock_check_cache:
            with patch.object(
                session_loader,
                "_extract_session_results",
                return_value=sample_race_result,
            ) as mock_session_results:
                with patch.object(
                    session_loader,
                    "_extract_weather_data",
                    return_value=sample_weather_data,
                ) as mock_weather_data:
                    with patch.object(
                        session_loader,
                        "_extract_race_control_messages",
                        return_value=sample_race_control_messages_data,
                    ) as mock_race_control_messages:
                        with patch.object(
                            session_loader,
                            "_load_partial_session_data",
                            return_value={
                                "session_info": sample_session_info,
                                "laps": sample_lap_data,
                                "results": mock_session_results.return_value,
                                "weather": mock_weather_data.return_value,
                                "race_control_messages": mock_race_control_messages.return_value,
                                "session_status": sample_session_status_data,
                                "track_status": sample_track_status_data,
                            },
                        ) as mock_partial_load:
                            with patch.object(
                                session_loader, "_load_complete_session_from_api"
                            ) as mock_complete_load:
                                # Main function call
                                session_data = session_loader.load_session_data(
                                    year, event_name, session_type
                                )

                                # Assertions to check
                                mock_check_cache.assert_called_once_with(
                                    year, event_name, session_type
                                )
                                mock_partial_load.assert_called_once()

                                # Complete load function should not be called
                                mock_complete_load.assert_not_called()

                                # Value checks
                                assert session_data["laps"] == sample_lap_data
                                assert session_data["results"] == sample_race_result
                                assert session_data["weather"] == sample_weather_data
                                assert (
                                    session_data["race_control_messages"]
                                    == sample_race_control_messages_data
                                )
                                assert (
                                    session_data["session_status"]
                                    == sample_session_status_data
                                )
                                assert (
                                    session_data["track_status"]
                                    == sample_track_status_data
                                )


@pytest.mark.unit
class TestLoadMultipleSessions:
    """Tests for loading multiple sessions"""

    def test_load_multiple_sessions(
        self,
        session_loader,
        sample_session_info,
        sample_lap_data_df,
        sample_race_results_df,
        sample_weather_data_df,
        sample_race_control_messages_data_df,
        sample_session_status_data_df,
        sample_track_status_data_df,
    ):
        """Test loading multiple sessions successfully"""

        year = 2024
        events = ["Italian Grand Prix", "Monaco Grand Prix"]
        session_types = ["Q", "R"]

        # Mock function to return dummy session data
        with patch.object(ScheduleLoader, "get_sessions_to_load") as mock_get_sessions:
            with patch.object(
                session_loader,
                "load_session_data",
                return_value={
                    "session_info": sample_session_info,
                    "laps": sample_lap_data_df,
                    "results": sample_race_results_df,
                    "weather": sample_weather_data_df,
                    "race_control_messages": sample_race_control_messages_data_df,
                    "session_status": sample_session_status_data_df,
                    "track_status": sample_track_status_data_df,
                },
            ) as mock_load_session_data:
                # Main function call
                all_data = session_loader.load_multiple_sessions(
                    year, events, session_types
                )

                # Check following assertions
                mock_get_sessions.assert_not_called()

                assert mock_load_session_data.call_count == 4
                assert list(all_data.keys()) == [
                    "Italian Grand Prix",
                    "Monaco Grand Prix",
                ]
                assert list(all_data["Italian Grand Prix"].keys()) == ["Q", "R"]
                assert list(all_data["Italian Grand Prix"]["Q"].keys()) == [
                    "session_info",
                    "laps",
                    "results",
                    "weather",
                    "race_control_messages",
                    "session_status",
                    "track_status",
                ]
