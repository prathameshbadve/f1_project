"""
Unit tests for Schedule Loader.

Tests with mocked FastF1 responses (no API calls).
"""

# pylint: disable=protected-access

from unittest.mock import patch

import pytest

from src.clients.storage_client import StorageClient
from src.clients.fastf1_client import FastF1Client


@pytest.mark.unit
class TestScheduleLoaderConfig:
    """Tests for ScheduleLoader configuration"""

    def test_schedule_loader_init(self, schedule_loader):
        """Test initialization of ScheduleLoader"""

        # Check for some basic attributes
        assert schedule_loader is not None
        assert hasattr(schedule_loader, "client")
        assert hasattr(schedule_loader, "storage_client")
        assert hasattr(schedule_loader, "config")
        assert hasattr(schedule_loader, "storage_config")
        assert hasattr(schedule_loader, "logger")

        # Check for the values of some attributes
        assert schedule_loader.logger.name == "data_ingestion.schedule_loader"
        assert schedule_loader.data_type == "schedule"


@pytest.mark.unit
class TestLoadSeasonSchedule:
    """Tests for loading season schedule"""

    def test_load_season_schedule_uncached(
        self,
        schedule_loader,
        sample_season_schedule,
    ):
        """Test loading season schedule that has no cached file"""

        year = 2024

        schedule_from_api = sample_season_schedule.copy().drop(columns=["Season"])

        # Configure mock return values
        with patch.object(
            schedule_loader, "_load_schedule_from_storage", return_value=None
        ) as mock_load_from_storage:
            with patch.object(
                FastF1Client, "get_season_schedule", return_value=schedule_from_api
            ) as mock_get_season_schedule:
                with patch.object(
                    schedule_loader,
                    "_enhance_schedule_data",
                    return_value=sample_season_schedule,
                ) as mock_enhance_schedule:
                    with patch.object(
                        StorageClient, "upload_dataframe"
                    ) as mock_upload_df:
                        # Main function call
                        schedule = schedule_loader.load_season_schedule(year)

                        # Check returned schedule
                        assert len(schedule) == 3
                        assert len(schedule.columns) == 24
                        assert "RoundNumber" in schedule.columns
                        assert "EventName" in schedule.columns

                        # Check that methods were called
                        mock_load_from_storage.assert_called_once_with(year)
                        mock_get_season_schedule.assert_called_once_with(year)
                        mock_enhance_schedule.assert_called_once_with(
                            schedule_from_api, year
                        )
                        mock_upload_df.assert_not_called()

    def test_load_season_schedule_cached(
        self,
        schedule_loader,
        sample_season_schedule,
    ):
        """Test loading season schedule that has no cached file"""

        year = 2024

        schedule_from_api = sample_season_schedule.copy().drop(columns=["Season"])

        # Configure mock return values
        with patch.object(
            schedule_loader,
            "_load_schedule_from_storage",
            return_value=sample_season_schedule,
        ) as mock_load_from_storage:
            with patch.object(
                FastF1Client, "get_season_schedule", return_value=schedule_from_api
            ) as mock_get_season_schedule:
                with patch.object(
                    schedule_loader,
                    "_enhance_schedule_data",
                    return_value=sample_season_schedule,
                ) as mock_enhance_schedule:
                    with patch.object(StorageClient, "upload_dataframe") as mock_upload:
                        # Main function call
                        schedule = schedule_loader.load_season_schedule(year)

                        # Check returned schedule
                        assert len(schedule) == 3
                        assert len(schedule.columns) == 24
                        assert "RoundNumber" in schedule.columns
                        assert "EventName" in schedule.columns

                        # Check that methods were called
                        mock_load_from_storage.assert_called_once_with(year)
                        mock_get_season_schedule.assert_not_called()
                        mock_enhance_schedule.assert_not_called()
                        mock_upload.assert_not_called()

    def test_load_season_schedule_uncached_save_file(
        self,
        schedule_loader,
        sample_season_schedule,
    ):
        """Test loading season schedule that has no cached file"""

        year = 2024

        schedule_from_api = sample_season_schedule.copy().drop(columns=["Season"])

        # Configure mock return values
        with patch.object(
            schedule_loader, "_load_schedule_from_storage", return_value=None
        ) as mock_load_from_storage:
            with patch.object(
                FastF1Client, "get_season_schedule", return_value=schedule_from_api
            ) as mock_get_season_schedule:
                with patch.object(
                    schedule_loader,
                    "_enhance_schedule_data",
                    return_value=sample_season_schedule,
                ) as mock_enhance_schedule:
                    with patch.object(
                        StorageClient,
                        "build_object_key",
                        return_value="schedules/season_2024_schedule.parquet",
                    ) as mock_build_object_key:
                        with patch.object(
                            StorageClient, "upload_dataframe", return_value=True
                        ) as mock_upload:
                            # Main function call
                            schedule = schedule_loader.load_season_schedule(
                                year, save_to_file=True
                            )

                            # Check returned schedule
                            assert len(schedule) == 3
                            assert len(schedule.columns) == 24
                            assert "RoundNumber" in schedule.columns
                            assert "EventName" in schedule.columns

                            # Check that methods were called
                            mock_load_from_storage.assert_called_once_with(year)
                            mock_get_season_schedule.assert_called_once_with(year)
                            mock_enhance_schedule.assert_called_once_with(
                                schedule_from_api, year
                            )
                            mock_build_object_key.assert_called_once()
                            mock_upload.assert_called_once_with(
                                schedule, "schedules/season_2024_schedule.parquet"
                            )

    def test_load_season_schedule_force_refresh(
        self,
        schedule_loader,
        sample_season_schedule,
    ):
        """Test loading season schedule that has no cached file"""

        year = 2024

        schedule_from_api = sample_season_schedule.copy().drop(columns=["Season"])

        # Configure mock return values
        with patch.object(
            schedule_loader, "_load_schedule_from_storage"
        ) as mock_load_from_storage:
            with patch.object(
                FastF1Client, "get_season_schedule", return_value=schedule_from_api
            ) as mock_get_season_schedule:
                with patch.object(
                    schedule_loader,
                    "_enhance_schedule_data",
                    return_value=sample_season_schedule,
                ) as mock_enhance_schedule:
                    with patch.object(StorageClient, "upload_dataframe") as mock_upload:
                        # Main function call
                        schedule = schedule_loader.load_season_schedule(
                            year, force_refresh=True
                        )

                        # Check returned schedule
                        assert len(schedule) == 3
                        assert len(schedule.columns) == 24
                        assert "RoundNumber" in schedule.columns
                        assert "EventName" in schedule.columns

                        # Check that methods were called
                        mock_load_from_storage.assert_not_called()
                        mock_get_season_schedule.assert_called_once_with(year)
                        mock_enhance_schedule.assert_called_once_with(
                            schedule_from_api, year
                        )
                        mock_upload.assert_not_called()

    def test_load_season_schedule_api_error(self, schedule_loader):
        """Test loading season schedule when API errors out"""

        year = 2024

        # Configure mock to raise exception
        with patch.object(
            schedule_loader, "_load_schedule_from_storage", return_value=None
        ) as mock_load_from_storage:
            with patch.object(
                FastF1Client,
                "get_season_schedule",
                side_effect=Exception("FastF1 API error"),
            ) as mock_get_season_schedule:
                with patch.object(
                    schedule_loader, "_enhance_schedule_data"
                ) as mock_enhance_schedule:
                    with patch.object(StorageClient, "upload_dataframe") as mock_upload:
                        # Main function call and check for exception
                        with pytest.raises(Exception) as exc_info:
                            schedule_loader.load_season_schedule(year)

                        assert str(exc_info.value) == "FastF1 API error"

                        # Check that methods were called
                        mock_load_from_storage.assert_called_once_with(year)
                        mock_get_season_schedule.assert_called_once_with(year)
                        mock_enhance_schedule.assert_not_called()
                        mock_upload.assert_not_called()


@pytest.mark.unit
class TestLoadSeasonScheduleEdgeCases:
    """Tests for edge cases in loading season schedule"""

    def test_load_season_schedule_empty_cached_file(
        self,
        schedule_loader,
        sample_season_schedule,
    ):
        """Test loading season schedule when cached file is empty or corrupted"""

        year = 2024

        schedule_from_api = sample_season_schedule.copy().drop(columns=["Season"])

        # Configure mock to return False
        with patch.object(
            schedule_loader, "_is_schedule_file_valid", return_value=False
        ) as mock_check_valid:
            with patch.object(
                FastF1Client, "get_season_schedule", return_value=schedule_from_api
            ) as mock_get_season_schedule:
                with patch.object(
                    schedule_loader,
                    "_enhance_schedule_data",
                    return_value=sample_season_schedule,
                ) as mock_enhance_schedule:
                    with patch.object(StorageClient, "upload_dataframe") as mock_upload:
                        # Main function call
                        schedule = schedule_loader.load_season_schedule(year)

                        # Check returned schedule
                        assert schedule_loader._load_schedule_from_storage(year) is None
                        assert len(schedule) == 3
                        assert len(schedule.columns) == 24
                        assert "RoundNumber" in schedule.columns
                        assert "EventName" in schedule.columns

                        # Check that methods were called
                        assert mock_check_valid.call_count == 2
                        mock_get_season_schedule.assert_called_once_with(year)
                        mock_enhance_schedule.assert_called_once_with(
                            schedule_from_api, year
                        )
                        mock_upload.assert_not_called()


@pytest.mark.unit
class TestGetEventsForIngestion:
    """Tests for getting events for ingestion"""

    def test_get_events_for_ingestion(
        self,
        schedule_loader,
        sample_season_schedule,
    ):
        """Test getting events for ingestion"""

        year = 2024

        # Configure mock return values
        with patch.object(
            schedule_loader, "load_season_schedule", return_value=sample_season_schedule
        ) as mock_load_schedule:
            # Main function call
            events = schedule_loader.get_events_for_ingestion(year)

            # Check returned events
            assert len(events) == 3
            assert events == [
                "Saudi Arabian Grand Prix",
                "Australian Grand Prix",
                "Japanese Grand Prix",
            ]

            mock_load_schedule.assert_called_once_with(
                year, save_to_file=False, force_refresh=False
            )

    def test_get_sessions_to_load(self, schedule_loader, sample_season_schedule):
        """Test getting sessions to load for an event"""

        year = 2024
        event_name = "Australian Grand Prix"

        # Configure mock return values
        with patch.object(
            schedule_loader, "load_season_schedule", return_value=sample_season_schedule
        ) as mock_load_schedule:
            # Main function call
            sessions_to_load = schedule_loader.get_sessions_to_load(year, event_name)

            assert len(sessions_to_load) == 5
            assert sessions_to_load == [
                "Practice 1",
                "Practice 2",
                "Practice 3",
                "Qualifying",
                "Race",
            ]

            mock_load_schedule.assert_called_once_with(
                year, save_to_file=False, force_refresh=False
            )
