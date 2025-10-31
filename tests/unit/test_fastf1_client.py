"""
Unit tests for FastF1 Client.

Tests with mocked FastF1 responses (no API calls).
"""

# pylint: disable=import-outside-toplevel

from unittest.mock import patch

import pytest
import fastf1

from src.clients.fastf1_client import FastF1Client


@pytest.mark.unit
class TestCacheOperations:
    """Test cache-related operations"""

    @patch("fastf1.Cache.get_cache_info")
    def test_cache_info(self, mock_cache_info, fastf1_client):
        """Test getting cache info"""

        mock_cache_info.return_value = ("cache_dir", 12345)

        info = fastf1_client.cache_info()

        assert info == ("cache_dir", 12345)
        mock_cache_info.assert_called_once()

    @patch("fastf1.Cache.clear_cache")
    def test_clear_cache(self, mock_clear_cache, fastf1_client):
        """Test clearing cache"""

        fastf1_client.clear_cache(deep=True)

        mock_clear_cache.assert_called_once_with(deep=True)

    @patch("fastf1.Cache.clear_cache")
    def test_clear_cache_default(self, mock_clear_cache, fastf1_client):
        """Test clearing cache with default parameters"""

        fastf1_client.clear_cache()

        mock_clear_cache.assert_called_once_with(deep=False)


@pytest.mark.unit
class TestFastF1ClientConfiguration:
    """Test that client respects configuration"""

    def test_session_types_from_config(self, fastf1_client):
        """Test that session types come from config"""

        session_types = fastf1_client.config.session_types

        assert isinstance(session_types, list)
        assert "Race" in session_types  # Race should always be included

    def test_cache_dir_from_config(self, fastf1_client):
        """Test that cache directory comes from config"""

        cache_dir = fastf1_client.config.cache_dir

        assert cache_dir is not None
        assert "cache" in str(cache_dir).lower()


@pytest.mark.unit
class TestFastF1ClientPerformance:
    """Test performance-related aspects"""

    @patch("fastf1.get_session")
    def test_session_fetch_time(
        self, mock_get_session, fastf1_client, mock_fastf1_session, benchmark
    ):
        """Test session fetch performance"""
        mock_get_session.return_value = mock_fastf1_session

        def fetch_session():
            return fastf1_client.get_session(2024, "Italian Grand Prix", "R")

        try:
            result = benchmark(fetch_session)
            assert result is not None
        except Exception:  # pylint: disable=broad-exception-caught
            # If pytest-benchmark not installed
            result = fetch_session()
            assert result is not None


@pytest.mark.unit
def test_fastf1_import():
    """Test that fastf1 can be imported"""

    assert fastf1 is not None


@pytest.mark.unit
def test_fastf1_client_singleton_pattern():
    """Test if multiple instances share state (if implemented)"""
    client1 = FastF1Client()
    client2 = FastF1Client()

    # They should have the same config
    assert client1.config.cache_dir == client2.config.cache_dir


@pytest.mark.unit
class TestFastF1ClientIntegrationPoints:
    """Test integration points with other components"""

    def test_client_compatible_with_pipeline(self, fastf1_client):
        """Test that client interface is compatible with pipeline"""

        # Check that client has all required methods for pipeline
        assert hasattr(fastf1_client, "get_season_schedule")
        assert hasattr(fastf1_client, "get_session")
        assert callable(fastf1_client.get_season_schedule)
        assert callable(fastf1_client.get_session)

    def test_session_output_compatible_with_validator(self, mock_fastf1_session):
        """Test that session output is compatible with validator"""

        # Check that session has expected attributes
        assert hasattr(mock_fastf1_session, "results")
        assert hasattr(mock_fastf1_session, "laps")
        assert hasattr(mock_fastf1_session, "weather_data")
        assert hasattr(mock_fastf1_session, "event")
