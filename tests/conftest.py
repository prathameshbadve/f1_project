"""
Pytest configuration and shared fixtures.

This file provides common fixtures used across all tests.
"""

import os
import json
from datetime import timedelta, datetime
from pathlib import Path
from unittest.mock import Mock
import socket
import urllib.request
import time

from dotenv import load_dotenv
import pytest
import pandas as pd

from config.logging import setup_logging
from config.settings import StorageConfig

from src.data_ingestion.storage_client import StorageClient
from src.data_ingestion.fastf1_client import FastF1Client
from src.data_ingestion.schemas import DataValidator

load_dotenv()

# Setup logging for tests
setup_logging()


# ============================================================================
# Session-scoped fixtures (created once per test session)
# ============================================================================


@pytest.fixture(scope="session")
def test_bucket_name():
    """Test bucket name for MinIO"""

    return "f1-test-data"


@pytest.fixture(scope="session")
def test_storage_config(test_bucket_name):  # pylint: disable=redefined-outer-name
    """Storage configuration for tests"""

    return StorageConfig(
        endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=False,
        raw_bucket_name=test_bucket_name,
        processed_bucket_name=f"{test_bucket_name}-processed",
    )


@pytest.fixture(scope="session")
def test_storage_client(test_storage_config):  # pylint: disable=redefined-outer-name
    """Storage client configured for test bucket"""

    # Temporarily override the config
    original_bucket = test_storage_config.raw_bucket_name

    client = StorageClient()

    # Override bucket name for tests
    client.config.raw_bucket_name = test_storage_config.raw_bucket_name

    # Ensure test bucket exists
    try:
        if not client.client.bucket_exists(test_storage_config.raw_bucket_name):
            client.client.make_bucket(test_storage_config.raw_bucket_name)
    except Exception as e:  # pylint: disable=broad-except
        pytest.fail(f"Failed to create test bucket: {e}")

    yield client

    # Cleanup: Remove all test data after session
    # (Optional - comment out if you want to inspect test data)
    # cleanup_test_bucket(client, test_storage_config.raw_bucket_name)


@pytest.fixture(scope="session")
def fastf1_client():
    """FastF1 client for integration tests"""

    return FastF1Client()


@pytest.fixture(scope="session")
def data_validator():
    """Data validator instance"""

    return DataValidator()


# ============================================================================
# Test race configuration
# ============================================================================


@pytest.fixture(scope="session")
def test_race_config():
    """Configuration for test race (2024 Italian Grand Prix)"""

    return {
        "year": 2024,
        "event_name": "Italian Grand Prix",
        # "round_number": 16,
        "sessions": ["Q", "R"],
    }


# ============================================================================
# Sample data fixtures (function-scoped, created per test)
# ============================================================================


@pytest.fixture
def sample_race_result():
    """Sample race result row"""

    return {
        "DriverNumber": "1",
        "BroadcastName": "M VERSTAPPEN",
        "Abbreviation": "VER",
        "DriverId": "max_verstappen",
        "TeamName": "Red Bull Racing",
        "TeamColor": "3671C6",
        "TeamId": "red_bull",
        "FirstName": "Max",
        "LastName": "Verstappen",
        "FullName": "Max Verstappen",
        "HeadshotUrl": "google.com",
        "CountryCode": "NED",
        "Position": 1.0,
        "ClassifiedPosition": 1,
        "GridPosition": 1.0,
        "Q1": timedelta(minutes=1, seconds=30),
        "Q2": timedelta(minutes=1, seconds=29),
        "Q3": timedelta(minutes=1, seconds=28),
        "Time": timedelta(hours=1, minutes=30, seconds=45),
        "Status": "Finished",
        "Points": 25.0,
        "Year": 2024,
        "EventName": "Italian Grand Prix",
        "SessionName": "Race",
        "SessionDate": pd.Timestamp("2024-09-01 13:00:00"),
    }


@pytest.fixture
def sample_race_results_df():
    """Sample race result dataframe"""

    return pd.DataFrame(
        {
            "DriverNumber": ["1", "11", "16", "55", "44"],
            "BroadcastName": [
                "M VERSTAPPEN",
                "S PEREZ",
                "C LECLERC",
                "C SAINZ",
                "L HAMILTON",
            ],
            "Abbreviation": ["VER", "PER", "LEC", "SAI", "HAM"],
            "DriverId": ["max_verstappen", "perez", "leclerc", "sainz", "hamilton"],
            "TeamName": [
                "Red Bull Racing",
                "Red Bull Racing",
                "Ferrari",
                "Ferrari",
                "Mercedes",
            ],
            "TeamColor": ["3671C6", "3671C6", "E80020", "E80020", "27F4D2"],
            "TeamId": ["red_bull", "red_bull", "ferrari", "ferrari", "mercedes"],
            "FirstName": ["Max", "Sergio", "Charles", "Carlos", "Lewis"],
            "LastName": ["Verstappen", "Perez", "Leclerc", "Sainz", "Hamilton"],
            "FullName": [
                "Max Verstappen",
                "Sergio Perez",
                "Charles Leclerc",
                "Carlos Sainz",
                "Lewis Hamilton",
            ],
            "HeadshotUrl": ["google.com"] * 5,
            "CountryCode": ["NED", "MEX", "MON", "ESP", "GBR"],
            "Position": [1.0, 2.0, 3.0, 4.0, 5.0],
            "ClassifiedPosition": [1, 2, 3, 4, 5],
            "GridPosition": [1.0, 2.0, 3.0, 4.0, 5.0],
            "Q1": [
                timedelta(minutes=1, seconds=30),
                timedelta(minutes=1, seconds=35),
                timedelta(minutes=1, seconds=37),
                timedelta(minutes=1, seconds=41),
                timedelta(minutes=1, seconds=45),
            ],
            "Q2": [
                timedelta(minutes=1, seconds=30),
                timedelta(minutes=1, seconds=35),
                timedelta(minutes=1, seconds=37),
                timedelta(minutes=1, seconds=41),
                timedelta(minutes=1, seconds=45),
            ],
            "Q3": [
                timedelta(minutes=1, seconds=30),
                timedelta(minutes=1, seconds=35),
                timedelta(minutes=1, seconds=37),
                timedelta(minutes=1, seconds=41),
                timedelta(minutes=1, seconds=45),
            ],
            "Time": [timedelta(hours=1, minutes=30, seconds=45)] * 5,
            "Status": ["Finished"] * 5,
            "Points": [26.0, 18.0, 15.0, 12.0, 10.0],
            "Year": [2024] * 5,
            "EventName": ["Italian Grand Prix"] * 5,
            "SessionName": ["Race"] * 5,
            "SessionDate": [pd.Timestamp("2024-09-01 13:00:00")] * 5,
        }
    )


@pytest.fixture
def sample_lap_data():
    """Sample lap data row"""

    return {
        # Timing data
        "Time": pd.Timedelta("00:15:23.456"),
        "LapTime": pd.Timedelta("00:01:32.847"),
        "LapNumber": 8.0,
        "LapStartTime": pd.Timedelta("00:13:50.609"),
        "LapStartDate": pd.Timestamp("2024-09-01 13:13:50.609"),
        # Driver info
        "Driver": "VER",
        "DriverNumber": "1",
        "Team": "Red Bull Racing",
        # Stint info
        "Stint": 1.0,
        "PitOutTime": pd.Timedelta("00:00:15.234"),
        "PitInTime": None,
        # Sector times
        "Sector1Time": pd.Timedelta("00:00:28.456"),
        "Sector2Time": pd.Timedelta("00:00:35.123"),
        "Sector3Time": pd.Timedelta("00:00:29.268"),
        "Sector1SessionTime": pd.Timedelta("00:14:19.065"),
        "Sector2SessionTime": pd.Timedelta("00:14:54.188"),
        "Sector3SessionTime": pd.Timedelta("00:15:23.456"),
        # Speed traps
        "SpeedI1": 312.5,
        "SpeedI2": 298.3,
        "SpeedFL": 305.7,
        "SpeedST": 328.2,
        # Lap quality
        "IsPersonalBest": True,
        "Deleted": False,
        "DeletedReason": "",
        "IsAccurate": True,
        "FastF1Generated": False,
        # Tyre info
        "Compound": "SOFT",
        "TyreLife": 8,
        "FreshTyre": False,
        # Track conditions
        "TrackStatus": "1",  # Green flag
        "Position": 1.0,
        # Metadata
        "EventName": "Italian Grand Prix",
        "SessionName": "Race",
        "SessionDate": pd.Timestamp("2024-09-01 13:00:00"),
    }


@pytest.fixture
def sample_weather_data():
    """Sample weather data row"""

    return {
        "Time": pd.Timedelta("00:00:15.234"),
        "AirTemp": 28.5,
        "Humidity": 45.2,
        "Pressure": 1013.2,
        "Rainfall": False,
        "TrackTemp": 42.3,
        "WindDirection": 180,
        "WindSpeed": 3.5,
        "EventName": "Italian Grand Prix",
        "SessionName": "Race",
        "SessionDate": pd.Timestamp("2024-09-01 13:00:00"),
    }


@pytest.fixture
def invalid_race_result():
    """Sample invalid race result row"""

    return {
        "DriverNumber": "ABC",
        "BroadcastName": "M VERSTAPPEN",
        "Abbreviation": "VER",
        "DriverId": "max_verstappen",
        "TeamName": "Red Bull Racing",
        "TeamColor": "3671C6",
        "TeamId": "red_bull",
        "FirstName": "Max",
        "LastName": "Verstappen",
        "FullName": "Max Verstappen",
        "HeadshotUrl": "google.com",
        "CountryCode": "NED",
        "Position": 25.0,
        "ClassifiedPosition": 1,
        "GridPosition": 25.0,
        "Q1": timedelta(minutes=1, seconds=30),
        "Q2": timedelta(minutes=1, seconds=29),
        "Q3": timedelta(minutes=1, seconds=28),
        "Time": timedelta(hours=1, minutes=30, seconds=45),
        "Status": "Finished",
        "Points": 25.0,
        "Year": 2024,
        "EventName": "Italian Grand Prix",
        "SessionName": "Race",
        "SessionDate": pd.Timestamp("2024-09-01 13:00:00"),
    }


# ============================================================================
# Mock data fixtures
# ============================================================================


@pytest.fixture
def mock_fastf1_session():
    """Mock FastF1 session object"""

    session = Mock()
    session.event = {
        "EventName": "Italian Grand Prix",
        "EventDate": pd.Timestamp("2024-09-01"),
        "RoundNumber": 16,
        "Country": "Italy",
    }
    session.name = "Race"

    # Mock results
    results_data = {
        "DriverNumber": ["1", "11", "16"],
        "BroadcastName": ["M VERSTAPPEN", "S PEREZ", "C LECLERC"],
        "Abbreviation": ["VER", "PER", "LEC"],
        "TeamName": ["Red Bull Racing", "Red Bull Racing", "Ferrari"],
        "Position": [1.0, 2.0, 3.0],
        "GridPosition": [1.0, 2.0, 3.0],
        "Points": [25.0, 18.0, 15.0],
        "Status": ["Finished", "Finished", "Finished"],
    }
    session.results = pd.DataFrame(results_data)

    # Mock laps
    laps_data = {
        "DriverNumber": ["1", "1", "1"],
        "LapNumber": [1, 2, 3],
        "LapTime": [timedelta(minutes=1, seconds=32)] * 3,
        "Compound": ["SOFT", "SOFT", "SOFT"],
        "TyreLife": [1, 2, 3],
    }
    session.laps = pd.DataFrame(laps_data)

    # Mock weather
    weather_data = {
        "Time": [datetime.now()] * 3,
        "AirTemp": [28.5, 28.6, 28.7],
        "TrackTemp": [42.0, 42.1, 42.2],
        "Humidity": [45.0, 45.1, 45.2],
        "Pressure": [1013.0, 1013.1, 1013.2],
        "Rainfall": [False, False, False],
    }
    session.weather_data = pd.DataFrame(weather_data)

    return session


@pytest.fixture
def mock_empty_fastf1_session():
    """Mock FastF1 session with no data (for testing error handling)"""

    session = Mock()
    session.event = {
        "EventName": "Test Event",
        "EventDate": pd.Timestamp("2024-09-01"),
        "RoundNumber": 99,
        "Country": "Test",
    }
    session.name = "R"
    session.results = None
    session.laps = None
    session.weather_data = None

    return session


# ============================================================================
# Fixture files
# ============================================================================


@pytest.fixture
def fixtures_dir():
    """Directory for test fixtures"""
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def save_fixture(fixtures_dir):
    """Helper to save test data as fixture files"""

    def _save(data, filename):
        fixtures_dir.mkdir(exist_ok=True)
        filepath = fixtures_dir / filename

        if isinstance(data, pd.DataFrame):
            data.to_parquet(filepath)
        elif isinstance(data, dict):
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, default=str)
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")

        return filepath

    return _save


@pytest.fixture
def load_fixture(fixtures_dir):
    """Helper to load test data from fixture files"""

    def _load(filename):
        filepath = fixtures_dir / filename

        if not filepath.exists():
            pytest.skip(f"Fixture file not found: {filename}")

        if filename.endswith(".parquet"):
            return pd.read_parquet(filepath)
        elif filename.endswith(".json"):
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        else:
            raise ValueError(f"Unsupported fixture type: {filename}")

    return _load


# ============================================================================
# Cleanup utilities
# ============================================================================


def cleanup_test_bucket(storage_client, bucket_name):
    """Clean up test bucket after tests"""
    try:
        objects = storage_client.list_objects(prefix="")
        for obj_key in objects:
            storage_client.delete_object(obj_key)
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Warning: Failed to cleanup test bucket: {e}")


@pytest.fixture
def cleanup_test_data(test_storage_client):
    """Fixture to clean up test data after each test"""
    yield
    # Cleanup after test
    try:
        objects = test_storage_client.list_objects(prefix="test/")
        for obj_key in objects:
            test_storage_client.delete_object(obj_key)
    except Exception:  # pylint: disable=broad-exception-caught
        pass  # Ignore cleanup errors


# ============================================================================
# Test markers
# ============================================================================


def pytest_configure(config):
    """Register custom markers"""
    config.addinivalue_line("markers", "unit: Unit tests (fast, use mocks)")
    config.addinivalue_line(
        "markers", "integration: Integration tests (slower, use real APIs)"
    )
    config.addinivalue_line(
        "markers", "slow: Slow tests (skip unless explicitly requested)"
    )
    config.addinivalue_line(
        "markers", "requires_docker: Tests that require Docker containers"
    )


@pytest.fixture
def skip_if_no_docker():
    """Skip test if Docker is not running"""

    def is_port_open(host, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0

    if not is_port_open("localhost", 9000):
        pytest.skip("Docker/MinIO not running")


@pytest.fixture
def skip_if_no_internet():
    """Skip test if no internet connection"""

    try:
        urllib.request.urlopen("http://www.google.com", timeout=2)
    except Exception:  # pylint: disable=broad-exception-caught
        pytest.skip("No internet connection")


# ============================================================================
# Performance monitoring
# ============================================================================


@pytest.fixture(autouse=True)
def log_test_duration(request):
    """Automatically log test duration"""

    start = time.time()
    yield
    duration = time.time() - start

    if duration > 5.0:  # Log if test takes more than 5 seconds
        print(f"\n⚠️  Slow test: {request.node.name} took {duration:.2f}s")
