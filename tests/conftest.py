"""
Pytest configuration and shared fixtures.

This file provides common fixtures used across all tests.
"""

# pylint: disable=redefined-outer-name

import os
import json
from datetime import timedelta
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
        "ClassifiedPosition": "1",
        "GridPosition": 1.0,
        "Q1": pd.Timedelta(minutes=1, seconds=30),
        "Q2": pd.Timedelta(minutes=1, seconds=29),
        "Q3": pd.Timedelta(minutes=1, seconds=28),
        "Time": pd.Timedelta(hours=1, minutes=30, seconds=45),
        "Status": "Finished",
        "Points": 25.0,
        "Laps": 53.0,
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
            "ClassifiedPosition": ["1", "2", "3", "4", "5"],
            "GridPosition": [1.0, 2.0, 3.0, 4.0, 5.0],
            "Q1": [
                pd.Timedelta(minutes=1, seconds=30),
                pd.Timedelta(minutes=1, seconds=35),
                pd.Timedelta(minutes=1, seconds=37),
                pd.Timedelta(minutes=1, seconds=41),
                pd.Timedelta(minutes=1, seconds=45),
            ],
            "Q2": [
                pd.Timedelta(minutes=1, seconds=30),
                pd.Timedelta(minutes=1, seconds=35),
                pd.Timedelta(minutes=1, seconds=37),
                pd.Timedelta(minutes=1, seconds=41),
                pd.Timedelta(minutes=1, seconds=45),
            ],
            "Q3": [
                pd.Timedelta(minutes=1, seconds=30),
                pd.Timedelta(minutes=1, seconds=35),
                pd.Timedelta(minutes=1, seconds=37),
                pd.Timedelta(minutes=1, seconds=41),
                pd.Timedelta(minutes=1, seconds=45),
            ],
            "Time": [pd.Timedelta(hours=1, minutes=30, seconds=45)] * 5,
            "Status": ["Finished"] * 5,
            "Points": [26.0, 18.0, 15.0, 12.0, 10.0],
            "Laps": [53.0] * 5,
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
        "Time": pd.Timedelta("00:15:23.456"),
        "LapTime": pd.Timedelta("00:01:32.847"),
        "LapNumber": 8.0,
        "LapStartTime": pd.Timedelta("00:13:50.609"),
        "LapStartDate": pd.Timestamp("2024-09-01 13:13:50.609"),
        "Driver": "VER",
        "DriverNumber": "1",
        "Team": "Red Bull Racing",
        "Stint": 1.0,
        "PitOutTime": pd.Timedelta("00:00:15.234"),
        "PitInTime": None,
        "Sector1Time": pd.Timedelta("00:00:28.456"),
        "Sector2Time": pd.Timedelta("00:00:35.123"),
        "Sector3Time": pd.Timedelta("00:00:29.268"),
        "Sector1SessionTime": pd.Timedelta("00:14:19.065"),
        "Sector2SessionTime": pd.Timedelta("00:14:54.188"),
        "Sector3SessionTime": pd.Timedelta("00:15:23.456"),
        "SpeedI1": 312.5,
        "SpeedI2": 298.3,
        "SpeedFL": 305.7,
        "SpeedST": 328.2,
        "IsPersonalBest": True,
        "Deleted": False,
        "DeletedReason": "",
        "IsAccurate": True,
        "FastF1Generated": False,
        "Compound": "SOFT",
        "TyreLife": 8,
        "FreshTyre": False,
        "TrackStatus": "1",  # Green flag
        "Position": 1.0,
        "EventName": "Italian Grand Prix",
        "SessionName": "Race",
        "SessionDate": pd.Timestamp("2024-09-01 13:00:00"),
    }


@pytest.fixture
def sample_lap_data_df():
    """Sample lap data df"""

    return pd.DataFrame(
        {
            "Time": [pd.Timedelta("00:15:23.456")] * 3,
            "LapTime": [pd.Timedelta("00:01:32.847")] * 3,
            "LapNumber": [8.0, 9.0, 10.0],
            "LapStartTime": [pd.Timedelta("00:13:50.609")] * 3,
            "LapStartDate": [pd.Timestamp("2024-09-01 13:13:50.609")] * 3,
            "Driver": ["VER", "HAM", "LEC"],
            "DriverNumber": ["1", "44", "16"],
            "Team": ["Red Bull Racing", "Mercedes", "Ferrari"],
            "Stint": [1.0, 2.0, 2.0],
            "PitOutTime": [pd.Timedelta("00:00:15.234")] * 3,
            "PitInTime": [None] * 3,
            "Sector1Time": [pd.Timedelta("00:00:28.456")] * 3,
            "Sector2Time": [pd.Timedelta("00:00:35.123")] * 3,
            "Sector3Time": [pd.Timedelta("00:00:29.268")] * 3,
            "Sector1SessionTime": [pd.Timedelta("00:14:19.065")] * 3,
            "Sector2SessionTime": [pd.Timedelta("00:14:54.188")] * 3,
            "Sector3SessionTime": [pd.Timedelta("00:15:23.456")] * 3,
            "SpeedI1": [312.5] * 3,
            "SpeedI2": [298.3] * 3,
            "SpeedFL": [305.7] * 3,
            "SpeedST": [328.2] * 3,
            "IsPersonalBest": [True] * 3,
            "Deleted": [False] * 3,
            "DeletedReason": [""] * 3,
            "IsAccurate": [True] * 3,
            "FastF1Generated": [False] * 3,
            "Compound": ["SOFT", "HARD", "MEDIUM"],
            "TyreLife": [8, 8, 9],
            "FreshTyre": [False] * 3,
            "TrackStatus": ["1"] * 3,  # Green flag
            "Position": [1.0, 2.0, 3.0],
            "EventName": ["Italian Grand Prix"] * 3,
            "SessionName": ["Race"] * 3,
            "SessionDate": [pd.Timestamp("2024-09-01 13:00:00")] * 3,
        }
    )


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
def sample_weather_data_df():
    """Sample weather data df"""

    return pd.DataFrame(
        {
            "Time": [pd.Timedelta("00:00:15.234")] * 3,
            "AirTemp": [28.5] * 3,
            "Humidity": [45.2] * 3,
            "Pressure": [1013.2] * 3,
            "Rainfall": [False] * 3,
            "TrackTemp": [42.3] * 3,
            "WindDirection": [180] * 3,
            "WindSpeed": [3.5] * 3,
            "EventName": ["Italian Grand Prix"] * 3,
            "SessionName": ["Race"] * 3,
            "SessionDate": [pd.Timestamp("2024-09-01 13:00:00")] * 3,
        }
    )


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
    session.event = pd.DataFrame(
        {
            "RoundNumber": [9],
            "Country": ["Austria"],
            "Location": ["Spielberg"],
            "OfficialEventName": ["FORMULA 1 ROLEX GROSSER PREIS VON ÖSTERREICH 2023"],
            "EventDate": [pd.Timestamp("2023-07-02 00:00:00")],
            "EventName": ["Austrian Grand Prix"],
            "EventFormat": ["sprint_shootout"],
            "Session1": ["Practice 1"],
            "Session1Date": [pd.Timestamp("2023-06-30 13:30:00+02:00")],
            "Session1DateUtc": [pd.Timestamp("2023-06-30 11:30:00")],
            "Session2": ["Qualifying"],
            "Session2Date": [pd.Timestamp("2023-06-30 17:00:00+02:00")],
            "Session2DateUtc": [pd.Timestamp("2023-06-30 15:00:00")],
            "Session3": ["Sprint Shootout"],
            "Session3Date": [pd.Timestamp("2023-07-01 12:00:00+02:00")],
            "Session3DateUtc": [pd.Timestamp("2023-07-01 10:00:00")],
            "Session4": ["Sprint"],
            "Session4Date": [pd.Timestamp("2023-07-01 16:30:00+02:00")],
            "Session4DateUtc": [pd.Timestamp("2023-07-01 14:30:00")],
            "Session5": ["Race"],
            "Session5Date": [pd.Timestamp("2023-07-02 15:00:00+02:00")],
            "Session5DateUtc": [pd.Timestamp("2023-07-02 13:00:00")],
            "F1ApiSupport": [True],
        }
    )

    session.name = "Race"

    # Mock results
    results_data = {
        "DriverNumber": ["63", "81", "55", "44", "1"],
        "BroadcastName": [
            "G RUSSELL",
            "O PIASTRI",
            "C SAINZ",
            "L HAMILTON",
            "M VERSTAPPEN",
        ],
        "Abbreviation": ["RUS", "PIA", "SAI", "HAM", "VER"],
        "DriverId": ["russell", "piastri", "sainz", "hamilton", "max_verstappen"],
        "TeamName": ["Mercedes", "McLaren", "Ferrari", "Mercedes", "Red Bull Racing"],
        "TeamColor": ["27F4D2", "FF8000", "E80020", "27F4D2", "3671C6"],
        "TeamId": ["mercedes", "mclaren", "ferrari", "mercedes", "red_bull"],
        "FirstName": ["George", "Oscar", "Carlos", "Lewis", "Max"],
        "LastName": ["Russell", "Piastri", "Sainz", "Hamilton", "Verstappen"],
        "FullName": [
            "George Russell",
            "Oscar Piastri",
            "Carlos Sainz",
            "Lewis Hamilton",
            "Max Verstappen",
        ],
        "HeadshotUrl": ["google.com"] * 5,
        "CountryCode": ["GBR", "AUS", "ESP", "GBR", "NED"],
        "Position": [1.0, 2.0, 3.0, 4.0, 5.0],
        "ClassifiedPosition": ["1", "2", "3", "4", "5"],
        "GridPosition": [3.0, 7.0, 4.0, 5.0, 1.0],
        "Q1": [None, None, None, None, None],
        "Q2": [None, None, None, None, None],
        "Q3": [None, None, None, None, None],
        "Time": [
            pd.Timedelta("0 days 01:24:22.798000"),
            pd.Timedelta("0 days 00:00:01.906000"),
            pd.Timedelta("0 days 00:00:04.533000"),
            pd.Timedelta("0 days 00:00:23.142000"),
            pd.Timedelta("0 days 00:00:37.253000"),
        ],
        "Status": ["Finished", "Finished", "Finished", "Finished", "Finished"],
        "Points": [25.0, 18.0, 15.0, 12.0, 10.0],
        "Laps": [71.0, 71.0, 71.0, 71.0, 71.0],
        "EventName": ["Austrian Grand Prix"] * 5,
        "SessionName": ["Race"] * 5,
        "SessionDate": [pd.Timestamp("2024-06-28 12:30:00+0200", tz="UTC+02:00")] * 5,
    }
    session.results = pd.DataFrame(results_data)

    # Mock laps
    laps_data = {
        "Time": [
            pd.Timedelta("0 days 01:00:17.176000"),
            pd.Timedelta("0 days 01:00:13.035000"),
            pd.Timedelta("0 days 01:00:18.001000"),
        ],
        "Driver": ["RUS", "VER", "HAM"],
        "DriverNumber": ["63", "1", "44"],
        "LapTime": [
            pd.Timedelta("0 days 00:01:10.467000"),
            pd.Timedelta("0 days 00:01:09.903000"),
            pd.Timedelta("0 days 00:01:10.621000"),
        ],
        "LapNumber": [4.0, 4.0, 4.0],
        "Stint": [1.0, 1.0, 1.0],
        "PitOutTime": [None, None, None],
        "PitInTime": [None, None, None],
        "Sector1Time": [
            pd.Timedelta("0 days 00:00:17.555000"),
            pd.Timedelta("0 days 00:00:17.571000"),
            pd.Timedelta("0 days 00:00:17.336000"),
        ],
        "Sector2Time": [
            pd.Timedelta("0 days 00:00:31.631000"),
            pd.Timedelta("0 days 00:00:31.180000"),
            pd.Timedelta("0 days 00:00:31.922000"),
        ],
        "Sector3Time": [
            pd.Timedelta("0 days 00:00:21.281000"),
            pd.Timedelta("0 days 00:00:21.152000"),
            pd.Timedelta("0 days 00:00:21.363000"),
        ],
        "Sector1SessionTime": [
            pd.Timedelta("0 days 00:59:24.312000"),
            pd.Timedelta("0 days 00:59:20.752000"),
            pd.Timedelta("0 days 00:59:24.773000"),
        ],
        "Sector2SessionTime": [
            pd.Timedelta("0 days 00:59:55.943000"),
            pd.Timedelta("0 days 00:59:51.932000"),
            pd.Timedelta("0 days 00:59:56.695000"),
        ],
        "Sector3SessionTime": [
            pd.Timedelta("0 days 01:00:17.224000"),
            pd.Timedelta("0 days 01:00:13.084000"),
            pd.Timedelta("0 days 01:00:18.058000"),
        ],
        "SpeedI1": [296.0, 293.0, 309.0],
        "SpeedI2": [228.0, 230.0, 229.0],
        "SpeedFL": [271.0, 270.0, 276.0],
        "SpeedST": [289.0, 289.0, 297.0],
        "IsPersonalBest": [False, False, False],
        "Compound": ["MEDIUM", "MEDIUM", "MEDIUM"],
        "TyreLife": [4.0, 4.0, 4.0],
        "FreshTyre": [True, True, True],
        "Team": ["Mercedes", "Red Bull Racing", "Mercedes"],
        "LapStartTime": [
            pd.Timedelta("0 days 00:59:06.709000"),
            pd.Timedelta("0 days 00:59:03.132000"),
            pd.Timedelta("0 days 00:59:07.380000"),
        ],
        "LapStartDate": [None, None, None],
        "TrackStatus": ["1", "1", "1"],
        "Position": [3.0, 1.0, 4.0],
        "Deleted": [False, False, False],
        "DeletedReason": ["", "", ""],
        "FastF1Generated": [False, False, False],
        "IsAccurate": [True, True, True],
        "EventName": ["Austrian Grand Prix"] * 3,
        "SessionName": ["Race"] * 3,
        "SessionDate": [pd.Timestamp("2024-06-28 12:30:00+0200", tz="UTC+02:00")] * 3,
    }
    session.laps = pd.DataFrame(laps_data)

    # Mock weather
    weather_data = {
        "Time": [
            pd.Timedelta("0 days 00:00:59.837000"),
            pd.Timedelta("0 days 00:01:59.841000"),
            pd.Timedelta("0 days 00:02:59.844000"),
        ],
        "AirTemp": [29.1, 29.0, 29.0],
        "Humidity": [36.0, 35.0, 36.0],
        "Pressure": [932.9, 932.8, 932.9],
        "Rainfall": [False, False, False],
        "TrackTemp": [46.2, 45.9, 45.5],
        "WindDirection": [272, 346, 235],
        "WindSpeed": [1.8, 2.3, 1.7],
        "EventName": ["Austrian Grand Prix"] * 3,
        "SessionName": ["Race"] * 3,
        "SessionDate": [pd.Timestamp("2024-06-28 12:30:00+0200", tz="UTC+02:00")] * 3,
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


def cleanup_test_bucket(storage_client):
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
