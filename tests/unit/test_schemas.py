"""
Unit tests for data validation schemas.

Tests Pydantic schemas with mock data (no API calls).
"""

import pytest
import pandas as pd
from datetime import timedelta, datetime

from src.data_ingestion.schemas import (
    ResultSchema,
    LapsSchema,
    WeatherSchema,
    DataValidator,
    validate_session_data,
)


@pytest.mark.unit
class TestRaceResultSchema:
    """Test RaceResultSchema validation"""

    def test_valid_race_result(self, sample_race_result):
        """Test that valid race result passes validation"""

        result = ResultSchema(**sample_race_result)

        assert result.DriverNumber == "1"
        assert result.Abbreviation == "VER"
        assert result.Position == 1.0
        assert result.Points == 25.0

    def test_abbreviation_uppercase_conversion(self):
        """Test that abbreviation is converted to uppercase"""

        data = {
            "DriverNumber": "1",
            "BroadcastName": "M VERSTAPPEN",
            "Abbreviation": "ver",  # lowercase
            "TeamName": "Red Bull Racing",
            "Position": 1.0,
            "GridPosition": 1.0,
            "Points": 25.0,
            "Status": "Finished",
            "Year": 2024,
            "RoundNumber": 16,
            "EventName": "Italian Grand Prix",
            "SessionType": "R",
        }

        result = ResultSchema(**data)
        assert result.Abbreviation == "VER"  # Should be uppercase

    def test_invalid_position(self):
        """Test that invalid position fails validation"""

        data = {
            "DriverNumber": "1",
            "BroadcastName": "M VERSTAPPEN",
            "Abbreviation": "VER",
            "TeamName": "Red Bull Racing",
            "Position": 25.0,  # Invalid: > 22
            "GridPosition": 1.0,
            "Points": 25.0,
            "Status": "Finished",
            "Year": 2024,
            "RoundNumber": 16,
            "EventName": "Italian Grand Prix",
            "SessionType": "R",
        }

        with pytest.raises(Exception):
            ResultSchema(**data)

    def test_invalid_driver_number(self):
        """Test that non-numeric driver number fails"""
        data = {
            "DriverNumber": "ABC",  # Invalid: not numeric
            "BroadcastName": "M VERSTAPPEN",
            "Abbreviation": "VER",
            "TeamName": "Red Bull Racing",
            "Position": 1.0,
            "GridPosition": 1.0,
            "Points": 25.0,
            "Status": "Finished",
            "Year": 2024,
            "RoundNumber": 16,
            "EventName": "Italian Grand Prix",
            "SessionType": "R",
        }

        with pytest.raises(Exception):
            ResultSchema(**data)

    def test_optional_time_field(self):
        """Test that Time field is optional (for DNF)"""
        data = {
            "DriverNumber": "1",
            "BroadcastName": "M VERSTAPPEN",
            "Abbreviation": "VER",
            "TeamName": "Red Bull Racing",
            "Position": 15.0,
            "GridPosition": 1.0,
            "Points": 0.0,
            "Status": "DNF",
            "Time": None,  # No time for DNF
            "Year": 2024,
            "RoundNumber": 16,
            "EventName": "Italian Grand Prix",
            "SessionType": "R",
        }

        result = ResultSchema(**data)
        assert result.Time is None
        assert result.Status == "DNF"


@pytest.mark.unit
class TestLapsSchema:
    """Test LapsSchema validation"""

    def test_valid_lap_data(self, sample_lap_data):
        """Test that valid lap data passes validation"""
        lap = LapsSchema(**sample_lap_data)

        assert lap.DriverNumber == "1"
        assert lap.LapNumber == 1
        assert lap.Compound == "SOFT"

    def test_compound_normalization(self):
        """Test that tyre compound is normalized"""

        data = {
            "DriverNumber": "1",
            "LapNumber": 1,
            "Time": datetime.now(),
            "LapTime": timedelta(minutes=1, seconds=32),
            "Compound": "soft",  # lowercase
            "Year": 2024,
            "RoundNumber": 16,
            "EventName": "Italian Grand Prix",
            "SessionType": "R",
        }

        lap = LapsSchema(**data)
        assert lap.Compound == "SOFT"  # Should be uppercase

    def test_invalid_lap_number(self):
        """Test that invalid lap number fails validation"""

        data = {
            "DriverNumber": "1",
            "LapNumber": 0,  # Invalid: must be >= 1
            "Time": datetime.now(),
            "LapTime": timedelta(minutes=1, seconds=32),
            "Year": 2024,
            "RoundNumber": 16,
            "EventName": "Italian Grand Prix",
            "SessionType": "R",
        }

        with pytest.raises(Exception):
            LapsSchema(**data)

    def test_invalid_tyre_life(self):
        """Test that invalid tyre life fails validation"""
        data = {
            "DriverNumber": "1",
            "LapNumber": 1,
            "Time": datetime.now(),
            "LapTime": timedelta(minutes=1, seconds=32),
            "TyreLife": 100,  # Invalid: > 60
            "Year": 2024,
            "RoundNumber": 16,
            "EventName": "Italian Grand Prix",
            "SessionType": "R",
        }

        with pytest.raises(Exception):
            LapsSchema(**data)


@pytest.mark.unit
class TestWeatherSchema:
    """Test WeatherSchema validation"""

    def test_valid_weather_data(self, sample_weather_data):
        """Test that valid weather data passes validation"""
        weather = WeatherSchema(**sample_weather_data)

        assert weather.AirTemp == 28.5
        assert weather.TrackTemp == 42.3
        assert weather.Rainfall is False

    def test_invalid_temperature(self):
        """Test that invalid temperature fails validation"""
        data = {
            "Time": datetime.now(),
            "AirTemp": 100.0,  # Invalid: too hot
            "TrackTemp": 42.0,
            "Humidity": 45.0,
            "Pressure": 1013.0,
            "Rainfall": False,
            "Year": 2024,
            "RoundNumber": 16,
            "EventName": "Italian Grand Prix",
            "SessionType": "R",
        }

        with pytest.raises(Exception):
            WeatherSchema(**data)

    def test_invalid_humidity(self):
        """Test that invalid humidity fails validation"""
        data = {
            "Time": datetime.now(),
            "AirTemp": 28.0,
            "TrackTemp": 42.0,
            "Humidity": 150.0,  # Invalid: > 100
            "Pressure": 1013.0,
            "Rainfall": False,
            "Year": 2024,
            "RoundNumber": 16,
            "EventName": "Italian Grand Prix",
            "SessionType": "R",
        }

        with pytest.raises(Exception):
            WeatherSchema(**data)


@pytest.mark.unit
class TestDataValidator:
    """Test DataValidator class"""

    def test_validate_race_results_all_valid(
        self, sample_race_results_df, data_validator
    ):
        """Test validating DataFrame with all valid rows"""
        valid_df, errors = data_validator.validate_race_results(sample_race_results_df)

        assert len(valid_df) == len(sample_race_results_df)
        assert len(errors) == 0

    def test_validate_race_results_with_invalid_rows(self, data_validator):
        """Test validating DataFrame with some invalid rows"""
        df = pd.DataFrame(
            {
                "DriverNumber": ["1", "ABC", "16"],  # "ABC" is invalid
                "BroadcastName": ["M VERSTAPPEN", "S PEREZ", "C LECLERC"],
                "Abbreviation": ["VER", "PER", "LEC"],
                "TeamName": ["Red Bull Racing", "Red Bull Racing", "Ferrari"],
                "Position": [1.0, 2.0, 3.0],
                "GridPosition": [1.0, 2.0, 3.0],
                "Points": [25.0, 18.0, 15.0],
                "Status": ["Finished", "Finished", "Finished"],
                "Year": [2024, 2024, 2024],
                "RoundNumber": [16, 16, 16],
                "EventName": ["Italian GP", "Italian GP", "Italian GP"],
                "SessionType": ["R", "R", "R"],
            }
        )

        valid_df, errors = data_validator.validate_race_results(df)

        assert len(valid_df) == 2  # Only 2 valid rows
        assert len(errors) == 1  # 1 error
        assert "ABC" in errors[0]  # Error message contains invalid value

    def test_validate_empty_dataframe(self, data_validator):
        """Test validating empty DataFrame"""
        df = pd.DataFrame()

        valid_df, errors = data_validator.validate_race_results(df)

        assert valid_df.empty
        assert len(errors) == 0

    def test_validate_with_raise_on_error(self, data_validator):
        """Test that raise_on_error works"""
        df = pd.DataFrame(
            {
                "DriverNumber": ["ABC"],  # Invalid
                "BroadcastName": ["Test"],
                "Abbreviation": ["TST"],
                "TeamName": ["Test Team"],
                "Position": [1.0],
                "GridPosition": [1.0],
                "Points": [25.0],
                "Status": ["Finished"],
                "Year": [2024],
                "RoundNumber": [16],
                "EventName": ["Test"],
                "SessionType": ["R"],
            }
        )

        with pytest.raises(ValueError):
            data_validator.validate_race_results(df, raise_on_error=True)

    def test_get_validation_summary(self, sample_race_results_df, data_validator):
        """Test validation summary"""
        summary = data_validator.get_validation_summary(
            sample_race_results_df, ResultSchema
        )

        assert summary["total_rows"] == len(sample_race_results_df)
        assert summary["valid_rows"] == len(sample_race_results_df)
        assert summary["invalid_rows"] == 0
        assert summary["validation_rate"] == 100.0


@pytest.mark.unit
class TestValidateSessionData:
    """Test validate_session_data convenience function"""

    def test_validate_complete_race_session(
        self, sample_race_results_df, data_validator
    ):
        """Test validating complete race session"""
        # Create sample laps and weather
        laps_data = {
            "DriverNumber": ["1", "1", "1"],
            "LapNumber": [1, 2, 3],
            "Time": [datetime.now()] * 3,
            "LapTime": [timedelta(minutes=1, seconds=32)] * 3,
            "Year": [2024] * 3,
            "RoundNumber": [16] * 3,
            "EventName": ["Italian Grand Prix"] * 3,
            "SessionType": ["R"] * 3,
        }
        laps_df = pd.DataFrame(laps_data)

        weather_data = {
            "Time": [datetime.now()] * 3,
            "AirTemp": [28.5, 28.6, 28.7],
            "TrackTemp": [42.0, 42.1, 42.2],
            "Humidity": [45.0, 45.1, 45.2],
            "Pressure": [1013.0, 1013.1, 1013.2],
            "Rainfall": [False, False, False],
            "Year": [2024] * 3,
            "RoundNumber": [16] * 3,
            "EventName": ["Italian Grand Prix"] * 3,
            "SessionType": ["R"] * 3,
        }
        weather_df = pd.DataFrame(weather_data)

        session_data = {
            "results": sample_race_results_df,
            "laps": laps_df,
            "weather": weather_df,
        }

        validated = validate_session_data(session_data)

        assert "results" in validated
        assert "laps" in validated
        assert "weather" in validated

        # Check each has valid data and errors
        for key in ["results", "laps", "weather"]:
            valid_df, errors = validated[key]
            assert isinstance(valid_df, pd.DataFrame)
            assert isinstance(errors, list)
            assert not valid_df.empty
            assert len(errors) == 0

    def test_validate_qualifying_session(self):
        """Test validating qualifying session"""
        quali_data = {
            "DriverNumber": ["1", "11", "16"],
            "BroadcastName": ["M VERSTAPPEN", "S PEREZ", "C LECLERC"],
            "Abbreviation": ["VER", "PER", "LEC"],
            "TeamName": ["Red Bull Racing", "Red Bull Racing", "Ferrari"],
            "Position": [1.0, 2.0, 3.0],
            "Q1": [timedelta(minutes=1, seconds=30)] * 3,
            "Q2": [timedelta(minutes=1, seconds=29)] * 3,
            "Q3": [timedelta(minutes=1, seconds=28)] * 3,
            "Year": [2024] * 3,
            "RoundNumber": [16] * 3,
            "EventName": ["Italian Grand Prix"] * 3,
            "SessionType": ["Q"] * 3,
        }
        quali_df = pd.DataFrame(quali_data)

        session_data = {"results": quali_df}
        validated = validate_session_data(session_data)

        assert "results" in validated
        valid_df, errors = validated["results"]
        assert len(valid_df) == 3
        assert len(errors) == 0

    def test_validate_session_with_none_data(self):
        """Test validating session with None data"""
        session_data = {"results": None, "laps": None, "weather": None}

        validated = validate_session_data(session_data)

        # Should handle None gracefully
        assert len(validated) == 0  # No data to validate


@pytest.mark.unit
class TestSchemaEdgeCases:
    """Test edge cases and boundary conditions"""

    def test_minimum_valid_position(self):
        """Test position = 1 (minimum)"""
        data = {
            "DriverNumber": "1",
            "BroadcastName": "M VERSTAPPEN",
            "Abbreviation": "VER",
            "TeamName": "Red Bull Racing",
            "Position": 1.0,  # Minimum valid
            "GridPosition": 1.0,
            "Points": 25.0,
            "Status": "Finished",
            "Year": 2024,
            "RoundNumber": 16,
            "EventName": "Italian Grand Prix",
            "SessionType": "R",
        }

        result = ResultSchema(**data)
        assert result.Position == 1.0

    def test_maximum_valid_position(self):
        """Test position = 22 (maximum)"""
        data = {
            "DriverNumber": "1",
            "BroadcastName": "M VERSTAPPEN",
            "Abbreviation": "VER",
            "TeamName": "Red Bull Racing",
            "Position": 22.0,  # Maximum valid
            "GridPosition": 20.0,
            "Points": 0.0,
            "Status": "Finished",
            "Year": 2024,
            "RoundNumber": 16,
            "EventName": "Italian Grand Prix",
            "SessionType": "R",
        }

        result = ResultSchema(**data)
        assert result.Position == 22.0

    def test_pit_lane_start(self):
        """Test grid position = 0 (pit lane start)"""
        data = {
            "DriverNumber": "1",
            "BroadcastName": "M VERSTAPPEN",
            "Abbreviation": "VER",
            "TeamName": "Red Bull Racing",
            "Position": 15.0,
            "GridPosition": 0.0,  # Pit lane start
            "Points": 0.0,
            "Status": "Finished",
            "Year": 2024,
            "RoundNumber": 16,
            "EventName": "Italian Grand Prix",
            "SessionType": "R",
        }

        result = ResultSchema(**data)
        assert result.GridPosition == 0.0

    def test_maximum_points(self):
        """Test maximum points (25 + fastest lap)"""
        data = {
            "DriverNumber": "1",
            "BroadcastName": "M VERSTAPPEN",
            "Abbreviation": "VER",
            "TeamName": "Red Bull Racing",
            "Position": 1.0,
            "GridPosition": 1.0,
            "Points": 26.0,  # 25 + 1 for fastest lap
            "Status": "Finished",
            "Year": 2024,
            "RoundNumber": 16,
            "EventName": "Italian Grand Prix",
            "SessionType": "R",
        }

        result = ResultSchema(**data)
        assert result.Points == 26.0

    def test_very_long_lap_time(self):
        """Test very long lap time (e.g., safety car)"""
        data = {
            "DriverNumber": "1",
            "LapNumber": 1,
            "Time": datetime.now(),
            "LapTime": timedelta(minutes=3, seconds=0),  # Very slow
            "Year": 2024,
            "RoundNumber": 16,
            "EventName": "Italian Grand Prix",
            "SessionType": "R",
        }

        # Should accept but log warning (check logs manually)
        lap = LapsSchema(**data)
        assert (lap.LapTime / timedelta(seconds=1)) == 180

    def test_extreme_weather_conditions(self):
        """Test extreme weather conditions"""
        data = {
            "Time": datetime.now(),
            "AirTemp": -5.0,  # Very cold
            "TrackTemp": 0.0,
            "Humidity": 95.0,
            "Pressure": 950.0,  # Low pressure
            "Rainfall": True,
            "Year": 2024,
            "RoundNumber": 16,
            "EventName": "Italian Grand Prix",
            "SessionType": "R",
        }

        weather = WeatherSchema(**data)
        assert weather.AirTemp == -5.0
        assert weather.Rainfall is True


@pytest.mark.unit
def test_all_tyre_compounds():
    """Test all valid tyre compounds"""
    compounds = ["SOFT", "MEDIUM", "HARD", "INTERMEDIATE", "WET"]

    for compound in compounds:
        data = {
            "DriverNumber": "1",
            "LapNumber": 1,
            "Time": datetime.now(),
            "LapTime": timedelta(minutes=1, seconds=32),
            "Compound": compound,
            "Year": 2024,
            "RoundNumber": 16,
            "EventName": "Italian Grand Prix",
            "SessionType": "R",
        }

        lap = LapsSchema(**data)
        assert lap.Compound == compound


@pytest.mark.unit
def test_schema_with_missing_optional_fields():
    """Test that schemas work with missing optional fields"""
    # Race result with minimal fields
    data = {
        "DriverNumber": "1",
        "BroadcastName": "M VERSTAPPEN",
        "Abbreviation": "VER",
        "TeamName": "Red Bull Racing",
        "Position": 1.0,
        "GridPosition": 1.0,
        "Points": 25.0,
        "Status": "Finished",
        "Year": 2024,
        "RoundNumber": 16,
        "EventName": "Italian Grand Prix",
        "SessionType": "R",
        # No Time, Q1, Q2, Q3
    }

    result = ResultSchema(**data)
    assert result.Time is None
    assert result.Q1 is None
    assert result.Q2 is None
    assert result.Q3 is None


@pytest.mark.unit
def test_validation_performance(sample_race_results_df, data_validator, benchmark):
    """Benchmark validation performance"""

    # Pytest-benchmark will automatically measure performance
    def validate():
        return data_validator.validate_race_results(sample_race_results_df)

    # This requires pytest-benchmark: pip install pytest-benchmark
    try:
        valid_df, errors = benchmark(validate)
        assert len(valid_df) == len(sample_race_results_df)
    except Exception:  # pylint: disable=broad-exception-caught
        # If pytest-benchmark not installed, just run normally
        valid_df, errors = validate()
        assert len(valid_df) == len(sample_race_results_df)
