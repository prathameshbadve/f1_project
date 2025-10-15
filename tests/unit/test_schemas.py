"""
Unit tests for data validation schemas.

Tests Pydantic schemas with mock data (no API calls).
"""

from datetime import timedelta, datetime

import pytest
import pandas as pd

from src.data_ingestion.schemas import (
    ResultSchema,
    LapsSchema,
    WeatherSchema,
    validate_session_data,
)


@pytest.mark.unit
class TestResultSchema:
    """Test ResultSchema validation"""

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

        result = ResultSchema(**data)
        assert result.Abbreviation == "VER"  # Should be uppercase

    def test_invalid_position(self):
        """Test that invalid position fails validation"""

        data = {
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
            "Position": 25.0,  # Invalid: > 20
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

        with pytest.raises(Exception):
            ResultSchema(**data)

    def test_invalid_driver_number(self):
        """Test that non-numeric driver number fails"""
        data = {
            "DriverNumber": "ABC",  # Invalid: not numeric
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

        with pytest.raises(Exception):
            ResultSchema(**data)

    def test_optional_time_field(self):
        """Test that Time field is optional (for DNF)"""
        data = {
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
            "Time": None,  # No time DNF
            "Status": "DNF",
            "Points": 25.0,
            "Laps": 53.0,
            "Year": 2024,
            "EventName": "Italian Grand Prix",
            "SessionName": "Race",
            "SessionDate": pd.Timestamp("2024-09-01 13:00:00"),
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
        assert lap.LapNumber == 8.0
        assert lap.Compound == "SOFT"

    def test_compound_normalization(self):
        """Test that tyre compound is normalized"""

        data = {
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
            "Compound": "soft",  # Invalid: lowercase
            "TyreLife": 8,
            "FreshTyre": False,
            "TrackStatus": "1",  # Green flag
            "Position": 1.0,
            "EventName": "Italian Grand Prix",
            "SessionName": "Race",
            "SessionDate": pd.Timestamp("2024-09-01 13:00:00"),
        }

        lap = LapsSchema(**data)
        assert lap.Compound == "SOFT"  # Should be uppercase

    def test_invalid_lap_number(self):
        """Test that invalid lap number fails validation"""

        data = {
            "Time": pd.Timedelta("00:15:23.456"),
            "LapTime": pd.Timedelta("00:01:32.847"),
            "LapNumber": 0,  # Invalid: < 1
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

        with pytest.raises(Exception):
            LapsSchema(**data)

    def test_invalid_tyre_life(self):
        """Test that invalid tyre life fails validation"""
        data = {
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
            "TyreLife": 62,  # Invalid: > 60
            "FreshTyre": False,
            "TrackStatus": "1",  # Green flag
            "Position": 1.0,
            "EventName": "Italian Grand Prix",
            "SessionName": "Race",
            "SessionDate": pd.Timestamp("2024-09-01 13:00:00"),
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

        with pytest.raises(Exception):
            WeatherSchema(**data)

    def test_invalid_humidity(self):
        """Test that invalid humidity fails validation"""
        data = {
            "Time": datetime.now(),
            "AirTemp": 28.0,
            "Humidity": 150.0,  # Invalid: > 100
            "Pressure": 1013.2,
            "Rainfall": False,
            "TrackTemp": 42.3,
            "WindDirection": 180,
            "WindSpeed": 3.5,
            "EventName": "Italian Grand Prix",
            "SessionName": "Race",
            "SessionDate": pd.Timestamp("2024-09-01 13:00:00"),
        }

        with pytest.raises(Exception):
            WeatherSchema(**data)

    def test_invalid_track_temperature(self):
        """Test that invalid track temperature relative to air temperature"""
        data = {
            "Time": datetime.now(),
            "AirTemp": 28.0,
            "Humidity": 45.2,
            "Pressure": 1013.2,
            "Rainfall": False,
            "TrackTemp": 20.0,  # Invalid: < AirTemp
            "WindDirection": 180,
            "WindSpeed": 3.5,
            "EventName": "Italian Grand Prix",
            "SessionName": "Race",
            "SessionDate": pd.Timestamp("2024-09-01 13:00:00"),
        }

        with pytest.raises(Exception):
            WeatherSchema(**data)


@pytest.mark.unit
class TestDataValidator:
    """Test DataValidator class"""

    def test_validate_results_all_valid(self, sample_race_results_df, data_validator):
        """Test validating DataFrame with all valid rows"""
        valid_df, errors = data_validator.validate_results(sample_race_results_df)

        assert len(valid_df) == len(sample_race_results_df)
        assert len(errors) == 0

    def test_validate_race_results_with_invalid_rows(self, data_validator):
        """Test validating DataFrame with some invalid rows"""
        df = pd.DataFrame(
            {
                "DriverNumber": ["1", "11", "ABC", "55", "44"],  # Invalid: ABC
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
                "Year": [2024] * 5,
                "EventName": ["Italian Grand Prix"] * 5,
                "SessionName": ["Race"] * 5,
                "SessionDate": [pd.Timestamp("2024-09-01 13:00:00")] * 5,
            }
        )

        valid_df, errors = data_validator.validate_results(df)

        assert len(valid_df) == 4  # Only 4 valid rows
        assert len(errors) == 1  # 1 error
        assert "ABC" in errors[0]  # Error message contains invalid value

    def test_validate_empty_dataframe(self, data_validator):
        """Test validating empty DataFrame"""
        df = pd.DataFrame()

        valid_df, errors = data_validator.validate_results(df)

        assert valid_df.empty
        assert len(errors) == 0

    def test_validate_with_raise_on_error(self, data_validator):
        """Test that raise_on_error works"""
        df = pd.DataFrame(
            {
                "DriverNumber": ["ABC"],  # Invalid: numeric required
                "BroadcastName": ["M VERSTAPPEN"],
                "Abbreviation": ["VER"],
                "DriverId": ["max_verstappen"],
                "TeamName": ["Red Bull Racing"],
                "TeamColor": ["3671C6"],
                "TeamId": ["red_bull"],
                "FirstName": ["Max"],
                "LastName": ["Verstappen"],
                "FullName": ["Max Verstappen"],
                "HeadshotUrl": ["google.com"],
                "CountryCode": ["NED"],
                "Position": [1.0],
                "ClassifiedPosition": ["1"],
                "GridPosition": [1.0],
                "Q1": [pd.Timedelta(minutes=1, seconds=30)],
                "Q2": [pd.Timedelta(minutes=1, seconds=29)],
                "Q3": [pd.Timedelta(minutes=1, seconds=28)],
                "Time": [pd.Timedelta(hours=1, minutes=30, seconds=45)],
                "Status": ["Finished"],
                "Points": [25.0],
                "Year": [2024],
                "EventName": ["Italian Grand Prix"],
                "SessionName": ["Race"],
                "SessionDate": [pd.Timestamp("2024-09-01 13:00:00")],
            }
        )

        with pytest.raises(ValueError):
            data_validator.validate_results(df, raise_on_error=True)

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

    def test_validate_complete_race_session(self, sample_race_results_df):
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
            "Humidity": [45.0, 45.1, 45.2],
            "Pressure": [1013.0, 1013.1, 1013.2],
            "Rainfall": [False, False, False],
            "TrackTemp": [42.0, 42.1, 42.2],
            "WindDirection": [180, 175, 190],
            "WindSpeed": [3.5, 3.4, 3.1],
            "EventName": ["Italian Grand Prix"] * 3,
            "SessionName": ["Race"] * 3,
            "SessionDate": [pd.Timestamp("2024-09-01 13:00:00")] * 3,
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
        return data_validator.validate_results(sample_race_results_df)

    # This requires pytest-benchmark: pip install pytest-benchmark
    try:
        valid_df, _ = benchmark(validate)
        assert len(valid_df) == len(sample_race_results_df)

    except Exception:  # pylint: disable=broad-exception-caught
        # If pytest-benchmark not installed, just run normally

        valid_df, _ = validate()
        assert len(valid_df) == len(sample_race_results_df)
