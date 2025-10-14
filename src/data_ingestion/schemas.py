"""
Data validation schemas for F1 data using Pydantic.

Provides validation models for race results, qualifying results, lap data,
and weather data to ensure data quality before storage.
"""

from typing import Optional, List
from enum import Enum

import pandas as pd
from pydantic import BaseModel, Field, field_validator, ConfigDict

from config.logging import get_logger

logger = get_logger("data_ingestion.schemas")


class SessionType(str, Enum):
    """Valid F1 session types"""

    RACE = "R"
    QUALIFYING = "Q"
    SPRINT = "S"
    SPRINT_SHOOTOUT = "SS"
    SPRINT_QUALIFYING = "SQ"
    FP1 = "FP1"
    FP2 = "FP2"
    FP3 = "FP3"


class DriverStatus(str, Enum):
    """Possible driver status at end of race"""

    FINISHED = "Finished"
    RETIRED = "Retired"
    DNF = "DNF"
    DISQUALIFIED = "Disqualified"
    NOT_CLASSIFIED = "Not Classified"
    DNS = "DNS"  # Did Not Start
    LAPPED = "Lapped"


class TyreCompound(str, Enum):
    """F1 tyre compounds"""

    SOFT = "SOFT"
    MEDIUM = "MEDIUM"
    HARD = "HARD"
    INTERMEDIATE = "INTERMEDIATE"
    WET = "WET"


class TrackStatus(str, Enum):
    """Track status flags"""

    GREEN = "1"
    YELLOW = "2"
    DOUBLE_YELLOW = "4"
    SAFETY_CAR = "5"
    VIRTUAL_SAFETY_CAR = "6"
    RED_FLAG = "7"


class SessionInfoSchema(BaseModel):
    """Schema for session info"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    event_name: str = Field(..., description="Event name")
    location: str = Field(..., description="Location")
    country: str = Field(..., description="Country")
    session_name: str = Field(..., description="Session name - Race, Qualifying, etc.")
    session_date: pd.Timestamp = Field(..., description="Date of session")
    total_laps: Optional[int] = Field(
        None, ge=0, le=100, description="Scheduled laps for the session"
    )
    event_format: str = Field(
        ..., description="Format of the weekend - conventional or sprint"
    )
    round_number: int = Field(..., description="Round number in the season")
    official_event_name: str = Field(..., description="Official name of the event")


class ResultSchema(BaseModel):
    """Schema for race/sprint/qualifying/FP results"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # Driver Identifiers
    DriverNumber: str = Field(..., description="Driver number as string")
    BroadcastName: str = Field(..., description="Driver broadcast name")
    Abbreviation: str = Field(
        ..., min_length=3, max_length=3, description="3-letter driver code"
    )
    DriverId: str = Field(..., description="Lowercase last name of dirver")
    FirstName: str = Field(..., description="Driver first name")
    LastName: str = Field(..., description="Driver last name")
    FullName: str = Field(..., description="Driver full name")
    CountryCode: str = Field(..., description="3-letter country code")

    # Team Identifiers
    TeamName: str = Field(..., description="Constructor/team name")
    TeamColor: str = Field(..., description="Hex code of team color")
    TeamId: str = Field(..., description="Lowercase name of team")

    # Race results
    Position: float = Field(..., description="Finishing position")
    ClassifiedPosition: str = Field(..., description="Classified position")
    GridPosition: float = Field(
        ..., ge=0, le=22, description="Starting grid position (0 for pit start)"
    )
    Points: float = Field(..., ge=0, le=26, description="Championship points earned")
    Status: str = Field(..., description="Finish status")
    Laps: float = Field(..., ge=0, le=100, description="Laps completed by the driver")

    # Timing (Optional - may be null for DNF)
    Time: Optional[pd.Timedelta] = Field(
        None, description="Race time or time behind winner"
    )

    # Qualifying times (may be in race results)
    Q1: Optional[pd.Timedelta] = Field(None, description="Q1 time")
    Q2: Optional[pd.Timedelta] = Field(None, description="Q2 time")
    Q3: Optional[pd.Timedelta] = Field(None, description="Q3 time")

    # Metadata
    EventName: str = Field(..., description="Event name")
    SessionName: str = Field(..., description="Session name")
    SessionDate: pd.Timestamp = Field(..., description="Date of event session")

    @field_validator("Abbreviation")
    @classmethod
    def validate_abbreviation(cls, v: str) -> str:
        """Ensure abbreviation is uppercase"""
        return v.upper()

    @field_validator("DriverNumber")
    @classmethod
    def validate_driver_number(cls, v: str) -> str:
        """Ensure driver number is valid"""
        try:
            num = int(v)
            if not 1 <= num <= 99:
                raise ValueError(f"Driver number must be between 1 and 99, got {num}")
        except ValueError as e:
            logger.warning("Invalid driver number: %s", v)
            raise ValueError(f"Driver number must be numeric, got {v}") from e
        return v


class LapsSchema(BaseModel):
    """Schema for session laps"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    Time: pd.Timedelta = Field(
        ..., description="Session time when the lap time was set (end of lap)"
    )
    Driver: str = Field(
        ..., min_length=3, max_length=3, description="3-letter driver code"
    )
    DriverNumber: str = Field(..., description="Driver number as string")
    LapTime: pd.Timedelta = Field(..., description="Recorded lap time")
    LapNumber: float = Field(..., description="Recorded lap number")
    Stint: float = Field(..., description="Stint number")
    PitOutTime: Optional[pd.Timedelta] = Field(
        None, description="Session time when car exited the pit"
    )
    PitInTime: Optional[pd.Timedelta] = Field(
        None, description="Session time when car entered the pit"
    )
    Sector1Time: Optional[pd.Timedelta] = Field(
        None, description="Session 1 recorded time"
    )
    Sector2Time: Optional[pd.Timedelta] = Field(
        None, description="Session 2 recorded time"
    )
    Sector3Time: Optional[pd.Timedelta] = Field(
        None, description="Session 3 recorded time"
    )
    Sector1SessionTime: Optional[pd.Timedelta] = Field(
        None, description="Session time when the Sector 1 time was set"
    )
    Sector2SessionTime: Optional[pd.Timedelta] = Field(
        None, description="Session time when the Sector 2 time was set"
    )
    Sector3SessionTime: Optional[pd.Timedelta] = Field(
        None, description="Session time when the Sector 3 time was set"
    )
    SpeedI1: Optional[float] = Field(None, description="Speedtrap sector 1 [km/h]")
    SpeedI2: Optional[float] = Field(None, description="Speedtrap sector 2 [km/h]")
    SpeedFL: Optional[float] = Field(
        None, description="Speedtrap at finish line [km/h]"
    )
    SpeedST: Optional[float] = Field(
        None, description="Speedtrap on longest straight (Not sure) [km/h]"
    )
    IsPersonalBest: bool = Field(
        ...,
        description="Flag to indicate if the lap was the driver's personal best time",
    )
    Compound: Optional[str] = Field(None, description="Tyre compound")
    TyreLife: Optional[int] = Field(None, ge=0, le=60, description="Tyre age in laps")
    FreshTyre: Optional[bool] = Field(None, description="Whether tyre is fresh")
    Team: str = Field(..., description="Team name")
    LapStartTime: pd.Timedelta = Field(
        ..., description="Session time when the lap started"
    )
    LapStartDate: Optional[pd.Timestamp] = Field(
        None, description="Timestamp at the start of the lap"
    )
    TrackStatus: str = Field(..., description="Track status code")
    Position: Optional[float] = Field(
        None, description="Position of the driver at the end of the lap"
    )
    Deleted: bool = Field(
        ..., description="Flag to indicate if the lap time was deleted"
    )
    DeletedReason: Optional[str] = Field(
        "", description="Reason for lap deletion if deleted"
    )
    FastF1Generated: bool = Field(
        ..., description=" Indicates that this lap was added by FastF1"
    )
    IsAccurate: bool = Field(
        ...,
        description="Indicates that the lap start and end time "
        "are synced correctly with other laps",
    )

    # Metadata
    EventName: str = Field(..., description="Event name")
    SessionName: str = Field(..., description="Session name")
    SessionDate: pd.Timestamp = Field(..., description="Date of event session")

    @field_validator("Compound")
    @classmethod
    def validate_compound(cls, v: Optional[str]) -> Optional[str]:
        """Normalize tyre compound names"""

        if v is None:
            return v

        # Normalize compound names
        compound_map = {
            "SOFT": "SOFT",
            "MEDIUM": "MEDIUM",
            "HARD": "HARD",
            "INTERMEDIATE": "INTERMEDIATE",
            "WET": "WET",
        }

        normalized = v.upper()
        if normalized in compound_map:
            return compound_map[normalized]

        logger.warning("Unknown tyre compound: %s", v)
        return v

    @field_validator("LapTime")
    @classmethod
    def validate_lap_time(cls, v: pd.Timedelta) -> pd.Timedelta:
        """Ensure lap time is reasonable"""

        # Reasonable lap time: between 1 minute and 3 minutes
        if v.total_seconds() < 60 or v.total_seconds() > 300:
            logger.warning("Unusual lap time: %s seconds", v.total_seconds())
        return v


class WeatherSchema(BaseModel):
    """Schema for weather data"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    Time: pd.Timedelta = Field(..., description="Time of the data point")
    AirTemp: float = Field(..., ge=-10, le=60, description="Air temperature in Celcius")
    Humidity: float = Field(..., ge=0, le=100, description="Humidity")
    Pressure: float = Field(
        ..., ge=900, le=1100, description="Atmospheric pressure at the track in mbar"
    )
    Rainfall: bool = Field(..., description="Whether it is raining?")
    TrackTemp: float = Field(
        ..., ge=-10, le=80, description="Track temperature in Celsius"
    )
    WindDirection: Optional[float] = Field(
        None, ge=0, le=50, description="Wind speed in m/s"
    )
    WindDirection: Optional[int] = Field(
        None, ge=0, le=360, description="Wind direction in degrees"
    )

    # Metadata
    EventName: str = Field(..., description="Event name")
    SessionName: str = Field(..., description="Session name")
    SessionDate: pd.Timestamp = Field(..., description="Date of event session")

    @field_validator("TrackTemp")
    @classmethod
    def validate_track_temp(cls, v: float, info) -> float:
        """Ensure track temp is warmer than air temp (usually)"""

        air_temp = info.data.get("AirTemp")
        if air_temp and v < air_temp - 5:
            logger.warning(
                "Track temp (%.1f) is significantly lower than air temp (%.1f)",
                v,
                air_temp,
            )
        return v


class RaceControlMessagesSchema(BaseModel):
    """Schema for race control messages"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    Time: pd.Timestamp = Field(..., description="Time of the message")
    Category: str = Field(
        ..., description="Category of the message - track, flag, drs, etc."
    )
    Message: str = Field(..., description="Content of the message")
    Status: Optional[str] = Field(
        None, description="Disabled/Enabled if relevant to the message"
    )
    Flag: Optional[str] = Field(None, description="Flag color if shown")
    Scope: Optional[str] = Field(
        None, description="Scope of the message - driver, track, etc."
    )
    Sector: Optional[str] = Field(
        None, description="Sector for which the message is relevant"
    )
    RacingNumber: Optional[str] = Field(
        None,
        description="Racing number of the driver if message is in scope of the driver",
    )
    Lap: int = Field(..., description="Lap when the message was released")

    # Metadata
    EventName: str = Field(..., description="Event name")
    SessionName: str = Field(..., description="Session name")
    SessionDate: pd.Timestamp = Field(..., description="Date of event session")


class SessionStatusSchema(BaseModel):
    """Schema for session status"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    Time: pd.Timedelta = Field(..., description="Time of session status")
    Status: str = Field(
        ..., description="Status message - inactive, started, finished, end."
    )

    # Metadata
    EventName: str = Field(..., description="Event name")
    SessionName: str = Field(..., description="Session name")
    SessionDate: pd.Timestamp = Field(..., description="Date of event session")


class TrackStatusSchema(BaseModel):
    """Schema for track status"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    Time: pd.Timedelta = Field(..., description="Time of track status")
    Status: str = Field(..., description="Status code")
    Message: str = Field(..., description="Status message")

    # Metadata
    EventName: str = Field(..., description="Event name")
    SessionName: str = Field(..., description="Session name")
    SessionDate: pd.Timestamp = Field(..., description="Date of event session")


class DataValidator:
    """
    Validates DataFrames against Pydantic schemas.

    Handles batch validation of pandas DataFrames and provides
    detailed error reporting.
    """

    def __init__(self):
        self.logger = get_logger("data_ingestion.validator")

    def validate_session_info(
        self,
        df: pd.DataFrame,
        raise_on_error: bool = False,
    ) -> tuple[pd.DataFrame, List[str]]:
        """
        Validate session info DataFrame.
        """
        return self._validate_dataframe(df, SessionInfoSchema, raise_on_error)

    def validate_results(
        self, df: pd.DataFrame, raise_on_error: bool = False
    ) -> tuple[pd.DataFrame, List[str]]:
        """
        Validate race results DataFrame.

        Args:
            df: DataFrame with race results
            raise_on_error: Whether to raise exception on validation errors

        Returns:
            Tuple of (valid_df, list_of_errors)
        """
        return self._validate_dataframe(df, ResultSchema, raise_on_error)

    def validate_lap_data(
        self, df: pd.DataFrame, raise_on_error: bool = False
    ) -> tuple[pd.DataFrame, List[str]]:
        """Validate lap data DataFrame."""

        return self._validate_dataframe(df, LapsSchema, raise_on_error)

    def validate_weather_data(
        self, df: pd.DataFrame, raise_on_error: bool = False
    ) -> tuple[pd.DataFrame, List[str]]:
        """Validate weather data DataFrame."""

        return self._validate_dataframe(df, WeatherSchema, raise_on_error)

    def validate_race_control_messages(
        self, df: pd.DataFrame, raise_on_error: bool = False
    ) -> tuple[pd.DataFrame, List[str]]:
        """Validate race control messages DataFrame"""

        return self._validate_dataframe(df, RaceControlMessagesSchema, raise_on_error)

    def validate_session_status(
        self, df: pd.DataFrame, raise_on_error: bool = False
    ) -> tuple[pd.DataFrame, List[str]]:
        """Validate session status DataFrame"""

        return self._validate_dataframe(df, SessionStatusSchema, raise_on_error)

    def validate_track_status(
        self, df: pd.DataFrame, raise_on_error: bool = False
    ) -> tuple[pd.DataFrame, List[str]]:
        """Validate track status DataFrame"""

        return self._validate_dataframe(df, TrackStatusSchema, raise_on_error)

    def _validate_dataframe(
        self, df: pd.DataFrame, schema: type[BaseModel], raise_on_error: bool = False
    ) -> tuple[pd.DataFrame, List[str]]:
        """
        Generic DataFrame validation against a Pydantic schema.

        Args:
            df: DataFrame to validate
            schema: Pydantic model to validate against
            raise_on_error: Whether to raise exception on validation errors

        Returns:
            Tuple of (valid_rows_df, list_of_errors)
        """

        if df is None or df.empty:
            self.logger.warning("Empty DataFrame provided for validation")
            return df, []

        errors = []
        valid_indices = []

        self.logger.info(
            "Validating %d rows against %s schema", len(df), schema.__name__
        )

        for idx, row in df.iterrows():
            try:
                # Convert row to dict and validate
                row_dict = row.to_dict()
                schema.model_validate(row_dict)
                valid_indices.append(idx)

            except Exception as e:  # pylint: disable=broad-except
                error_msg = f"Row {idx}: {str(e)}"
                errors.append(error_msg)
                self.logger.debug("Validation error: %s", error_msg)

        # Filter to valid rows only
        valid_df = df.loc[valid_indices].copy() if valid_indices else pd.DataFrame()

        # Report results
        if errors:
            self.logger.warning(
                "Validation completed: %d/%d rows valid, %d errors",
                len(valid_df),
                len(df),
                len(errors),
            )
            if raise_on_error:
                raise ValueError(
                    f"Validation failed with {len(errors)} errors:\n"
                    + "\n".join(errors[:5])
                )
        else:
            self.logger.info("✅ All %d rows validated successfully", len(df))

        return valid_df, errors

    def get_validation_summary(self, df: pd.DataFrame, schema: type[BaseModel]) -> dict:
        """
        Get validation summary without filtering data.

        Args:
            df: DataFrame to validate
            schema: Pydantic model to validate against

        Returns:
            Dictionary with validation statistics
        """

        if df is None or df.empty:
            return {
                "total_rows": 0,
                "valid_rows": 0,
                "invalid_rows": 0,
                "validation_rate": 0.0,
                "errors": [],
            }

        _, errors = self._validate_dataframe(df, schema, raise_on_error=False)

        return {
            "total_rows": len(df),
            "valid_rows": len(df) - len(errors),
            "invalid_rows": len(errors),
            "validation_rate": (len(df) - len(errors)) / len(df) * 100,
            "errors": errors[:10],  # First 10 errors
        }


# Convenience function for quick validation
def validate_session_data(
    session_data: dict[str, Optional[pd.DataFrame]],
) -> dict[str, tuple[pd.DataFrame, List[str]]]:
    """
    Validate all data for a session.

    Args:
        session_data: Dictionary with keys 'results', 'laps', 'weather'

    Returns:
        Dictionary with validation results for each data type

    Example:
        >>> session_data = {
        ...     'results': results_df,
        ...     'laps': laps_df,
        ...     'weather': weather_df
        ... }
        >>> validated = validate_session_data(session_data, 'R')
        >>> valid_results, errors = validated['results']
    """
    validator = DataValidator()
    validation_results = {}

    # Validate session info
    if "session_info" in session_data and session_data["session_info"] is not None:
        validation_results["session_info"] = validator.validate_results(
            session_data["session_info"]
        )

    # Validate results (race or qualifying based on session type)
    if "results" in session_data and session_data["results"] is not None:
        validation_results["results"] = validator.validate_results(
            session_data["results"]
        )

    # Validate laps
    if "laps" in session_data and session_data["laps"] is not None:
        validation_results["laps"] = validator.validate_lap_data(session_data["laps"])

    # Validate weather
    if "weather" in session_data and session_data["weather"] is not None:
        validation_results["weather"] = validator.validate_weather_data(
            session_data["weather"]
        )

    # Validate race control message
    if (
        "race_control_messages" in session_data
        and session_data["race_control_messages"] is not None
    ):
        validation_results["race_control_messages"] = (
            validator.validate_race_control_messages(
                session_data["race_control_messages"]
            )
        )

    # Validate session status
    if "session_status" in session_data and session_data["session_status"] is not None:
        validation_results["session_status"] = validator.validate_session_status(
            session_data["session_status"]
        )

    # Validate track status
    if "track_status" in session_data and session_data["track_status"] is not None:
        validation_results["track_status"] = validator.validate_track_status(
            session_data["track_status"]
        )

    return validation_results


# Module-level logger for validation
validation_logger = get_logger("data_ingestion.schemas")
