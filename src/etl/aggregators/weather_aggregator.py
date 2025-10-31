"""
Weather Aggregator

Aggregates weather data for each race session into summary statistics.
Creates features for ML models including temperature, humidity, rainfall, wind conditions.
"""

from typing import Optional, Dict, Any, List
import pandas as pd
import numpy as np

from src.clients.storage_client import StorageClient
from src.etl.aggregators.base_aggregator import BaseAggregator, AggregationResult


class WeatherAggregator(BaseAggregator):
    """
    Aggregate weather conditions for each race session.

    Weather data is sampled every ~1 minute during a session. This aggregator
    computes statistics (mean, min, max, std) for each weather metric per session.

    Output features per race:
    - Temperature metrics (air temp, track temp - avg, min, max, range)
    - Humidity (avg, min, max, std)
    - Pressure (avg, min, max)
    - Rainfall indicators (any rain, rain percentage, rain duration)
    - Wind conditions (avg speed, max speed, dominant direction)
    - Weather changes (temp delta, rain start/end times)
    - Session conditions (wet/dry, temperature category)

    Output schema:
    - Race identifiers: race_id, year, round, event_name, circuit, race_date, session_name
    - Temperature: air_temp_avg/min/max/std, track_temp_avg/min/max/std, temp_range
    - Humidity: humidity_avg/min/max/std
    - Pressure: pressure_avg/min/max
    - Rain: any_rain, rain_pct, rain_duration_minutes
    - Wind: wind_speed_avg/max, wind_direction_dominant
    - Conditions: is_wet_session, temp_category, weather_stability
    """

    def __init__(
        self,
        storage_client: StorageClient,
        raw_bucket: str = "dev-f1-data-raw",
        processed_bucket: str = "dev-f1-data-processed",
        logger_name: str = "etl.weather_aggregator",
    ):
        super().__init__(
            storage_client=storage_client,
            raw_bucket=raw_bucket,
            processed_bucket=processed_bucket,
            logger_name=logger_name,
        )

    def aggregate(  # pylint: disable=arguments-differ
        self,
        catalog_df: pd.DataFrame,
        sample_size: Optional[int] = None,
        session_types: Optional[List[str]] = None,
    ) -> AggregationResult[pd.DataFrame]:
        """
        Aggregate weather data for all sessions.

        Args:
            catalog_df: Validated catalog DataFrame
            sample_size: If provided, only process first N sessions (for testing)
            session_types: List of session types to include (default: ['race', 'qualifying'])

        Returns:
            AggregationResult with weather features per session
        """

        self._log_aggregation_header("Starting Weather Aggregation")
        self._reset_counters()

        # Default to race and qualifying sessions
        if session_types is None:
            session_types = ["Race"]

        # Filter to complete sessions with requested types
        sessions = catalog_df[
            (catalog_df["session"].isin(session_types)) & (catalog_df["is_complete"])
        ].copy()

        if sample_size:
            sessions = sessions.head(sample_size)
            self.logger.info("Processing sample of %d sessions", sample_size)

        total_sessions = len(sessions)
        self.logger.info(
            "Found %d complete sessions to process (types: %s)",
            total_sessions,
            session_types,
        )

        if total_sessions == 0:
            self._record_error("No complete sessions found in catalog!")
            return AggregationResult(
                data=pd.DataFrame(),
                items_processed=0,
                items_skipped=0,
                skipped_items=[],
                warnings=["No complete sessions in catalog"],
                errors=["No data to aggregate"],
            )

        # Process each session
        all_weather = []

        for idx, session_info in sessions.iterrows():
            session_id = f"{session_info['year']}_R{session_info['round']:02d}_{session_info['session']}"  # pylint: disable=line-too-long

            self._log_progress(idx + 1, total_sessions)

            try:
                weather_features = self._process_single_item(session_info)

                if weather_features is not None and not weather_features.empty:
                    all_weather.append(weather_features)
                    self.items_processed += 1
                else:
                    self._record_skip(
                        session_id, "No weather data or failed to compute features"
                    )

            except Exception as e:  # pylint: disable=broad-except
                self.logger.error("Failed to process %s: %s", session_id, str(e))
                self._record_skip(session_id, str(e))

        # Combine all weather data
        if not all_weather:
            self._record_error("No sessions were successfully processed!")
            return AggregationResult(
                data=pd.DataFrame(),
                items_processed=0,
                items_skipped=total_sessions,
                skipped_items=self.skipped_items,
                warnings=self.warnings,
                errors=["All sessions failed to process"] + self.errors,
            )

        combined_df = pd.concat(all_weather, ignore_index=True)

        # Sort by date and session
        combined_df = combined_df.sort_values(["race_date", "session_name"])

        # Validate output
        self._validate_output(combined_df, total_sessions)

        # Log summary
        self._log_aggregation_summary(
            total_items=total_sessions,
            output_size=len(combined_df),
            output_description="session weather records",
        )

        return AggregationResult(
            data=combined_df,
            items_processed=self.items_processed,
            items_skipped=self.items_skipped,
            skipped_items=self.skipped_items,
            warnings=self.warnings,
            errors=self.errors,
        )

    def _process_single_item(self, session_info: pd.Series) -> Optional[pd.DataFrame]:  # pylint: disable=arguments-renamed, arguments-differ
        """
        Process a single session: load weather data, compute aggregate features.

        Args:
            session_info: Row from catalog for this session

        Returns:
            DataFrame with one row of weather features for this session
        """

        year = session_info["year"]
        round_num = session_info["round"]
        session_name = session_info["session"]
        session_id = f"{year}_R{round_num:02d}_{session_name}"

        # Load weather data
        weather_df = self._load_weather_data(session_info)
        if weather_df is None or weather_df.empty:
            self.logger.warning("%s: No weather data found", session_id)
            return None

        # Compute aggregate features
        features = self._compute_weather_features(weather_df, session_info)

        if features is None:
            return None

        # Create DataFrame with one row
        features_df = pd.DataFrame([features])

        # Add session metadata
        features_df["race_id"] = f"{year}_R{round_num:02d}"
        features_df["year"] = year
        features_df["round"] = round_num
        features_df["event_name"] = session_info["event_name"]
        features_df["circuit"] = session_info["circuit"]
        features_df["race_date"] = pd.to_datetime(session_info["session_date"])
        features_df["session_name"] = session_name

        # Standardize column order
        features_df = self._standardize_columns(features_df)

        return features_df

    def _load_weather_data(self, session_info: pd.Series) -> Optional[pd.DataFrame]:
        """Load weather data file from storage"""

        session_path = session_info["session_path"]
        weather_key = f"{session_path}weather.parquet"

        # Required columns for weather features
        required_cols = ["Time", "AirTemp", "TrackTemp", "Humidity", "Rainfall"]

        return self._load_parquet_safe(
            key=weather_key, bucket=self.raw_bucket, required_columns=required_cols
        )

    def _compute_weather_features(
        self,
        weather_df: pd.DataFrame,
        session_info: pd.Series,  # pylint: disable=unused-argument
    ) -> Optional[Dict[str, Any]]:
        """
        Compute comprehensive weather features from raw weather data.

        Args:
            weather_df: Raw weather data (sampled every ~1 minute)
            session_info: Session metadata from catalog

        Returns:
            Dictionary of weather features
        """

        if len(weather_df) < 2:
            # Need at least 2 samples for meaningful statistics
            return None

        features = {}

        # Number of weather samples
        features["num_weather_samples"] = len(weather_df)

        # Temperature features
        features.update(self._compute_temperature_features(weather_df))

        # Humidity features
        features.update(self._compute_humidity_features(weather_df))

        # Pressure features
        features.update(self._compute_pressure_features(weather_df))

        # Rainfall features
        features.update(self._compute_rainfall_features(weather_df))

        # Wind features
        features.update(self._compute_wind_features(weather_df))

        # Weather stability/change features
        features.update(self._compute_stability_features(weather_df))

        # Categorical features
        features.update(self._compute_categorical_features(features))

        return features

    def _compute_temperature_features(
        self, weather_df: pd.DataFrame
    ) -> Dict[str, float]:
        """Compute temperature-related features"""

        features = {}

        # Air temperature
        if "AirTemp" in weather_df.columns:
            air_temp = weather_df["AirTemp"].dropna()
            features["air_temp_avg"] = air_temp.mean()
            features["air_temp_min"] = air_temp.min()
            features["air_temp_max"] = air_temp.max()
            features["air_temp_std"] = air_temp.std()
            features["air_temp_range"] = air_temp.max() - air_temp.min()
        else:
            for key in [
                "air_temp_avg",
                "air_temp_min",
                "air_temp_max",
                "air_temp_std",
                "air_temp_range",
            ]:
                features[key] = np.nan

        # Track temperature
        if "TrackTemp" in weather_df.columns:
            track_temp = weather_df["TrackTemp"].dropna()
            features["track_temp_avg"] = track_temp.mean()
            features["track_temp_min"] = track_temp.min()
            features["track_temp_max"] = track_temp.max()
            features["track_temp_std"] = track_temp.std()
            features["track_temp_range"] = track_temp.max() - track_temp.min()
        else:
            for key in [
                "track_temp_avg",
                "track_temp_min",
                "track_temp_max",
                "track_temp_std",
                "track_temp_range",
            ]:
                features[key] = np.nan

        # Temperature differential (track - air)
        if "AirTemp" in weather_df.columns and "TrackTemp" in weather_df.columns:
            temp_diff = weather_df["TrackTemp"] - weather_df["AirTemp"]
            features["temp_diff_avg"] = temp_diff.mean()
            features["temp_diff_max"] = temp_diff.max()
        else:
            features["temp_diff_avg"] = np.nan
            features["temp_diff_max"] = np.nan

        return features

    def _compute_humidity_features(self, weather_df: pd.DataFrame) -> Dict[str, float]:
        """Compute humidity features"""

        features = {}

        if "Humidity" in weather_df.columns:
            humidity = weather_df["Humidity"].dropna()
            features["humidity_avg"] = humidity.mean()
            features["humidity_min"] = humidity.min()
            features["humidity_max"] = humidity.max()
            features["humidity_std"] = humidity.std()
            features["humidity_range"] = humidity.max() - humidity.min()
        else:
            for key in [
                "humidity_avg",
                "humidity_min",
                "humidity_max",
                "humidity_std",
                "humidity_range",
            ]:
                features[key] = np.nan

        return features

    def _compute_pressure_features(self, weather_df: pd.DataFrame) -> Dict[str, float]:
        """Compute atmospheric pressure features"""

        features = {}

        if "Pressure" in weather_df.columns:
            pressure = weather_df["Pressure"].dropna()
            features["pressure_avg"] = pressure.mean()
            features["pressure_min"] = pressure.min()
            features["pressure_max"] = pressure.max()
            features["pressure_range"] = pressure.max() - pressure.min()
        else:
            for key in [
                "pressure_avg",
                "pressure_min",
                "pressure_max",
                "pressure_range",
            ]:
                features[key] = np.nan

        return features

    def _compute_rainfall_features(self, weather_df: pd.DataFrame) -> Dict[str, Any]:
        """Compute rainfall-related features"""

        features = {}

        if "Rainfall" in weather_df.columns:
            rainfall = weather_df["Rainfall"]

            # Any rain during session
            features["any_rain"] = rainfall.any()

            # Percentage of samples with rain
            features["rain_pct"] = (rainfall.sum() / len(rainfall)) * 100

            # Approximate rain duration (samples * ~1 minute per sample)
            features["rain_samples"] = rainfall.sum()

            # Rain start/end (rough approximation)
            if rainfall.any():
                rain_indices = weather_df[rainfall].index
                features["rain_started_early"] = rain_indices[0] < len(weather_df) * 0.3
                features["rain_started_late"] = rain_indices[0] > len(weather_df) * 0.7
            else:
                features["rain_started_early"] = False
                features["rain_started_late"] = False
        else:
            features["any_rain"] = False
            features["rain_pct"] = 0.0
            features["rain_samples"] = 0
            features["rain_started_early"] = False
            features["rain_started_late"] = False

        return features

    def _compute_wind_features(self, weather_df: pd.DataFrame) -> Dict[str, float]:
        """Compute wind-related features"""

        features = {}

        # Wind speed
        if "WindSpeed" in weather_df.columns:
            wind_speed = weather_df["WindSpeed"].dropna()
            features["wind_speed_avg"] = wind_speed.mean()
            features["wind_speed_max"] = wind_speed.max()
            features["wind_speed_std"] = wind_speed.std()
        else:
            features["wind_speed_avg"] = np.nan
            features["wind_speed_max"] = np.nan
            features["wind_speed_std"] = np.nan

        # Wind direction (dominant/most common)
        if "WindDirection" in weather_df.columns:
            wind_dir = weather_df["WindDirection"].dropna()
            if len(wind_dir) > 0:
                # Use mode (most common direction)
                features["wind_direction_dominant"] = (
                    wind_dir.mode().iloc[0] if len(wind_dir.mode()) > 0 else np.nan
                )
                features["wind_direction_std"] = wind_dir.std()
            else:
                features["wind_direction_dominant"] = np.nan
                features["wind_direction_std"] = np.nan
        else:
            features["wind_direction_dominant"] = np.nan
            features["wind_direction_std"] = np.nan

        return features

    def _compute_stability_features(self, weather_df: pd.DataFrame) -> Dict[str, float]:
        """Compute weather stability/change features"""

        features = {}

        # Temperature change over session
        if "AirTemp" in weather_df.columns:
            air_temp = weather_df["AirTemp"].dropna()
            if len(air_temp) >= 2:
                # Linear trend (positive = getting warmer)
                try:
                    x = np.arange(len(air_temp))
                    slope, _ = np.polyfit(x, air_temp.values, 1)
                    features["air_temp_trend"] = slope
                except:  # pylint: disable=bare-except  # noqa: E722
                    features["air_temp_trend"] = 0.0

                # First vs last third comparison
                third = max(1, len(air_temp) // 3)
                features["air_temp_start"] = air_temp.iloc[:third].mean()
                features["air_temp_end"] = air_temp.iloc[-third:].mean()
                features["air_temp_delta"] = (
                    features["air_temp_end"] - features["air_temp_start"]
                )
            else:
                features["air_temp_trend"] = 0.0
                features["air_temp_start"] = air_temp.mean()
                features["air_temp_end"] = air_temp.mean()
                features["air_temp_delta"] = 0.0
        else:
            for key in [
                "air_temp_trend",
                "air_temp_start",
                "air_temp_end",
                "air_temp_delta",
            ]:
                features[key] = np.nan

        # Track temperature change
        if "TrackTemp" in weather_df.columns:
            track_temp = weather_df["TrackTemp"].dropna()
            if len(track_temp) >= 2:
                try:
                    x = np.arange(len(track_temp))
                    slope, _ = np.polyfit(x, track_temp.values, 1)
                    features["track_temp_trend"] = slope
                except:  # pylint: disable=bare-except  # noqa: E722
                    features["track_temp_trend"] = 0.0

                third = max(1, len(track_temp) // 3)
                features["track_temp_delta"] = (
                    track_temp.iloc[-third:].mean() - track_temp.iloc[:third].mean()
                )
            else:
                features["track_temp_trend"] = 0.0
                features["track_temp_delta"] = 0.0
        else:
            features["track_temp_trend"] = np.nan
            features["track_temp_delta"] = np.nan

        return features

    def _compute_categorical_features(self, features: Dict[str, Any]) -> Dict[str, Any]:
        """Compute categorical/derived features"""

        categorical = {}

        # Is this a wet session?
        categorical["is_wet_session"] = (
            features.get("any_rain", False) or features.get("rain_pct", 0) > 5
        )

        # Temperature category
        air_temp = features.get("air_temp_avg", np.nan)
        if pd.notna(air_temp):
            if air_temp < 15:
                categorical["temp_category"] = "cold"
            elif air_temp < 25:
                categorical["temp_category"] = "moderate"
            elif air_temp < 35:
                categorical["temp_category"] = "warm"
            else:
                categorical["temp_category"] = "hot"
        else:
            categorical["temp_category"] = "unknown"

        # Weather stability (based on temperature std)
        air_temp_std = features.get("air_temp_std", np.nan)
        if pd.notna(air_temp_std):
            if air_temp_std < 0.5:
                categorical["weather_stability"] = "stable"
            elif air_temp_std < 1.5:
                categorical["weather_stability"] = "moderate"
            else:
                categorical["weather_stability"] = "variable"
        else:
            categorical["weather_stability"] = "unknown"

        return categorical

    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names and order"""

        # Define desired column order
        column_order = [
            # Session identifiers
            "race_id",
            "year",
            "round",
            "event_name",
            "circuit",
            "race_date",
            "session_name",
            # Metadata
            "num_weather_samples",
            # Air temperature
            "air_temp_avg",
            "air_temp_min",
            "air_temp_max",
            "air_temp_std",
            "air_temp_range",
            "air_temp_trend",
            "air_temp_start",
            "air_temp_end",
            "air_temp_delta",
            # Track temperature
            "track_temp_avg",
            "track_temp_min",
            "track_temp_max",
            "track_temp_std",
            "track_temp_range",
            "track_temp_trend",
            "track_temp_delta",
            # Temperature differential
            "temp_diff_avg",
            "temp_diff_max",
            # Humidity
            "humidity_avg",
            "humidity_min",
            "humidity_max",
            "humidity_std",
            "humidity_range",
            # Pressure
            "pressure_avg",
            "pressure_min",
            "pressure_max",
            "pressure_range",
            # Rainfall
            "any_rain",
            "rain_pct",
            "rain_samples",
            "rain_started_early",
            "rain_started_late",
            # Wind
            "wind_speed_avg",
            "wind_speed_max",
            "wind_speed_std",
            "wind_direction_dominant",
            "wind_direction_std",
            # Categorical
            "is_wet_session",
            "temp_category",
            "weather_stability",
        ]

        # Reorder columns (keep any extra columns at end)
        available_cols = [col for col in column_order if col in df.columns]
        extra_cols = [col for col in df.columns if col not in column_order]

        return df[available_cols + extra_cols]

    def _validate_output(self, df: pd.DataFrame, total_sessions: int):  # pylint: disable=arguments-renamed
        """
        Validate aggregated weather data.

        Checks:
        - Expected number of records (one per session)
        - No duplicate sessions
        - Required columns present
        - Reasonable value ranges
        """

        self.logger.info("Validating weather data...")

        # Check record count (should match total sessions)
        self._validate_record_count(
            actual=len(df),
            expected_min=total_sessions - 5,  # Allow some failures
            expected_max=total_sessions,
            data_description="session weather records",
        )

        # Check for duplicates
        self._check_duplicates(
            df=df,
            subset=["race_id", "session_name"],
            description="race-session combinations",
        )

        # Check required columns
        required_cols = [
            "race_id",
            "session_name",
            "air_temp_avg",
            "track_temp_avg",
            "humidity_avg",
            "any_rain",
        ]
        self._check_required_columns(df, required_cols)

        # Check for reasonable temperature values (-10 to 60°C)
        if "air_temp_avg" in df.columns:
            invalid_temps = df[(df["air_temp_avg"] < -10) | (df["air_temp_avg"] > 60)]
            if len(invalid_temps) > 0:
                self._record_warning(
                    f"Found {len(invalid_temps)} sessions with unrealistic air temperatures"
                )

        # Check null rates
        self._check_null_threshold(
            df=df,
            column="air_temp_avg",
            threshold=0.1,
            description="average air temperatures",
        )

        self._check_null_threshold(
            df=df,
            column="track_temp_avg",
            threshold=0.1,
            description="average track temperatures",
        )

        self.logger.info("✅ Validation complete")


# def main():
#     """Example usage and testing"""

#     # Load catalog
#     storage = StorageClient()
#     catalog_df = storage.download_dataframe(
#         bucket_name="dev-f1-data-processed", object_key="catalog/race_catalog.parquet"
#     )

#     # Aggregate weather (test with 5 sessions)
#     aggregator = WeatherAggregator(storage)
#     result = aggregator.aggregate(
#         catalog_df, sample_size=5, session_types=["race", "qualifying"]
#     )

#     print(f"\n{'=' * 80}")
#     print("WEATHER AGGREGATION RESULTS")
#     print(f"{'=' * 80}")
#     print(f"Sessions processed: {result.items_processed}")
#     print(f"Sessions skipped: {result.items_skipped}")
#     print(f"Success rate: {result.success_rate:.1%}")
#     print(f"Output shape: {result.data.shape}")

#     if len(result.warnings) > 0:
#         print(f"\nWarnings ({len(result.warnings)}):")
#         for warning in result.warnings[:5]:
#             print(f"  - {warning}")

#     if not result.data.empty:
#         print(f"\n{'=' * 80}")
#         print("SAMPLE OUTPUT")
#         print(f"{'=' * 80}")
#         print(result.data.head())

#         print(f"\n{'=' * 80}")
#         print("FEATURE COLUMNS")
#         print(f"{'=' * 80}")
#         for col in result.data.columns:
#             print(f"  - {col}")

#         print(f"\n{'=' * 80}")
#         print("WEATHER STATISTICS")
#         print(f"{'=' * 80}")
#         feature_cols = [
#             "air_temp_avg",
#             "track_temp_avg",
#             "humidity_avg",
#             "rain_pct",
#             "wind_speed_avg",
#         ]
#         available = [c for c in feature_cols if c in result.data.columns]
#         print(result.data[available].describe())

#     return result


# if __name__ == "__main__":
#     result = main()
