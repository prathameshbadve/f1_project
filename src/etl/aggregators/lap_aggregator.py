"""
Lap Features Aggregator

Aggregates lap-level statistics for each driver in each race session.
Creates features for ML models including pace, consistency, tire management, etc.
"""

from typing import Optional, Dict, Any
import pandas as pd
import numpy as np

from src.clients.storage_client import StorageClient
from src.etl.aggregators.base_aggregator import BaseAggregator, AggregationResult


class LapFeaturesAggregator(BaseAggregator):
    """
    Aggregate lap-level features for each driver in each race.

    Output features per driver per race:
    - Pace metrics (avg/median/best lap time)
    - Consistency metrics (std, CV, IQR of lap times)
    - Tire management (degradation rate, stint analysis)
    - Position changes (overtakes, positions lost)
    - Sector performance (best sectors, sector consistency)
    - Track evolution (improvement rate over race)

    Output schema:
    - Race identifiers: race_id, year, round, event_name, circuit, race_date
    - Driver info: driver_id, driver_number, full_name, team_name
    - Lap statistics: total_laps, valid_laps, deleted_laps
    - Pace features: avg_lap_time, median_lap_time, best_lap_time, worst_lap_time
    - Consistency: lap_time_std, lap_time_cv, lap_time_iqr
    - Degradation: lap_time_trend, tire_deg_per_lap
    - Sector stats: avg_sector1/2/3, best_sector1/2/3
    - Position: avg_position, position_changes, net_position_change
    """

    # Reasonable lap time bounds (in seconds)
    MIN_LAP_TIME = 60  # 1 minute
    MAX_LAP_TIME = 180  # 3 minutes

    def __init__(
        self,
        storage_client: StorageClient,
        raw_bucket: str = "dev-f1-data-raw",
        processed_bucket: str = "dev-f1-data-processed",
        logger_name: str = "etl.lap_aggregator",
    ):
        super().__init__(
            storage_client=storage_client,
            raw_bucket=raw_bucket,
            processed_bucket=processed_bucket,
            logger_name=logger_name,
        )

    def aggregate(
        self,
        catalog_df: pd.DataFrame,
        sample_size: Optional[int] = None,
        **kwargs,
    ) -> AggregationResult[pd.DataFrame]:
        """
        Aggregate lap features for all race sessions.

        Args:
            catalog_df: Validated catalog DataFrame
            sample_size: If provided, only process first N races (for testing)

        Returns:
            AggregationResult with lap features per driver per race
        """

        self._log_aggregation_header("Starting Lap Features Aggregation")
        self._reset_counters()

        # Filter to complete race sessions only
        race_sessions = catalog_df[
            (catalog_df["session"] == "Race") & catalog_df["is_complete"]
        ].copy()

        if sample_size:
            race_sessions = race_sessions.head(sample_size)
            self.logger.info("Processing sample of %d races", sample_size)

        total_races = len(race_sessions)
        self.logger.info("Found %d complete race sessions to process", total_races)

        if total_races == 0:
            self._record_error("No complete race sessions found in catalog!")
            return AggregationResult(
                data=pd.DataFrame(),
                items_processed=0,
                items_skipped=0,
                skipped_items=[],
                warnings=["No complete race sessions in catalog"],
                errors=["No data to aggregate"],
            )

        # Process each race
        all_features = []

        for idx, race_info in race_sessions.iterrows():
            race_id = race_info["race_id"]

            self._log_progress(idx + 1, total_races)

            try:
                lap_features_df = self._process_single_item(race_info)

                if lap_features_df is not None and not lap_features_df.empty:
                    all_features.append(lap_features_df)
                    self.items_processed += 1
                else:
                    self._record_skip(
                        race_id, "No lap data or failed to compute features"
                    )

            except Exception as e:  # pylint: disable=broad-except
                self.logger.error("Failed to process %s: %s", race_id, str(e))
                self._record_skip(race_id, str(e))

        # Combine all features
        if not all_features:
            self._record_error("No races were successfully processed!")
            return AggregationResult(
                data=pd.DataFrame(),
                items_processed=0,
                items_skipped=total_races,
                skipped_items=self.skipped_items,
                warnings=self.warnings,
                errors=["All races failed to process"] + self.errors,
            )

        combined_df = pd.concat(all_features, ignore_index=True)

        # Sort by date and driver
        combined_df = combined_df.sort_values(["race_date", "driver_number"])

        # Validate output
        self._validate_output(combined_df, total_races)

        # Log summary
        self._log_aggregation_summary(
            total_items=total_races,
            output_size=len(combined_df),
            output_description="driver-race lap feature records",
        )

        return AggregationResult(
            data=combined_df,
            items_processed=self.items_processed,
            items_skipped=self.items_skipped,
            skipped_items=self.skipped_items,
            warnings=self.warnings,
            errors=self.errors,
        )

    # pylint: disable=arguments-renamed
    def _process_single_item(
        self,
        race_info: pd.Series,
        **kwargs,
    ) -> Optional[pd.DataFrame]:
        """
        Process a single race: load lap data, compute features per driver.

        Args:
            race_info: Row from catalog for this race

        Returns:
            DataFrame with lap features per driver for this race
        """

        year = race_info["year"]
        round_num = race_info["round"]
        race_id = race_info["race_id"]

        self.logger.info("Started processed Race: %s", race_id)

        # Load lap data
        laps_df = self._load_lap_data(race_info)
        if laps_df is None or laps_df.empty:
            self.logger.warning("%s: No lap data found", race_id)
            return None

        # Compute features per driver
        features_list = []

        # Get unique drivers
        drivers = laps_df["DriverNumber"].unique()

        for driver_num in drivers:
            driver_laps = laps_df[laps_df["DriverNumber"] == driver_num].copy()

            driver_features = self._compute_driver_lap_features(driver_laps, driver_num)

            if driver_features is not None:
                features_list.append(driver_features)

        if not features_list:
            self.logger.warning("%s: No features computed for any driver", race_id)
            return None

        # Combine all driver features
        features_df = pd.DataFrame(features_list)

        # Add race metadata
        features_df["race_id"] = race_id
        features_df["year"] = year
        features_df["round"] = round_num
        features_df["event_name"] = race_info["event_name"]
        features_df["circuit"] = race_info["circuit"]
        features_df["race_date"] = pd.to_datetime(race_info["session_date"])

        # Standardize column order
        features_df = self._standardize_columns(features_df)

        return features_df

    def _load_lap_data(
        self,
        race_info: pd.Series,
    ) -> Optional[pd.DataFrame]:
        """Load lap data file from storage with required columns"""

        race_path = race_info["session_path"]
        laps_key = f"{race_path}laps.parquet"

        self.logger.info("Loading lap data from: %s", laps_key)

        # Required columns for lap features
        required_cols = [
            "DriverNumber",
            "Team",
            "LapNumber",
            "Position",
            "Stint",
            "FreshTyre",
            "Compound",
            "TyreLife",
            "LapTime",
            "LapTimeSeconds",
            "Sector1Time",
            "Sector2Time",
            "Sector3Time",
            "IsPersonalBest",
            "Deleted",
            "DeletedReason",
            "TrackStatus",
        ]

        laps_df = self._load_parquet_safe(
            key=laps_key,
            bucket=self.raw_bucket,
            required_columns=required_cols,
        )

        if laps_df is None:
            return None

        # Convert LapTimeSeconds if it's not already numeric
        if "LapTimeSeconds" in laps_df.columns:
            if not pd.api.types.is_numeric_dtype(laps_df["LapTimeSeconds"]):
                try:
                    laps_df["LapTimeSeconds"] = pd.to_numeric(
                        laps_df["LapTimeSeconds"], errors="coerce"
                    )
                except:  # pylint: disable=bare-except  # noqa: E722
                    # Fallback: convert from LapTime
                    if "LapTime" in laps_df.columns:
                        laps_df["LapTimeSeconds"] = pd.to_timedelta(
                            laps_df["LapTime"]
                        ).dt.total_seconds()

        return laps_df[required_cols]

    def _compute_driver_lap_features(
        self, driver_laps: pd.DataFrame, driver_num: int
    ) -> Optional[Dict[str, Any]]:
        """
        Compute comprehensive lap-level features for a single driver.

        Args:
            driver_laps: All laps for this driver in this race
            driver_num: Driver number

        Returns:
            Dictionary of features, or None if insufficient data
        """

        self.logger.info("| | | Computing Driver Lap Features")

        # Get basic lap times
        lap_times = driver_laps["LapTimeSeconds"].copy()

        # Filter out invalid laps (outliers, pit laps, safety car)
        valid_mask = (
            (lap_times > self.MIN_LAP_TIME)
            & (lap_times < self.MAX_LAP_TIME)
            & (lap_times.notna())
        )

        # Track Status 1 indicates clear track, all others indicate yellow/red flag, safety car/VSC
        if "TrackStatus" in driver_laps.columns:
            valid_mask = valid_mask & (driver_laps["TrackStatus"] == "1")

        # Also exclude deleted laps if that column exists
        if "Deleted" in driver_laps.columns:
            valid_mask = valid_mask & (~driver_laps["Deleted"])

        valid_laps = lap_times[valid_mask]
        # valid_lap_numbers = driver_laps.loc[valid_mask, "LapNumber"]

        if len(valid_laps) < 5:  # Need at least 5 valid laps for meaningful stats
            return None

        # Initialize feature dict
        features = {
            "driver_number": driver_num,
        }

        # Get driver info if available
        if "Driver" in driver_laps.columns:
            features["driver_abbreviation"] = driver_laps["Driver"].iloc[0]
        else:
            features["driver_abbreviation"] = f"#{driver_num}"

        if "Team" in driver_laps.columns:
            features["team_name"] = driver_laps["Team"].iloc[0]
        else:
            features["team_name"] = "Unknown"

        # Lap counts
        features.update(self._compute_lap_counts(driver_laps, valid_mask))

        # Pace metrics
        features.update(self._compute_pace_metrics(valid_laps))

        # Consistency metrics
        features.update(self._compute_consistency_metrics(valid_laps))

        # Degradation/trend
        features.update(
            self._compute_degradation_metrics(driver_laps, valid_laps, valid_mask)
        )

        # Sector performance (if available)
        features.update(self._compute_sector_metrics(driver_laps, valid_mask))

        # Position metrics (if available)
        features.update(self._compute_position_metrics(driver_laps))

        # Tire metrics (if available)
        features.update(self._compute_tire_metrics(driver_laps))

        return features

    def _compute_lap_counts(
        self, driver_laps: pd.DataFrame, valid_mask: pd.Series
    ) -> Dict[str, int]:
        """Compute lap count statistics"""

        self.logger.info("| | | | | | Computing Lap Counts")

        total_laps = len(driver_laps)
        valid_laps = valid_mask.sum()
        deleted_laps = (
            driver_laps["Deleted"].sum() if "Deleted" in driver_laps.columns else 0
        )

        return {
            "total_laps": total_laps,
            "valid_laps": valid_laps,
            "deleted_laps": deleted_laps,
            "invalid_laps": total_laps - valid_laps,
        }

    def _compute_pace_metrics(self, valid_laps: pd.Series) -> Dict[str, float]:
        """Compute pace-related metrics"""

        self.logger.info("| | | | | | Computing Pace Metrics")

        return {
            "avg_lap_time": valid_laps.mean(),
            "median_lap_time": valid_laps.median(),
            "best_lap_time": valid_laps.min(),
            "worst_lap_time": valid_laps.max(),
            "lap_time_range": valid_laps.max() - valid_laps.min(),
            "pct_95_lap_time": valid_laps.quantile(0.95),  # 95th percentile
            "pct_05_lap_time": valid_laps.quantile(0.05),  # 5th percentile (fast laps)
        }

    def _compute_consistency_metrics(self, valid_laps: pd.Series) -> Dict[str, float]:
        """Compute consistency-related metrics"""

        self.logger.info("| | | | | | Computing Consistency Counts")

        lap_std = valid_laps.std()
        lap_mean = valid_laps.mean()

        # Coefficient of variation (std / mean)
        cv = lap_std / lap_mean if lap_mean > 0 else 0

        # Interquartile range
        q75 = valid_laps.quantile(0.75)
        q25 = valid_laps.quantile(0.25)
        iqr = q75 - q25

        # Seconds off personal best (consistency measure)
        seconds_off_best = lap_mean - valid_laps.min()

        return {
            "lap_time_std": lap_std,
            "lap_time_cv": cv,
            "lap_time_iqr": iqr,
            "seconds_off_best": seconds_off_best,
        }

    def _compute_degradation_metrics(
        self,
        driver_laps: pd.DataFrame,
        valid_laps: pd.Series,
        valid_mask: pd.Series,
    ) -> Dict[str, float]:
        """
        Compute tire degradation and trend metrics.

        Positive trend = getting slower (tire deg)
        Negative trend = getting faster (track evolution, fuel load)
        """

        self.logger.info("| | | | | | Computing Degradation Metrics")

        if len(valid_laps) < 3:
            return {
                "lap_time_trend": 0.0,
                "tire_deg_per_lap": 0.0,
                "first_stint_avg": valid_laps.mean(),
                "last_stint_avg": valid_laps.mean(),
            }

        # Linear trend (slope)
        try:
            x = np.arange(len(valid_laps))
            y = valid_laps.values

            # Use polyfit for linear regression
            slope, _ = np.polyfit(x, y, 1)

        except Exception as e:  # pylint: disable=broad-except
            self.logger.warning("Failed to compute lap time trend: %s", str(e))
            slope = 0.0

        # First vs last stint comparison
        # First stint: first 1/3 of laps
        # Last stint: last 1/3 of laps
        # n_laps = len(valid_laps)
        # stint_size = max(5, n_laps // 3)

        # first_stint = valid_laps.iloc[:stint_size]
        # last_stint = valid_laps.iloc[-stint_size:]

        # first_stint_avg = first_stint.mean()
        # last_stint_avg = last_stint.mean()

        valid_driver_laps = driver_laps[valid_mask]

        stint_wise_means = (
            valid_driver_laps[["LapTimeSeconds", "Stint"]]
            .groupby("Stint")["LapTimeSeconds"]
            .mean()
        )

        first_stint_avg = float(stint_wise_means.iloc[0])
        last_stint_avg = float(stint_wise_means.iloc[-1])

        return {
            "lap_time_trend": slope,
            "tire_deg_per_lap": slope,  # Alias for clarity
            "first_stint_avg": first_stint_avg,
            "last_stint_avg": last_stint_avg,
            "stint_delta": last_stint_avg - first_stint_avg,
        }

    def _compute_sector_metrics(
        self, driver_laps: pd.DataFrame, valid_mask: pd.Series
    ) -> Dict[str, float]:
        """Compute sector-level metrics if sector data available"""

        self.logger.info("| | | | | | Computing Sector Metrics")

        features = {}

        # Check for sector columns
        sector_cols = ["Sector1Time", "Sector2Time", "Sector3Time"]
        available_sectors = [col for col in sector_cols if col in driver_laps.columns]

        if not available_sectors:
            # No sector data available
            return {
                "avg_sector1": np.nan,
                "avg_sector2": np.nan,
                "avg_sector3": np.nan,
                "best_sector1": np.nan,
                "best_sector2": np.nan,
                "best_sector3": np.nan,
            }

        # Compute sector stats
        for i, col in enumerate(["Sector1Time", "Sector2Time", "Sector3Time"], 1):
            if col not in driver_laps.columns:
                features[f"avg_sector{i}"] = np.nan
                features[f"best_sector{i}"] = np.nan
                continue

            # Convert to seconds if needed
            sector_times = driver_laps.loc[valid_mask, col]

            if sector_times.dtype == "object" or pd.api.types.is_timedelta64_dtype(
                sector_times
            ):
                sector_times = pd.to_timedelta(sector_times).dt.total_seconds()

            # Filter valid sector times (15-60 seconds typical)
            valid_sectors = sector_times[
                (sector_times > 15) & (sector_times < 60) & (sector_times.notna())
            ]

            if len(valid_sectors) > 0:
                features[f"avg_sector{i}"] = valid_sectors.mean()
                features[f"best_sector{i}"] = valid_sectors.min()
            else:
                features[f"avg_sector{i}"] = np.nan
                features[f"best_sector{i}"] = np.nan

        return features

    def _compute_position_metrics(self, driver_laps: pd.DataFrame) -> Dict[str, float]:
        """Compute position-related metrics if position data available"""

        self.logger.info("| | | | | | Computing Position Metrics")

        if "Position" not in driver_laps.columns:
            return {
                "avg_position": np.nan,
                "best_position": np.nan,
                "worst_position": np.nan,
                "position_changes": 0,
                "net_position_change": 0,
            }

        positions = driver_laps["Position"].dropna()

        if len(positions) == 0:
            return {
                "avg_position": np.nan,
                "best_position": np.nan,
                "worst_position": np.nan,
                "position_changes": 0,
                "net_position_change": 0,
            }

        # Position changes (number of times position changed lap-to-lap)
        position_diffs = positions.diff().abs()
        position_changes = (position_diffs > 0).sum()

        # Net position change (start to finish)
        if len(positions) >= 2:
            net_change = (
                positions.iloc[0] - positions.iloc[-1]
            )  # Positive = gained positions
        else:
            net_change = 0

        return {
            "avg_position": positions.mean(),
            "best_position": positions.min(),
            "worst_position": positions.max(),
            "position_changes": position_changes,
            "net_position_change": net_change,
        }

    def _compute_tire_metrics(
        self,
        driver_laps: pd.DataFrame,
    ) -> Dict[str, Any]:
        """Compute tire-related metrics if tire data available"""

        self.logger.info("| | | | | | Computing Tire Metrics")

        features = {}

        # Check for tire columns
        if "Compound" not in driver_laps.columns:
            return {
                "num_pit_stops": 0,
                "num_tire_stints": 0,
                "avg_stint_length": np.nan,
            }

        # Count pit stops (Number of stints - 1)
        num_stints = int(driver_laps["Stint"].nunique())
        num_pit_stops = num_stints - 1
        avg_stint_length = len(driver_laps) / num_stints if num_stints > 0 else np.nan

        features["num_pit_stops"] = num_pit_stops
        features["num_tire_stints"] = num_stints
        features["avg_stint_length"] = avg_stint_length

        return features

    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names and order"""

        self.logger.info("| | | | | | Standardizing Columns")

        # Define desired column order
        column_order = [
            # Race identifiers
            "race_id",
            "year",
            "round",
            "event_name",
            "circuit",
            "race_date",
            # Driver info
            "driver_number",
            "driver_abbreviation",
            "team_name",
            # Lap counts
            "total_laps",
            "valid_laps",
            "deleted_laps",
            "invalid_laps",
            # Pace metrics
            "avg_lap_time",
            "median_lap_time",
            "best_lap_time",
            "worst_lap_time",
            "lap_time_range",
            "pct_05_lap_time",
            "pct_95_lap_time",
            # Consistency
            "lap_time_std",
            "lap_time_cv",
            "lap_time_iqr",
            "seconds_off_best",
            # Degradation
            "lap_time_trend",
            "tire_deg_per_lap",
            "first_stint_avg",
            "last_stint_avg",
            "stint_delta",
            # Sectors
            "avg_sector1",
            "avg_sector2",
            "avg_sector3",
            "best_sector1",
            "best_sector2",
            "best_sector3",
            # Position
            "avg_position",
            "best_position",
            "worst_position",
            "position_changes",
            "net_position_change",
            # Tires
            "num_pit_stops",
            "num_tire_stints",
            "avg_stint_length",
        ]

        # Reorder columns (keep any extra columns at end)
        available_cols = [col for col in column_order if col in df.columns]
        extra_cols = [col for col in df.columns if col not in column_order]

        return df[available_cols + extra_cols]

    def _validate_output(self, df: pd.DataFrame, total_races: int):  # pylint: disable=arguments-renamed
        """
        Validate aggregated lap features.

        Checks:
        - Expected number of records (drivers per race)
        - No duplicate driver-race combinations
        - Required columns present
        - Reasonable value ranges
        """

        self.logger.info("Validating lap features...")

        # Check record count (expect 15-25 drivers per race)
        expected_min = total_races * 15
        expected_max = total_races * 25

        self._validate_record_count(
            actual=len(df),
            expected_min=expected_min,
            expected_max=expected_max,
            data_description="driver-race lap feature records",
        )

        # Check for duplicates
        self._check_duplicates(
            df=df,
            subset=["race_id", "driver_number"],
            description="driver-race combinations",
        )

        # Check required columns
        required_cols = [
            "race_id",
            "driver_number",
            "avg_lap_time",
            "best_lap_time",
            "lap_time_std",
            "total_laps",
            "valid_laps",
        ]
        self._check_required_columns(df, required_cols)

        # Check for reasonable lap times (60-180 seconds)
        if "avg_lap_time" in df.columns:
            invalid_times = df[
                (df["avg_lap_time"] < self.MIN_LAP_TIME)
                | (df["avg_lap_time"] > self.MAX_LAP_TIME)
            ]
            if len(invalid_times) > 0:
                self._record_warning(
                    f"Found {len(invalid_times)} records with unrealistic avg lap times"
                )

        # Check for high null rates in key metrics
        self._check_null_threshold(
            df=df, column="avg_lap_time", threshold=0.1, description="average lap times"
        )

        self._check_null_threshold(
            df=df, column="best_lap_time", threshold=0.1, description="best lap times"
        )

        # Check valid lap percentage
        if "valid_laps" in df.columns and "total_laps" in df.columns:
            df["valid_lap_pct"] = df["valid_laps"] / df["total_laps"]
            low_valid = df[df["valid_lap_pct"] < 0.5]
            if len(low_valid) > 0:
                self._record_warning(
                    f"Found {len(low_valid)} drivers with <50% valid laps"
                )

        self.logger.info("✅ Validation complete")


# def main():
#     """Example usage and testing"""

#     # Load catalog
#     storage = StorageClient()
#     catalog_df = storage.download_dataframe(
#         bucket_name="dev-f1-data-processed", object_key="catalog/race_catalog.parquet"
#     )

#     # Aggregate lap features (test with 3 races)
#     aggregator = LapFeaturesAggregator(storage)
#     result = aggregator.aggregate(catalog_df, sample_size=3)

#     print(f"\n{'=' * 80}")
#     print("LAP FEATURES AGGREGATION RESULTS")
#     print(f"{'=' * 80}")
#     print(f"Races processed: {result.items_processed}")
#     print(f"Races skipped: {result.items_skipped}")
#     print(f"Success rate: {result.success_rate:.1%}")
#     print(f"Output shape: {result.data.shape}")

#     if len(result.warnings) > 0:
#         print(f"\nWarnings ({len(result.warnings)}):")
#         for warning in result.warnings[:5]:  # Show first 5
#             print(f"  - {warning}")

#     if len(result.errors) > 0:
#         print(f"\nErrors ({len(result.errors)}):")
#         for error in result.errors[:5]:  # Show first 5
#             print(f"  - {error}")

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
#         print("FEATURE STATISTICS")
#         print(f"{'=' * 80}")
#         feature_cols = [
#             "avg_lap_time",
#             "best_lap_time",
#             "lap_time_std",
#             "lap_time_cv",
#             "tire_deg_per_lap",
#             "num_pit_stops",
#         ]
#         available_features = [c for c in feature_cols if c in result.data.columns]
#         print(result.data[available_features].describe())

#     return result


# if __name__ == "__main__":
#     result = main()
