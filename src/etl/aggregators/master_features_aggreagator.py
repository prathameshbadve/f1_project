"""
Master Features Aggregator

Combines all feature sources into a single ML-ready dataset:
- Race results (base + target variable)
- Qualifying features
- Lap features (driver performance)
- Weather features (environmental conditions)
- Circuit features (track characteristics)
- Historical features (rolling averages)

Target: Probability of finishing in top 10
"""

from typing import Optional
import pandas as pd
import numpy as np

from src.clients.storage_client import StorageClient
from src.etl.aggregators.base_aggregator import BaseAggregator, AggregationResult


class MasterFeaturesAggregator(BaseAggregator):
    """
    Aggregate all features into master ML dataset.

    Output schema:
    - Race/Driver identifiers
    - Target variable: top_10_finish (0/1)
    - Qualifying features (Q times, gaps, normalized positions)
    - Lap features (pace, consistency, tire management)
    - Weather features (track conditions)
    - Circuit features (track type, characteristics)
    - Historical features (rolling averages over last 5 races)
    - Team features (teammate comparisons)
    """

    # Constants
    HISTORICAL_WINDOW = 5  # Number of previous races for rolling features

    def __init__(
        self,
        storage_client: StorageClient,
        raw_bucket: str = "dev-f1-data-raw",
        processed_bucket: str = "dev-f1-data-processed",
        logger_name: str = "etl.master_features",
    ):
        super().__init__(
            storage_client=storage_client,
            raw_bucket=raw_bucket,
            processed_bucket=processed_bucket,
            logger_name=logger_name,
        )

        # Cache for loaded data
        self._race_results_df = None
        self._lap_features_df = None
        self._weather_features_df = None
        self._circuit_metadata = None

    def aggregate(
        self,
        catalog_df: pd.DataFrame,
        sample_size: Optional[int] = None,
        **kwargs,
    ) -> AggregationResult[pd.DataFrame]:
        """
        Create master features dataset.

        Args:
            catalog_df: Validated catalog DataFrame
            sample_size: If provided, only process first N races (for testing)

        Returns:
            AggregationResult with master features dataset
        """

        self._log_aggregation_header("Starting Master Features Aggregation")
        self._reset_counters()

        # Step 1: Load all feature sources
        self.logger.info("Step 1: Loading feature sources...")
        if not self._load_all_features():
            return self._return_empty_result("Failed to load feature sources")

        # Step 2: Start with race results as base
        self.logger.info("Step 2: Preparing base dataset from race results...")
        master_df = self._prepare_base_dataset()

        if master_df.empty:
            return self._return_empty_result("No race results found")

        # Apply sample size if specified
        if sample_size:
            master_df = master_df.head(sample_size)
            self.logger.info("Processing sample of %d records", sample_size)

        # Step 3: Merge qualifying features
        self.logger.info("Step 3: Adding qualifying features...")
        master_df = self._add_qualifying_features(master_df)

        # Step 4: Merge lap features
        self.logger.info("Step 4: Adding lap features...")
        master_df = self._add_lap_features(master_df)

        # Step 5: Merge weather features
        self.logger.info("Step 5: Adding weather features...")
        master_df = self._add_weather_features(master_df)

        # Step 6: Add circuit characteristics
        # self.logger.info("Step 6: Adding circuit characteristics...")
        # master_df = self._add_circuit_features(master_df)

        # Step 7: Compute team-based features
        self.logger.info("Step 7: Computing team features...")
        master_df = self._add_team_features(master_df)

        # Step 8: Compute historical rolling features
        self.logger.info("Step 8: Computing historical rolling features...")
        master_df = self._add_historical_features(master_df)

        # Step 9: Create target variable
        self.logger.info("Step 9: Creating target variable...")
        master_df = self._create_target_variable(master_df)

        # Step 10: Final cleanup and validation
        self.logger.info("Step 10: Final validation...")
        master_df = self._finalize_dataset(master_df)

        # Log summary
        self._log_aggregation_summary(
            total_items=len(master_df),
            output_size=len(master_df),
            output_description="driver-race master feature records",
        )

        return AggregationResult(
            data=master_df,
            items_processed=len(master_df),
            items_skipped=0,
            skipped_items=[],
            warnings=self.warnings,
            errors=self.errors,
        )

    def _load_all_features(self) -> bool:
        """Load all required feature datasets from storage."""

        try:
            # Load race results
            self.logger.info("  Loading race results...")
            self._race_results_df = self.storage.download_dataframe(
                bucket_name=self.processed_bucket,
                object_key="aggregated/race_results_all.parquet",
            )
            self.logger.info(
                "    ✓ Loaded %d race result records", len(self._race_results_df)
            )

            # Load lap features
            self.logger.info("  Loading lap features...")
            self._lap_features_df = self.storage.download_dataframe(
                bucket_name=self.processed_bucket,
                object_key="aggregated/lap_features_all.parquet",
            )
            self.logger.info(
                "    ✓ Loaded %d lap feature records", len(self._lap_features_df)
            )

            # Load weather features
            self.logger.info("  Loading weather features...")
            self._weather_features_df = self.storage.download_dataframe(
                bucket_name=self.processed_bucket,
                object_key="aggregated/weather_all.parquet",
            )
            self.logger.info(
                "    ✓ Loaded %d weather feature records",
                len(self._weather_features_df),
            )

            # Load circuit metadata
            # self.logger.info("  Loading circuit metadata...")
            # self._circuit_metadata = self._load_circuit_metadata()
            # self.logger.info(
            #     "    ✓ Loaded %d circuit records", len(self._circuit_metadata)
            # )

            return True

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to load feature sources: %s", str(e))
            self._record_error(f"Feature loading failed: {str(e)}")
            return False

    def _load_circuit_metadata(self) -> pd.DataFrame:
        """Load circuit characteristics metadata."""

        # Try to load from reference data first
        try:
            circuit_meta = self.storage.download_dataframe(
                object_key="reference/circuit_characteristics.csv"
            )
            return circuit_meta
        except Exception:  # pylint: disable=broad-exception-caught
            self.logger.warning(
                "Circuit characteristics file not found, using basic metadata"
            )

            # Fallback: load basic circuit info from any circuits.csv in raw data
            # We'll create a basic version with just circuit_id
            return pd.DataFrame(
                columns=["circuit_id", "circuit_type", "circuit_technical"]
            )

    def _prepare_base_dataset(self) -> pd.DataFrame:
        """Prepare base dataset from race results."""

        base_df = self._race_results_df.copy()

        # Select core columns for master dataset
        base_columns = [
            "race_id",
            "year",
            "round",
            "event_name",
            "circuit",
            "race_date",
            "driver_number",
            "driver_id",
            "driver_abbreviation",
            "driver_name",
            "team_id",
            "team_name",
            "q1_seconds",
            "q2_seconds",
            "q3_seconds",
            "classified_position",
            "finish_position",
            "grid_position",
            "points",
            "status",
            "finished_race",
            "dnf",
            "position_gain",  # Already computed in race_results_aggregator
            "finished_top10",
        ]

        # Keep only columns that exist
        available_columns = [col for col in base_columns if col in base_df.columns]
        base_df = base_df[available_columns].copy()

        # Sort by date and position for proper historical feature computation
        base_df = base_df.sort_values(["race_date", "finish_position"])

        self.logger.info("  Base dataset: %d records", len(base_df))

        return base_df

    def _add_qualifying_features(self, master_df: pd.DataFrame) -> pd.DataFrame:
        """Add qualifying-based features."""

        # Qualifying data is already in race_results (Q1, Q2, Q3 columns)
        # We'll compute additional derived features

        if (
            "q1_seconds" in master_df.columns
            or "q2_seconds" in master_df.columns
            or "q3_seconds" in master_df.columns
        ):
            # Best qualifying time (Q3 > Q2 > Q1)
            master_df["q_best_time"] = master_df[
                ["q3_seconds", "q2_seconds", "q1_seconds"]
            ].min(axis=1)

            # Compute qualifying gaps per race
            # Gap to pole (P1)
            master_df["q_gap_to_pole"] = master_df.groupby("race_id")[
                "q_best_time"
            ].transform(lambda x: x - x.min())

            # Normalized qualifying position (0-1, where 0 is pole)
            master_df["q_position_normalized"] = master_df.groupby("race_id")[
                "grid_position"
            ].transform(lambda x: (x - 1) / (x.max() - 1) if x.max() > 1 else 0)

            self.logger.info("  Added qualifying features")

        return master_df

    def _add_lap_features(self, master_df: pd.DataFrame) -> pd.DataFrame:
        """Merge lap-level performance features."""

        if self._lap_features_df is None or self._lap_features_df.empty:
            self.logger.warning("  No lap features to merge")
            return master_df

        # Merge on race_id + driver_number
        lap_feature_cols = [
            "race_id",
            "driver_number",
            "avg_lap_time",
            "median_lap_time",
            "best_lap_time",
            "lap_time_std",
            "lap_time_cv",
            "lap_time_iqr",
            "tire_deg_per_lap",
            "lap_time_trend",
            "num_pit_stops",
            "avg_stint_length",
            "total_laps",
            "valid_laps",
            "invalid_laps",
        ]

        # Keep only columns that exist
        available_lap_cols = [
            col for col in lap_feature_cols if col in self._lap_features_df.columns
        ]
        lap_subset = self._lap_features_df[available_lap_cols].copy()

        # Merge
        master_df = master_df.merge(
            lap_subset,
            on=["race_id", "driver_number"],
            how="left",
            suffixes=("", "_lap"),
        )

        # Compute pace vs winner
        if "avg_lap_time" in master_df.columns:
            master_df["pace_vs_winner"] = master_df.groupby("race_id")[
                "avg_lap_time"
            ].transform(lambda x: x - x.min())

        self.logger.info(
            "  Merged lap features: %d lap feature records", len(lap_subset)
        )

        return master_df

    def _add_weather_features(self, master_df: pd.DataFrame) -> pd.DataFrame:
        """Merge weather condition features."""

        if self._weather_features_df is None or self._weather_features_df.empty:
            self.logger.warning("  No weather features to merge")
            return master_df

        # Weather is per race (not per driver), so merge on race_id only
        weather_cols = [
            "race_id",
            "air_temp_start",
            "air_temp_avg",
            "air_temp_range",
            "track_temp_avg",
            "track_temp_range",
            "humidity_avg",
            "humidity_range",
            "pressure_avg",
            "pressure_range",
            "any_rain",
            "rain_pct",
            "wind_speed_avg",
            "wind_direction_dominant",
            "wind_direction_std",
            "is_wet_session",
            "temp_category",
            "weather_stability",
        ]

        # Keep only columns that exist
        available_weather_cols = [
            col for col in weather_cols if col in self._weather_features_df.columns
        ]
        weather_subset = self._weather_features_df[available_weather_cols].copy()

        # Merge
        master_df = master_df.merge(weather_subset, on="race_id", how="left")

        self.logger.info("  Merged weather features")

        return master_df

    # ToDo: Need to check this after adding circuit aggregator
    def _add_circuit_features(self, master_df: pd.DataFrame) -> pd.DataFrame:
        """Add circuit characteristic features."""

        if self._circuit_metadata is None or self._circuit_metadata.empty:
            self.logger.warning("  No circuit metadata available")
            # Add default categories
            master_df["circuit_type"] = "unknown"
            master_df["circuit_technical"] = "unknown"
            return master_df

        # Merge circuit characteristics
        # Map circuit name to circuit_id if needed
        master_df = master_df.merge(
            self._circuit_metadata, left_on="circuit", right_on="circuit_id", how="left"
        )

        # One-hot encode circuit type and technical level
        if "circuit_type" in master_df.columns:
            master_df = pd.get_dummies(
                master_df,
                columns=["circuit_type"],
                prefix="circuit_type",
                drop_first=False,
            )

        if "circuit_technical" in master_df.columns:
            master_df = pd.get_dummies(
                master_df,
                columns=["circuit_technical"],
                prefix="technical",
                drop_first=False,
            )

        self.logger.info("  Added circuit features")

        return master_df

    def _add_team_features(self, master_df: pd.DataFrame) -> pd.DataFrame:
        """Compute team-based comparative features."""

        # Team average position per race
        master_df["team_avg_position"] = master_df.groupby(["race_id", "team_id"])[
            "finish_position"
        ].transform("mean")

        # Teammate comparison (if driver has a teammate in the race)
        def get_teammate_position(row):
            """Get teammate's position in the same race."""
            race_team_drivers = master_df[
                (master_df["race_id"] == row["race_id"])
                & (master_df["team_id"] == row["team_id"])
                & (master_df["driver_number"] != row["driver_number"])
            ]
            if len(race_team_drivers) > 0:
                return race_team_drivers["finish_position"].iloc[0]
            return np.nan

        master_df["teammate_position"] = master_df.apply(get_teammate_position, axis=1)

        # Position delta vs teammate
        master_df["position_vs_teammate"] = (
            master_df["finish_position"] - master_df["teammate_position"]
        )

        self.logger.info("  Added team features")

        return master_df

    def _add_historical_features(self, master_df: pd.DataFrame) -> pd.DataFrame:
        """Compute historical rolling features over last N races."""

        # Sort by driver and date
        master_df = master_df.sort_values(["driver_number", "race_date"])

        # Rolling features for last N races (per driver)
        rolling_window = self.HISTORICAL_WINDOW

        # Average position in last N races
        master_df["driver_avg_position_last_5"] = master_df.groupby("driver_number")[
            "finish_position"
        ].transform(
            lambda x: x.rolling(window=rolling_window, min_periods=1).mean().shift(1)
        )

        # Podiums (top 3) in last N races
        master_df["podium_indicator"] = (master_df["finish_position"] <= 3).astype(int)
        master_df["driver_podiums_last_5"] = master_df.groupby("driver_number")[
            "podium_indicator"
        ].transform(
            lambda x: x.rolling(window=rolling_window, min_periods=1).sum().shift(1)
        )

        # Wins in last N races
        master_df["win_indicator"] = (master_df["finish_position"] == 1).astype(int)
        master_df["driver_wins_last_5"] = master_df.groupby("driver_number")[
            "win_indicator"
        ].transform(
            lambda x: x.rolling(window=rolling_window, min_periods=1).sum().shift(1)
        )

        # DNF rate in last N races
        master_df["dnf_indicator"] = (
            ~master_df["status"].str.contains("Finished", na=False)
        ).astype(int)
        master_df["driver_dnf_rate_last_5"] = master_df.groupby("driver_number")[
            "dnf_indicator"
        ].transform(
            lambda x: x.rolling(window=rolling_window, min_periods=1).mean().shift(1)
        )

        # Points per race average in last N races
        master_df["driver_avg_points_last_5"] = master_df.groupby("driver_number")[
            "points"
        ].transform(
            lambda x: x.rolling(window=rolling_window, min_periods=1).mean().shift(1)
        )

        # Circuit-specific: driver's previous result at this circuit
        master_df["driver_prev_position_at_circuit"] = master_df.groupby(
            ["driver_number", "circuit"]
        )["finish_position"].shift(1)

        # Drop intermediate indicator columns
        master_df = master_df.drop(
            columns=["podium_indicator", "win_indicator", "dnf_indicator"],
            errors="ignore",
        )

        self.logger.info(
            "  Added historical rolling features (window=%d)", rolling_window
        )

        return master_df

    def _create_target_variable(self, master_df: pd.DataFrame) -> pd.DataFrame:
        """Create target variable: top_10_finish (binary classification)."""

        # Top 10 finish = points scored (positions 1-10)
        master_df["top_10_finish"] = master_df["finished_top10"].astype(int)

        # Log class distribution
        top_10_count = master_df["top_10_finish"].sum()
        total_count = len(master_df)
        top_10_rate = top_10_count / total_count if total_count > 0 else 0

        self.logger.info(
            "  Target variable created: %.1f%% top 10 finishes (%d / %d)",
            top_10_rate * 100,
            top_10_count,
            total_count,
        )

        return master_df

    def _finalize_dataset(self, master_df: pd.DataFrame) -> pd.DataFrame:
        """Final cleanup and validation."""

        # Sort by date and driver
        master_df = master_df.sort_values(["race_date", "driver_number"])

        # Reset index
        master_df = master_df.reset_index(drop=True)

        # Log missing value summary
        missing_summary = master_df.isnull().sum()
        high_missing_cols = missing_summary[missing_summary > len(master_df) * 0.3]

        if len(high_missing_cols) > 0:
            self.logger.warning("  Columns with >30%% missing values:")
            for col, count in high_missing_cols.items():
                pct = count / len(master_df) * 100
                self.logger.warning("    - %s: %.1f%%", col, pct)

        self.logger.info("  Final dataset shape: %s", master_df.shape)
        self.logger.info("  Total features: %d", len(master_df.columns))

        return master_df

    def _return_empty_result(self, error_msg: str) -> AggregationResult:
        """Return empty result with error."""
        self._record_error(error_msg)
        return AggregationResult(
            data=pd.DataFrame(),
            items_processed=0,
            items_skipped=0,
            skipped_items=[],
            warnings=self.warnings,
            errors=self.errors,
        )

    def _process_single_item(self, item_info, **kwargs):
        return super()._process_single_item(item_info, **kwargs)

    def _validate_output(self, df, total_items):
        return super()._validate_output(df, total_items)
