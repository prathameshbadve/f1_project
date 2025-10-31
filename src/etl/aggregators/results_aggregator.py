"""
Race Results Aggregator

Combines race results with qualifying data into a single unified dataset.
Processes all races from catalog and creates training-ready table.
"""

from typing import Optional
import pandas as pd
import numpy as np

from src.clients.storage_client import StorageClient
from src.etl.aggregators.base_aggregator import BaseAggregator, AggregationResult


class RaceResultsAggregator(BaseAggregator):
    """
    Aggregate race results and qualifying data across all races.

    Output schema:
    - Race identifiers (race_id, year, round, event_name, circuit, date)
    - Driver info (driver_id, driver_name, driver_number, abbreviation)
    - Team info (team_id, team_name, team_color)
    - Qualifying results (Q1, Q2, Q3 times)
    - Race results (grid, finish position, points, status)
    - Derived features (position_gain, finished_top10)
    """

    RACE_RESULT_COLUMNS = [
        "DriverNumber",
        "DriverId",
        "Abbreviation",
        "FirstName",
        "LastName",
        "TeamId",
        "TeamName",
        "TeamColor",
        "Position",
        "ClassifiedPosition",
        "GridPosition",
        "Time",
        "Status",
        "Points",
        "Laps",
    ]

    QUALIFYING_COLUMNS = ["DriverNumber", "DriverId", "Abbreviation", "Q1", "Q2", "Q3"]

    def __init__(
        self,
        storage_client: StorageClient,
        raw_bucket: str = "dev-f1-data-raw",
        processed_bucket: str = "dev-f1-data-processed",
        logger_name: str = "etl.results_aggregator",
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
        Aggregate all race results from catalog.

        Args:
            catalog_df: Validated catalog DataFrame
            sample_size: If provided, only process first N races (for testing)

        Returns:
            AggregationResult with combined data and metadata
        """

        self._log_aggregation_header("Starting Race Results Aggregation")

        # Reset counters
        self._reset_counters()

        # Filter to complete race sessions only
        race_sessions = catalog_df[
            (catalog_df["session"] == "Race") & (catalog_df["is_complete"])
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
        all_results = []

        for idx, race_info in race_sessions.iterrows():
            race_id = f"{race_info['year']}_R{race_info['round']:02d}"

            self._log_progress(idx + 1, total_races)

            try:
                race_result_df = self._process_single_item(race_info, catalog_df)

                if race_result_df is not None and not race_result_df.empty:
                    all_results.append(race_result_df)
                    self.items_processed += 1
                else:
                    self._record_skip(race_id, "Empty result after processing")

            except Exception as e:  # pylint: disable=broad-except
                self.logger.error("Failed to process %s: %s", race_id, e)
                self._record_skip(race_id, str(e))

        # Combine all results
        if not all_results:
            self._record_error("No races were successfully processed!")
            return AggregationResult(
                data=pd.DataFrame(),
                items_processed=0,
                items_skipped=total_races,
                skipped_items=self.skipped_items,
                warnings=self.warnings,
                errors=["All races failed to process"] + self.errors,
            )

        combined_df = pd.concat(all_results, ignore_index=True)

        # Sort by date and position
        combined_df = combined_df.sort_values(["race_date", "finish_position"])

        # Validate output
        self._validate_output(combined_df, total_races)

        # Log summary
        self._log_aggregation_summary(
            total_items=total_races,
            output_size=len(combined_df),
            output_description="driver-race records",
        )

        return AggregationResult(
            data=combined_df,
            items_processed=self.items_processed,
            items_skipped=self.items_skipped,
            skipped_items=self.skipped_items,
            warnings=self.warnings,
            errors=self.errors,
        )

    # pylint: disable=arguments-renamed, arguments-differ
    def _process_single_item(
        self,
        race_info: pd.Series,
        catalog_df: pd.DataFrame,
        **kwargs,
    ) -> Optional[pd.DataFrame]:
        """
        Process a single race: load results, load qualifying, merge.

        Args:
            race_info: Row from catalog for this race
            catalog_df: Full catalog (to find qualifying session)

        Returns:
            DataFrame with combined race + qualifying data
        """

        year = race_info["year"]
        round_num = race_info["round"]
        event_name = race_info["event_name"]
        race_id = race_info["race_id"]

        # Load race results
        race_results = self._load_race_results(race_info)
        if race_results is None or race_results.empty:
            self.logger.warning("%s: No race results found", race_id)
            return None

        # Load qualifying results
        qualifying_results = self._load_qualifying_results(year, round_num, catalog_df)

        # Merge race and qualifying
        combined = self._merge_race_and_qualifying(
            race_results, qualifying_results, race_id
        )

        # Add race metadata
        combined["race_id"] = race_id
        combined["year"] = year
        combined["round"] = round_num
        combined["event_name"] = event_name
        combined["circuit"] = race_info["circuit"]
        combined["race_date"] = pd.to_datetime(race_info["session_date"])

        # Add derived features
        combined = self._add_derived_features(combined)

        # Standardize column order
        combined = self._standardize_columns(combined)

        return combined

    def _load_race_results(self, race_info: pd.Series) -> Optional[pd.DataFrame]:
        """Load race results file from storage"""

        race_path = race_info["session_path"]
        results_key = f"{race_path}results.parquet"

        required_columns = [
            "DriverId",
            "DriverNumber",
            "Abbreviation",
            "FullName",
            "TeamId",
            "TeamName",
            "TeamColor",
            "GridPosition",
            "Position",
            "ClassifiedPosition",
            "Points",
            "Laps",
            "Status",
            "Time",
        ]

        race_results = self._load_parquet_safe(
            key=results_key,
            bucket=self.raw_bucket,
            required_columns=required_columns,
        )

        return race_results[required_columns]

    def _load_qualifying_results(
        self, year: int, round_num: int, catalog_df: pd.DataFrame
    ) -> Optional[pd.DataFrame]:
        """Load qualifying results for this race"""

        # Find qualifying session in catalog
        quali_session = catalog_df[
            (catalog_df["year"] == year)
            & (catalog_df["round"] == round_num)
            & (catalog_df["session"] == "Qualifying")
            & catalog_df["is_complete"]
        ]

        if len(quali_session) == 0:
            self.logger.warning(
                "%d_R%02d: No qualifying session found", year, round_num
            )
            return None

        quali_path = quali_session.iloc[0]["session_path"]
        quali_key = f"{quali_path}results.parquet"

        try:
            quali_df = self._load_parquet_safe(
                bucket=self.raw_bucket,
                key=quali_key,
            )

            if quali_df.empty:
                self.logger.warning("Empty qualifying file: %s", quali_key)
                return None

            # Select relevant columns (Q1, Q2, Q3, DriverNumber)
            quali_cols = [
                "DriverNumber",
            ]
            for col in ["Q1", "Q2", "Q3"]:
                if col in quali_df.columns:
                    quali_cols.append(col)

            return quali_df[quali_cols]

        except Exception as e:  # pylint: disable=broad-except
            self.logger.warning("Failed to load qualifying %s: %s ", quali_key, str(e))
            return None

    def _merge_race_and_qualifying(
        self, race_df: pd.DataFrame, quali_df: Optional[pd.DataFrame], race_id: str
    ) -> pd.DataFrame:
        """
        Merge race results with qualifying data.

        Strategy:
        - Merge on DriverNumber (most reliable)
        - Fallback to DriverId if DriverNumber missing
        - Handle missing qualifying data gracefully
        """

        # Start with race results
        combined = race_df.copy()

        if quali_df is None or quali_df.empty:
            # No qualifying data - add empty columns
            combined["Q1"] = np.nan
            combined["Q2"] = np.nan
            combined["Q3"] = np.nan
            self._record_warning(f"{race_id}: No qualifying data")
            return combined

        # Merge on DriverNumber
        combined = combined.merge(
            quali_df[["DriverNumber", "Q1", "Q2", "Q3"]],
            on="DriverNumber",
            how="left",
            # suffixes=("", "_quali"),
        )

        # Check for drivers without qualifying data
        missing_quali = combined["Q1"].isnull().sum()
        if missing_quali > 0:
            self._record_warning(
                f"{race_id}: {missing_quali} drivers missing qualifying times"
            )

        return combined

    def _add_derived_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived features for analysis and modeling"""

        # Position gain (grid - finish, positive = gained positions)
        df["position_gain"] = df["GridPosition"] - df["Position"]

        # Binary target: finished in top 10
        df["finished_top10"] = (df["Position"] <= 10) & (df["Position"].notna())

        # Did driver finish the race?
        df["finished_race"] = df["Status"].isin(
            ["Finished", "Lapped", "+1 Lap", "+2 Laps"]
        )

        # DNF flag
        df["dnf"] = ~df["finished_race"]

        # Convert qualifying times to seconds (if they're timedelta)
        for q_col in ["Q1", "Q2", "Q3", "Time"]:
            if q_col in df.columns:
                if df[q_col].dtype == "object" or pd.api.types.is_timedelta64_dtype(
                    df[q_col]
                ):
                    df[f"{q_col}_seconds"] = pd.to_timedelta(
                        df[q_col]
                    ).dt.total_seconds()
                else:
                    df[f"{q_col}_seconds"] = df[q_col]

        return df

    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names and order"""

        # Rename to consistent snake_case
        column_mapping = {
            "DriverNumber": "driver_number",
            "DriverId": "driver_id",
            "Abbreviation": "driver_abbreviation",
            "FullName": "full_name",
            "TeamName": "team_name",
            "TeamId": "team_id",
            "TeamColor": "team_color",
            "GridPosition": "grid_position",
            "Position": "finish_position",
            "ClassifiedPosition": "classified_position",
            "Points": "points",
            "Laps": "laps_completed",
            "Status": "status",
            "Q1": "q1",
            "Q2": "q2",
            "Q3": "q3",
            "Time": "race_time",
            "Q1_seconds": "q1_seconds",
            "Q2_seconds": "q2_seconds",
            "Q3_seconds": "q3_seconds",
            "Time_seconds": "race_time_seconds",
        }

        df = df.rename(columns=column_mapping)

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
            "driver_id",
            "driver_abbreviation",
            "full_name",
            # Team info
            "team_id",
            "team_name",
            "team_color",
            # Qualifying
            "q1",
            "q2",
            "q3",
            "q1_seconds",
            "q2_seconds",
            "q3_seconds",
            # Race results
            "grid_position",
            "finish_position",
            "classified_position",
            "position_gain",
            "points",
            "laps_completed",
            "status",
            "race_time",
            "race_time_seconds",
            "finished_race",
            "dnf",
            # Target variable
            "finished_top10",
        ]

        # Reorder columns (keep any extra columns at end)
        available_cols = [col for col in column_order if col in df.columns]
        extra_cols = [col for col in df.columns if col not in column_order]

        return df[available_cols + extra_cols]

    def _validate_output(self, df: pd.DataFrame, total_races: int):
        """
        Validate aggregated output.

        Checks:
        - Expected number of records
        - No duplicate driver-race combinations
        - Required columns present
        - Data quality thresholds
        """

        self.logger.info("Validating aggregated data...")

        # Check record count
        expected_min = total_races * 15  # At least 15 drivers per race
        expected_max = total_races * 25  # At most 25 (some races have reserves)

        self._validate_record_count(
            actual=len(df),
            expected_min=expected_min,
            expected_max=expected_max,
            data_description="driver-race records",
        )

        # Check for duplicates
        self._check_duplicates(
            df=df,
            subset=["race_id", "driver_id"],
            description="driver-race combinations",
        )

        # Check required columns
        required_cols = [
            "race_id",
            "driver_id",
            "team_id",
            "grid_position",
            "finish_position",
            "finished_top10",
        ]
        self._check_required_columns(df, required_cols)

        # Check data quality - null thresholds
        self._check_null_threshold(
            df=df,
            column="finish_position",
            threshold=0.3,
            description="finish positions",
        )

        self._check_null_threshold(
            df=df, column="grid_position", threshold=0.05, description="grid positions"
        )

        # Value range checks
        invalid_positions = df[
            (df["finish_position"] < 1) | (df["finish_position"] > 25)
        ]
        if len(invalid_positions) > 0:
            self._record_warning(
                f"Found {len(invalid_positions)} invalid finish positions"
            )

        self.logger.info("✅ Validation complete")


# def main():
#     """Example usage"""

#     # Load catalog
#     storage = StorageClient()
#     catalog_df = storage.download_dataframe(
#         bucket_name="dev-f1-data-processed", object_key="catalog/race_catalog.parquet"
#     )

#     # Aggregate
#     aggregator = RaceResultsAggregator(storage)
#     result = aggregator.aggregate(catalog_df, sample_size=5)  # Test with 5 races

#     print(f"Processed: {result.items_processed} races")
#     print(f"Skipped: {result.items_skipped} races")
#     print(f"Success rate: {result.success_rate:.1%}")
#     print(f"Output shape: {result.data.shape}")
#     print("First few records:")
#     print(result.data.head())

#     return result


# if __name__ == "__main__":
#     result = main()
