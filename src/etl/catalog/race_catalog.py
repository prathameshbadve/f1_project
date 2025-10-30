"""
Race Catalog Builder

Scans raw data bucket and creates comprehensive catalog of all available race data.
Validates data completeness and file integrity.
"""

# pylint: disable=broad-except

import io
from dataclasses import dataclass, field
from typing import List, Optional
import pandas as pd

from config.logging import get_logger
from src.clients.storage_client import StorageClient
from src.clients.redis_client import RedisClient


@dataclass
class SessionFiles:
    """Files available for a session"""

    results: bool = False
    laps: bool = False
    weather: bool = False
    session_info: bool = False
    track_status: bool = False
    session_status: bool = False
    race_control_messages: bool = False

    def is_complete(self) -> bool:
        """Check if session has all critical files"""

        # Results, laps, and session_info are critical
        return self.results and self.laps and self.session_info

    def completeness_score(self) -> float:
        """Calculate completeness percentage"""

        total_files = 7
        available = sum(
            [
                self.results,
                self.laps,
                self.weather,
                self.session_info,
                self.track_status,
                self.session_status,
                self.race_control_messages,
            ]
        )
        return (available / total_files) * 100


@dataclass
class RaceCatalogEntry:
    """Single race catalog entry"""

    year: int
    round_number: int
    event_name: str
    circuit_name: str
    session_name: str
    session_date: Optional[str] = None
    session_path: str = ""

    # File availability
    files: SessionFiles = field(default_factory=SessionFiles)

    # Metadata
    total_drivers: Optional[int] = None
    total_laps: Optional[int] = None
    event_format: Optional[str] = None  # 'conventional' or 'sprint'

    # Data quality
    has_qualifying_data: bool = False
    has_null_positions: bool = False
    has_null_grid: bool = False
    completeness_score: float = 0.0

    def to_dict(self) -> dict:
        """Convert to dictionary for DataFrame"""

        return {
            "year": self.year,
            "round": self.round_number,
            "event_name": self.event_name,
            "circuit": self.circuit_name,
            "session": self.session_name,
            "session_date": self.session_date,
            "session_path": self.session_path,
            # File flags
            "has_results": self.files.results,
            "has_laps": self.files.laps,
            "has_weather": self.files.weather,
            "has_session_info": self.files.session_info,
            "has_track_status": self.files.track_status,
            "has_session_status": self.files.session_status,
            "has_race_control": self.files.race_control_messages,
            # Metadata
            "total_drivers": self.total_drivers,
            "total_laps": self.total_laps,
            "event_format": self.event_format,
            "has_qualifying_data": self.has_qualifying_data,
            # Quality flags
            "has_null_positions": self.has_null_positions,
            "has_null_grid": self.has_null_grid,
            "is_complete": self.files.is_complete(),
            "completeness_score": self.completeness_score,
        }


class RaceCatalogBuilder:
    """
    Build comprehensive catalog of all race data

    Scans raw bucket structure:
    dev-f1-data-raw/
        └── {year}/
            └── {event_name}/
                └── {session}/
                    ├── results.parquet
                    ├── laps.parquet
                    ├── weather.parquet
                    └── ...
    """

    EXPECTED_FILES = [
        "results.parquet",
        "laps.parquet",
        "weather.parquet",
        "session_info.parquet",
        "track_status.parquet",
        "session_status.parquet",
        "race_control_messages.parquet",
    ]

    SESSION_TYPES = [
        "Practice 1",
        "Practice 2",
        "Practice 3",
        "Qualifying",
        "Sprint",
        "Sprint Qualifying",
        "Sprint Shootout",
        "Race",
    ]

    def __init__(
        self,
        storage_client: StorageClient,
        redis_client: Optional[RedisClient] = None,
        raw_bucket: str = "dev-f1-data-raw",
        processed_bucket: str = "dev-f1-data-processed",
    ):
        self.storage = storage_client
        self.redis = redis_client
        self.raw_bucket = raw_bucket
        self.processed_bucket = processed_bucket
        self.logger = get_logger("data_processing.etl.race_catalog")

        self.catalog_entries: List[RaceCatalogEntry] = []

    def build_catalog(self, years: Optional[List[int]] = None) -> pd.DataFrame:
        """
        Build complete catalog by scanning raw bucket

        Args:
            years: List of years to scan. If None, scan all available

        Returns:
            DataFrame with catalog entries
        """

        self.logger.info("=" * 80)
        self.logger.info("Building Race Catalog")
        self.logger.info("=" * 80)

        # Determine years to scan
        if years is None:
            years = self._discover_years()

        self.logger.info("Scanning years: %d", years)

        # Scan each year
        for year in sorted(years):
            self.logger.info("=" * 80)
            self.logger.info("Scanning Year: %d", year)
            self.logger.info("=" * 80)
            self._scan_year(year)

        # Convert to DataFrame
        catalog_df = self._to_dataframe()

        # Add derived columns
        catalog_df = self._add_derived_features(catalog_df)

        # Validate catalog
        self._validate_catalog(catalog_df)

        # Save catalog
        self._save_catalog(catalog_df)

        # Cache in Redis
        if self.redis and self.redis.is_available():
            self._cache_catalog(catalog_df)

        # Print summary
        self._print_summary(catalog_df)

        return catalog_df

    def _discover_years(self) -> List[int]:
        """Discover all available years in raw bucket"""

        self.logger.info("Discovering years in bucket: %s", self.raw_bucket)

        try:
            # List all prefixes at root level (years)
            prefixes = self.storage.list_prefixes(bucket=self.raw_bucket, prefix="")

            years = []
            for prefix in prefixes:
                # Extract year from prefix (e.g., "2024/" -> 2024)
                year_str = prefix.rstrip("/").split("/")[-1]
                try:
                    year = int(year_str)
                    if 2020 <= year <= 2030:  # Sanity check
                        years.append(year)
                except ValueError:
                    self.logger.warning("Skipping non-year prefix: %s", prefix)
                    continue

            self.logger.info("Discovered years: %s", sorted(years))
            return sorted(years)

        except Exception as e:
            self.logger.error("Failed to discover years: %s", str(e))
            # Fallback to known years
            return [2022, 2023, 2024]

    def _scan_year(self, year: int):
        """Scan all events in a year"""

        year_prefix = f"{year}/"

        try:
            # List all event directories
            event_prefixes = self.storage.list_objects(
                prefix=year_prefix,
                recursive=False,
            )

            if f"{year_prefix}circuits.parquet" in event_prefixes:
                self.logger.info("Removing the circuits file from event prefixes")
                event_prefixes.remove(f"{year_prefix}circuits.parquet")
            if f"{year_prefix}season_schedule.parquet" in event_prefixes:
                self.logger.info(
                    "Removing the season schedule file from event prefixes"
                )
                event_prefixes.remove(f"{year_prefix}season_schedule.parquet")

            self.logger.info("Found %d events for %d", len(event_prefixes), year)

            for event_prefix in event_prefixes:
                event_name = event_prefix.rstrip("/").split("/")[-1]
                self._scan_event(year, event_name, event_prefix)

        except Exception as e:
            self.logger.error("Failed to scan year %d: %s", year, str(e))

    def _scan_event(self, year: int, event_name: str, event_prefix: str):
        """Scan all sessions in an event"""

        self.logger.info("  Scanning event: %s", event_name)

        try:
            # List all session directories
            session_prefixes = self.storage.list_objects(
                prefix=event_prefix, recursive=False
            )

            for session_prefix in session_prefixes:
                session_name = session_prefix.rstrip("/").split("/")[-1]
                self.logger.info(
                    "      Found session: %s (%s)", session_name, session_prefix
                )

                # Skip if not a valid session type
                if session_name not in self.SESSION_TYPES:
                    self.logger.debug(
                        "    Skipping non-session directory: %s", session_name
                    )
                    continue

                self._scan_session(year, event_name, session_name, session_prefix)

        except Exception as e:
            self.logger.error("Failed to scan event %s: %s", event_name, str(e))

    def _scan_session(
        self, year: int, event_name: str, session_name: str, session_prefix: str
    ):
        """Scan files in a session and create catalog entry"""

        self.logger.info("          Session: %s", session_name)

        try:
            # List all files in session
            files = self.storage.list_objects(prefix=session_prefix)

            # Check which expected files exist
            session_files = SessionFiles()
            for file_path in files:
                file_name = file_path.split("/")[-1]

                if file_name == "results.parquet":
                    session_files.results = True
                elif file_name == "laps.parquet":
                    session_files.laps = True
                elif file_name == "weather.parquet":
                    session_files.weather = True
                elif file_name == "session_info.parquet":
                    session_files.session_info = True
                elif file_name == "track_status.parquet":
                    session_files.track_status = True
                elif file_name == "session_status.parquet":
                    session_files.session_status = True
                elif file_name == "race_control_messages.parquet":
                    session_files.race_control_messages = True

            # Extract metadata from session_info if available
            session_date = None
            circuit_name = None
            total_laps = None
            event_format = None
            round_number = None

            if session_files.session_info:
                try:
                    session_info = self.storage.download_dataframe(
                        # bucket_name=self.raw_bucket,
                        object_key=f"{session_prefix}session_info.parquet",
                    )

                    if not session_info.empty:
                        session_date = session_info["session_date"].iloc[0]
                        circuit_name = session_info["location"].iloc[0]
                        total_laps = session_info["total_laps"].iloc[0]
                        event_format = session_info["event_format"].iloc[0]
                        round_number = session_info["round_number"].iloc[0]

                except Exception as e:
                    self.logger.warning("      Could not read session_info: %s", str(e))

            # Extract quality metrics from results if available
            total_drivers = None
            has_null_positions = False
            has_null_grid = False

            if session_files.results:
                try:
                    results = self.storage.download_dataframe(
                        object_key=f"{session_prefix}results.parquet"
                    )

                    if not results.empty:
                        total_drivers = len(results)
                        has_null_positions = results["Position"].isnull().any()
                        has_null_grid = (
                            results.get("GridPosition", pd.Series([False]))
                            .isnull()
                            .any()
                        )

                except Exception as e:
                    self.logger.warning("      Could not read results: %s", str(e))

            # Create catalog entry
            entry = RaceCatalogEntry(
                year=year,
                round_number=round_number,
                event_name=event_name,
                circuit_name=circuit_name,
                session_name=session_name,
                session_date=session_date,
                session_path=session_prefix,
                files=session_files,
                total_drivers=total_drivers,
                total_laps=total_laps,
                event_format=event_format,
                has_null_positions=has_null_positions,
                has_null_grid=has_null_grid,
                completeness_score=session_files.completeness_score(),
            )

            self.catalog_entries.append(entry)

            # Log status
            status = (
                "          ✅ COMPLETE"
                if entry.files.is_complete()
                else "⚠️  INCOMPLETE"
            )
            self.logger.info("%s - %0f complete", status, entry.completeness_score)

        except Exception as e:
            self.logger.error("Failed to scan session %s: %s", session_name, str(e))

    def _to_dataframe(self) -> pd.DataFrame:
        """Convert catalog entries to DataFrame"""

        if not self.catalog_entries:
            self.logger.warning("No catalog entries found!")
            return pd.DataFrame()

        data = [entry.to_dict() for entry in self.catalog_entries]
        df = pd.DataFrame(data)

        # Sort by year, round, session
        df = df.sort_values(["year", "round", "session"]).reset_index(drop=True)

        return df

    def _add_derived_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived columns to catalog"""

        if df.empty:
            return df

        # Add race_id
        df["race_id"] = df.apply(
            lambda row: f"{row['year']}_R{row['round']:02d}", axis=1
        )

        # Add days since first race (for temporal ordering)
        if "session_date" in df.columns:
            df["session_date"] = pd.to_datetime(df["session_date"])
            min_date = df["session_date"].min()
            df["days_since_start"] = (df["session_date"] - min_date).dt.days

        # Add season progress (0-1)
        df["season_progress"] = df.groupby("year")["round"].transform(
            lambda x: (x - x.min()) / (x.max() - x.min()) if x.max() > x.min() else 0
        )

        return df

    def _validate_catalog(self, df: pd.DataFrame):
        """Validate catalog for data quality issues"""

        self.logger.info("=" * 80)
        self.logger.info("Validating Catalog")
        self.logger.info("=" * 80)

        issues = []

        # Check for missing race sessions
        race_sessions = df[df["session"] == "Race"]
        if len(race_sessions) == 0:
            issues.append("❌ No race sessions found!")

        # Check for incomplete race sessions
        incomplete_races = race_sessions[~race_sessions["is_complete"]]
        if len(incomplete_races) > 0:
            issues.append(f"⚠️  {len(incomplete_races)} incomplete race sessions")
            for _, race in incomplete_races.iterrows():
                self.logger.warning(
                    "    Incomplete: %s Round %s - %s (%.0f)",
                    race["year"],
                    race["round"],
                    race["event_name"],
                    race["completeness_score"],
                )

        # Check for missing qualifying
        races_without_quali = []
        for year in df["year"].unique():
            for round_num in df[df["year"] == year]["round"].unique():
                race_event = df[
                    (df["year"] == year)
                    & (df["round"] == round_num)
                    & (df["session"] == "Race")
                ]

                if len(race_event) == 0:
                    continue

                quali_event = df[
                    (df["year"] == year)
                    & (df["round"] == round_num)
                    & (df["session"] == "Qualifying")
                ]

                if len(quali_event) == 0:
                    races_without_quali.append(f"{year} R{round_num}")

        if races_without_quali:
            issues.append(
                f"⚠️  {len(races_without_quali)} races missing qualifying data"
            )
            self.logger.warning(
                "    Missing qualifying: %s", ", ".join(races_without_quali[:5])
            )

        # Check for data quality issues
        null_positions = df["has_null_positions"].sum()
        if null_positions > 0:
            issues.append(f"⚠️  {null_positions} sessions have null positions")

        # Summary
        if issues:
            self.logger.warning("⚠️  Found %d issues:", len(issues))
            for issue in issues:
                self.logger.warning("  %s", issue)
        else:
            self.logger.info("✅ Catalog validation passed - no issues found")

    def _save_catalog(self, df: pd.DataFrame):
        """Save catalog to processed bucket"""
        try:
            catalog_key = "catalog/race_catalog.parquet"

            self.storage.upload_dataframe(
                df=df, bucket_name=self.processed_bucket, object_key=catalog_key
            )

            self.logger.info(
                "✅ Catalog saved to: s3://%s/%s", self.processed_bucket, catalog_key
            )

            # Also save as CSV for easy viewing
            csv_key = "catalog/race_catalog.csv"
            self.storage.upload_csv(
                df=df, bucket_name=self.processed_bucket, object_key=csv_key
            )

            self.logger.info(
                "✅ Catalog CSV saved to: s3://%s/%s", self.processed_bucket, csv_key
            )

        except Exception as e:
            self.logger.error("Failed to save catalog: %s", str(e))
            raise

    def _cache_catalog(self, df: pd.DataFrame):
        """Cache catalog in Redis for fast access"""
        try:
            cache_key = "f1:catalog:complete"

            # Convert to parquet bytes
            buffer = io.BytesIO()
            df.to_parquet(buffer, engine="pyarrow", compression="snappy", index=False)
            parquet_bytes = buffer.getvalue()

            # Store in Redis with 24hr TTL
            ttl_seconds = 24 * 3600
            self.redis.client.setex(cache_key, ttl_seconds, parquet_bytes)

            size_mb = len(parquet_bytes) / (1024 * 1024)
            self.logger.info(
                "✅ Catalog cached in Redis: %s (%2f MB, TTL: 24h)", cache_key, size_mb
            )

        except Exception as e:
            self.logger.warning("Failed to cache catalog in Redis: %s", str(e))

    def _print_summary(self, df: pd.DataFrame):
        """Print catalog summary statistics"""
        self.logger.info("=" * 80)
        self.logger.info("Catalog Summary")
        self.logger.info("=" * 80)

        if df.empty:
            self.logger.warning("Empty catalog!")
            return

        # Overall stats
        self.logger.info("Total entries: %d", len(df))
        self.logger.info("Years: %s", df["year"].unique())
        self.logger.info("Total races: %d", len(df[df["session"] == "Race"]))
        self.logger.info(
            "Total qualifying sessions: %d", len(df[df["session"] == "Qualifying"])
        )

        # Completeness
        complete = df["is_complete"].sum()
        total = len(df)
        self.logger.info(
            "Complete sessions: %d/%d (%.1f %%)",
            complete,
            total,
            (100 * complete / total),
        )

        # By year
        self.logger.info("Breakdown by year:")
        for year in sorted(df["year"].unique()):
            year_df = df[df["year"] == year]
            races = len(year_df[year_df["session"] == "Race"])
            complete_races = len(
                year_df[(year_df["session"] == "Race") & (year_df["is_complete"])]
            )
            self.logger.info(
                "  %s: %s races (%s complete, %s incomplete)",
                year,
                races,
                complete_races,
                races - complete_races,
            )

        # Average completeness
        avg_completeness = df["completeness_score"].mean()
        self.logger.info("Average completeness: %.1f %%", avg_completeness)

        self.logger.info("=" * 80)


# def main():
#     """Example usage"""
#     from src.clients.storage_client import StorageClient
#     from src.clients.redis_client import RedisClient

#     # Initialize clients
#     storage = StorageClient()
#     redis = RedisClient()

#     # Build catalog
#     builder = RaceCatalogBuilder(
#         storage_client=storage,
#         redis_client=redis,
#         raw_bucket="dev-f1-data-raw",
#         processed_bucket="dev-f1-data-processed",
#     )

#     catalog_df = builder.build_catalog()

#     return catalog_df


# if __name__ == "__main__":
#     catalog = main()
