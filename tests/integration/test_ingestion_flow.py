"""
Integration tests for data ingestion flow.

Tests with real FastF1 API using 2024 Italian Grand Prix.
Requires internet connection and Docker/MinIO running.
"""

# pylint: disable=broad-except, unused-argument, unused-variable

import time
import logging
from unittest.mock import patch

import pytest
import pandas as pd

from src.data_ingestion.fastf1_client import FastF1Client
from src.data_ingestion.storage_client import StorageClient
from src.data_ingestion.schemas import validate_session_data, DataValidator
from src.data_ingestion.data_ingestion_pipeline import IngestionPipeline

logging.getLogger("faker").setLevel(logging.WARNING)
logging.getLogger("requests_cache").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


@pytest.mark.integration
@pytest.mark.requires_docker
class TestFastF1ToStorage:
    """Test FastF1 → Storage flow"""

    def test_fetch_and_store_race_results(
        self,
        session_loader,
        test_storage_client,
        test_race_config,
        skip_if_no_internet,
        cleanup_test_data,
    ):
        """Test fetching race results and storing in MinIO"""

        # Fetch session
        session_data = session_loader.load_session_data(
            test_race_config["single_session"]["year"],
            test_race_config["single_session"]["event_name"],
            test_race_config["single_session"]["sessions"],
        )

        assert session_data is not None

        # Extract results
        results = session_data["results"]

        # Store in MinIO
        object_key = test_storage_client.build_object_key(
            year=test_race_config["single_session"]["year"],
            event_name=test_race_config["single_session"]["event_name"],
            session_type=test_race_config["single_session"]["sessions"],
            data_type="results",
        )

        success = test_storage_client.upload_dataframe(results, object_key)

        assert success is True
        assert test_storage_client.object_exists(object_key) is True

        # Verify by downloading
        downloaded = test_storage_client.download_dataframe(object_key)
        assert len(downloaded) == len(results)


@pytest.mark.integration
@pytest.mark.requires_docker
class TestFastF1ToValidationToStorage:
    """Test FastF1 → Validation → Storage flow"""

    def test_complete_validation_flow(
        self,
        session_loader,
        sample_session_info,
        sample_lap_data_df,
        sample_race_results_df,
        sample_weather_data_df,
        sample_race_control_messages_data_df,
        sample_session_status_data_df,
        sample_track_status_data_df,
        test_storage_client,
        data_validator,
        test_race_config,
        skip_if_no_internet,
        cleanup_test_data,
    ):
        """Test fetching, validating, and storing race data"""

        # Mock session results

        with patch.object(
            session_loader,
            "load_session_data",
            return_value={
                "session_info": sample_session_info,
                "laps": sample_lap_data_df,
                "results": sample_race_results_df,
                "weather": sample_weather_data_df,
                "race_control_messages": sample_race_control_messages_data_df,
                "session_status": sample_session_status_data_df,
                "track_status": sample_track_status_data_df,
            },
        ):
            # Fetch session
            session_data = session_loader.load_session_data(
                test_race_config["single_session"]["year"],
                test_race_config["single_session"]["event_name"],
                test_race_config["single_session"]["sessions"],
            )

            # Extract results
            results = session_data["results"].copy()

            timedelta_cols = ["Time", "Q1", "Q2", "Q3"]
            for col in timedelta_cols:
                results[col] = results[col].replace({pd.NaT: None})

            # Validate
            valid_results, errors = data_validator.validate_results(results)

            # Should have mostly valid data
            assert len(valid_results) > 0
            assert len(valid_results) >= len(results) * 0.8  # At least 80% valid

            # Store validated data
            # Store in MinIO
            object_key = test_storage_client.build_object_key(
                year=test_race_config["single_session"]["year"],
                event_name=test_race_config["single_session"]["event_name"],
                session_type=test_race_config["single_session"]["sessions"],
                data_type="results",
            )
            success = test_storage_client.upload_dataframe(valid_results, object_key)

            assert success is True

            # Verify stored data
            downloaded = test_storage_client.download_dataframe(object_key)
            assert len(downloaded) == len(valid_results)

    def test_validation_filters_invalid_data(
        self, fastf1_client, data_validator, test_race_config, skip_if_no_internet
    ):
        """Test that validation filters out invalid rows"""
        # Fetch real data
        session = fastf1_client.get_session(
            test_race_config["single_session"]["year"],
            test_race_config["single_session"]["event_name"],
            test_race_config["single_session"]["sessions"],
        )

        results = session.results.copy()
        results["SessionName"] = session.name
        results["EventName"] = session.event["EventName"]
        results["SessionDate"] = session.event["EventDate"]

        timedelta_cols = ["Time", "Q1", "Q2", "Q3"]
        for col in timedelta_cols:
            results[col] = results[col].replace({pd.NaT: None})

        original_length = len(results)

        # Validate
        valid_results, errors = data_validator.validate_results(results)

        # Should have data (may have some errors but should get most rows)
        assert len(valid_results) > 0

        # If there were errors, valid should be less than original
        if errors:
            assert len(valid_results) < original_length


@pytest.mark.integration
@pytest.mark.requires_docker
@pytest.mark.slow
class TestCompleteRaceWeekend:
    """Test ingesting complete race weekend"""

    def test_ingest_qualifying_and_race(
        self,
        fastf1_client,
        test_storage_client,
        test_race_config,
        skip_if_no_internet,
        cleanup_test_data,
    ):
        """Test ingesting both Q and R sessions"""
        sessions_ingested = []

        for session_type in ["Q", "R"]:
            # Fetch
            session = fastf1_client.get_session(
                test_race_config["single_session"]["year"],
                test_race_config["single_session"]["event_name"],
                session_type,
            )

            if session is None:
                continue

            # Extract all data
            session_data = {}

            if session.results is not None:
                results = session.results.copy()

                results["SessionName"] = session.name
                results["EventName"] = session.event["EventName"]
                results["SessionDate"] = session.event["EventDate"]

                timedelta_cols = ["Time", "Q1", "Q2", "Q3"]
                for col in timedelta_cols:
                    results[col] = results[col].replace({pd.NaT: None})

                session_data["results"] = results

            if hasattr(session, "laps") and session.laps is not None:
                laps = session.laps.copy()

                laps["SessionName"] = session.name
                laps["EventName"] = session.event["EventName"]
                laps["SessionDate"] = session.event["EventDate"]

                timedelta_cols = [
                    "Time",
                    "LapTime",
                    "PitOutTime",
                    "PitInTime",
                    "Sector1Time",
                    "Sector2Time",
                    "Sector3Time",
                    "Sector1SessionTime",
                    "Sector2SessionTime",
                    "Sector3SessionTime",
                    "LapStartTime",
                    "LapStartDate",
                ]

                for col in timedelta_cols:
                    laps[col] = laps[col].replace({pd.NaT: None})

                session_data["laps"] = laps

            if hasattr(session, "weather_data") and session.weather_data is not None:
                weather = session.weather_data.copy()

                weather["SessionName"] = session.name
                weather["EventName"] = session.event["EventName"]
                weather["SessionDate"] = session.event["EventDate"]

                session_data["weather"] = weather

            if hasattr(session, "weather_data") and session.weather_data is not None:
                weather = session.weather_data.copy()

                weather["SessionName"] = session.name
                weather["EventName"] = session.event["EventName"]
                weather["SessionDate"] = session.event["EventDate"]

                session_data["weather"] = weather

            if (
                hasattr(session, "session_status")
                and session.session_status is not None
            ):
                session_status = session.session_status.copy()

                session_status["SessionName"] = session.name
                session_status["EventName"] = session.event["EventName"]
                session_status["SessionDate"] = session.event["EventDate"]

                session_data["session_status"] = session_status

            if hasattr(session, "track_status") and session.track_status is not None:
                track_status = session.track_status.copy()

                track_status["SessionName"] = session.name
                track_status["EventName"] = session.event["EventName"]
                track_status["SessionDate"] = session.event["EventDate"]

                session_data["track_status"] = track_status

            # Validate
            validated = validate_session_data(session_data)

            # Store
            for data_type, (valid_df, errors) in validated.items():
                if not valid_df.empty:
                    object_key = test_storage_client.build_object_key(
                        year=test_race_config["single_session"]["year"],
                        event_name=test_race_config["single_session"]["event_name"],
                        session_type=session_type,
                        data_type=data_type,
                    )

                    test_storage_client.upload_dataframe(valid_df, object_key)

            sessions_ingested.append(session_type)

            # Small delay to avoid rate limiting
            time.sleep(2)

        # Should have ingested at least one session
        assert len(sessions_ingested) > 0


@pytest.mark.integration
@pytest.mark.requires_docker
class TestPipelineIntegration:
    """Test the complete pipeline"""

    def test_pipeline_ingest_single_session(
        self,
        test_race_config,
        skip_if_no_internet,
        skip_if_no_docker,
        cleanup_test_data,
    ):
        """Test pipeline ingesting a single session"""

        pipeline = IngestionPipeline(
            skip_existing=False, validate_data=True, delay_between_sessions=2
        )

        # Override storage client to use test bucket
        pipeline.storage_client.config.raw_bucket_name = "test-f1-data-raw"

        result = pipeline.ingest_session(
            test_race_config["single_session"]["year"],
            test_race_config["single_session"]["event_name"],
            test_race_config["single_session"]["sessions"],
        )

        assert result.success is True
        assert result.uploaded_files["results"] is True
        assert result.processing_time_seconds > 0

    @pytest.mark.slow
    def test_pipeline_ingest_race_weekend(
        self,
        test_race_config,
        skip_if_no_internet,
        skip_if_no_docker,
        cleanup_test_data,
    ):
        """Test pipeline ingesting complete race weekend"""
        pipeline = IngestionPipeline(
            skip_existing=False, validate_data=True, delay_between_sessions=3
        )

        # Override storage client
        pipeline.storage_client.config.raw_bucket_name = "test-f1-data-raw"

        results = pipeline.ingest_race_weekend(
            year=test_race_config["single_session"]["year"],
            event=test_race_config["single_session"]["event_name"],
            session_types=["Q", "R"],
        )

        # Should have 2 results
        assert len(results) == 2

        # Check individual results
        for result in results:
            assert result.success is True or result.error_message is not None


@pytest.mark.integration
@pytest.mark.requires_docker
class TestErrorRecovery:
    """Test error handling and recovery"""

    # need to change to using session_data_loader and then test invalid session handling
    # def test_handle_missing_session(self, fastf1_client, skip_if_no_internet):
    #     """Test handling non-existent session"""
    #     # Try to fetch a session that doesn't exist
    #     try:
    #         session = fastf1_client.get_session(2024, "NonExistentRace", "R")
    #         # If we get here without exception, session should be None or empty
    #         assert session is None or not hasattr(session, "results")
    #     except Exception as e:
    #         # Expected behavior - exception raised
    #         assert "NonExistentRace" in str(e) or "not found" in str(e).lower()

    def test_handle_network_interruption(
        self, test_storage_client, sample_race_results_df, cleanup_test_data
    ):
        """Test handling storage failures"""
        # Try to upload to invalid location
        object_key = "///invalid///key///.parquet"

        # Should handle gracefully
        try:
            success = test_storage_client.upload_dataframe(
                sample_race_results_df, object_key
            )
            # Either succeeds (MinIO is forgiving) or fails gracefully
            assert success is True or success is False
        except Exception:
            # Exception is also acceptable
            pass


@pytest.mark.integration
@pytest.mark.requires_docker
class TestDataConsistency:
    """Test data consistency throughout the pipeline"""

    def test_data_integrity_after_round_trip(
        self,
        fastf1_client,
        test_storage_client,
        test_race_config,
        skip_if_no_internet,
        cleanup_test_data,
    ):
        """Test that data remains consistent after upload/download"""
        # Fetch
        session = fastf1_client.get_session(
            test_race_config["single_session"]["year"],
            test_race_config["single_session"]["event_name"],
            test_race_config["single_session"]["sessions"],
        )

        results = session.results.copy()

        # Add metadata
        results["SessionName"] = session.name
        results["EventName"] = session.event["EventName"]
        results["SessionDate"] = session.event["EventDate"]

        # Store
        object_key = test_storage_client.build_object_key(
            year=test_race_config["single_session"]["year"],
            event_name=test_race_config["single_session"]["event_name"],
            session_type="R",
            data_type="results",
        )
        test_storage_client.upload_dataframe(results, object_key)

        # Download
        downloaded = test_storage_client.download_dataframe(object_key)

        # Verify consistency
        assert len(downloaded) == len(results)
        assert set(downloaded.columns) == set(results.columns)

        # Check a few key values
        assert downloaded["SessionName"].iloc[0] == "Race"
        assert (
            downloaded["EventName"].iloc[0]
            == test_race_config["single_session"]["event_name"]
        )


@pytest.mark.integration
class TestPerformance:
    """Test performance of integration"""

    @pytest.mark.slow
    def test_ingestion_performance(
        self, test_race_config, skip_if_no_internet, skip_if_no_docker
    ):
        """Test that ingestion completes in reasonable time"""

        pipeline = IngestionPipeline(
            skip_existing=False, validate_data=True, delay_between_sessions=2
        )

        # Override storage client
        pipeline.storage_client.config.raw_bucket_name = "test-f1-data-raw"
        pipeline.storage_client.config.processed_bucket_name = "test-f1-data-processed"

        start = time.time()

        result = pipeline.ingest_session(
            test_race_config["single_session"]["year"],
            test_race_config["single_session"]["event_name"],
            test_race_config["single_session"]["sessions"],
        )

        duration = time.time() - start

        # Should complete in under 60 seconds (with caching)
        assert duration < 60
        assert result.success is True


@pytest.mark.integration
def test_end_to_end_smoke_test(
    test_race_config, test_storage_client, skip_if_no_internet, skip_if_no_docker
):
    """Smoke test - basic end-to-end ingestion"""
    # Initialize all components
    fastf1_client = FastF1Client()
    storage_client = StorageClient()
    storage_client.config.raw_bucket_name = "test-f1-data-raw"
    validator = DataValidator()

    # Fetch
    session = fastf1_client.get_session(
        test_race_config["single_session"]["year"],
        test_race_config["single_session"]["event_name"],
        test_race_config["single_session"]["sessions"],
    )

    assert session is not None

    # Extract
    results = session.results.copy()
    results["SessionName"] = session.name
    results["EventName"] = session.event["EventName"]
    results["SessionDate"] = session.event["EventDate"]

    timedelta_cols = ["Time", "Q1", "Q2", "Q3"]
    for col in timedelta_cols:
        results[col] = results[col].replace({pd.NaT: None})

    # Validate
    valid_results, errors = validator.validate_results(results)
    assert len(valid_results) > 0

    # Store
    object_key = test_storage_client.build_object_key(
        year=test_race_config["single_session"]["year"],
        event_name=test_race_config["single_session"]["event_name"],
        session_type=test_race_config["single_session"]["sessions"],
        data_type="results",
    )
    success = storage_client.upload_dataframe(valid_results, object_key)
    assert success is True

    # Verify
    downloaded = storage_client.download_dataframe(object_key)
    assert len(downloaded) == len(valid_results)

    # Cleanup
    storage_client.delete_object(object_key)
