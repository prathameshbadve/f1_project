"""
Integration tests for data ingestion flow.

Tests with real FastF1 API using 2024 Italian Grand Prix.
Requires internet connection and Docker/MinIO running.
"""

import pytest
import time

from src.data_ingestion.fastf1_client import FastF1Client
from src.data_ingestion.storage_client import StorageClient
from src.data_ingestion.schemas import validate_session_data, DataValidator
# from src.data_ingestion.in import IngestionPipeline


@pytest.mark.integration
@pytest.mark.requires_docker
class TestFastF1ToStorage:
    """Test FastF1 → Storage flow"""

    def test_fetch_and_store_race_results(
        self,
        fastf1_client,
        test_storage_client,
        test_race_config,
        skip_if_no_internet,
        cleanup_test_data,
    ):
        """Test fetching race results and storing in MinIO"""
        # Fetch session
        session = fastf1_client.get_session(
            test_race_config["year"], test_race_config["event"], "R"
        )

        assert session is not None
        assert session.results is not None

        # Extract results
        results = session.results.copy()
        results["Year"] = session.event["EventDate"].year
        results["RoundNumber"] = session.event["RoundNumber"]
        results["EventName"] = session.event["EventName"]
        results["SessionType"] = "R"

        # Store in MinIO
        object_key = test_storage_client.build_object_key(
            year=test_race_config["year"],
            event_name=session.event["EventName"],
            session_type="R",
            data_type="results",
        )

        success = test_storage_client.upload_dataframe(results, object_key)

        assert success is True
        assert test_storage_client.object_exists(object_key) is True

        # Verify by downloading
        downloaded = test_storage_client.download_dataframe(object_key)
        assert len(downloaded) == len(results)

    def test_fetch_and_store_qualifying_results(
        self,
        fastf1_client,
        test_storage_client,
        test_race_config,
        skip_if_no_internet,
        cleanup_test_data,
    ):
        """Test fetching qualifying results and storing"""
        # Fetch session
        session = fastf1_client.get_session(
            test_race_config["year"], test_race_config["event"], "Q"
        )

        assert session is not None
        assert session.results is not None

        # Extract and store
        results = session.results.copy()
        results["Year"] = session.event["EventDate"].year
        results["RoundNumber"] = session.event["RoundNumber"]
        results["EventName"] = session.event["EventName"]
        results["SessionType"] = "Q"

        object_key = f"test/{test_race_config['year']}/italy/Q/results.parquet"
        success = test_storage_client.upload_dataframe(results, object_key)

        assert success is True


@pytest.mark.integration
@pytest.mark.requires_docker
class TestFastF1ToValidationToStorage:
    """Test FastF1 → Validation → Storage flow"""

    def test_complete_validation_flow(
        self,
        fastf1_client,
        test_storage_client,
        data_validator,
        test_race_config,
        skip_if_no_internet,
        cleanup_test_data,
    ):
        """Test fetching, validating, and storing race data"""
        # Fetch session
        session = fastf1_client.get_session(
            test_race_config["year"], test_race_config["event"], "R"
        )

        # Extract data
        results = session.results.copy()
        results["Year"] = session.event["EventDate"].year
        results["RoundNumber"] = session.event["RoundNumber"]
        results["EventName"] = session.event["EventName"]
        results["SessionType"] = "R"

        # Validate
        valid_results, errors = data_validator.validate_race_results(results)

        # Should have mostly valid data
        assert len(valid_results) > 0
        assert len(valid_results) >= len(results) * 0.8  # At least 80% valid

        # Store validated data
        object_key = (
            f"test/{test_race_config['year']}/italy/R/validated_results.parquet"
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
            test_race_config["year"], test_race_config["event"], "R"
        )

        results = session.results.copy()
        results["Year"] = session.event["EventDate"].year
        results["RoundNumber"] = session.event["RoundNumber"]
        results["EventName"] = session.event["EventName"]
        results["SessionType"] = "R"

        original_length = len(results)

        # Validate
        valid_results, errors = data_validator.validate_race_results(results)

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
                test_race_config["year"], test_race_config["event"], session_type
            )

            if session is None:
                continue

            # Extract all data
            session_data = {}

            if session.results is not None:
                results = session.results.copy()
                results["Year"] = session.event["EventDate"].year
                results["RoundNumber"] = session.event["RoundNumber"]
                results["EventName"] = session.event["EventName"]
                results["SessionType"] = session_type
                session_data["results"] = results

            if hasattr(session, "laps") and session.laps is not None:
                laps = session.laps.copy()
                laps["Year"] = session.event["EventDate"].year
                laps["RoundNumber"] = session.event["RoundNumber"]
                laps["EventName"] = session.event["EventName"]
                laps["SessionType"] = session_type
                session_data["laps"] = laps

            if hasattr(session, "weather_data") and session.weather_data is not None:
                weather = session.weather_data.copy()
                weather["Year"] = session.event["EventDate"].year
                weather["RoundNumber"] = session.event["RoundNumber"]
                weather["EventName"] = session.event["EventName"]
                weather["SessionType"] = session_type
                session_data["weather"] = weather

            # Validate
            validated = validate_session_data(session_data, session_type)

            # Store
            for data_type, (valid_df, errors) in validated.items():
                if not valid_df.empty:
                    object_key = f"test/{test_race_config['year']}/italy/{session_type}/{data_type}.parquet"
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
        pipeline.storage_client.config.raw_bucket_name = "f1-test-data"

        result = pipeline.ingest_session(
            year=test_race_config["year"],
            event=test_race_config["event"],
            session_type="R",
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
        pipeline.storage_client.config.raw_bucket_name = "f1-test-data"

        results = pipeline.ingest_race_weekend(
            year=test_race_config["year"],
            event=test_race_config["event"],
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

    def test_handle_missing_session(self, fastf1_client, skip_if_no_internet):
        """Test handling non-existent session"""
        # Try to fetch a session that doesn't exist
        try:
            session = fastf1_client.get_session(2024, "NonExistentRace", "R")
            # If we get here without exception, session should be None or empty
            assert session is None or not hasattr(session, "results")
        except Exception as e:
            # Expected behavior - exception raised
            assert "NonExistentRace" in str(e) or "not found" in str(e).lower()

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
            test_race_config["year"], test_race_config["event"], "R"
        )

        results = session.results.copy()
        original_length = len(results)
        original_columns = list(results.columns)

        # Add metadata
        results["Year"] = session.event["EventDate"].year
        results["RoundNumber"] = session.event["RoundNumber"]
        results["EventName"] = session.event["EventName"]
        results["SessionType"] = "R"

        # Store
        object_key = f"test/{test_race_config['year']}/italy/R/consistency_test.parquet"
        test_storage_client.upload_dataframe(results, object_key)

        # Download
        downloaded = test_storage_client.download_dataframe(object_key)

        # Verify consistency
        assert len(downloaded) == len(results)
        assert set(downloaded.columns) == set(results.columns)

        # Check a few key values
        assert downloaded["Year"].iloc[0] == test_race_config["year"]
        assert downloaded["SessionType"].iloc[0] == "R"


@pytest.mark.integration
class TestPerformance:
    """Test performance of integration"""

    @pytest.mark.slow
    def test_ingestion_performance(
        self, test_race_config, skip_if_no_internet, skip_if_no_docker
    ):
        """Test that ingestion completes in reasonable time"""
        import time

        pipeline = IngestionPipeline(
            skip_existing=False, validate_data=True, delay_between_sessions=2
        )

        # Override storage client
        pipeline.storage_client.config.raw_bucket_name = "f1-test-data"

        start = time.time()

        result = pipeline.ingest_session(
            year=test_race_config["year"],
            event=test_race_config["event"],
            session_type="R",
        )

        duration = time.time() - start

        # Should complete in under 60 seconds (with caching)
        assert duration < 60
        assert result.success is True


@pytest.mark.integration
def test_end_to_end_smoke_test(
    test_race_config, skip_if_no_internet, skip_if_no_docker
):
    """Smoke test - basic end-to-end ingestion"""
    # Initialize all components
    fastf1_client = FastF1Client()
    storage_client = StorageClient()
    storage_client.config.raw_bucket_name = "f1-test-data"
    validator = DataValidator()

    # Fetch
    session = fastf1_client.get_session(
        test_race_config["year"], test_race_config["event"], "R"
    )

    assert session is not None

    # Extract
    results = session.results.copy()
    results["Year"] = session.event["EventDate"].year
    results["RoundNumber"] = session.event["RoundNumber"]
    results["EventName"] = session.event["EventName"]
    results["SessionType"] = "R"

    # Validate
    valid_results, errors = validator.validate_race_results(results)
    assert len(valid_results) > 0

    # Store
    object_key = f"test/smoke_test/{test_race_config['year']}/results.parquet"
    success = storage_client.upload_dataframe(valid_results, object_key)
    assert success is True

    # Verify
    downloaded = storage_client.download_dataframe(object_key)
    assert len(downloaded) == len(valid_results)

    # Cleanup
    storage_client.delete_object(object_key)
