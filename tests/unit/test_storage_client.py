"""
Unit tests for Storage Client.

Tests storage operations with test bucket (requires Docker/MinIO running).
"""

from datetime import datetime

import pandas as pd
import pytest

from src.data_ingestion.storage_client import StorageClient


@pytest.mark.unit
@pytest.mark.requires_docker
class TestStorageClientBasicOperations:
    """Test basic storage operations"""

    def test_upload_dataframe(
        self, test_storage_client, sample_race_results_df, cleanup_test_data
    ):
        """Test uploading a DataFrame"""

        object_key = "test/race_results.parquet"

        success = test_storage_client.upload_dataframe(
            sample_race_results_df, object_key
        )

        assert success is True

    def test_upload_empty_dataframe(self, test_storage_client, cleanup_test_data):
        """Test uploading empty DataFrame"""

        df = pd.DataFrame()
        object_key = "test/empty.parquet"

        success = test_storage_client.upload_dataframe(df, object_key)

        assert success is False  # Should reject empty DataFrame

    def test_upload_none_dataframe(self, test_storage_client, cleanup_test_data):
        """Test uploading None DataFrame"""
        object_key = "test/none.parquet"

        success = test_storage_client.upload_dataframe(None, object_key)

        assert success is False  # Should reject None

    def test_download_dataframe(
        self, test_storage_client, sample_race_results_df, cleanup_test_data
    ):
        """Test downloading a DataFrame"""
        object_key = "test/race_results.parquet"

        # Upload first
        test_storage_client.upload_dataframe(sample_race_results_df, object_key)

        # Download
        downloaded_df = test_storage_client.download_dataframe(object_key)

        assert downloaded_df is not None
        assert len(downloaded_df) == len(sample_race_results_df)
        assert list(downloaded_df.columns) == list(sample_race_results_df.columns)

    def test_download_nonexistent_file(self, test_storage_client):
        """Test downloading non-existent file"""
        object_key = "test/nonexistent.parquet"

        downloaded_df = test_storage_client.download_dataframe(object_key)

        assert downloaded_df is None

    def test_object_exists(
        self, test_storage_client, sample_race_results_df, cleanup_test_data
    ):
        """Test checking if object exists"""
        object_key = "test/race_results.parquet"

        # Should not exist initially
        assert test_storage_client.object_exists(object_key) is False

        # Upload
        test_storage_client.upload_dataframe(sample_race_results_df, object_key)

        # Should exist now
        assert test_storage_client.object_exists(object_key) is True

    def test_delete_object(
        self, test_storage_client, sample_race_results_df, cleanup_test_data
    ):
        """Test deleting an object"""
        object_key = "test/race_results.parquet"

        # Upload
        test_storage_client.upload_dataframe(sample_race_results_df, object_key)
        assert test_storage_client.object_exists(object_key) is True

        # Delete
        success = test_storage_client.delete_object(object_key)

        assert success is True
        assert test_storage_client.object_exists(object_key) is False


@pytest.mark.unit
@pytest.mark.requires_docker
class TestStorageClientListOperations:
    """Test listing operations"""

    def test_list_objects_empty_bucket(self, test_storage_client):
        """Test listing objects in empty bucket"""
        objects = test_storage_client.list_objects(prefix="nonexistent/")

        assert isinstance(objects, list)
        assert len(objects) == 0

    def test_list_objects_with_prefix(
        self, test_storage_client, sample_race_results_df, cleanup_test_data
    ):
        """Test listing objects with prefix"""
        # Upload multiple files
        test_storage_client.upload_dataframe(
            sample_race_results_df, "test/2024/race1.parquet"
        )
        test_storage_client.upload_dataframe(
            sample_race_results_df, "test/2024/race2.parquet"
        )
        test_storage_client.upload_dataframe(
            sample_race_results_df, "test/2023/race1.parquet"
        )

        # List with prefix
        objects_2024 = test_storage_client.list_objects(prefix="test/2024/")
        objects_2023 = test_storage_client.list_objects(prefix="test/2023/")

        assert len(objects_2024) == 2
        assert len(objects_2023) == 1

    def test_list_all_objects(
        self, test_storage_client, sample_race_results_df, cleanup_test_data
    ):
        """Test listing all objects"""
        # Upload files
        test_storage_client.upload_dataframe(
            sample_race_results_df, "test/file1.parquet"
        )
        test_storage_client.upload_dataframe(
            sample_race_results_df, "test/file2.parquet"
        )

        # List all
        objects = test_storage_client.list_objects(prefix="test/")

        assert len(objects) >= 2


@pytest.mark.unit
@pytest.mark.requires_docker
class TestStorageClientMetadata:
    """Test metadata operations"""

    def test_get_object_metadata(
        self, test_storage_client, sample_race_results_df, cleanup_test_data
    ):
        """Test getting object metadata"""
        object_key = "test/race_results.parquet"

        # Upload
        test_storage_client.upload_dataframe(sample_race_results_df, object_key)

        # Get metadata
        metadata = test_storage_client.get_object_metadata(object_key)

        assert metadata is not None
        assert "size_bytes" in metadata
        assert "size_mb" in metadata
        assert "last_modified" in metadata
        assert metadata["size_bytes"] > 0

    def test_get_metadata_nonexistent_file(self, test_storage_client):
        """Test getting metadata for non-existent file"""
        metadata = test_storage_client.get_object_metadata("test/nonexistent.parquet")

        assert metadata is None


@pytest.mark.unit
@pytest.mark.requires_docker
class TestStorageClientKeyBuilder:
    """Test object key building"""

    def test_build_object_key(self, test_storage_client):
        """Test building standardized object key"""
        key = test_storage_client.build_object_key(
            year=2024,
            event_name="Italian Grand Prix",
            session_type="R",
            data_type="results",
        )

        expected = "2024/italian_grand_prix/R/results.parquet"
        assert key == expected

    def test_build_object_key_with_spaces(self, test_storage_client):
        """Test that spaces are replaced with underscores"""
        key = test_storage_client.build_object_key(
            year=2024,
            event_name="Saudi Arabian Grand Prix",
            session_type="Q",
            data_type="laps",
        )

        assert " " not in key
        assert "saudi_arabian_grand_prix" in key

    def test_build_object_key_lowercase(self, test_storage_client):
        """Test that event name is lowercased"""
        key = test_storage_client.build_object_key(
            year=2024,
            event_name="ITALIAN GRAND PRIX",
            session_type="R",
            data_type="weather",
        )

        assert "ITALIAN" not in key
        assert "italian" in key


@pytest.mark.unit
@pytest.mark.requires_docker
class TestStorageClientSessionData:
    """Test session data upload"""

    def test_upload_session_data_complete(
        self, test_storage_client, sample_race_results_df, cleanup_test_data
    ):
        """Test uploading complete session data"""
        # Create sample laps and weather
        laps_df = pd.DataFrame(
            {
                "DriverNumber": ["1", "1", "1"],
                "LapNumber": [1, 2, 3],
                "LapTime": [pd.Timedelta(minutes=1, seconds=32)] * 3,
            }
        )

        weather_df = pd.DataFrame(
            {
                "Time": [datetime.now()] * 3,
                "AirTemp": [28.5, 28.6, 28.7],
                "TrackTemp": [42.0, 42.1, 42.2],
                "Humidity": [45.0] * 3,
                "Pressure": [1013.0] * 3,
                "Rainfall": [False] * 3,
            }
        )

        session_data = {
            "results": sample_race_results_df,
            "laps": laps_df,
            "weather": weather_df,
        }

        upload_status = test_storage_client.upload_session_data(
            year=2024,
            event_name="Test Race",
            session_type="R",
            session_data=session_data,
        )

        assert upload_status["results"] is True
        assert upload_status["laps"] is True
        assert upload_status["weather"] is True

    def test_upload_session_data_partial(
        self, test_storage_client, sample_race_results_df, cleanup_test_data
    ):
        """Test uploading partial session data"""
        session_data = {
            "results": sample_race_results_df,
            "laps": None,  # No laps data
            "weather": None,  # No weather data
        }

        upload_status = test_storage_client.upload_session_data(
            year=2024,
            event_name="Test Race",
            session_type="R",
            session_data=session_data,
        )

        assert upload_status["results"] is True
        assert upload_status["laps"] is False
        assert upload_status["weather"] is False

    def test_upload_season_schedule(self, test_storage_client, cleanup_test_data):
        """Test uploading season schedule"""
        schedule_df = pd.DataFrame(
            {
                "RoundNumber": [1, 2, 3],
                "EventName": ["Bahrain GP", "Saudi Arabian GP", "Australian GP"],
                "EventDate": [datetime.now()] * 3,
            }
        )

        upload_status = test_storage_client.upload_season_schedule(2024, schedule_df)

        assert upload_status["season_schedule"] is True

        # Verify it was uploaded
        key = "2024/season_schedule.parquet"
        assert test_storage_client.object_exists(key) is True


@pytest.mark.unit
@pytest.mark.requires_docker
class TestStorageClientIngestionSummary:
    """Test ingestion summary"""

    def test_get_ingestion_summary_empty(self, test_storage_client):
        """Test getting summary for empty bucket"""
        summary = test_storage_client.get_ingestion_summary(year=2099)

        assert summary["total_objects"] == 0
        assert summary["by_type"]["results"] == 0
        assert summary["by_type"]["laps"] == 0
        assert summary["by_type"]["weather"] == 0
        assert summary["total_size_mb"] == 0.0

    def test_get_ingestion_summary_with_data(
        self, test_storage_client, sample_race_results_df, cleanup_test_data
    ):
        """Test getting summary with data"""
        # Upload some test data
        test_storage_client.upload_dataframe(
            sample_race_results_df, "test/2024/italian_grand_prix/R/results.parquet"
        )
        test_storage_client.upload_dataframe(
            sample_race_results_df, "test/2024/italian_grand_prix/R/laps.parquet"
        )

        summary = test_storage_client.get_ingestion_summary()

        assert summary["total_objects"] >= 2
        assert summary["by_type"]["results"] >= 1
        assert summary["by_type"]["laps"] >= 1
        assert summary["total_size_mb"] > 0


@pytest.mark.unit
@pytest.mark.requires_docker
class TestStorageClientCompression:
    """Test compression options"""

    def test_upload_with_snappy_compression(
        self, test_storage_client, sample_race_results_df, cleanup_test_data
    ):
        """Test upload with snappy compression (default)"""
        object_key = "test/snappy.parquet"

        success = test_storage_client.upload_dataframe(
            sample_race_results_df, object_key, compression="snappy"
        )

        assert success is True

        # Download and verify
        downloaded = test_storage_client.download_dataframe(object_key)
        assert len(downloaded) == len(sample_race_results_df)

    def test_upload_with_gzip_compression(
        self, test_storage_client, sample_race_results_df, cleanup_test_data
    ):
        """Test upload with gzip compression"""
        object_key = "test/gzip.parquet"

        success = test_storage_client.upload_dataframe(
            sample_race_results_df, object_key, compression="gzip"
        )

        assert success is True

        # Download and verify
        downloaded = test_storage_client.download_dataframe(object_key)
        assert len(downloaded) == len(sample_race_results_df)

    def test_compression_size_difference(self, test_storage_client, cleanup_test_data):
        """Test that compression reduces file size"""
        # Create larger DataFrame for noticeable compression
        large_df = pd.DataFrame(
            {
                "col1": ["text"] * 1000,
                "col2": list(range(1000)),
                "col3": [1.23456789] * 1000,
            }
        )

        # Upload without compression
        key_none = "test/no_compression.parquet"
        test_storage_client.upload_dataframe(large_df, key_none, compression="none")
        size_none = test_storage_client.get_object_metadata(key_none)["size_bytes"]

        # Upload with compression
        key_snappy = "test/with_compression.parquet"
        test_storage_client.upload_dataframe(large_df, key_snappy, compression="snappy")
        size_snappy = test_storage_client.get_object_metadata(key_snappy)["size_bytes"]

        # Compressed should be smaller (or at least not larger)
        assert size_snappy <= size_none


@pytest.mark.unit
@pytest.mark.requires_docker
class TestStorageClientErrorHandling:
    """Test error handling"""

    def test_upload_to_invalid_bucket(self):
        """Test uploading to non-existent bucket"""
        # This should be handled gracefully by the client
        # The test verifies error handling, not that it succeeds
        pass  # Implementation depends on how you want to handle this

    def test_download_corrupted_file(self, test_storage_client, cleanup_test_data):
        """Test downloading corrupted file"""
        # This is difficult to test without manually corrupting a file
        # Skip for now, or implement if needed
        pass

    def test_concurrent_uploads(
        self, test_storage_client, sample_race_results_df, cleanup_test_data
    ):
        """Test concurrent uploads to same key"""
        object_key = "test/concurrent.parquet"

        # Upload twice quickly
        success1 = test_storage_client.upload_dataframe(
            sample_race_results_df, object_key
        )
        success2 = test_storage_client.upload_dataframe(
            sample_race_results_df, object_key
        )

        # Both should succeed (second overwrites first)
        assert success1 is True
        assert success2 is True

        # Should be able to download
        downloaded = test_storage_client.download_dataframe(object_key)
        assert downloaded is not None
