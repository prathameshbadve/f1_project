"""
Integration test for FastF1 Client + Storage Client
"""

import sys
from pathlib import Path

current = Path.cwd()
indicators = ["pyproject.toml"]

for parent in [current] + list(current.parents):
    if any((parent / indicator).exists() for indicator in indicators):
        project_root = parent
        break
else:
    raise RuntimeError("Project root with required indicators not found.")

sys.path.insert(0, str(project_root))

# pylint: disable=wrong-import-position

from config.logging import setup_logging  # noqa: E402
from src.data_ingestion.fastf1_client import FastF1Client  # noqa: E402
from src.data_ingestion.storage_client import StorageClient  # noqa: E402

# Setup logging
setup_logging()


def test_fetch_and_store():
    """Test fetching data from FastF1 and storing in MinIO"""

    print("\n" + "=" * 60)
    print("Integration Test: Fetch + Store")
    print("=" * 60)

    # Initialize clients
    print("\n1. Initializing clients...")
    fastf1_client = FastF1Client()
    storage_client = StorageClient()

    # Fetch data for a single race
    print("\n2. Fetching Bahrain 2024 Race data...")
    schedule_2024 = fastf1_client.get_season_schedule(year=2024)

    # Upload to storage
    print("\n3. Uploading to MinIO...")
    upload_status = storage_client.upload_season_schedule(
        year=2024, schedule_df=schedule_2024
    )

    print("\n4. Upload Status:")
    for data_type, success in upload_status.items():
        status = "✅" if success else "❌"
        print(f"  {status} {data_type}")

    # # Verify by downloading
    # print("\n5. Verifying data by downloading...")
    # results_key = storage_client.build_object_key(
    #     year=2024, event_name="Bahrain Grand Prix", data_type="results"
    # )

    # downloaded_df = storage_client.download_dataframe(results_key)
    # if downloaded_df is not None:
    #     print(f"✅ Successfully downloaded {len(downloaded_df)} rows")
    #     print("\nTop 3 finishers:")
    #     print(downloaded_df[["Abbreviation", "Position", "Points"]].head(3))
    # else:
    #     print("❌ Failed to download data")

    # Get summary
    print("\n6. Storage Summary:")
    summary = storage_client.get_ingestion_summary(year=2024)
    print(f"  Total objects: {summary['total_objects']}")
    print(f"  Total size: {summary['total_size_mb']:.2f} MB")
    print(f"  By type: {summary['by_type']}")

    print("\n✅ Integration test completed!")


if __name__ == "__main__":
    test_fetch_and_store()
