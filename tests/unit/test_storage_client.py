"""
Test to check if the storage client works as expected.
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

import pandas as pd  # noqa: E402
from config.logging import setup_logging  # noqa: E402
from src.data_ingestion.storage_client import StorageClient  # noqa: E402

setup_logging()


print("\n" + "=" * 60)
print("Storage Client Tests")
print("=" * 60)

# Initialize storage client
print("\n1. Initializing storage client...")
storage = StorageClient()
print("✅ Storage client initialized")

# Test 1: Upload a test DataFrame
print("\n2. Testing DataFrame upload...")
test_df = pd.DataFrame(
    {
        "DriverNumber": [1, 11, 16],
        "Abbreviation": ["VER", "PER", "LEC"],
        "Position": [1, 2, 3],
        "Points": [25, 18, 15],
    }
)

test_key = "test/sample_results.parquet"
success = storage.upload_dataframe(test_df, test_key)
print(f"Upload status: {'✅ Success' if success else '❌ Failed'}")

# Test 2: Check if object exists
print("\n3. Testing object existence check...")
exists = storage.object_exists(test_key)
print(f"Object exists: {'✅ Yes' if exists else '❌ No'}")

# Test 3: Download the DataFrame
print("\n4. Testing DataFrame download...")
downloaded_df = storage.download_dataframe(test_key)
if downloaded_df is not None:
    print("✅ Download successful")
    print(downloaded_df)
else:
    print("❌ Download failed")

# Test 4: List objects
print("\n5. Testing object listing...")
objects = storage.list_objects(prefix="test/")
print(f"Found {len(objects)} objects with prefix 'test/':")
for obj in objects:
    print(f"  - {obj}")

# Test 5: Get metadata
print("\n6. Testing object metadata...")
metadata = storage.get_object_metadata(test_key)
if metadata:
    print(f"  Size: {metadata['size_mb']:.4f} MB")
    print(f"  Last modified: {metadata['last_modified']}")

# Test 6: Build object key
print("\n7. Testing object key builder...")
key = storage.build_object_key(
    year=2024,
    event_name="Bahrain Grand Prix",
    session_type="R",
    data_type="race_results",
)
print(f"Generated key: {key}")

# Test 7: Upload session data
print("\n8. Testing session data upload...")
session_data = {
    "results": test_df,
    "laps": pd.DataFrame({"lap": [1, 2, 3]}),
    "weather": pd.DataFrame({"temp": [25, 26, 27]}),
}

status = storage.upload_session_data(
    year=2024,
    event_name="Test Race",
    session_type="R",
    session_data=session_data,
)
print("Upload status:", status)

# Test 8: Get ingestion summary
print("\n9. Testing ingestion summary...")
summary = storage.get_ingestion_summary()
print(f"Total objects: {summary['total_objects']}")
print(f"Total size: {summary['total_size_mb']:.2f} MB")
print(f"By type: {summary['by_type']}")

# Cleanup test data
print("\n10. Cleaning up test data...")
storage.delete_object(test_key)
for data_type in ["results", "laps", "weather"]:
    test_session_key = storage.build_object_key(2024, "Test Race", "R", data_type)
    storage.delete_object(test_session_key)

print("\n✅ All tests completed!")
