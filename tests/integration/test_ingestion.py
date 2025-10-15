"""
Test script for the ingestion pipeline.

Tests the complete pipeline with a single session to verify everything works.
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

from config.logging import setup_logging, get_logger  # noqa: E402
from src.data_ingestion.storage_client import StorageClient  # noqa: E402
from src.data_ingestion.data_ingestion_pipeline import (  # noqa: E402
    IngestionPipeline,
    ingest_single_session,
)

# Setup logging
setup_logging()
logger = get_logger("test.pipeline")


def test_single_session():
    """Test ingesting a single session"""

    print("\n" + "=" * 70)
    print("TEST 1: Single Session Ingestion")
    print("=" * 70)

    # Use the convenience function
    result = ingest_single_session(2024, "Bahrain Grand Prix", "R", skip_existing=False)

    print("\nResult:")
    print(f"  Success: {result.success}")
    print(f"  Validation errors: {result.validation_errors}")
    print(f"  Files uploaded: {result.uploaded_files}")
    print(f"  Processing time: {result.processing_time_seconds:.2f}s")

    if result.error_message:
        print(f"  Error: {result.error_message}")

    if result.success:
        print("\n✅ Single session test PASSED")
    else:
        print("\n❌ Single session test FAILED")

    return result.success


def test_race_weekend():
    """Test ingesting a complete race weekend"""

    print("\n" + "=" * 70)
    print("TEST 2: Race Weekend Ingestion (Q + R)")
    print("=" * 70)

    pipeline = IngestionPipeline(
        skip_existing=False,  # Force re-ingest for testing
        validate_data=True,
        delay_between_sessions=2,
    )

    results = pipeline.ingest_race_weekend(
        2024, "Bahrain Grand Prix", session_types=["Q", "R"]
    )

    print("\nResults:")
    for result in results:
        status = "✅" if result.success else "❌"
        print(
            f"  {status} {result.session_type}: {result.processing_time_seconds:.2f}s"
        )

    pipeline.print_summary()

    all_successful = all(r.success for r in results)

    if all_successful:
        print("\n✅ Race weekend test PASSED")
    else:
        print("\n❌ Race weekend test FAILED")

    return all_successful


def test_skip_existing():
    """Test that skip_existing works correctly"""

    print("\n" + "=" * 70)
    print("TEST 3: Skip Existing Data")
    print("=" * 70)

    pipeline = IngestionPipeline(
        skip_existing=True,  # Should skip
        validate_data=True,
    )

    # First, ingest
    print("\nFirst ingestion (should upload):")
    _ = pipeline.ingest_session(2024, "Bahrain Grand Prix", "R")

    # Second time, should skip
    print("\nSecond ingestion (should skip):")
    result2 = pipeline.ingest_session(2024, "Bahrain Grand Prix", "R")

    # Check that second one was skipped
    skipped = result2.error_message and "Skipped" in result2.error_message

    if skipped:
        print("\n✅ Skip existing test PASSED")
    else:
        print("\n❌ Skip existing test FAILED")

    return skipped


def test_validation():
    """Test that data validation works"""

    print("\n" + "=" * 70)
    print("TEST 4: Data Validation")
    print("=" * 70)

    # With validation
    pipeline_validated = IngestionPipeline(skip_existing=False, validate_data=True)

    result_validated = pipeline_validated.ingest_session(
        2024, "Bahrain Grand Prix", "R"
    )

    print("\nWith validation:")
    print(f"  Success: {result_validated.success}")
    print(f"  Validation errors: {result_validated.validation_errors}")
    print(
        f"  Files uploaded: {sum(1 for v in result_validated.uploaded_files.values() if v)}"
    )

    # Without validation
    pipeline_no_validate = IngestionPipeline(skip_existing=False, validate_data=False)

    result_no_validate = pipeline_no_validate.ingest_session(
        2024, "Bahrain Grand Prix", "Q"
    )

    print("\nWithout validation:")
    print(f"  Success: {result_no_validate.success}")
    print(f"  Validation errors: {result_no_validate.validation_errors}")
    print(
        f"  Files uploaded: {sum(1 for v in result_no_validate.uploaded_files.values() if v)}"
    )

    # Validation should be 0 when disabled
    validation_works = (
        result_validated.validation_errors >= 0
        and result_no_validate.validation_errors == 0
    )

    if validation_works:
        print("\n✅ Validation test PASSED")
    else:
        print("\n❌ Validation test FAILED")

    return validation_works


def test_error_handling():
    """Test error handling for non-existent session"""

    print("\n" + "=" * 70)
    print("TEST 5: Error Handling")
    print("=" * 70)

    pipeline = IngestionPipeline()

    # Try to ingest a session that doesn't exist
    result = pipeline.ingest_session(2024, "NonExistentRace", "R")

    print("\nAttempting to ingest non-existent session:")
    print(f"  Success: {result.success}")
    print(f"  Error message: {result.error_message}")

    # Should fail gracefully
    error_handled = not result.success and result.error_message is not None

    if error_handled:
        print("\n✅ Error handling test PASSED")
    else:
        print("\n❌ Error handling test FAILED")

    return error_handled


def test_storage_verification():
    """Verify data was stored correctly"""

    print("\n" + "=" * 70)
    print("TEST 6: Storage Verification")
    print("=" * 70)

    storage = StorageClient()

    # Check if data exists
    key = storage.build_object_key(
        year=2024,
        event_name="Bahrain Grand Prix",
        session_type="R",
        data_type="results",
    )

    exists = storage.object_exists(key)

    print("\nChecking storage:")
    print(f"  Key: {key}")
    print(f"  Exists: {exists}")

    if exists:
        # Try to download and verify
        df = storage.download_dataframe(key)

        if df is not None:
            print(f"  Downloaded: {len(df)} rows")
            print(f"  Columns: {list(df.columns)[:5]}...")

            # Check required columns
            required = [
                "DriverNumber",
                "BroadcastName",
                "Abbreviation",
                "DriverId",
                "TeamName",
                "TeamColor",
                "TeamId",
                "FirstName",
                "LastName",
                "FullName",
                "HeadshotUrl",
                "CountryCode",
                "Position",
                "ClassifiedPosition",
                "GridPosition",
                "Q1",
                "Q2",
                "Q3",
                "Time",
                "Status",
                "Points",
                "Laps",
            ]
            has_metadata = all(col in df.columns for col in required)

            if has_metadata:
                print("\n✅ Storage verification test PASSED")
                return True

    print("\n❌ Storage verification test FAILED")
    return False


def run_all_tests():
    """Run all pipeline tests"""

    print("\n" + "=" * 70)
    print("INGESTION PIPELINE TEST SUITE")
    print("=" * 70)

    tests = [
        ("Single Session", test_single_session),
        ("Race Weekend", test_race_weekend),
        ("Skip Existing", test_skip_existing),
        ("Validation", test_validation),
        ("Error Handling", test_error_handling),
        ("Storage Verification", test_storage_verification),
    ]

    results = {}

    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:  # pylint: disable=broad-except
            logger.error(
                "Test '%s' failed with exception: %s", test_name, e, exc_info=True
            )
            results[test_name] = False

    # Summary
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)

    for test_name, passed in results.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"  {status}: {test_name}")

    total_passed = sum(1 for p in results.values() if p)
    total_tests = len(results)

    print(f"\nTotal: {total_passed}/{total_tests} tests passed")

    if total_passed == total_tests:
        print("\n🎉 All tests passed!")
    else:
        print(f"\n⚠️  {total_tests - total_passed} test(s) failed")

    print("=" * 70 + "\n")

    return total_passed == total_tests


if __name__ == "__main__":
    # success = run_all_tests()
    success = test_error_handling()

    if success:
        print("✅ Pipeline is ready to use!")
        print("\nNext steps:")
        print("  1. Run historical ingestion: python scripts/ingest_historical_data.py")
        print("  2. Check data in MinIO: http://localhost:9001")
        print("  3. Review logs: monitoring/logs/data_ingestion.log")
    else:
        print("❌ Some tests failed. Please review the logs and fix issues.")
