"""
Test script for data validation schemas.

Run this to test the validation schemas with real F1 data.
"""

# pylint: disable=wrong-import-position

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

from config.logging import get_logger, setup_logging  # noqa: E402
from src.data_ingestion.schemas import (  # noqa: E402
    DataValidator,
    ResultSchema,
    validate_session_data,
)
from src.data_ingestion.session_data_loader import SessionLoader  # noqa: E402

# Setup logging
setup_logging()
logger = get_logger("test.schemas")


def test_schema_validation():
    """Test schema validation with real F1 data"""

    print("\n" + "=" * 70)
    print("Testing Data Validation Schemas")
    print("=" * 70)

    # Initialize clients
    session_loader = SessionLoader()
    validator = DataValidator()

    session_data = session_loader.load_session_data(2024, "Bahrain Grand Prix", "R")

    # Test 1: Fetch and validate race results
    print("\n" + "-" * 70)
    print("TEST 1: Validating Race Results")
    print("-" * 70)

    try:
        results_df = session_data["results"]
        valid_results, errors = validator.validate_results(results_df)

        print(f"Total rows: {len(results_df)}")
        print(f"Valid rows: {len(valid_results)}")
        print(f"Invalid rows: {len(errors)}")

        if errors:
            print("\nFirst 3 errors:")
            for error in errors[:3]:
                print(f"  - {error}")
        else:
            print("\n✅ All race results validated successfully!")

        # Show sample of valid data
        print("\nSample of validated data:")
        print(valid_results[["Abbreviation", "Position", "Points"]].head(3).to_string())

    except Exception as e:  # pylint: disable=broad-except
        logger.error("Failed to validate race results: %s", e)
        print(f"❌ Error: {e}")

    # Test 3: Fetch and validate lap data
    print("\n" + "-" * 70)
    print("TEST 3: Validating Lap Data")
    print("-" * 70)

    try:
        laps_df = session_data["laps"]

        # Validate first 100 laps (for speed)
        sample_laps = laps_df.head(100)
        valid_laps, errors = validator.validate_lap_data(sample_laps)

        print(f"Total rows: {len(sample_laps)}")
        print(f"Valid rows: {len(valid_laps)}")
        print(f"Invalid rows: {len(errors)}")

        if errors:
            print("\nFirst 3 errors:")
            for error in errors[:3]:
                print(f"  - {error}")
        else:
            print("\n✅ All lap data validated successfully!")

        # Show sample
        print("\nSample of validated data:")
        if not valid_laps.empty:
            print(
                valid_laps[["DriverNumber", "LapNumber", "LapTime", "Compound"]]
                .head(3)
                .to_string()
            )
        else:
            print("No valid laps found.")

    except Exception as e:  # pylint: disable=broad-except
        logger.error("Failed to validate lap data: %s", e)
        print(f"❌ Error: {e}")

    # Test 4: Fetch and validate weather data
    print("\n" + "-" * 70)
    print("TEST 4: Validating Weather Data")
    print("-" * 70)

    try:
        weather_df = session_data["weather"]

        # Validate first 50 weather readings (for speed)
        sample_weather = weather_df.head(50)
        valid_weather, errors = validator.validate_weather_data(sample_weather)

        print(f"Total rows: {len(sample_weather)}")
        print(f"Valid rows: {len(valid_weather)}")
        print(f"Invalid rows: {len(errors)}")

        if errors:
            print("\nFirst 3 errors:")
            for error in errors[:3]:
                print(f"  - {error}")
        else:
            print("\n✅ All weather data validated successfully!")

        # Show sample
        print("\nSample of validated data:")
        print(
            valid_weather[["Time", "AirTemp", "TrackTemp", "Humidity"]]
            .head(3)
            .to_string()
        )

    except Exception as e:  # pylint: disable=broad-except
        logger.error("Failed to validate weather data: %s", e)
        print(f"❌ Error: {e}")

    # Test 5: Validate complete session using convenience function
    print("\n" + "-" * 70)
    print("TEST 5: Validating Complete Session (Convenience Function)")
    print("-" * 70)

    try:
        results_df = session_data["results"].copy()
        laps_df = session_data["laps"].head(100).copy()
        weather_df = session_data["weather"].head(50).copy()

        session_data_sample = {
            "results": results_df,
            "laps": laps_df,
            "weather": weather_df,
        }

        # Validate all at once
        validated = validate_session_data(session_data_sample)

        print("\nValidation Summary:")
        for data_type, (valid_df, errors) in validated.items():
            print(
                f"  {data_type:10s}: {len(valid_df):4d} valid, {len(errors):2d} errors"
            )

        print("\n✅ Complete session validation successful!")

    except Exception as e:  # pylint: disable=broad-except
        logger.error("Failed to validate complete session: %s", e)
        print(f"❌ Error: {e}")

    # Test 6: Test validation summary
    print("\n" + "-" * 70)
    print("TEST 6: Validation Summary Statistics")
    print("-" * 70)

    try:
        results_df = session_data["results"]
        summary = validator.get_validation_summary(results_df, ResultSchema)

        print(f"Total rows:       {summary['total_rows']}")
        print(f"Valid rows:       {summary['valid_rows']}")
        print(f"Invalid rows:     {summary['invalid_rows']}")
        print(f"Validation rate:  {summary['validation_rate']:.2f}%")

        if summary["errors"]:
            print("\nSample errors:")
            for error in summary["errors"][:3]:
                print(f"  - {error}")

    except Exception as e:  # pylint: disable=broad-except
        logger.error("Failed to get validation summary: %s", e)
        print(f"❌ Error: {e}")

    print("\n" + "=" * 70)
    print("All schema validation tests completed!")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    # Run tests
    # test_individual_schema_validation()
    test_schema_validation()

    print("\n✅ All tests completed! Check the logs for detailed information.")
