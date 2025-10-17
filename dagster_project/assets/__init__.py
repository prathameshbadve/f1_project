"""
Dagster assets for F1 data pipeline
Assets represent data products that are created by the pipeline
"""

import json
from datetime import datetime
from typing import Dict, Any

from dagster import asset, AssetExecutionContext, Output, MetadataValue

from dagster_project.resources import (
    StorageResource,
    DatabaseResource,
    F1APIResource,
)
from config.settings import is_development

# ============================================================================
# RAW DATA INGESTION ASSETS
# ============================================================================


@asset(
    group_name="ingestion",
    compute_kind="api",
    description="Raw race results data from FastF1 API stored in S3/MinIO",
)
def raw_race_results(
    context: AssetExecutionContext,
    storage: StorageResource,
    f1_api: F1APIResource,
) -> Output[Dict[str, Any]]:
    """
    Fetch race results from F1 API and store in raw bucket.

    In local mode: Fetches only the last race
    In production: Fetches all races for current season
    """

    current_year = f1_api.get_current_season()

    if is_development():
        # Local: fetch only last race for testing

        rounds_to_fetch = [1]  # Just first race for testing
        context.log.info(f"DEVELOPMENT MODE: Fetching only round 1 for {current_year}")
    else:
        # Production: fetch all races
        # Get race schedule to determine number of rounds

        schedule_data = f1_api._make_request(f"{current_year}")
        total_rounds = len(schedule_data["MRData"]["RaceTable"]["Races"])
        rounds_to_fetch = range(1, total_rounds + 1)
        context.log.info(
            f"PRODUCTION MODE: Fetching {total_rounds} rounds for {current_year}"
        )

    results = []
    for round_num in rounds_to_fetch:
        try:
            race_data = f1_api.get_race_results(current_year, round_num)
            results.append(race_data)

            # Store in MinIO/S3
            object_name = f"race_results/{current_year}/round_{round_num:02d}.json"
            storage.upload_json(
                bucket=storage.bucket_raw,
                object_name=object_name,
                data=json.dumps(race_data, indent=2),
            )

            context.log.info(f"Stored race results: {current_year} Round {round_num}")

        except Exception as e:  # pylint: disable=broad-except
            context.log.error(f"Failed to fetch round {round_num}: {str(e)}")
            continue

    metadata = {
        "year": current_year,
        "rounds_fetched": len(results),
        "storage_bucket": storage.bucket_raw,
        "preview": MetadataValue.json(results[0] if results else {}),
    }

    return Output(
        value={"year": current_year, "rounds": len(results)}, metadata=metadata
    )


@asset(
    group_name="ingestion",
    compute_kind="api",
    description="Raw qualifying results data from F1 API stored in S3/MinIO",
)
def raw_qualifying_results(
    context: AssetExecutionContext,
    storage: StorageResource,
    f1_api: F1APIResource,
) -> Output[Dict[str, Any]]:
    """
    Fetch qualifying results from F1 API and store in raw bucket.
    """
    current_year = f1_api.get_current_season()

    if is_development():
        rounds_to_fetch = [1]
        context.log.info(f"LOCAL MODE: Fetching only round 1 for {current_year}")
    else:
        schedule_data = f1_api._make_request(f"{current_year}")
        total_rounds = len(schedule_data["MRData"]["RaceTable"]["Races"])
        rounds_to_fetch = range(1, total_rounds + 1)
        context.log.info(f"PRODUCTION MODE: Fetching {total_rounds} rounds")

    results = []
    for round_num in rounds_to_fetch:
        try:
            quali_data = f1_api.get_qualifying_results(current_year, round_num)
            results.append(quali_data)

            object_name = (
                f"qualifying_results/{current_year}/round_{round_num:02d}.json"
            )
            storage.upload_json(
                bucket=storage.bucket_raw,
                object_name=object_name,
                data=json.dumps(quali_data, indent=2),
            )

            context.log.info(
                f"Stored qualifying results: {current_year} Round {round_num}"
            )

        except Exception as e:  # pylint: disable=broad-except
            context.log.error(
                f"Failed to fetch qualifying for round {round_num}: {str(e)}"
            )
            continue

    return Output(
        value={"year": current_year, "rounds": len(results)},
        metadata={
            "year": current_year,
            "rounds_fetched": len(results),
            "storage_bucket": storage.bucket_raw,
        },
    )


@asset(
    group_name="ingestion",
    compute_kind="api",
    description="Raw driver standings data from F1 API stored in S3/MinIO",
)
def raw_driver_standings(
    context: AssetExecutionContext,
    storage: StorageResource,
    f1_api: F1APIResource,
) -> Output[Dict[str, Any]]:
    """
    Fetch driver standings from F1 API and store in raw bucket.
    """
    current_year = f1_api.get_current_season()

    try:
        standings_data = f1_api.get_driver_standings(current_year)

        object_name = f"driver_standings/{current_year}/current.json"
        storage.upload_json(
            bucket=storage.bucket_raw,
            object_name=object_name,
            data=json.dumps(standings_data, indent=2),
        )

        context.log.info(f"Stored driver standings for {current_year}")

        num_drivers = len(
            standings_data["MRData"]["StandingsTable"]["StandingsLists"][0][
                "DriverStandings"
            ]
        )

        return Output(
            value={"year": current_year, "drivers": num_drivers},
            metadata={
                "year": current_year,
                "num_drivers": num_drivers,
                "storage_bucket": storage.bucket_raw,
            },
        )

    except Exception as e:  # pylint: disable=broad-except
        context.log.error(f"Failed to fetch driver standings: {str(e)}")
        raise


# ============================================================================
# PROCESSED DATA ASSETS
# ============================================================================


@asset(
    group_name="processing",
    compute_kind="python",
    description="Processed and cleaned race data stored in PostgreSQL",
    deps=[raw_race_results, raw_qualifying_results],
)
def processed_race_data(
    context: AssetExecutionContext,
    storage: StorageResource,
    database: DatabaseResource,
) -> Output[Dict[str, Any]]:
    """
    Process raw race and qualifying data into structured format.
    Combines race results with qualifying data and stores in PostgreSQL.
    """
    # List all race result files
    race_files = storage.list_objects(bucket=storage.bucket_raw, prefix="race_results/")

    context.log.info(f"Processing {len(race_files)} race result files")

    records_processed = 0

    for race_file in race_files:
        try:
            # Download and parse race data
            race_json = storage.download_json(storage.bucket_raw, race_file)
            race_data = json.loads(race_json)

            # Extract race info
            races = race_data["MRData"]["RaceTable"]["Races"]
            if not races:
                continue

            race = races[0]
            year = int(race["season"])
            round_num = int(race["round"])

            # Process each result
            for result in race.get("Results", []):
                driver = result["Driver"]
                constructor = result["Constructor"]

                # Store processed data (simplified example)
                processed_record = {
                    "year": year,
                    "round": round_num,
                    "race_name": race["raceName"],
                    "driver_id": driver["driverId"],
                    "driver_name": f"{driver['givenName']} {driver['familyName']}",
                    "constructor": constructor["name"],
                    "position": int(result.get("position", 0)),
                    "points": float(result.get("points", 0)),
                    "grid": int(result.get("grid", 0)),
                    "laps": int(result.get("laps", 0)),
                    "status": result.get("status", ""),
                }

                # Save to processed bucket as JSON
                object_name = f"processed/races/{year}/round_{round_num:02d}_{driver['driverId']}.json"  # pylint: disable=line-too-long
                storage.upload_json(
                    bucket=storage.bucket_processed,
                    object_name=object_name,
                    data=json.dumps(processed_record, indent=2),
                )

                records_processed += 1

            context.log.info(
                f"Processed race: {year} Round {round_num} - {race['raceName']}"
            )

        except Exception as e:  # pylint: disable=broad-except
            context.log.error(f"Error processing {race_file}: {str(e)}")
            continue

    return Output(
        value={"records_processed": records_processed},
        metadata={
            "records_processed": records_processed,
            "storage_bucket": storage.bucket_processed,
            "timestamp": datetime.now().isoformat(),
        },
    )


@asset(
    group_name="ml",
    compute_kind="python",
    description="ML-ready features derived from processed race data",
    deps=[processed_race_data, raw_driver_standings],
)
def ml_features(
    context: AssetExecutionContext,
    storage: StorageResource,
) -> Output[Dict[str, Any]]:
    """
    Generate ML features from processed data.
    Creates feature sets for model training.
    """
    # List all processed race files
    processed_files = storage.list_objects(
        bucket=storage.bucket_processed, prefix="processed/races/"
    )

    context.log.info(
        f"Generating features from {len(processed_files)} processed records"
    )

    features_generated = 0

    # Placeholder for actual feature engineering logic
    # In real implementation, this would:
    # - Load processed data
    # - Calculate rolling averages, win rates, etc.
    # - Create lagged features
    # - Store feature matrix

    for processed_file in processed_files[:10] if is_development() else processed_files:
        try:
            data_json = storage.download_json(storage.bucket_processed, processed_file)
            data = json.loads(data_json)

            # Example feature engineering (simplified)
            features = {
                **data,
                "points_per_race": data["points"] / max(data["laps"], 1),
                "grid_position_change": data["position"] - data["grid"],
                # Add more sophisticated features here
            }

            # Store features
            object_name = processed_file.replace("processed/races/", "features/")
            storage.upload_json(
                bucket=storage.bucket_processed,
                object_name=object_name,
                data=json.dumps(features, indent=2),
            )

            features_generated += 1

        except Exception as e:  # pylint: disable=broad-except
            context.log.error(
                f"Error generating features for {processed_file}: {str(e)}"
            )
            continue

    return Output(
        value={"features_generated": features_generated},
        metadata={
            "features_generated": features_generated,
            "storage_bucket": storage.bucket_processed,
            "timestamp": datetime.now().isoformat(),
        },
    )
