# Data Ingestion Pipeline

Complete documentation for the F1 data ingestion pipeline.

## Overview

The ingestion pipeline orchestrates:
1. **Fetching** data from FastF1 API
2. **Validating** data quality using Pydantic schemas
3. **Storing** data in MinIO/S3 as Parquet files
4. **Tracking** progress with checkpoints
5. **Error handling** and retry logic

## Quick Start

### Ingest a Single Session

```bash
python scripts/ingest_data.py --year 2024 --event "Bahrain" --session R
```

### Ingest a Full Race Weekend

```bash
python scripts/ingest_data.py --year 2024 --event "Bahrain"
```

### Ingest a Complete Season

```bash
python scripts/ingest_data.py --year 2024
```

### Ingest All Historical Data (2022-2025)

```bash
python scripts/ingest_historical_data.py
```

## Architecture

### Pipeline Components

```
┌─────────────────────────────────────────────────────────────┐
│                    INGESTION PIPELINE                        │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  1. FastF1Client ──> Fetch Data from API                    │
│                                                               │
│  2. DataValidator ──> Validate Data Quality                 │
│                                                               │
│  3. StorageClient ──> Store in MinIO/S3                     │
│                                                               │
│  4. Progress Tracking ──> Checkpoints & Stats                │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

```
FastF1 API
    ↓
FastF1Client.get_session()
    ↓
Extract: Results, Laps, Weather
    ↓
Add Metadata (Year, Round, Event, Session)
    ↓
DataValidator.validate()
    ↓
Filter Valid Rows
    ↓
StorageClient.upload_dataframe()
    ↓
MinIO/S3 (Parquet files)
```

## Usage

### IngestionPipeline Class

```python
from src.data_ingestion.pipeline import IngestionPipeline

# Initialize pipeline
pipeline = IngestionPipeline(
    skip_existing=True,      # Skip already ingested data
    validate_data=True,      # Validate before storing
    delay_between_sessions=3 # Delay between API calls (seconds)
)

# Ingest a single session
result = pipeline.ingest_session(2024, "Bahrain", "R")

# Ingest a race weekend (Q + R)
results = pipeline.ingest_race_weekend(2024, "Bahrain")

# Ingest a complete season
results = pipeline.ingest_season(2024)

# Ingest multiple seasons
results = pipeline.ingest_multiple_seasons([2022, 2023, 2024])

# Print summary
pipeline.print_summary()

# Save results to file
pipeline.save_results("results.json")
```

### Command Line Interface

#### Basic Commands

```bash
# Single session
python scripts/ingest_data.py --year 2024 --event "Monaco" --session Q

# Race weekend (all configured sessions)
python scripts/ingest_data.py --year 2024 --event "Monaco"

# Full season
python scripts/ingest_data.py --year 2024

# Multiple seasons
python scripts/ingest_data.py --years 2022 2023 2024

# Specific events only
python scripts/ingest_data.py --year 2024 --events "Bahrain" "Saudi Arabia" "Australia"
```

#### Advanced Options

```bash
# Force re-ingestion (overwrite existing)
python scripts/ingest_data.py --year 2024 --force

# Skip validation (faster but not recommended)
python scripts/ingest_data.py --year 2024 --no-validate

# Adjust delay between API calls
python scripts/ingest_data.py --year 2024 --delay 5

# Save results to JSON file
python scripts/ingest_data.py --year 2024 --save-results results.json
```

### Historical Data Ingestion

Special script for ingesting all historical data (2022-2025):

```bash
# Ingest all historical years
python scripts/ingest_historical_data.py

# Specific years only
python scripts/ingest_historical_data.py --years 2023 2024

# Dry run (check what would be ingested)
python scripts/ingest_historical_data.py --dry-run

# Resume after interruption
python scripts/ingest_historical_data.py --resume
```

## Features

### 1. Progress Tracking

The pipeline tracks:
- Total sessions processed
- Successful/failed/skipped sessions
- Validation errors count
- Files uploaded count
- Processing time per session
- Total execution time

**Example output:**
```
======================================================================
PIPELINE EXECUTION SUMMARY
======================================================================
Total sessions processed:  48
  ✅ Successful:           45
  ❌ Failed:               2
  ⏭️  Skipped (existing):   1

Validation errors:         23
Files uploaded:            135
Total processing time:     1845.32 seconds (30.76 minutes)
Wall clock time:           1920.15 seconds (32.00 minutes)
======================================================================
```

### 2. Resumability

The pipeline automatically saves checkpoints after each event:

```json
{
  "last_completed_year": 2024,
  "last_completed_event": "Monaco Grand Prix",
  "timestamp": "2025-01-15T10:30:45.123456"
}
```

**To resume:**
```bash
python scripts/ingest_historical_data.py --resume
```

### 3. Skip Existing Data

By default, the pipeline skips already ingested sessions:

```python
# Check if session exists
if pipeline.skip_existing and pipeline._session_exists(year, event, session):
    logger.info("⏭️  Session already exists, skipping")
    return
```

**Override:**
```bash
python scripts/ingest_data.py --year 2024 --force
```

### 4. Data Validation

All data is validated before storage:

```python
if pipeline.validate_data:
    validated = validate_session_data(session_data, session_type)
    
    for data_type, (valid_df, errors) in validated.items():
        session_data[data_type] = valid_df  # Use only valid rows
        validation_errors += len(errors)
```

**Skip validation (not recommended):**
```bash
python scripts/ingest_data.py --year 2024 --no-validate
```

### 5. Error Handling

The pipeline handles errors gracefully:

- **API failures**: Logged and skipped
- **Validation errors**: Invalid rows filtered out
- **Upload failures**: Reported in results
- **Interruptions**: Progress saved to checkpoint

### 6. Retry Failed Sessions

```python
# Get failed sessions
failed = pipeline.get_failed_sessions()

# Retry them
retry_results = pipeline.retry_failed_sessions()
```

## Configuration

### Environment Variables

Set in `.env` file:

```bash
# FastF1 Configuration
FASTF1_CACHE_DIR=data/external/cache
FASTF1_CACHE_ENABLED=True
FASTF1_LOG_LEVEL=INFO

# Session Types to Ingest
INCLUDE_QUALIFYING=True
INCLUDE_SPRINT=True
INCLUDE_FREE_PRACTICE=False
INCLUDE_TESTING=False

# Data Features
ENABLE_LAPS=True
ENABLE_WEATHER_DATA=True
ENABLE_TELEMETRY=False
ENABLE_RACE_CONTROL_MSGS=False

# Storage
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET_RAW=f1-raw-data
MINIO_SECURE=false
```

### Code Configuration

```python
from config.settings import fastf1_config

# Current session types from config
print(fastf1_config.session_types)  # ['R', 'Q', 'S', 'SS', 'SQ']

# Modify for specific ingestion
pipeline.ingest_season(
    year=2024,
    session_types=['R', 'Q']  # Only Race and Qualifying
)
```

## Data Storage Structure

### MinIO/S3 Organization

```
f1-raw-data/
├── 2022/
│   ├── season_schedule.parquet
│   ├── bahrain_grand_prix/
│   │   ├── Q/
│   │   │   ├── results.parquet
│   │   │   ├── laps.parquet
│   │   │   └── weather.parquet
│   │   └── R/
│   │       ├── results.parquet
│   │       ├── laps.parquet
│   │       └── weather.parquet
│   ├── saudi_arabian_grand_prix/
│   └── ...
├── 2023/
├── 2024/
└── 2025/
```

### File Naming Convention

Built by `StorageClient.build_object_key()`:

```
{year}/{event_name}/{session_type}/{data_type}.parquet

Examples:
- 2024/bahrain_grand_prix/R/results.parquet
- 2024/bahrain_grand_prix/Q/laps.parquet
- 2024/monaco_grand_prix/R/weather.parquet
```

## Performance

### Timing Estimates

**Single session:** ~15-30 seconds (first time), ~5 seconds (cached)

**Race weekend (Q + R):** ~30-60 seconds (first time), ~10 seconds (cached)

**Full season (~24 races):** ~20-40 minutes (first time), ~5-10 minutes (cached)

**All historical data (2022-2025):** ~2-3 hours (first time), ~30-60 minutes (cached)

### Optimization Tips

1. **Use FastF1 cache** (enabled by default)
2. **Adjust delay** between API calls if rate limited
3. **Skip validation** for testing (but validate for production)
4. **Use --dry-run** to preview before ingesting
5. **Resume** from checkpoints instead of restarting

## Monitoring

### Logs

All ingestion activity is logged:

```
monitoring/logs/
├── app.log              # General application logs
├── data_ingestion.log   # Ingestion-specific logs
├── fastf1.log          # FastF1 API logs
└── error.log           # Errors only
```

### Progress Tracking

Watch logs in real-time:

```bash
# All logs
tail -f monitoring/logs/data_ingestion.log

# Errors only
tail -f monitoring/logs/error.log

# FastF1 API calls
tail -f monitoring/logs/fastf1.log
```

### Results Files

Pipeline saves results to JSON:

```bash
monitoring/logs/
├── ingestion_results_20250115_103045.json
└── ingestion_checkpoint.json
```

**Results format:**
```json
{
  "stats": {
    "total_sessions": 48,
    "successful_sessions": 45,
    "failed_sessions": 2,
    "skipped_sessions": 1,
    "total_validation_errors": 23,
    "total_files_uploaded": 135,
    "total_processing_time_seconds": 1845.32,
    "start_time": "2025-01-15T10:00:00",
    "end_time": "2025-01-15T10:32:00"
  },
  "sessions": [
    {
      "year": 2024,
      "event_name": "Bahrain Grand Prix",
      "session_type": "R",
      "success": true,
      "validation_errors": 0,
      "uploaded_files": {
        "results": true,
        "laps": true,
        "weather": true
      },
      "error_message": null,
      "processing_time_seconds": 28.45
    }
  ]
}
```

## Troubleshooting

### Issue: Session fails to load

**Error:** `Failed to fetch session: No data available`

**Solutions:**
1. Check if session actually happened (cancelled races)
2. Verify FastF1 API has data for this session
3. Check internet connection
4. Increase retry delay: `--delay 5`

### Issue: Validation errors

**Warning:** `Validation found 15 errors`

**Solutions:**
1. Check logs for specific validation errors
2. Review data quality from FastF1
3. Adjust validation schemas if needed (edge cases)
4. Invalid rows are filtered out automatically

### Issue: Upload fails

**Error:** `Failed to upload results.parquet`

**Solutions:**
1. Check MinIO/S3 is running: `docker ps`
2. Verify credentials in `.env`
3. Check bucket exists
4. Check disk space

### Issue: Pipeline interrupted

**Error:** User pressed Ctrl+C

**Solutions:**
1. Resume with: `python scripts/ingest_historical_data.py --resume`
2. Checkpoint saved automatically
3. Progress not lost

### Issue: Rate limited by FastF1

**Error:** `Too many requests`

**Solutions:**
1. Increase delay: `--delay 10`
2. FastF1 cache helps avoid repeated requests
3. Wait a few minutes and retry

## Best Practices

### 1. Always Start with Dry Run

```bash
# Check what would be ingested
python scripts/ingest_historical_data.py --dry-run

# Then proceed
python scripts/ingest_historical_data.py
```

### 2. Use Validation in Production

```bash
# ✅ Good: Validate data
python scripts/ingest_data.py --year 2024

# ❌ Bad: Skip validation in production
python scripts/ingest_data.py --year 2024 --no-validate
```

### 3. Monitor Progress

```bash
# In one terminal: run ingestion
python scripts/ingest_historical_data.py

# In another terminal: watch logs
tail -f monitoring/logs/data_ingestion.log
```

### 4. Save Results

```bash
python scripts/ingest_data.py --year 2024 --save-results results.json
```

### 5. Check Storage After Ingestion

```python
from src.data_ingestion.storage_client import StorageClient

storage = StorageClient()
summary = storage.get_ingestion_summary(year=2024)

print(f"Total objects: {summary['total_objects']}")
print(f"Total size: {summary['total_size_mb']:.2f} MB")
print(f"By type: {summary['by_type']}")
```

## Examples

### Example 1: Ingest Single Race Weekend

```bash
python scripts/ingest_data.py \
    --year 2024 \
    --event "Monaco" \
    --delay 3 \
    --save-results monaco_2024.json
```

### Example 2: Ingest Specific Events

```bash
python scripts/ingest_data.py \
    --year 2024 \
    --events "Bahrain" "Saudi Arabia" "Australia" "Japan" "Qatar"
```

### Example 3: Re-ingest with Validation

```bash
python scripts/ingest_data.py \
    --year 2024 \
    --event "Monaco" \
    --force \
    --save-results monaco_validated.json
```

### Example 4: Programmatic Usage

```python
from src.data_ingestion.pipeline import IngestionPipeline

# Initialize
pipeline = IngestionPipeline(skip_existing=True, validate_data=True)

# Ingest 2024 season
results = pipeline.ingest_season(2024)

# Check results
successful = sum(1 for r in results if r.success)
print(f"Successfully ingested {successful}/{len(results)} sessions")

# Retry failures
if pipeline.stats.failed_sessions > 0:
    retry_results = pipeline.retry_failed_sessions()

# Save results
pipeline.save_results("ingestion_results.json")
```

## Next Steps

After ingestion completes:

1. **Verify data in MinIO**: http://localhost:9001
2. **Check logs** for any warnings or errors
3. **Review validation errors** if any
4. **Proceed to ETL** for data processing
5. **Start feature engineering** for ML models

## Related Documentation

- [FastF1 Client](./FASTF1_CLIENT.md)
- [Storage Client](./STORAGE_CLIENT.md)
- [Data Schemas](./DATA_SCHEMAS.md)
- [Configuration](./CONFIGURATION.md)

## Support

For issues:
1. Check logs in `monitoring/logs/`
2. Review error messages in console output
3. Check results JSON for failed sessions
4. Verify configuration in `.env`
5. Test with `--dry-run` first