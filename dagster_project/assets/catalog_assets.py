"""
Dagster asset to create race catalog
"""

import pandas as pd
from dagster import (
    asset,
    AssetExecutionContext,
    Output,
    MetadataValue,
    AssetKey,
    AssetIn,
)

from dagster_project.resources import (
    StorageResource,
    RedisResource,
    CatalogConfig,
)

from src.etl.catalog.race_catalog import RaceCatalogBuilder


@asset(
    name="raw_data_catalog",
    key_prefix=["catalog"],
    group_name="catalog",
    compute_kind="python",
    description="Complete inventory of F1 race data from raw bucket",
)
def f1_raw_race_catalog(
    context: AssetExecutionContext,
    storage_resource: StorageResource,
    redis_resource: RedisResource,
    catalog_config: CatalogConfig,
) -> Output[pd.DataFrame]:
    """
    Creates catalog of all race data from the raw bucket
    """

    context.log.info("=" * 80)
    context.log.info("Building Raw Data Catalog")
    context.log.info("=" * 80)

    storage_client = storage_resource.create_client()
    redis_client = redis_resource.create_client()

    if redis_client:
        context.log.info("✅ Redis available - caching enabled")
    else:
        context.log.info("⚠️  Redis not available - proceeding without cache")

    # Build catalog
    builder = RaceCatalogBuilder(
        storage_client=storage_client,
        redis_client=redis_client,
        raw_bucket=catalog_config.raw_bucket,
        processed_bucket=catalog_config.processed_bucket,
    )

    catalog_df = builder.build_catalog(years=catalog_config.years)

    # Calculate statistics
    total_entries = len(catalog_df)
    complete_entries = int(catalog_df["is_complete"].sum())
    incomplete_entries = total_entries - complete_entries
    avg_completeness = catalog_df["completeness_score"].mean()

    races = catalog_df[catalog_df["session"] == "Race"]
    qualifying = catalog_df[catalog_df["session"] == "Qualifying"]
    practice = catalog_df[catalog_df["session"].str.startswith("practice")]

    years_found = sorted(catalog_df["year"].unique().tolist())

    # Create preview table for metadata
    preview_df = catalog_df[
        ["year", "round", "event_name", "session", "is_complete", "completeness_score"]
    ].head(20)

    context.log.info("✅ Catalog built successfully!")
    context.log.info(f"   Total entries: {total_entries}")
    context.log.info(
        f"   Complete: {complete_entries} ({100 * complete_entries / total_entries:.1f}%)"
    )
    context.log.info(f"   Years: {years_found}")

    # Return with rich metadata
    return Output(
        value=catalog_df,
        metadata={
            # Summary stats
            "total_entries": total_entries,
            "complete_entries": complete_entries,
            "incomplete_entries": incomplete_entries,
            "avg_completeness_pct": round(avg_completeness, 2),
            # By session type
            "num_races": len(races),
            "num_qualifying": len(qualifying),
            "num_practice": len(practice),
            # By year
            "years": MetadataValue.json(years_found),
            "races_by_year": MetadataValue.json(races.groupby("year").size().to_dict()),
            # Data preview
            "preview": MetadataValue.md(preview_df.to_markdown(index=False)),
            # Warnings
            "incomplete_sessions": incomplete_entries,
        },
    )


@asset(
    name="validated_catalog",
    key_prefix=["catalog"],
    group_name="catalog",
    compute_kind="python",
    description="Catalog with validation checks and quality flags",
    ins={
        "raw_data_catalog": AssetIn(key=AssetKey(["catalog", "raw_data_catalog"])),
    },
)
def validated_catalog(
    context: AssetExecutionContext,
    raw_data_catalog: pd.DataFrame,
    storage_resource: StorageResource,
) -> Output[pd.DataFrame]:
    """
    Add validation layer to catalog with graceful error handling.

    This asset:
    - Validates data quality (with grace for expected nulls)
    - Checks for missing qualifying sessions
    - Identifies races without complete weekend data
    - Adds validation flags
    - Logs warnings (doesn't fail on issues)

    Returns:
        Enriched catalog with validation columns
    """

    context.log.info("=" * 80)
    context.log.info("Validating Catalog")
    context.log.info("=" * 80)

    catalog_df = raw_data_catalog.copy()

    storage_client = storage_resource.create_client()

    # Initialize validation columns
    catalog_df["validation_passed"] = True
    catalog_df["validation_warnings"] = ""
    catalog_df["validation_errors"] = ""

    warnings_list = []
    errors_list = []

    # Validation 1: Check for races without qualifying
    context.log.info("Checking for missing qualifying sessions...")

    races_without_quali = []
    for year in catalog_df["year"].unique():
        for round_num in catalog_df[catalog_df["year"] == year]["round"].unique():
            has_race = (
                len(
                    catalog_df[
                        (catalog_df["year"] == year)
                        & (catalog_df["round"] == round_num)
                        & (catalog_df["session"] == "Race")
                    ]
                )
                > 0
            )

            has_quali = (
                len(
                    catalog_df[
                        (catalog_df["year"] == year)
                        & (catalog_df["round"] == round_num)
                        & (catalog_df["session"] == "Qualifying")
                    ]
                )
                > 0
            )

            if has_race and not has_quali:
                races_without_quali.append(f"{year}_R{round_num:02d}")

                # Add warning to race entry
                race_idx = catalog_df[
                    (catalog_df["year"] == year)
                    & (catalog_df["round"] == round_num)
                    & (catalog_df["session"] == "Race")
                ].index

                if len(race_idx) > 0:
                    catalog_df.loc[race_idx[0], "validation_warnings"] += (
                        "Missing qualifying session; "
                    )

    if races_without_quali:
        warning_msg = f"⚠️  {len(races_without_quali)} races missing qualifying: {', '.join(races_without_quali[:5])}"  # pylint: disable=line-too-long
        context.log.warning(warning_msg)
        warnings_list.append(warning_msg)

    # Validation 2: Check incomplete race sessions
    context.log.info("Checking incomplete race sessions...")

    incomplete_races = catalog_df[
        (catalog_df["session"] == "Race") & (~catalog_df["is_complete"])
    ]

    if len(incomplete_races) > 0:
        warning_msg = f"⚠️  {len(incomplete_races)} incomplete race sessions"
        context.log.warning(warning_msg)
        warnings_list.append(warning_msg)

        for idx, race in incomplete_races.iterrows():
            missing = []
            if not race["has_results"]:
                missing.append("results")
            if not race["has_laps"]:
                missing.append("laps")
            if not race["has_session_info"]:
                missing.append("session_info")

            catalog_df.loc[idx, "validation_warnings"] += (
                f"Missing files: {', '.join(missing)}; "
            )

            context.log.warning(
                f"    {race['year']} R{race['round']:02d} {race['event_name']}: "
                f"missing {', '.join(missing)}"
            )

    # Validation 3: Check for null positions (with grace)
    context.log.info("Checking for null positions in results...")

    # Null positions are OK for DNFs, so just log info
    sessions_with_nulls = catalog_df[catalog_df["has_null_positions"]]

    if len(sessions_with_nulls) > 0:
        info_msg = f"ℹ️  {len(sessions_with_nulls)} sessions have null positions (expected for DNFs)"  # pylint: disable=line-too-long
        context.log.info(info_msg)

        # Only warn if it's a high percentage
        for idx, _ in sessions_with_nulls.iterrows():
            # This is informational, not a validation error
            catalog_df.loc[idx, "validation_warnings"] += (
                "Contains null positions (may be DNFs); "
            )

    # Validation 4: Check for data freshness
    context.log.info("Checking data freshness...")

    if "session_date" in catalog_df.columns:
        catalog_df["session_date"] = pd.to_datetime(catalog_df["session_date"])
        latest_race_date = catalog_df[catalog_df["session"] == "Race"][
            "session_date"
        ].max()

        days_since_latest = (pd.Timestamp.now() - latest_race_date).days

        if days_since_latest > 21:  # More than 3 weeks
            warning_msg = (
                f"⚠️  Latest race is {days_since_latest} days old - may need update"
            )
            context.log.warning(warning_msg)
            warnings_list.append(warning_msg)

    # Validation 5: Check for duplicate entries
    context.log.info("Checking for duplicate entries...")

    duplicates = catalog_df[
        catalog_df.duplicated(subset=["year", "round", "session"], keep=False)
    ]

    if len(duplicates) > 0:
        error_msg = f"❌ {len(duplicates)} duplicate entries found!"
        context.log.error(error_msg)
        errors_list.append(error_msg)

        # Mark duplicates with error
        dup_indices = catalog_df[
            catalog_df.duplicated(subset=["year", "round", "session"], keep=False)
        ].index

        catalog_df.loc[dup_indices, "validation_passed"] = False
        catalog_df.loc[dup_indices, "validation_errors"] += "Duplicate entry; "

    # Calculate overall validation status
    total_warnings = len(warnings_list)
    total_errors = len(errors_list)

    validation_passed = total_errors == 0

    # Summary
    context.log.info("=" * 80)
    context.log.info("Validation Summary")
    context.log.info("=" * 80)
    context.log.info(f"Total warnings: {total_warnings}")
    context.log.info(f"Total errors: {total_errors}")
    context.log.info(
        f"Overall status: {'✅ PASSED' if validation_passed else '❌ FAILED'}"
    )

    if not validation_passed:
        context.log.warning("⚠️  Validation found errors but not failing asset")
        context.log.warning("    Review validation_errors column in output")

    storage_client.upload_dataframe(
        df=catalog_df,
        bucket_name="dev-f1-data-processed",
        object_key="catalog/validated_catalog.parquet",
    )

    # Return with validation metadata
    return Output(
        value=catalog_df,
        metadata={
            "validation_passed": validation_passed,
            "total_warnings": total_warnings,
            "total_errors": total_errors,
            "warnings": MetadataValue.json(warnings_list),
            "errors": MetadataValue.json(errors_list),
            # Specific checks
            "races_missing_qualifying": len(races_without_quali),
            "incomplete_race_sessions": len(incomplete_races),
            "sessions_with_null_positions": len(sessions_with_nulls),
            "duplicate_entries": len(duplicates),
            # Data quality score
            "data_quality_score": round(
                100
                * (
                    1
                    - (total_errors * 0.5 + total_warnings * 0.1)
                    / max(len(catalog_df), 1)
                ),
                2,
            ),
        },
    )


# pylint: disable=redefined-outer-name
@asset(
    name="catalog_summary_report",
    key_prefix=["catalog"],
    group_name="catalog",
    compute_kind="markdown",
    description="Human-readable analysis report of catalog",
    ins={
        "validated_catalog": AssetIn(key=AssetKey(["catalog", "validated_catalog"])),
    },
)
def catalog_summary_report(
    context: AssetExecutionContext,
    storage_resource: StorageResource,
    validated_catalog: pd.DataFrame,
    catalog_config: CatalogConfig,
) -> Output[str]:
    """
    Generate comprehensive markdown report summarizing catalog.

    This asset:
    - Creates formatted markdown report
    - Includes summary statistics
    - Lists issues found during validation
    - Provides breakdown by year
    - Saves to processed bucket

    Returns:
        Report content as string
    """

    context.log.info("Generating catalog summary report...")

    catalog_df = validated_catalog.copy()

    # Build report
    lines = []
    lines.append("# F1 Race Data Catalog Report\n\n")
    lines.append(
        f"**Generated:** {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    )

    # Overall summary
    lines.append("## Overall Summary\n\n")
    lines.append(f"- **Total entries:** {len(catalog_df)}\n")
    lines.append(
        f"- **Years covered:** {sorted(catalog_df['year'].unique().tolist())}\n"
    )
    lines.append(
        f"- **Total races:** {len(catalog_df[catalog_df['session'] == 'Race'])}\n"
    )
    lines.append(
        f"- **Complete sessions:** {catalog_df['is_complete'].sum()}/{len(catalog_df)}\n"
    )
    lines.append(
        f"- **Average completeness:** {catalog_df['completeness_score'].mean():.1f}%\n\n"
    )

    # Validation summary
    lines.append("## Validation Summary\n\n")
    warnings = catalog_df["validation_warnings"].apply(lambda x: len(x) > 0).sum()
    errors = catalog_df["validation_errors"].apply(lambda x: len(x) > 0).sum()

    lines.append(f"- **Entries with warnings:** {warnings}\n")
    lines.append(f"- **Entries with errors:** {errors}\n")
    lines.append(
        f"- **Validation status:** {'✅ Passed' if errors == 0 else '❌ Failed'}\n\n"
    )

    # By year
    lines.append("## Breakdown by Year\n\n")
    for year in sorted(catalog_df["year"].unique()):
        year_df = catalog_df[catalog_df["year"] == year]
        lines.append(f"### {year}\n\n")

        session_counts = year_df["session"].value_counts()
        for session, count in session_counts.items():
            complete = len(
                year_df[(year_df["session"] == session) & (year_df["is_complete"])]
            )
            lines.append(f"- **{session}:** {count} total ({complete} complete)\n")

        lines.append(
            f"- **Average completeness:** {year_df['completeness_score'].mean():.1f}%\n\n"
        )

    # Issues
    incomplete = catalog_df[~catalog_df["is_complete"]]
    if len(incomplete) > 0:
        lines.append(f"## ⚠️ Incomplete Sessions ({len(incomplete)})\n\n")
        lines.append("| Year | Round | Event | Session | Completeness |\n")
        lines.append("|------|-------|-------|---------|-------------|\n")

        for _, row in incomplete.head(20).iterrows():
            lines.append(
                f"| {row['year']} | {row['round']} | {row['event_name']} | "
                f"{row['session']} | {row['completeness_score']:.0f}% |\n"
            )

        if len(incomplete) > 20:
            lines.append(f"\n*... and {len(incomplete) - 20} more*\n\n")
        else:
            lines.append("\n")

    # Entries with validation warnings
    with_warnings = catalog_df[catalog_df["validation_warnings"].str.len() > 0]
    if len(with_warnings) > 0:
        lines.append(f"## Validation Warnings ({len(with_warnings)})\n\n")
        lines.append("| Year | Round | Event | Session | Warnings |\n")
        lines.append("|------|-------|-------|---------|----------|\n")

        for _, row in with_warnings.head(20).iterrows():
            warnings = row["validation_warnings"].rstrip("; ")
            lines.append(
                f"| {row['year']} | {row['round']} | {row['event_name']} | "
                f"{row['session']} | {warnings} |\n"
            )

        if len(with_warnings) > 20:
            lines.append(f"\n*... and {len(with_warnings) - 20} more*\n\n")
        else:
            lines.append("\n")

    report_content = "".join(lines)

    # Save to storage
    storage_client = storage_resource.create_client()
    report_key = "catalog/catalog_summary_report.md"

    try:
        # Change this to the correct minio function
        storage_client.upload_file(
            report_content,
            object_key=report_key,
            bucket_name=catalog_config.processed_bucket,
        )

        context.log.info(
            f"✅ Report saved to: s3://{catalog_config.processed_bucket}/{report_key}"
        )

    except Exception as e:  # pylint: disable=broad-except
        context.log.warning(f"Failed to save report to storage: {e}")

    # Return report with metadata
    return Output(
        value=report_content,
        metadata={
            "report_size_chars": len(report_content),
            "report_lines": len(lines),
            "storage_path": f"s3://{catalog_config.processed_bucket}/{report_key}",
            "preview": MetadataValue.md(
                report_content[:2000] + "\n\n*[Report truncated for preview]*"
            ),
        },
    )
