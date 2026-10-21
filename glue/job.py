"""AWS Glue PySpark ETL entry point for IoT building-sensor lakehouse (Section 8.3).

Executes end-to-end:
1. Read raw CSV for run_date with explicit string schema
2. Cast schema and validate with 5 quality checks
3. Quarantine invalid rows with diagnostics
4. Deduplicate on (sensor_id, event_time)
5. Write curated Parquet partitioned by building_id and date (idempotent overwrite)
6. Write gold 1-minute and 1-hour rollups
7. Write run metrics JSON summary
"""

import argparse
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

try:
    from glue.quality import evaluate_quality_checks
    from glue.transforms import (
        aggregate_gold_hourly,
        aggregate_gold_minute,
        cast_raw_telemetry,
        deduplicate_readings,
        read_raw_csv,
    )
except ImportError:
    from quality import evaluate_quality_checks
    from transforms import (
        aggregate_gold_hourly,
        aggregate_gold_minute,
        cast_raw_telemetry,
        deduplicate_readings,
        read_raw_csv,
    )


def get_spark_session() -> SparkSession:
    """Create or retrieve SparkSession configured for local or Glue runtime."""
    builder = (
        SparkSession.builder.appName("AWS-IoT-Lakehouse-ETL")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
    )

    # In local development on Windows, point to local hadoop and python if present
    hadoop_home = os.environ.get(
        "HADOOP_HOME", str(Path(__file__).resolve().parent.parent / ".hadoop")
    )
    if os.path.exists(hadoop_home):
        os.environ["HADOOP_HOME"] = hadoop_home
        builder = builder.config("spark.hadoop.home.dir", hadoop_home)

    return builder.getOrCreate()


def run_pipeline(
    run_date: str,
    raw_path: str,
    curated_path: str,
    gold_minute_path: str,
    gold_hourly_path: str,
    quarantine_path: str,
    metrics_path: str,
    spark: SparkSession | None = None,
) -> dict[str, Any]:
    """Execute end-to-end lakehouse ETL pipeline for a single date."""
    start_time = time.time()
    spark_session = spark or get_spark_session()

    # Step 1: Read raw CSV for the date across all building partitions
    # Path pattern: raw_path/building_id=*/date={run_date}/*.csv
    input_path = f"{raw_path.rstrip('/')}/building_id=*/date={run_date}/*.csv"
    print(f"Reading raw CSV data from {input_path}...")
    raw_df = read_raw_csv(spark_session, input_path)
    rows_read = raw_df.count()
    print(f"  Rows read: {rows_read:,}")

    # Step 2: Cast schema
    typed_df = cast_raw_telemetry(raw_df)

    # Step 3: Evaluate 5 quality checks & quarantine bad rows
    clean_candidates_df, quarantine_df = evaluate_quality_checks(typed_df, run_date)
    quarantine_count = quarantine_df.count()
    print(f"  Quarantined rows: {quarantine_count:,}")

    # Write quarantine Parquet if any failed rows exist
    quarantine_date_path = f"{quarantine_path.rstrip('/')}/date={run_date}"
    if quarantine_count > 0:
        quarantine_df.write.mode("overwrite").parquet(quarantine_date_path)
        print(f"  Quarantine Parquet written to {quarantine_date_path}")

    # Step 4: Deduplicate clean candidates on (sensor_id, event_time)
    curated_df, duplicates_removed = deduplicate_readings(clean_candidates_df)
    curated_count = curated_df.count()
    print(f"  Duplicates removed: {duplicates_removed:,}")
    print(f"  Clean curated rows: {curated_count:,}")

    # Step 5: Write curated Parquet partitioned by building_id and date
    curated_df.write.mode("overwrite").partitionBy("building_id", "date").parquet(curated_path)
    print(f"  Curated Parquet written to {curated_path}")

    # Step 6: Compute & write gold aggregations
    gold_min_df = aggregate_gold_minute(curated_df)
    gold_min_count = gold_min_df.count()
    gold_min_df.write.mode("overwrite").partitionBy("building_id", "date").parquet(gold_minute_path)
    print(f"  Gold 1-minute Parquet written ({gold_min_count:,} rows)")

    gold_hr_df = aggregate_gold_hourly(curated_df)
    gold_hr_count = gold_hr_df.count()
    gold_hr_df.write.mode("overwrite").partitionBy("building_id", "date").parquet(gold_hourly_path)
    print(f"  Gold 1-hour Parquet written ({gold_hr_count:,} rows)")

    duration_sec = round(time.time() - start_time, 2)

    # Step 7: Record metrics summary
    metrics_summary = {
        "run_date": run_date,
        "rows_read": rows_read,
        "rows_clean_curated": curated_count,
        "rows_quarantined": quarantine_count,
        "duplicates_removed": duplicates_removed,
        "gold_minute_rows": gold_min_count,
        "gold_hourly_rows": gold_hr_count,
        "reconciliation_check": (
            rows_read == (curated_count + quarantine_count + duplicates_removed)
        ),
        "execution_duration_seconds": duration_sec,
        "timestamp_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    # Write metrics JSON
    metrics_date_dir = Path(metrics_path) / f"run_date={run_date}"
    metrics_date_dir.mkdir(parents=True, exist_ok=True)
    metrics_file = metrics_date_dir / "summary.json"
    with open(metrics_file, "w", encoding="utf-8") as f:
        json.dump(metrics_summary, f, indent=2)

    print(f"ETL completed in {duration_sec}s. Metrics saved to {metrics_file}")
    return metrics_summary


def parse_args():
    parser = argparse.ArgumentParser(description="AWS Glue PySpark Lakehouse ETL Job")
    parser.add_argument(
        "--run_date",
        type=str,
        required=True,
        help="Date to process in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default="",
        help="Optional S3 bucket name (derives paths automatically if provided)",
    )
    parser.add_argument("--raw_path", type=str, default="", help="Path to raw CSV dataset")
    parser.add_argument(
        "--curated_path",
        type=str,
        default="",
        help="Output path for curated Parquet",
    )
    parser.add_argument(
        "--gold_minute_path",
        type=str,
        default="",
        help="Output path for gold minute Parquet",
    )
    parser.add_argument(
        "--gold_hourly_path",
        type=str,
        default="",
        help="Output path for gold hourly Parquet",
    )
    parser.add_argument(
        "--quarantine_path",
        type=str,
        default="",
        help="Output path for quarantine Parquet",
    )
    parser.add_argument(
        "--metrics_path",
        type=str,
        default="",
        help="Output path for run metrics JSON",
    )

    args, _ = parser.parse_known_args()

    # If bucket is passed (e.g. on AWS Glue), default to S3 prefixes
    if args.bucket:
        b = args.bucket.strip("/")
        if not args.raw_path:
            args.raw_path = f"s3://{b}/raw"
        if not args.curated_path:
            args.curated_path = f"s3://{b}/curated/readings"
        if not args.gold_minute_path:
            args.gold_minute_path = f"s3://{b}/gold/minute"
        if not args.gold_hourly_path:
            args.gold_hourly_path = f"s3://{b}/gold/hourly"
        if not args.quarantine_path:
            args.quarantine_path = f"s3://{b}/quarantine"
        if not args.metrics_path:
            args.metrics_path = f"s3://{b}/metrics"
    else:
        # Default to local directory paths
        base = "./data"
        if not args.raw_path:
            args.raw_path = f"{base}/raw"
        if not args.curated_path:
            args.curated_path = f"{base}/curated/readings"
        if not args.gold_minute_path:
            args.gold_minute_path = f"{base}/gold/minute"
        if not args.gold_hourly_path:
            args.gold_hourly_path = f"{base}/gold/hourly"
        if not args.quarantine_path:
            args.quarantine_path = f"{base}/quarantine"
        if not args.metrics_path:
            args.metrics_path = f"{base}/metrics"

    return args


def main():
    args = parse_args()
    summary = run_pipeline(
        run_date=args.run_date,
        raw_path=args.raw_path,
        curated_path=args.curated_path,
        gold_minute_path=args.gold_minute_path,
        gold_hourly_path=args.gold_hourly_path,
        quarantine_path=args.quarantine_path,
        metrics_path=args.metrics_path,
    )
    print("Run Summary:")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
