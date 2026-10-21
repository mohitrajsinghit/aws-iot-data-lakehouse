"""Reconciliation and idempotency integration tests (Section 9)."""

import json
from pathlib import Path

from pyspark.sql import SparkSession

from generator.config import GeneratorConfig
from generator.generate import generate_day
from glue.job import run_pipeline


def test_end_to_end_reconciliation_and_idempotency(spark: SparkSession, tmp_path: Path):
    """Verify reconciliation accounting identity and idempotency."""
    # Scaled down test dataset
    cfg = GeneratorConfig(
        buildings=["B01", "B02"],
        sensors_per_building=10,
        floors_per_building=2,
        interval_seconds=60,
        seed=100,
    )

    data_dir = tmp_path / "recon_data"
    raw_path = str(data_dir / "raw")
    curated_path = str(data_dir / "curated" / "readings")
    gold_minute_path = str(data_dir / "gold" / "minute")
    gold_hourly_path = str(data_dir / "gold" / "hourly")
    quarantine_path = str(data_dir / "quarantine")
    metrics_path = str(data_dir / "metrics")

    # 1. Generate data
    manifest = generate_day("2026-10-01", cfg, out_dir=str(data_dir))
    expected_defects = manifest["expected_defects"]

    # 2. Run ETL pipeline (First run)
    summary_1 = run_pipeline(
        run_date="2026-10-01",
        raw_path=raw_path,
        curated_path=curated_path,
        gold_minute_path=gold_minute_path,
        gold_hourly_path=gold_hourly_path,
        quarantine_path=quarantine_path,
        metrics_path=metrics_path,
        spark=spark,
    )

    # 3. Verify Reconciliation Identity:
    # rows_read = rows_clean_curated + rows_quarantined + duplicates_removed
    assert summary_1["reconciliation_check"] is True
    assert (
        summary_1["rows_read"]
        == summary_1["rows_clean_curated"]
        + summary_1["rows_quarantined"]
        + summary_1["duplicates_removed"]
    )

    # Verify quarantine counts match ground truth
    expected_quarantined = (
        expected_defects["null_values"]
        + expected_defects["out_of_range"]
        + expected_defects["bad_timestamps"]
    )
    assert summary_1["rows_quarantined"] == expected_quarantined
    assert summary_1["duplicates_removed"] == expected_defects["exact_duplicates"]
    assert summary_1["rows_clean_curated"] == manifest["clean_rows"]

    # 4. Verify Idempotency: Re-running for same date yields identical outputs
    summary_2 = run_pipeline(
        run_date="2026-10-01",
        raw_path=raw_path,
        curated_path=curated_path,
        gold_minute_path=gold_minute_path,
        gold_hourly_path=gold_hourly_path,
        quarantine_path=quarantine_path,
        metrics_path=metrics_path,
        spark=spark,
    )

    assert summary_2["rows_clean_curated"] == summary_1["rows_clean_curated"]
    assert summary_2["rows_quarantined"] == summary_1["rows_quarantined"]
    assert summary_2["duplicates_removed"] == summary_1["duplicates_removed"]
    assert summary_2["gold_minute_rows"] == summary_1["gold_minute_rows"]
    assert summary_2["gold_hourly_rows"] == summary_1["gold_hourly_rows"]

    # Check metrics JSON persisted on disk
    metrics_file = Path(metrics_path) / "run_date=2026-10-01" / "summary.json"
    assert metrics_file.exists()
    with open(metrics_file, encoding="utf-8") as f:
        disk_metrics = json.load(f)
    assert disk_metrics["rows_clean_curated"] == summary_1["rows_clean_curated"]
