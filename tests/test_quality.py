"""Unit tests for the 5 data quality checks in glue/quality.py."""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from glue.quality import evaluate_quality_checks

TYPED_SCHEMA = StructType(
    [
        StructField("event_time", TimestampType(), True),
        StructField("building_id", StringType(), True),
        StructField("floor", IntegerType(), True),
        StructField("sensor_id", StringType(), True),
        StructField("temperature_c", DoubleType(), True),
        StructField("energy_kwh", DoubleType(), True),
        StructField("co2_ppm", DoubleType(), True),
        StructField("occupancy_count", IntegerType(), True),
    ]
)


def test_clean_row_passes(spark: SparkSession):
    """Verify valid row passes all checks with CLEAN status."""
    data = [
        (
            datetime(2026, 10, 1, 10, 0, 0),
            "B01",
            2,
            "S0015",
            22.5,
            1.25,
            550.0,
            15,
        )
    ]
    df = spark.createDataFrame(data, schema=TYPED_SCHEMA)
    clean_df, quarantine_df = evaluate_quality_checks(df, "2026-10-01")

    assert clean_df.count() == 1
    assert quarantine_df.count() == 0


def test_check_2_null_values(spark: SparkSession):
    """Check 2: Nulls in any primary column are caught and quarantined."""
    data = [
        # Null temperature
        (
            datetime(2026, 10, 1, 10, 0, 0),
            "B01",
            2,
            "S0015",
            None,
            1.25,
            550.0,
            15,
        ),
        # Null sensor_id
        (
            datetime(2026, 10, 1, 10, 0, 10),
            "B01",
            2,
            None,
            22.5,
            1.25,
            550.0,
            15,
        ),
        # Null event_time
        (
            None,
            "B01",
            2,
            "S0015",
            22.5,
            1.25,
            550.0,
            15,
        ),
    ]
    df = spark.createDataFrame(data, schema=TYPED_SCHEMA)
    clean_df, quarantine_df = evaluate_quality_checks(df, "2026-10-01")

    assert clean_df.count() == 0
    assert quarantine_df.count() == 3

    reasons = [row["failure_reason"] for row in quarantine_df.collect()]
    assert all("NULL_VALUE" in r for r in reasons)


def test_check_3_out_of_range(spark: SparkSession):
    """Check 3: Values outside physical limits are caught."""
    data = [
        # Temp too high (95 C)
        (
            datetime(2026, 10, 1, 10, 0, 0),
            "B01",
            1,
            "S0001",
            95.0,
            1.0,
            500.0,
            10,
        ),
        # Temp too low (-25 C)
        (
            datetime(2026, 10, 1, 10, 0, 10),
            "B01",
            1,
            "S0001",
            -25.0,
            1.0,
            500.0,
            10,
        ),
        # Negative energy
        (
            datetime(2026, 10, 1, 10, 0, 20),
            "B01",
            1,
            "S0001",
            22.0,
            -2.5,
            500.0,
            10,
        ),
        # CO2 extreme (9999 ppm)
        (
            datetime(2026, 10, 1, 10, 0, 30),
            "B01",
            1,
            "S0001",
            22.0,
            1.0,
            9999.0,
            10,
        ),
        # Negative occupancy
        (
            datetime(2026, 10, 1, 10, 0, 40),
            "B01",
            1,
            "S0001",
            22.0,
            1.0,
            500.0,
            -5,
        ),
    ]
    df = spark.createDataFrame(data, schema=TYPED_SCHEMA)
    clean_df, quarantine_df = evaluate_quality_checks(df, "2026-10-01")

    assert clean_df.count() == 0
    assert quarantine_df.count() == 5
    reasons = [row["failure_reason"] for row in quarantine_df.collect()]
    assert all("OUT_OF_RANGE" in r for r in reasons)


def test_check_5_bad_timestamps(spark: SparkSession):
    """Check 5: Timestamps outside partition date or future dates are caught."""
    data = [
        # Future year (2099)
        (
            datetime(2099, 12, 31, 23, 59, 59),
            "B01",
            1,
            "S0001",
            22.0,
            1.0,
            500.0,
            10,
        ),
        # Previous month (2026-09-15)
        (
            datetime(2026, 9, 15, 12, 0, 0),
            "B01",
            1,
            "S0001",
            22.0,
            1.0,
            500.0,
            10,
        ),
    ]
    df = spark.createDataFrame(data, schema=TYPED_SCHEMA)
    clean_df, quarantine_df = evaluate_quality_checks(df, "2026-10-01")

    assert clean_df.count() == 0
    assert quarantine_df.count() == 2
    reasons = [row["failure_reason"] for row in quarantine_df.collect()]
    assert all("BAD_TIMESTAMP" in r for r in reasons)
