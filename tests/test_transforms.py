"""Unit tests for PySpark transforms: cast, dedupe, and aggregations."""

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

from glue.transforms import (
    RAW_SCHEMA,
    aggregate_gold_hourly,
    aggregate_gold_minute,
    cast_raw_telemetry,
    deduplicate_readings,
)


def test_cast_raw_telemetry(spark: SparkSession):
    """Verify raw strings are cast to typed columns with date partition."""
    raw_data = [
        (
            "2026-10-01T08:30:00Z",
            "B01",
            "3",
            "S0025",
            "22.45",
            "1.234",
            "520.5",
            "15",
        )
    ]
    raw_df = spark.createDataFrame(raw_data, schema=RAW_SCHEMA)
    typed_df = cast_raw_telemetry(raw_df)

    assert isinstance(typed_df.schema["event_time"].dataType, TimestampType)
    assert isinstance(typed_df.schema["floor"].dataType, IntegerType)
    assert isinstance(typed_df.schema["temperature_c"].dataType, DoubleType)
    assert isinstance(typed_df.schema["energy_kwh"].dataType, DoubleType)
    assert isinstance(typed_df.schema["co2_ppm"].dataType, DoubleType)
    assert isinstance(typed_df.schema["occupancy_count"].dataType, IntegerType)

    row = typed_df.collect()[0]
    assert row["floor"] == 3
    assert row["temperature_c"] == 22.45
    assert str(row["date"]) == "2026-10-01"


def test_deduplicate_readings(spark: SparkSession):
    """Check 4: Verify deduplication removes exact copies and keeps one."""
    schema = StructType(
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

    dt1 = datetime(2026, 10, 1, 9, 0, 0)
    dt2 = datetime(2026, 10, 1, 9, 0, 10)

    # 4 rows: row 1 duplicated 3 times, row 2 once
    data = [
        (dt1, "B01", 1, "S0001", 21.0, 1.0, 450.0, 5),
        (dt1, "B01", 1, "S0001", 21.0, 1.0, 450.0, 5),
        (dt1, "B01", 1, "S0001", 21.0, 1.0, 450.0, 5),
        (dt2, "B01", 1, "S0001", 21.1, 1.1, 455.0, 6),
    ]

    df = spark.createDataFrame(data, schema=schema)
    deduped_df, duplicates_removed = deduplicate_readings(df)

    assert duplicates_removed == 2
    assert deduped_df.count() == 2


def test_aggregate_gold_minute_hand_computed(spark: SparkSession):
    """Verify 1-minute aggregation matches hand-computed expected values."""
    schema = StructType(
        [
            StructField("event_time", TimestampType(), True),
            StructField("building_id", StringType(), True),
            StructField("date", StringType(), True),
            StructField("temperature_c", DoubleType(), True),
            StructField("energy_kwh", DoubleType(), True),
            StructField("co2_ppm", DoubleType(), True),
            StructField("occupancy_count", IntegerType(), True),
        ]
    )

    # 3 readings in minute 10:00, 1 reading in minute 10:01
    data = [
        (datetime(2026, 10, 1, 10, 0, 0), "B01", "2026-10-01", 20.0, 1.0, 400.0, 10),
        (
            datetime(2026, 10, 1, 10, 0, 20),
            "B01",
            "2026-10-01",
            22.0,
            2.0,
            500.0,
            20,
        ),
        (
            datetime(2026, 10, 1, 10, 0, 40),
            "B01",
            "2026-10-01",
            24.0,
            3.0,
            600.0,
            30,
        ),
        (
            datetime(2026, 10, 1, 10, 1, 10),
            "B01",
            "2026-10-01",
            25.0,
            4.0,
            700.0,
            40,
        ),
    ]

    df = spark.createDataFrame(data, schema=schema)
    gold_min_df = aggregate_gold_minute(df)

    rows = {row["window_start"].minute: row for row in gold_min_df.collect()}

    # Minute 0: avg temp = (20+22+24)/3 = 22.0, sum energy = 1+2+3 = 6.0, avg co2 = 500.0, avg occ = 20.0, count = 3
    m0 = rows[0]
    assert m0["avg_temperature_c"] == 22.0
    assert m0["sum_energy_kwh"] == 6.0
    assert m0["avg_co2_ppm"] == 500.0
    assert m0["avg_occupancy_count"] == 20.0
    assert m0["reading_count"] == 3

    # Minute 1: count = 1
    m1 = rows[1]
    assert m1["reading_count"] == 1
    assert m1["sum_energy_kwh"] == 4.0


def test_aggregate_gold_hourly_hand_computed(spark: SparkSession):
    """Verify 1-hour aggregation matches hand-computed expected values."""
    schema = StructType(
        [
            StructField("event_time", TimestampType(), True),
            StructField("building_id", StringType(), True),
            StructField("date", StringType(), True),
            StructField("temperature_c", DoubleType(), True),
            StructField("energy_kwh", DoubleType(), True),
            StructField("co2_ppm", DoubleType(), True),
            StructField("occupancy_count", IntegerType(), True),
        ]
    )

    data = [
        (datetime(2026, 10, 1, 10, 15, 0), "B01", "2026-10-01", 21.0, 2.0, 450.0, 10),
        (datetime(2026, 10, 1, 10, 45, 0), "B01", "2026-10-01", 23.0, 3.0, 550.0, 20),
    ]

    df = spark.createDataFrame(data, schema=schema)
    gold_hr_df = aggregate_gold_hourly(df)
    results = gold_hr_df.collect()

    assert len(results) == 1
    row = results[0]
    assert row["reading_count"] == 2
    assert row["avg_temperature_c"] == 22.0
    assert row["sum_energy_kwh"] == 5.0
    assert row["avg_co2_ppm"] == 500.0
    assert row["avg_occupancy_count"] == 15.0
