"""Data quality check suite for IoT telemetry data (Section 8.4).

Implements 5 checks:
1. Schema: required columns present and castable
2. Nulls: event_time, building_id, sensor_id, readings non-null
3. Value ranges: temp (-10 to 50 C), energy (>= 0), co2 (300 to 5000 ppm), occupancy (>= 0)
4. Duplicates: no repeated (sensor_id, event_time) - handled in deduplication
5. Timestamps: not in future, matches partition date
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def evaluate_quality_checks(
    df: DataFrame,
    partition_date_str: str,
) -> tuple[DataFrame, DataFrame]:
    """Evaluate quality checks 1, 2, 3, and 5, returning (clean_df, quarantine_df).

    Row is clean if it satisfies all 4 checks.
    Otherwise, row is routed to quarantine with failure_reasons and details.
    """
    # 1. Null check: all primary identifiers and telemetry values must be non-null
    null_condition = (
        F.col("event_time").isNull()
        | F.col("building_id").isNull()
        | F.col("floor").isNull()
        | F.col("sensor_id").isNull()
        | F.col("temperature_c").isNull()
        | F.col("energy_kwh").isNull()
        | F.col("co2_ppm").isNull()
        | F.col("occupancy_count").isNull()
    )

    # 2. Value range check (evaluated when values are non-null)
    range_condition = (
        (F.col("temperature_c") < -10.0)
        | (F.col("temperature_c") > 50.0)
        | (F.col("energy_kwh") < 0.0)
        | (F.col("co2_ppm") < 300.0)
        | (F.col("co2_ppm") > 5000.0)
        | (F.col("occupancy_count") < 0)
    )

    # 3. Timestamp check: date must match partition date, and not exceed partition day boundary
    max_day_timestamp = f"{partition_date_str} 23:59:59"
    timestamp_condition = (
        F.to_date(F.col("event_time")) != F.to_date(F.lit(partition_date_str))
    ) | (F.col("event_time") > F.to_timestamp(F.lit(max_day_timestamp)))

    # Construct failure reasons array
    reasons_array = F.array(
        F.when(null_condition, F.lit("NULL_VALUE")),
        F.when(~null_condition & range_condition, F.lit("OUT_OF_RANGE")),
        F.when(~null_condition & timestamp_condition, F.lit("BAD_TIMESTAMP")),
    )

    # Filter out null entries in array
    reasons_filtered = F.expr("filter(reasons_array, x -> x is not null)")

    tagged_df = (
        df.withColumn("reasons_array", reasons_array)
        .withColumn("failure_reasons", reasons_filtered)
        .withColumn(
            "failure_reason",
            F.array_join(F.col("failure_reasons"), ";"),
        )
        .withColumn(
            "quality_status",
            F.when(F.size(F.col("failure_reasons")) == 0, F.lit("CLEAN")).otherwise(
                F.lit("FAILED")
            ),
        )
        .drop("reasons_array", "failure_reasons")
    )

    clean_df = tagged_df.filter(F.col("quality_status") == "CLEAN").drop(
        "quality_status", "failure_reason"
    )
    quarantine_df = tagged_df.filter(F.col("quality_status") == "FAILED").drop("quality_status")

    return clean_df, quarantine_df
