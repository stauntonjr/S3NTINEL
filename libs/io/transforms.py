# File: libs/io/transforms.py
"""Spark DataFrame transforms for core table normalization."""

from __future__ import annotations


def normalize_raw_telemetry(raw_df: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    if "timestamp_utc" in raw_df.columns and "timestamp" in raw_df.columns:
        timestamp_col = F.coalesce(F.col("timestamp_utc"), F.col("timestamp"))
    elif "timestamp_utc" in raw_df.columns:
        timestamp_col = F.col("timestamp_utc")
    elif "timestamp" in raw_df.columns:
        timestamp_col = F.col("timestamp")
    else:
        raise ValueError("raw telemetry input must include 'timestamp_utc' or 'timestamp' column")

    value_text = F.trim(F.col("parameter_value").cast("string"))

    normalized = (
        raw_df.select(
            F.col("tail_id").cast("string").alias("tail_id"),
            F.col("flight_id").cast("string").alias("flight_id"),
            timestamp_col.cast("timestamp").alias("timestamp_utc"),
            F.col("parameter_name").cast("string").alias("parameter_name"),
            value_text.alias("parameter_value"),
        )
        .where(F.col("tail_id").isNotNull() & F.col("flight_id").isNotNull())
        .where(F.col("timestamp_utc").isNotNull() & F.col("parameter_name").isNotNull())
        .withColumn("sensor", F.col("parameter_name"))
        .withColumn("val", F.expr("try_cast(parameter_value as double)"))
        
        .withColumn("unit", F.lit(None).cast("string"))
        .withColumn("rate_hz", F.lit(None).cast("double"))
        .withColumn("meta", F.expr("cast(map() as map<string,string>)"))
        .withColumn("date_utc", F.to_date(F.col("timestamp_utc")))
    )
    return normalized

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
