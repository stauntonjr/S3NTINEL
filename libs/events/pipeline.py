"""Shared Spark event-table builder for canonical normalized raw telemetry."""

from __future__ import annotations


def build_events_table(
    raw_df: "DataFrame",
    *,
    delta_threshold: float = 0.0,
    slope_source: str = "ema",
    ema_alpha: float = 0.2,
) -> "DataFrame":
    from pyspark.sql import functions as F

    from libs.events.categorical import build_categorical_events
    from libs.events.extrema import build_continuous_events

    required_columns = {
        "tail_id",
        "flight_id",
        "timestamp_utc",
        "parameter_name",
        "parameter_value",
        "sensor",
        "val",
        "date_utc",
    }
    missing_columns = required_columns.difference(raw_df.columns)
    if missing_columns:
        missing_list = ", ".join(sorted(missing_columns))
        raise ValueError(
            "build_events_table expects canonical normalized raw telemetry; "
            f"missing columns: {missing_list}"
        )

    continuous_events = build_continuous_events(
        raw_df,
        delta_threshold=delta_threshold,
        slope_source=slope_source,
        ema_alpha=ema_alpha,
    )
    categorical_events = build_categorical_events(raw_df)
    events_df = continuous_events.unionByName(categorical_events)
    if "sensor" in events_df.columns and "parameter_name" not in events_df.columns:
        events_df = events_df.withColumnRenamed("sensor", "parameter_name")
    if "ts" in events_df.columns and "timestamp_utc" not in events_df.columns:
        events_df = events_df.withColumnRenamed("ts", "timestamp_utc")
    if "anomaly_type_detected" not in events_df.columns:
        events_df = events_df.withColumn("anomaly_type_detected", F.lit(None).cast("string"))
    if "anomaly_score_detected" not in events_df.columns:
        events_df = events_df.withColumn("anomaly_score_detected", F.lit(None).cast("double"))
    return events_df


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
