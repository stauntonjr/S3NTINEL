"""Window diagnostics helpers for smoke and strategy comparison workflows."""

from __future__ import annotations

from typing import TYPE_CHECKING

from libs.io.delta import read_table

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def compute_window_diagnostics(
    spark: "SparkSession",
    table_format: str,
    windows_path: str,
    events_path: str,
) -> dict[str, object]:
    from pyspark.sql import functions as F

    windows_all_df = read_table(spark, path=windows_path, fmt=table_format)
    selected_columns = ["tail_id", "flight_id", "win_id", "t_start", "t_end", "duration_ms", "event_count"]
    if "close_reason" in windows_all_df.columns:
        selected_columns.append("close_reason")
    windows_df = windows_all_df.select(*selected_columns)

    if windows_df.rdd.isEmpty():
        return {
            "window_count": 0,
            "duration_ms_avg": None,
            "duration_ms_p50": None,
            "duration_ms_p95": None,
            "event_count_avg": None,
            "event_count_p50": None,
            "event_count_p95": None,
            "sensor_count_avg": None,
            "sensor_count_p50": None,
            "sensor_count_p95": None,
            "close_reason_counts": {},
        }

    events_df = read_table(spark, path=events_path, fmt=table_format).select(
        "tail_id",
        "flight_id",
        "parameter_name",
        "timestamp_utc",
    )

    sensor_counts = (
        windows_df.alias("w")
        .join(
            events_df.alias("e"),
            on=(
                (F.col("e.tail_id") == F.col("w.tail_id"))
                & (F.col("e.flight_id") == F.col("w.flight_id"))
                & (F.col("e.timestamp_utc") >= F.col("w.t_start"))
                & (F.col("e.timestamp_utc") <= F.col("w.t_end"))
            ),
            how="left",
        )
        .groupBy(
            F.col("w.tail_id").alias("tail_id"),
            F.col("w.flight_id").alias("flight_id"),
            F.col("w.win_id").alias("win_id"),
        )
        .agg(F.countDistinct(F.col("e.parameter_name")).cast("double").alias("sensor_count"))
    )

    enriched = (
        windows_df.alias("w")
        .join(sensor_counts.alias("s"), on=["tail_id", "flight_id", "win_id"], how="left")
        .withColumn("sensor_count", F.coalesce(F.col("sensor_count"), F.lit(0.0)))
        .select(
            F.col("duration_ms").cast("double").alias("duration_ms"),
            F.col("event_count").cast("double").alias("event_count"),
            F.col("sensor_count").cast("double").alias("sensor_count"),
        )
    )

    metrics_row = enriched.agg(
        F.count(F.lit(1)).cast("long").alias("window_count"),
        F.avg("duration_ms").alias("duration_ms_avg"),
        F.percentile_approx("duration_ms", F.lit(0.5), 1000).alias("duration_ms_p50"),
        F.percentile_approx("duration_ms", F.lit(0.95), 1000).alias("duration_ms_p95"),
        F.avg("event_count").alias("event_count_avg"),
        F.percentile_approx("event_count", F.lit(0.5), 1000).alias("event_count_p50"),
        F.percentile_approx("event_count", F.lit(0.95), 1000).alias("event_count_p95"),
        F.avg("sensor_count").alias("sensor_count_avg"),
        F.percentile_approx("sensor_count", F.lit(0.5), 1000).alias("sensor_count_p50"),
        F.percentile_approx("sensor_count", F.lit(0.95), 1000).alias("sensor_count_p95"),
    ).first()

    out: dict[str, object] = {}
    for key in metrics_row.asDict().keys():
        value = metrics_row[key]
        if value is None:
            out[key] = None
        elif key == "window_count":
            out[key] = int(value)
        else:
            out[key] = float(value)

    reason_counts: dict[str, int] = {}
    if "close_reason" in windows_df.columns:
        for row in windows_df.groupBy("close_reason").count().collect():
            reason = str(row["close_reason"]) if row["close_reason"] is not None else "unknown"
            reason_counts[reason] = int(row["count"])
    out["close_reason_counts"] = reason_counts
    return out


def close_reason_tv_distance(
    bucketed_counts: dict[str, int],
    parity_counts: dict[str, int],
) -> float:
    bucketed_total = max(sum(int(v) for v in bucketed_counts.values()), 1)
    parity_total = max(sum(int(v) for v in parity_counts.values()), 1)
    keys = set(bucketed_counts.keys()) | set(parity_counts.keys())

    distance_sum = 0.0
    for key in keys:
        p = float(bucketed_counts.get(key, 0)) / float(bucketed_total)
        q = float(parity_counts.get(key, 0)) / float(parity_total)
        distance_sum += abs(p - q)
    return 0.5 * distance_sum


def compute_numeric_deltas(
    baseline: dict[str, object],
    candidate: dict[str, object],
) -> dict[str, float | int | None]:
    delta_keys = set(baseline.keys()) | set(candidate.keys())
    deltas: dict[str, float | int | None] = {}
    for key in sorted(delta_keys):
        left = baseline.get(key)
        right = candidate.get(key)
        if isinstance(left, (int, float)) and isinstance(right, (int, float)):
            deltas[key] = float(right) - float(left)
        else:
            deltas[key] = None
    return deltas
