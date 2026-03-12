# File: libs/windows/adaptive.py
"""Adaptive window close/open logic."""

from __future__ import annotations

import pandas as pd

from libs.perf.annotations import hot_path
from libs.windows.stream import StreamWindowConfig, WindowStream
from libs.windows.window import WindowPolicy


def max_window_ms_from_min_sampling_rate(min_sampling_rate_hz: float) -> int:
    return WindowPolicy.max_ms_from_min_sampling_rate(min_sampling_rate_hz)


def should_close_window(duration_ms: int, event_count: int, max_ms: int, event_threshold: int) -> bool:
    return WindowPolicy(
        max_ms=int(max_ms),
        event_threshold=int(event_threshold),
        min_ms=50,
        inactivity_timeout_ms=0,
    ).should_close(duration_ms=duration_ms, event_count=event_count)


def close_reason_for_thresholds(duration_ms: int, event_count: int, max_ms: int, event_threshold: int) -> str:
    return WindowPolicy(
        max_ms=int(max_ms),
        event_threshold=int(event_threshold),
        min_ms=50,
        inactivity_timeout_ms=0,
    ).close_reason(duration_ms=duration_ms, event_count=event_count)


@hot_path
def build_adaptive_windows(
    events_df: "DataFrame",
    max_ms: int,
    event_threshold: int,
    min_ms: int,
) -> "DataFrame":
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    events_with_ms = (
        events_df.withColumn("timestamp_ms", F.unix_millis(F.col("timestamp_utc")))
        .withColumn("bucket_start_ms", (F.floor(F.col("timestamp_ms") / F.lit(max_ms)) * F.lit(max_ms)).cast("long"))
        .withColumn("bucket_ts", F.timestamp_millis(F.col("bucket_start_ms")))
    )

    order_window = Window.partitionBy("tail_id", "flight_id", "bucket_start_ms").orderBy("timestamp_utc")
    segmented = events_with_ms.withColumn("rn", F.row_number().over(order_window)).withColumn(
        "sub_bucket", F.floor((F.col("rn") - F.lit(1)) / F.lit(max(event_threshold, 1))).cast("long")
    )

    grouped = (
        segmented.groupBy("tail_id", "flight_id", "date_utc", "bucket_start_ms", "sub_bucket")
        .agg(
            F.min("timestamp_utc").alias("t_start"),
            F.max("timestamp_utc").alias("t_end"),
            F.count(F.lit(1)).alias("event_count"),
        )
        .withColumn("raw_duration_ms", F.unix_millis("t_end") - F.unix_millis("t_start"))
        .withColumn("duration_ms", F.greatest(F.unix_millis("t_end") - F.unix_millis("t_start"), F.lit(min_ms)))
        .withColumn(
            "close_reason",
            F.when(
                (F.col("raw_duration_ms") >= F.lit(max_ms)) & (F.col("event_count") >= F.lit(max(event_threshold, 1))),
                F.lit("event_threshold+max_ms"),
            )
            .when(F.col("event_count") >= F.lit(max(event_threshold, 1)), F.lit("event_threshold"))
            .otherwise(F.lit("max_ms")),
        )
    )

    win_order = Window.partitionBy("tail_id", "flight_id").orderBy("t_start", "sub_bucket")
    return grouped.withColumn("win_id", F.row_number().over(win_order).cast("long")).select(
        "tail_id",
        "flight_id",
        "win_id",
        "t_start",
        "t_end",
        "duration_ms",
        "event_count",
        "close_reason",
        F.lit(1).cast("int").alias("zoh_version"),
        "date_utc",
    )


def build_adaptive_windows_stream_parity(
    events_df: "DataFrame",
    max_ms: int,
    event_threshold: int,
    min_ms: int,
    inactivity_timeout_ms: int = 0,
) -> "DataFrame":
    from pyspark.sql import functions as F
    from libs.io.schemas import WINDOWS_COLUMNS, WINDOWS_SCHEMA

    schema = WINDOWS_SCHEMA

    base = (
        events_df.select("tail_id", "flight_id", "timestamp_utc", "date_utc")
        .where(F.col("tail_id").isNotNull() & F.col("flight_id").isNotNull() & F.col("timestamp_utc").isNotNull())
    )

    def _emit_windows(pdf: "pd.DataFrame") -> "pd.DataFrame":
        if pdf.empty:
            return pd.DataFrame(columns=WINDOWS_COLUMNS)
        first = pdf.iloc[0]
        events = [
            {
                "tail_id": str(row["tail_id"]),
                "flight_id": str(row["flight_id"]),
                "timestamp_utc": row["timestamp_utc"],
                "event_type_detected": "window_event",
                "parameter_name": "",
                "payload": {},
            }
            for row in pdf.sort_values(by=["timestamp_utc"], kind="mergesort").to_dict(orient="records")
            if not pd.isna(row.get("timestamp_utc"))
        ]
        config = StreamWindowConfig(
            max_ms=int(max_ms),
            min_ms=int(min_ms),
            event_threshold=int(event_threshold),
            inactivity_timeout_ms=int(inactivity_timeout_ms),
            include_window_events=False,
        )
        stream = WindowStream(config=config)
        rows = list(stream.iter_windows(events))
        for row in rows:
            row.pop("sensor_count", None)
            row.pop("event_type_counts", None)
            row.pop("zoh_snapshot", None)
        if not rows:
            return pd.DataFrame(columns=WINDOWS_COLUMNS)
        return pd.DataFrame(rows, columns=WINDOWS_COLUMNS)

    return base.groupBy("tail_id", "flight_id").applyInPandas(_emit_windows, schema=schema)

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from datetime import datetime
    import pandas as pd
