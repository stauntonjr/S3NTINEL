# File: libs/windows/adaptive.py
"""Adaptive window close/open logic."""

from __future__ import annotations

from datetime import date

from libs.perf.annotations import hot_path

@hot_path
def _window_date_from_start(start_ts: "datetime") -> date:
    return start_ts.date()


@hot_path
def should_close_window(duration_ms: int, event_count: int, max_ms: int, event_threshold: int) -> bool:
    # HOT PATH: evaluated for each sample/event tick; keep logic branch-light and deterministic.
    return duration_ms >= max_ms or event_count >= event_threshold


def close_reason_for_thresholds(duration_ms: int, event_count: int, max_ms: int, event_threshold: int) -> str:
    by_duration = duration_ms >= int(max_ms)
    by_count = event_count >= int(event_threshold)
    if by_duration and by_count:
        return "event_threshold+max_ms"
    if by_count:
        return "event_threshold"
    return "max_ms"


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
        events_df.withColumn("ts_ms", F.unix_millis(F.col("ts")))
        .withColumn("bucket_start_ms", (F.floor(F.col("ts_ms") / F.lit(max_ms)) * F.lit(max_ms)).cast("long"))
        .withColumn("bucket_ts", F.timestamp_millis(F.col("bucket_start_ms")))
    )

    order_window = Window.partitionBy("tail_id", "flight_id", "bucket_start_ms").orderBy("ts")
    segmented = events_with_ms.withColumn("rn", F.row_number().over(order_window)).withColumn(
        "sub_bucket", F.floor((F.col("rn") - F.lit(1)) / F.lit(max(event_threshold, 1))).cast("long")
    )

    grouped = (
        segmented.groupBy("tail_id", "flight_id", "date_utc", "bucket_start_ms", "sub_bucket")
        .agg(
            F.min("ts").alias("t_start"),
            F.max("ts").alias("t_end"),
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


@hot_path
def build_adaptive_windows_stream_parity(
    events_df: "DataFrame",
    max_ms: int,
    event_threshold: int,
    min_ms: int,
    inactivity_timeout_ms: int = 0,
) -> "DataFrame":
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    schema = T.StructType(
        [
            T.StructField("tail_id", T.StringType(), nullable=False),
            T.StructField("flight_id", T.StringType(), nullable=False),
            T.StructField("win_id", T.LongType(), nullable=False),
            T.StructField("t_start", T.TimestampType(), nullable=False),
            T.StructField("t_end", T.TimestampType(), nullable=False),
            T.StructField("duration_ms", T.LongType(), nullable=False),
            T.StructField("event_count", T.LongType(), nullable=False),
            T.StructField("close_reason", T.StringType(), nullable=False),
            T.StructField("zoh_version", T.IntegerType(), nullable=False),
            T.StructField("date_utc", T.DateType(), nullable=False),
        ]
    )

    base = (
        events_df.select("tail_id", "flight_id", "ts", "date_utc")
        .where(F.col("tail_id").isNotNull() & F.col("flight_id").isNotNull() & F.col("ts").isNotNull())
    )

    def _emit_windows(pdf: "pd.DataFrame") -> "pd.DataFrame":
        import pandas as pd

        if pdf.empty:
            return pd.DataFrame(
                columns=[
                    "tail_id",
                    "flight_id",
                    "win_id",
                    "t_start",
                    "t_end",
                    "duration_ms",
                    "event_count",
                    "close_reason",
                    "zoh_version",
                    "date_utc",
                ]
            )

        ordered = pdf.sort_values(by=["ts"], kind="mergesort")
        rows: list[dict[str, object]] = []

        first = ordered.iloc[0]
        tail_id = str(first["tail_id"])
        flight_id = str(first["flight_id"])

        window_start = None
        window_end = None
        event_count_current = 0
        win_id = 1
        timeout_ms = max(int(inactivity_timeout_ms), 0)

        for _, row in ordered.iterrows():
            ts = row["ts"]
            if pd.isna(ts):
                continue

            if window_start is None:
                window_start = ts
                window_end = ts
                event_count_current = 0
            elif timeout_ms > 0:
                inactivity_gap_ms = int((ts - window_end).total_seconds() * 1000.0)
                if inactivity_gap_ms >= timeout_ms and event_count_current > 0:
                    duration_ms = int((window_end - window_start).total_seconds() * 1000.0)
                    duration_ms_effective = max(duration_ms, int(min_ms))
                    rows.append(
                        {
                            "tail_id": tail_id,
                            "flight_id": flight_id,
                            "win_id": int(win_id),
                            "t_start": window_start,
                            "t_end": window_end,
                            "duration_ms": int(duration_ms_effective),
                            "event_count": int(event_count_current),
                            "close_reason": "inactivity_timeout",
                            "zoh_version": 1,
                            "date_utc": _window_date_from_start(window_start),
                        }
                    )
                    win_id += 1
                    window_start = ts
                    window_end = ts
                    event_count_current = 0

            window_end = ts
            event_count_current += 1
            duration_ms = int((window_end - window_start).total_seconds() * 1000.0)

            if should_close_window(
                duration_ms=duration_ms,
                event_count=event_count_current,
                max_ms=int(max_ms),
                event_threshold=int(event_threshold),
            ):
                duration_ms_effective = max(duration_ms, int(min_ms))
                close_reason = close_reason_for_thresholds(
                    duration_ms=duration_ms,
                    event_count=event_count_current,
                    max_ms=int(max_ms),
                    event_threshold=int(event_threshold),
                )
                rows.append(
                    {
                        "tail_id": tail_id,
                        "flight_id": flight_id,
                        "win_id": int(win_id),
                        "t_start": window_start,
                        "t_end": window_end,
                        "duration_ms": int(duration_ms_effective),
                        "event_count": int(event_count_current),
                        "close_reason": close_reason,
                        "zoh_version": 1,
                        "date_utc": _window_date_from_start(window_start),
                    }
                )
                win_id += 1
                window_start = None
                window_end = None
                event_count_current = 0

        if window_start is not None and window_end is not None and event_count_current > 0:
            duration_ms = int((window_end - window_start).total_seconds() * 1000.0)
            duration_ms_effective = max(duration_ms, int(min_ms))
            rows.append(
                {
                    "tail_id": tail_id,
                    "flight_id": flight_id,
                    "win_id": int(win_id),
                    "t_start": window_start,
                    "t_end": window_end,
                    "duration_ms": int(duration_ms_effective),
                    "event_count": int(event_count_current),
                    "close_reason": "end_of_stream",
                    "zoh_version": 1,
                    "date_utc": _window_date_from_start(window_start),
                }
            )

        return pd.DataFrame(rows)

    return base.groupBy("tail_id", "flight_id").applyInPandas(_emit_windows, schema=schema)

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from datetime import datetime
    import pandas as pd
