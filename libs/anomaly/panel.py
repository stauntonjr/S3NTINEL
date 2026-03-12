"""Panel and message context extraction for anomaly attribution."""

from __future__ import annotations

from libs.perf.annotations import hot_path


@hot_path
def build_window_panel_context_df(
    raw_df: "DataFrame",
    windows_df: "DataFrame",
    *,
    max_items: int = 5,
) -> "DataFrame":
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    ts_col = "timestamp_utc" if "timestamp_utc" in raw_df.columns else ("ts" if "ts" in raw_df.columns else None)
    if ts_col is None:
        return windows_df.select("tail_id", "flight_id", "win_id", "date_utc").where(F.lit(False)).select(
            "tail_id",
            "flight_id",
            "win_id",
            "date_utc",
            F.lit(None).cast("struct<text:array<string>,message_codes:array<string>,source:array<string>>").alias(
                "panel_context"
            ),
        )

    name_col = "parameter_name" if "parameter_name" in raw_df.columns else ("sensor" if "sensor" in raw_df.columns else None)
    value_col = "parameter_value" if "parameter_value" in raw_df.columns else ("state" if "state" in raw_df.columns else None)
    if name_col is None or value_col is None:
        return windows_df.select("tail_id", "flight_id", "win_id", "date_utc").where(F.lit(False)).select(
            "tail_id",
            "flight_id",
            "win_id",
            "date_utc",
            F.lit(None).cast("struct<text:array<string>,message_codes:array<string>,source:array<string>>").alias(
                "panel_context"
            ),
        )

    keyword_expr = "(lcd|panel|msg|message|cas|warn|warning|fault|caution|annunc|text)"
    candidates = (
        raw_df.select(
            F.col("tail_id"),
            F.col("flight_id"),
            F.col(ts_col).cast("timestamp").alias("ts"),
            F.col(name_col).cast("string").alias("source_name"),
            F.trim(F.col(value_col).cast("string")).alias("text_value"),
            F.col("date_utc"),
        )
        .where(F.col("ts").isNotNull())
        .where(F.col("source_name").isNotNull())
        .where(F.col("text_value").isNotNull() & (F.col("text_value") != F.lit("")))
        .where(F.expr("try_cast(text_value as double) is null"))
        .withColumn(
            "keyword_hit",
            (
                F.lower(F.col("source_name")).rlike(keyword_expr)
                | F.lower(F.col("text_value")).rlike(keyword_expr)
            ).cast("int"),
        )
    )

    events_in_windows = (
        candidates.alias("r")
        .join(
            windows_df.alias("w"),
            on=(
                (F.col("r.tail_id") == F.col("w.tail_id"))
                & (F.col("r.flight_id") == F.col("w.flight_id"))
                & (F.col("r.ts") >= F.col("w.t_start"))
                & (F.col("r.ts") <= F.col("w.t_end"))
            ),
            how="inner",
        )
        .select(
            F.col("w.tail_id").alias("tail_id"),
            F.col("w.flight_id").alias("flight_id"),
            F.col("w.win_id").alias("win_id"),
            F.col("w.date_utc").alias("date_utc"),
            F.col("r.ts").alias("ts"),
            F.col("r.source_name").alias("source_name"),
            F.col("r.text_value").alias("text_value"),
            F.col("r.keyword_hit").alias("keyword_hit"),
        )
    )

    rank_window = Window.partitionBy("tail_id", "flight_id", "win_id", "date_utc").orderBy(
        F.col("keyword_hit").desc(),
        F.col("ts").desc(),
        F.col("source_name").asc(),
        F.col("text_value").asc(),
    )

    limited = (
        events_in_windows.withColumn("rn", F.row_number().over(rank_window))
        .where(F.col("rn") <= F.lit(max(int(max_items), 1)))
        .groupBy("tail_id", "flight_id", "win_id", "date_utc")
        .agg(
            F.collect_list(
                F.struct(
                    F.col("rn").alias("rn"),
                    F.col("text_value").alias("text_value"),
                    F.col("source_name").alias("source_name"),
                )
            ).alias("items")
        )
        .withColumn("ordered_items", F.expr("array_sort(items)"))
        .drop("items")
    )

    return limited.select(
        "tail_id",
        "flight_id",
        "win_id",
        "date_utc",
        F.struct(
            F.array_sort(F.array_distinct(F.expr("transform(ordered_items, x -> x.text_value)"))).alias("text"),
            F.array_sort(
                F.array_distinct(
                    F.expr(
                        "filter(transform(ordered_items, x -> regexp_extract(x.text_value, '([A-Z]{2,}[A-Z0-9_-]*)', 1)), x -> x != '')"
                    )
                )
            ).alias("message_codes"),
            F.array_sort(F.array_distinct(F.expr("transform(ordered_items, x -> x.source_name)"))).alias("source"),
        ).alias("panel_context"),
    )


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
