# File: libs/signature/blocks.py
"""Build signature blocks used by scoring and phase detection."""

from __future__ import annotations

from libs.perf.annotations import hot_path


@hot_path
def build_signature_blocks() -> dict[str, list[float]]:
    # HOT PATH: block construction feeds every downstream stage; optimize sparse/dense concatenation.
    return {
        "pivot_block": [],
        "cur_block": [],
        "event_block": [],
        "cat_block": [],
    }


@hot_path
def build_signatures_df(
    raw_df: "DataFrame",
    events_df: "DataFrame",
    windows_df: "DataFrame",
    sampled_sensors_df: "DataFrame | None" = None,
    sig_version: int = 1,
    event_threshold: int = 20,
) -> "DataFrame":
    from pyspark.sql import functions as F

    windows = windows_df.select(
        "tail_id",
        "flight_id",
        "win_id",
        "t_start",
        "t_end",
        "duration_ms",
        "event_count",
        "date_utc",
    )

    events_in_windows = (
        events_df.alias("e")
        .join(
            windows.alias("w"),
            on=(
                (F.col("e.tail_id") == F.col("w.tail_id"))
                & (F.col("e.flight_id") == F.col("w.flight_id"))
                & (F.col("e.ts") >= F.col("w.t_start"))
                & (F.col("e.ts") <= F.col("w.t_end"))
            ),
            how="inner",
        )
        .select(
            F.col("w.tail_id").alias("tail_id"),
            F.col("w.flight_id").alias("flight_id"),
            F.col("w.win_id").alias("win_id"),
            F.col("w.date_utc").alias("date_utc"),
            F.col("e.event_type").alias("event_type"),
        )
    )

    raw_in_windows = (
        raw_df.alias("r")
        .join(
            windows.alias("w"),
            on=(
                (F.col("r.tail_id") == F.col("w.tail_id"))
                & (F.col("r.flight_id") == F.col("w.flight_id"))
                & (F.col("r.timestamp_utc") >= F.col("w.t_start"))
                & (F.col("r.timestamp_utc") <= F.col("w.t_end"))
            ),
            how="inner",
        )
        .select(
            F.col("w.tail_id").alias("tail_id"),
            F.col("w.flight_id").alias("flight_id"),
            F.col("w.win_id").alias("win_id"),
            F.col("w.date_utc").alias("date_utc"),
            F.col("w.duration_ms").alias("duration_ms"),
            F.col("r.sensor").alias("sensor"),
            F.col("r.val").alias("val"),
        )
    )

    raw_stats = raw_in_windows.groupBy("tail_id", "flight_id", "win_id", "date_utc").agg(
        F.count(F.when(F.col("val").isNotNull(), F.lit(1))).alias("val_count"),
        F.avg("val").alias("val_mean"),
        F.stddev_pop("val").alias("val_std"),
        F.min("val").alias("val_min"),
        F.max("val").alias("val_max"),
    )

    event_stats = events_in_windows.groupBy("tail_id", "flight_id", "win_id", "date_utc").agg(
        F.count(F.lit(1)).alias("event_total"),
        F.sum(F.when(F.col("event_type") == F.lit("threshold"), F.lit(1)).otherwise(F.lit(0))).alias(
            "threshold_count"
        ),
        F.sum(F.when(F.col("event_type") == F.lit("cooccur"), F.lit(1)).otherwise(F.lit(0))).alias(
            "cooccur_count"
        ),
        F.sum(F.when(F.col("event_type") == F.lit("transition"), F.lit(1)).otherwise(F.lit(0))).alias(
            "transition_count"
        ),
        F.sum(F.when(F.col("event_type") == F.lit("dropped"), F.lit(1)).otherwise(F.lit(0))).alias(
            "dropped_count"
        ),
        F.sum(F.when(F.col("event_type") == F.lit("state_enter"), F.lit(1)).otherwise(F.lit(0))).alias(
            "state_enter_count"
        ),
        F.sum(F.when(F.col("event_type") == F.lit("slope_pos"), F.lit(1)).otherwise(F.lit(0))).alias(
            "slope_pos_count"
        ),
        F.sum(F.when(F.col("event_type") == F.lit("slope_neg"), F.lit(1)).otherwise(F.lit(0))).alias(
            "slope_neg_count"
        ),
    )

    sampled_selected_sensor_count = 0
    sampled_window_stats = None
    if sampled_sensors_df is not None:
        sampled_sensors = sampled_sensors_df.select("sensor").distinct()
        sampled_selected_sensor_count = sampled_sensors.count()
        sampled_window_stats = (
            raw_in_windows.join(sampled_sensors, on="sensor", how="inner")
            .groupBy("tail_id", "flight_id", "win_id", "date_utc")
            .agg(
                F.countDistinct("sensor").alias("sampled_sensor_hits"),
                F.count(F.when(F.col("val").isNotNull(), F.lit(1))).alias("sampled_val_count"),
            )
        )

    signature_base = (
        windows.alias("w")
        .join(
            raw_stats.alias("r"),
            on=["tail_id", "flight_id", "win_id", "date_utc"],
            how="left",
        )
        .join(
            event_stats.alias("e"),
            on=["tail_id", "flight_id", "win_id", "date_utc"],
            how="left",
        )
        .join(
            sampled_window_stats.alias("cs") if sampled_window_stats is not None else windows.select(
                "tail_id",
                "flight_id",
                "win_id",
                "date_utc",
            )
            .withColumn("sampled_sensor_hits", F.lit(0))
            .withColumn("sampled_val_count", F.lit(0))
            .alias("cs"),
            on=["tail_id", "flight_id", "win_id", "date_utc"],
            how="left",
        )
        .withColumn("val_count", F.coalesce(F.col("val_count"), F.lit(0)).cast("double"))
        .withColumn("val_mean", F.coalesce(F.col("val_mean"), F.lit(0.0)).cast("double"))
        .withColumn("val_std", F.coalesce(F.col("val_std"), F.lit(0.0)).cast("double"))
        .withColumn("val_min", F.coalesce(F.col("val_min"), F.lit(0.0)).cast("double"))
        .withColumn("val_max", F.coalesce(F.col("val_max"), F.lit(0.0)).cast("double"))
        .withColumn("event_total", F.coalesce(F.col("event_total"), F.lit(0)).cast("double"))
        .withColumn("threshold_count", F.coalesce(F.col("threshold_count"), F.lit(0)).cast("double"))
        .withColumn("cooccur_count", F.coalesce(F.col("cooccur_count"), F.lit(0)).cast("double"))
        .withColumn("transition_count", F.coalesce(F.col("transition_count"), F.lit(0)).cast("double"))
        .withColumn("dropped_count", F.coalesce(F.col("dropped_count"), F.lit(0)).cast("double"))
        .withColumn("state_enter_count", F.coalesce(F.col("state_enter_count"), F.lit(0)).cast("double"))
        .withColumn("slope_pos_count", F.coalesce(F.col("slope_pos_count"), F.lit(0)).cast("double"))
        .withColumn("slope_neg_count", F.coalesce(F.col("slope_neg_count"), F.lit(0)).cast("double"))
        .withColumn("sampled_sensor_hits", F.coalesce(F.col("sampled_sensor_hits"), F.lit(0)).cast("double"))
        .withColumn("sampled_val_count", F.coalesce(F.col("sampled_val_count"), F.lit(0)).cast("double"))
        .withColumn(
            "sampled_sensor_coverage",
            F.when(
                F.lit(sampled_selected_sensor_count) > F.lit(0),
                F.col("sampled_sensor_hits") / F.lit(float(max(sampled_selected_sensor_count, 1))),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("range_val", (F.col("val_max") - F.col("val_min")).cast("double"))
        .withColumn(
            "slope_balance",
            F.when(
                F.col("event_total") > F.lit(0.0),
                (F.col("slope_pos_count") - F.col("slope_neg_count")) / F.col("event_total"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "breadth",
            F.when(
                F.col("event_total") > F.lit(0.0),
                F.least(F.lit(1.0), F.col("event_total") / F.lit(float(max(event_threshold, 1)))),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("drift_mag", F.abs(F.col("range_val")).cast("double"))
    )

    return signature_base.select(
        "tail_id",
        "flight_id",
        "win_id",
        F.lit(0).cast("int").alias("phase_id"),
        F.lit(sig_version).cast("int").alias("sig_version"),
        F.array("val_mean", "val_std", "val_min", "val_max").alias("pivot_block"),
        F.array(
            "val_count",
            "range_val",
            F.col("duration_ms").cast("double"),
            F.col("sampled_val_count").cast("double"),
            F.col("sampled_sensor_coverage").cast("double"),
        ).alias("cur_block"),
        F.array("event_total", "threshold_count", "cooccur_count").alias("event_block"),
        F.array("transition_count", "dropped_count", "state_enter_count").alias("cat_block"),
        F.col("breadth").cast("float").alias("breadth"),
        F.col("drift_mag").cast("float").alias("drift_mag"),
        F.array(F.col("slope_balance").cast("double")).alias("drift_dir"),
        "date_utc",
    )


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
