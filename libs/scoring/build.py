# File: libs/scoring/build.py
"""Spark-native anomaly scoring builders."""

from __future__ import annotations

import os

from libs.perf.annotations import hot_path


@hot_path
def build_window_subsystem_evidence_df(
    events_df: "DataFrame",
    windows_df: "DataFrame",
    subsystem_map_df: "DataFrame",
) -> "DataFrame":
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    mapped_events = events_df.join(subsystem_map_df.select("sensor", "subsystem_id"), on="sensor", how="inner")
    events_in_windows = (
        mapped_events.alias("e")
        .join(
            windows_df.alias("w"),
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
            F.col("e.subsystem_id").alias("subsystem_id"),
        )
    )

    counts = events_in_windows.groupBy("tail_id", "flight_id", "win_id", "date_utc", "subsystem_id").agg(
        F.count("*").alias("subsystem_event_count")
    )
    totals = counts.groupBy("tail_id", "flight_id", "win_id", "date_utc").agg(
        F.sum("subsystem_event_count").alias("window_event_total")
    )
    with_scores = (
        counts.alias("c")
        .join(totals.alias("t"), on=["tail_id", "flight_id", "win_id", "date_utc"], how="inner")
        .withColumn(
            "subsystem_score",
            F.when(F.col("window_event_total") > F.lit(0), F.col("subsystem_event_count") / F.col("window_event_total")).otherwise(F.lit(0.0)),
        )
    )
    rank_window = Window.partitionBy("tail_id", "flight_id", "win_id", "date_utc").orderBy(
        F.col("subsystem_event_count").desc(),
        F.col("subsystem_id").asc(),
    )
    dominant_df = (
        with_scores.withColumn("rn", F.row_number().over(rank_window))
        .where(F.col("rn") == F.lit(1))
        .select("tail_id", "flight_id", "win_id", "date_utc", F.col("subsystem_id").alias("dominant_subsystem"))
    )
    score_map_df = with_scores.groupBy("tail_id", "flight_id", "win_id", "date_utc").agg(
        F.map_from_entries(F.collect_list(F.struct(F.col("subsystem_id"), F.col("subsystem_score")))).alias("subsystem_scores")
    )
    return score_map_df.join(dominant_df, on=["tail_id", "flight_id", "win_id", "date_utc"], how="left")


@hot_path
def build_scores_df(
    signatures_df: "DataFrame",
    phase_windows_df: "DataFrame",
    subsystem_evidence_df: "DataFrame | None" = None,
) -> "DataFrame":
    from pyspark.sql import functions as F

    severity_low_threshold = float(os.getenv("S3NTINEL_SEVERITY_LOW_THRESHOLD", "0.25"))
    severity_medium_threshold = float(os.getenv("S3NTINEL_SEVERITY_MEDIUM_THRESHOLD", "0.75"))
    severity_high_threshold = float(os.getenv("S3NTINEL_SEVERITY_HIGH_THRESHOLD", "1.50"))

    severity_medium_threshold = max(severity_medium_threshold, severity_low_threshold)
    severity_high_threshold = max(severity_high_threshold, severity_medium_threshold)

    joined = signatures_df.alias("s").join(
        phase_windows_df.alias("p"),
        on=["tail_id", "flight_id", "win_id", "date_utc"],
        how="left",
    )

    with_raw_blocks = (
        joined.withColumn("pivot_score", F.expr("aggregate(s.pivot_block, 0D, (acc, x) -> acc + abs(x))"))
        .withColumn("cur_score", F.expr("aggregate(s.cur_block, 0D, (acc, x) -> acc + abs(x))"))
        .withColumn("event_score", F.expr("aggregate(s.event_block, 0D, (acc, x) -> acc + abs(x))"))
        .withColumn("cat_score", F.expr("aggregate(s.cat_block, 0D, (acc, x) -> acc + abs(x))"))
    )

    phase_keys = ["tail_id", "flight_id", "phase_id_out"]
    with_phase_id = with_raw_blocks.withColumn("phase_id_out", F.coalesce(F.col("p.phase_id"), F.lit(0)).cast("int"))
    with_phase_context = (
        with_phase_id.withColumn("phase_state_out", F.col("p.phase_state"))
        .withColumn("phase_confidence_out", F.coalesce(F.col("p.phase_confidence"), F.lit(0.0)).cast("double"))
        .withColumn("distance_to_centroid_out", F.coalesce(F.col("p.distance_to_centroid"), F.col("s.drift_mag")).cast("double"))
        .withColumn("drift_magnitude_out", F.coalesce(F.col("p.drift_magnitude"), F.col("s.drift_mag")).cast("double"))
        .withColumn("breadth_out", F.coalesce(F.col("p.breadth"), F.col("s.breadth")).cast("double"))
    )

    phase_medians = with_phase_context.groupBy(*phase_keys).agg(
        F.expr("percentile_approx(pivot_score, 0.5, 10000)").cast("double").alias("pivot_median"),
        F.expr("percentile_approx(cur_score, 0.5, 10000)").cast("double").alias("cur_median"),
        F.expr("percentile_approx(event_score, 0.5, 10000)").cast("double").alias("event_median"),
        F.expr("percentile_approx(cat_score, 0.5, 10000)").cast("double").alias("cat_median"),
    )

    with_medians = with_phase_context.join(phase_medians, on=phase_keys, how="left")
    with_abs_dev = (
        with_medians.withColumn("pivot_abs_dev", F.abs(F.col("pivot_score") - F.col("pivot_median")))
        .withColumn("cur_abs_dev", F.abs(F.col("cur_score") - F.col("cur_median")))
        .withColumn("event_abs_dev", F.abs(F.col("event_score") - F.col("event_median")))
        .withColumn("cat_abs_dev", F.abs(F.col("cat_score") - F.col("cat_median")))
    )

    phase_mads = with_abs_dev.groupBy(*phase_keys).agg(
        F.expr("percentile_approx(pivot_abs_dev, 0.5, 10000)").cast("double").alias("pivot_mad"),
        F.expr("percentile_approx(cur_abs_dev, 0.5, 10000)").cast("double").alias("cur_mad"),
        F.expr("percentile_approx(event_abs_dev, 0.5, 10000)").cast("double").alias("event_mad"),
        F.expr("percentile_approx(cat_abs_dev, 0.5, 10000)").cast("double").alias("cat_mad"),
    )

    mad_eps = F.lit(1.0e-9)
    score_floor = F.lit(1.0)
    rel_floor_scale = F.lit(0.10)
    norm_clip = F.lit(20.0)
    with_blocks = (
        with_abs_dev.join(phase_mads, on=phase_keys, how="left")
        .withColumn(
            "pivot_score_norm",
            F.least(
                F.col("pivot_abs_dev")
                / F.greatest(F.col("pivot_mad"), F.abs(F.col("pivot_median")) * rel_floor_scale, score_floor, mad_eps),
                norm_clip,
            ),
        )
        .withColumn(
            "cur_score_norm",
            F.least(
                F.col("cur_abs_dev")
                / F.greatest(F.col("cur_mad"), F.abs(F.col("cur_median")) * rel_floor_scale, score_floor, mad_eps),
                norm_clip,
            ),
        )
        .withColumn(
            "event_score_norm",
            F.least(
                F.col("event_abs_dev")
                / F.greatest(F.col("event_mad"), F.abs(F.col("event_median")) * rel_floor_scale, score_floor, mad_eps),
                norm_clip,
            ),
        )
        .withColumn(
            "cat_score_norm",
            F.least(
                F.col("cat_abs_dev")
                / F.greatest(F.col("cat_mad"), F.abs(F.col("cat_median")) * rel_floor_scale, score_floor, mad_eps),
                norm_clip,
            ),
        )
        .withColumn(
            "global_score",
            (
                F.col("pivot_score_norm")
                + F.col("cur_score_norm")
                + F.col("event_score_norm")
                + F.col("cat_score_norm")
            )
            / F.lit(4.0),
        )
        .withColumn(
            "dominant_block",
            F.when(
                F.col("event_score_norm")
                >= F.greatest(F.col("pivot_score_norm"), F.col("cur_score_norm"), F.col("cat_score_norm")),
                F.lit("event_block"),
            )
            .when(
                F.col("cur_score_norm") >= F.greatest(F.col("pivot_score_norm"), F.col("cat_score_norm")),
                F.lit("cur_block"),
            )
            .when(F.col("cat_score_norm") >= F.col("pivot_score_norm"), F.lit("cat_block"))
            .otherwise(F.lit("pivot_block")),
        )
        .withColumn(
            "severity",
            F.when(F.col("global_score") >= F.lit(severity_high_threshold), F.lit("high"))
            .when(F.col("global_score") >= F.lit(severity_medium_threshold), F.lit("medium"))
            .when(F.col("global_score") > F.lit(severity_low_threshold), F.lit("low"))
            .otherwise(F.lit("normal")),
        )
    )

    with_subsystem = with_blocks
    if subsystem_evidence_df is not None:
        with_subsystem = with_subsystem.join(
            subsystem_evidence_df.alias("d"),
            on=["tail_id", "flight_id", "win_id", "date_utc"],
            how="left",
        )
    else:
        with_subsystem = (
            with_subsystem.withColumn("dominant_subsystem", F.lit(None).cast("string")).withColumn(
                "subsystem_scores", F.lit(None).cast("map<string,double>")
            )
        )

    return with_subsystem.select(
        "tail_id",
        "flight_id",
        "win_id",
        F.col("phase_state_out").alias("phase_state"),
        F.col("phase_id_out").cast("int").alias("phase_id"),
        F.col("phase_confidence_out").cast("double").alias("phase_confidence"),
        F.col("distance_to_centroid_out").cast("double").alias("distance_to_centroid"),
        F.col("drift_magnitude_out").cast("double").alias("drift_magnitude"),
        F.col("breadth_out").cast("double").alias("breadth"),
        F.col("global_score").cast("double").alias("global_score"),
        F.lit(None).cast("double").alias("p_value"),
        "severity",
        F.coalesce(F.col("dominant_subsystem"), F.lit("unknown")).alias("dominant_subsystem"),
        "dominant_block",
        F.coalesce(F.col("subsystem_scores"), F.expr("cast(map() as map<string,double>)")).alias("subsystem_scores"),
        F.create_map(
            F.lit("pivot"),
            F.col("pivot_score_norm").cast("double"),
            F.lit("cur"),
            F.col("cur_score_norm").cast("double"),
            F.lit("events"),
            F.col("event_score_norm").cast("double"),
            F.lit("categorical"),
            F.col("cat_score_norm").cast("double"),
        ).alias("block_scores"),
        "date_utc",
    )


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
