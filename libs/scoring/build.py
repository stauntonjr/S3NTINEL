# File: libs/scoring/build.py
"""Spark-native anomaly scoring builders."""

from __future__ import annotations

from libs.perf.annotations import hot_path


@hot_path
def build_scores_df(signatures_df: "DataFrame", phase_windows_df: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    joined = signatures_df.alias("s").join(
        phase_windows_df.alias("p"),
        on=["tail_id", "flight_id", "win_id", "date_utc"],
        how="left",
    )

    with_blocks = (
        joined.withColumn("pivot_score", F.expr("aggregate(s.pivot_block, 0D, (acc, x) -> acc + abs(x))"))
        .withColumn("cur_score", F.expr("aggregate(s.cur_block, 0D, (acc, x) -> acc + abs(x))"))
        .withColumn("event_score", F.expr("aggregate(s.event_block, 0D, (acc, x) -> acc + abs(x))"))
        .withColumn("cat_score", F.expr("aggregate(s.cat_block, 0D, (acc, x) -> acc + abs(x))"))
        .withColumn(
            "global_score",
            (
                F.col("pivot_score")
                + F.col("cur_score")
                + F.col("event_score")
                + F.col("cat_score")
            )
            / F.lit(4.0),
        )
        .withColumn(
            "dominant_block",
            F.when(
                F.col("event_score") >= F.greatest(F.col("pivot_score"), F.col("cur_score"), F.col("cat_score")),
                F.lit("event_block"),
            )
            .when(
                F.col("cur_score") >= F.greatest(F.col("pivot_score"), F.col("cat_score")),
                F.lit("cur_block"),
            )
            .when(F.col("cat_score") >= F.col("pivot_score"), F.lit("cat_block"))
            .otherwise(F.lit("pivot_block")),
        )
        .withColumn(
            "severity",
            F.when(F.col("global_score") >= F.lit(10.0), F.lit("high"))
            .when(F.col("global_score") >= F.lit(5.0), F.lit("medium"))
            .when(F.col("global_score") > F.lit(1.0), F.lit("low"))
            .otherwise(F.lit("normal")),
        )
    )

    return with_blocks.select(
        "tail_id",
        "flight_id",
        "win_id",
        F.col("p.phase_state").alias("phase_state"),
        F.coalesce(F.col("p.phase_id"), F.lit(0)).cast("int").alias("phase_id"),
        F.coalesce(F.col("p.phase_confidence"), F.lit(0.0)).cast("double").alias("phase_confidence"),
        F.coalesce(F.col("p.distance_to_centroid"), F.col("s.drift_mag")).cast("double").alias(
            "distance_to_centroid"
        ),
        F.coalesce(F.col("p.drift_magnitude"), F.col("s.drift_mag")).cast("double").alias("drift_magnitude"),
        F.coalesce(F.col("p.breadth"), F.col("s.breadth")).cast("double").alias("breadth"),
        F.col("global_score").cast("double").alias("global_score"),
        F.lit(None).cast("double").alias("p_value"),
        "severity",
        F.lit("unknown").alias("dominant_subsystem"),
        "dominant_block",
        F.create_map(
            F.lit("pivot"),
            F.col("pivot_score").cast("double"),
            F.lit("cur"),
            F.col("cur_score").cast("double"),
            F.lit("events"),
            F.col("event_score").cast("double"),
            F.lit("categorical"),
            F.col("cat_score").cast("double"),
        ).alias("block_scores"),
        "date_utc",
    )


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
