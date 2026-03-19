"""Spark-native conformal calibration table builders."""

from __future__ import annotations

from libs.perf.annotations import hot_path


@hot_path
def build_calibrated_window_scores_table(scores_df: "DataFrame", min_warm: int) -> "DataFrame":
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    phase_window = Window.partitionBy("tail_id", "flight_id", "phase_id_detected")
    order_window = Window.partitionBy("tail_id", "flight_id", "phase_id_detected").orderBy("win_id")
    score_desc_window = Window.partitionBy("tail_id", "flight_id", "phase_id_detected").orderBy(F.col("global_score").desc())

    enriched = (
        scores_df.withColumn("phase_count", F.count(F.lit(1)).over(phase_window))
        .withColumn("phase_rank", F.row_number().over(order_window))
        .withColumn("warm", F.col("phase_count") >= F.lit(int(min_warm)))
    )

    with_pvalue = enriched.withColumn("empirical_tail", F.cume_dist().over(score_desc_window)).withColumn(
        "p_value",
        F.when(F.col("warm"), F.col("empirical_tail").cast("double")).otherwise(F.lit(None).cast("double")),
    )

    return with_pvalue.select(
        "tail_id",
        "flight_id",
        "win_id",
        "phase_state_detected",
        "phase_id_detected",
        "phase_confidence_detected",
        "distance_to_centroid_detected",
        "drift_magnitude",
        "breadth",
        "global_score",
        "p_value",
        "severity",
        "dominant_subsystem_id",
        "dominant_score_component",
        "subsystem_scores",
        "score_component_scores",
        "warm",
        F.col("warm").alias("emit_ready"),
        F.lit(int(min_warm)).alias("min_warm"),
        "date_utc",
    )


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
