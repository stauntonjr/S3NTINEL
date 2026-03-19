"""Window score table builders for Spark pipeline stages."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from libs.io.schemas import WINDOW_SCORES_RAW_COLUMNS
from libs.io.schemas import WINDOW_SCORES_RAW_SCHEMA
from libs.perf.annotations import hot_path
from libs.scoring.artifacts import WindowScoreArtifacts


@hot_path
def build_window_scores_raw_table(
    phase_windows_df: pd.DataFrame,
    phase_baselines_df: pd.DataFrame,
    hierarchy_sensor_map_df: pd.DataFrame,
) -> pd.DataFrame:
    if phase_windows_df.empty:
        return pd.DataFrame()
    return WindowScoreArtifacts.from_phase_rows(
        phase_windows_df.to_dict(orient="records"),
        phase_baselines_df.to_dict(orient="records"),
        hierarchy_sensor_map_df.to_dict(orient="records") if hierarchy_sensor_map_df is not None else [],
    ).to_df()


@hot_path
def build_window_scores_raw_spark_table(
    phase_windows_df: "DataFrame",
    phase_baselines_df: "DataFrame",
    hierarchy_sensor_map_df: "DataFrame",
) -> "DataFrame":
    """Score phase windows in Spark without driver-side dataframe materialization."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    baselines = phase_baselines_df.select(
        "tail_id",
        "phase_id_detected",
        "s_w_centroid",
        "reconstruction_median",
        "reconstruction_mad",
        "distance_median",
        "distance_mad",
    )

    joined = (
        phase_windows_df.alias("w")
        .join(
            F.broadcast(baselines).alias("b"),
            on=[
                F.col("w.tail_id") == F.col("b.tail_id"),
                F.col("w.phase_id_detected") == F.col("b.phase_id_detected"),
            ],
            how="left",
        )
        .withColumn(
            "structure_distance",
            F.when(
                F.col("b.s_w_centroid").isNull(),
                F.lit(None).cast("double"),
            ).otherwise(
                F.expr(
                    """
                    sqrt(
                      aggregate(
                        zip_with(
                          coalesce(w.s_w, array()),
                          coalesce(b.s_w_centroid, array()),
                          (x, y) -> pow(coalesce(x, 0D) - coalesce(y, 0D), 2D)
                        ),
                        cast(0.0 as double),
                        (acc, value) -> acc + value
                      )
                    )
                    """
                )
            ),
        )
        .withColumn(
            "structure_score",
            F.when(
                F.col("b.s_w_centroid").isNull(),
                F.lit(None).cast("double"),
            ).otherwise(
                F.greatest(
                    F.lit(0.0),
                    (F.col("structure_distance") - F.coalesce(F.col("b.distance_median"), F.lit(0.0)))
                    / F.greatest(F.coalesce(F.col("b.distance_mad"), F.lit(0.0)), F.lit(1e-6)),
                )
            ),
        )
        .withColumn(
            "reconstruction_score",
            F.when(
                F.col("b.s_w_centroid").isNull(),
                F.coalesce(F.col("w.backbone_reconstruction_error"), F.lit(0.0)).cast("double"),
            ).otherwise(
                F.greatest(
                    F.lit(0.0),
                    (
                        F.coalesce(F.col("w.backbone_reconstruction_error"), F.lit(0.0))
                        - F.coalesce(F.col("b.reconstruction_median"), F.lit(0.0))
                    )
                    / F.greatest(F.coalesce(F.col("b.reconstruction_mad"), F.lit(0.0)), F.lit(1e-6)),
                )
            ),
        )
        .withColumn(
            "global_score",
            F.when(
                F.col("b.s_w_centroid").isNull(),
                F.coalesce(F.col("w.backbone_reconstruction_error"), F.lit(0.0)).cast("double"),
            ).otherwise(
                (
                    F.coalesce(F.col("structure_score"), F.lit(0.0))
                    + F.coalesce(F.col("reconstruction_score"), F.lit(0.0))
                )
                / F.lit(2.0)
            ),
        )
        .withColumn(
            "severity",
            F.when(F.col("global_score") >= F.lit(6.0), F.lit("high"))
            .when(F.col("global_score") >= F.lit(3.0), F.lit("medium"))
            .when(F.col("global_score") > F.lit(1.0), F.lit("low"))
            .otherwise(F.lit("normal")),
        )
        .withColumn(
            "dominant_score_component",
            F.when(
                F.col("structure_score").isNotNull()
                & (F.col("structure_score") >= F.col("reconstruction_score")),
                F.lit("structure"),
            ).otherwise(F.lit("reconstruction")),
        )
    )

    residual_rows = (
        joined.select(
            F.col("w.tail_id").alias("tail_id"),
            F.col("w.flight_id").alias("flight_id"),
            F.col("w.win_id").alias("win_id"),
            F.explode_outer(F.map_entries(F.coalesce(F.col("w.backbone_residual_by_parameter"), F.expr("cast(map() as map<string,double>)")))).alias(
                "entry"
            ),
        )
        .select(
            "tail_id",
            "flight_id",
            "win_id",
            F.col("entry.key").cast("string").alias("parameter_name"),
            F.abs(F.col("entry.value").cast("double")).alias("residual_weight"),
        )
        .where(F.col("parameter_name").isNotNull())
    )

    subsystem_weights = (
        residual_rows.join(
            F.broadcast(hierarchy_sensor_map_df.select("parameter_name", "subsystem_id")),
            on="parameter_name",
            how="left",
        )
        .where(F.col("subsystem_id").isNotNull())
        .groupBy("tail_id", "flight_id", "win_id", "subsystem_id")
        .agg(F.sum("residual_weight").cast("double").alias("subsystem_weight"))
    )
    subsystem_totals = subsystem_weights.groupBy("tail_id", "flight_id", "win_id").agg(
        F.sum("subsystem_weight").cast("double").alias("subsystem_total")
    )
    subsystem_ranked = (
        subsystem_weights.join(subsystem_totals, on=["tail_id", "flight_id", "win_id"], how="inner")
        .withColumn(
            "subsystem_score",
            F.col("subsystem_weight") / F.greatest(F.col("subsystem_total"), F.lit(1e-12)),
        )
    )
    subsystem_scores = subsystem_ranked.groupBy("tail_id", "flight_id", "win_id").agg(
        F.map_from_entries(F.collect_list(F.struct(F.col("subsystem_id"), F.col("subsystem_score")))).alias("subsystem_scores")
    )
    dominant_window = Window.partitionBy("tail_id", "flight_id", "win_id").orderBy(
        F.col("subsystem_score").desc(),
        F.col("subsystem_id").asc(),
    )
    dominant_subsystems = (
        subsystem_ranked.withColumn("rn", F.row_number().over(dominant_window))
        .where(F.col("rn") == 1)
        .select("tail_id", "flight_id", "win_id", F.col("subsystem_id").alias("dominant_subsystem_id"))
    )

    result = (
        joined.select(
            F.col("w.tail_id").alias("tail_id"),
            F.col("w.flight_id").alias("flight_id"),
            F.col("w.win_id").alias("win_id"),
            F.col("w.phase_state_detected").alias("phase_state_detected"),
            F.col("w.phase_id_detected").alias("phase_id_detected"),
            F.col("w.phase_confidence_detected").alias("phase_confidence_detected"),
            F.col("w.distance_to_centroid_detected").alias("distance_to_centroid_detected"),
            F.col("w.drift_magnitude").alias("drift_magnitude"),
            F.col("w.breadth").alias("breadth"),
            F.col("global_score"),
            F.lit(1.0).cast("double").alias("p_value"),
            F.col("severity"),
            F.col("dominant_score_component"),
            F.create_map(
                F.lit("structure"),
                F.coalesce(F.col("structure_score"), F.lit(0.0)).cast("double"),
                F.lit("reconstruction"),
                F.coalesce(F.col("reconstruction_score"), F.lit(0.0)).cast("double"),
            ).alias("score_component_scores"),
            F.col("w.date_utc").alias("date_utc"),
        )
        .join(subsystem_scores, on=["tail_id", "flight_id", "win_id"], how="left")
        .join(dominant_subsystems, on=["tail_id", "flight_id", "win_id"], how="left")
        .withColumn("subsystem_scores", F.coalesce(F.col("subsystem_scores"), F.expr("cast(map() as map<string,double>)")))
        .select(*WINDOW_SCORES_RAW_COLUMNS)
    )
    return result


if TYPE_CHECKING:
    from pyspark.sql import DataFrame
