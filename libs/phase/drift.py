# File: libs/phase/drift.py
"""Drift magnitude, direction, and breadth computations."""

from __future__ import annotations

from libs.perf.annotations import hot_path


@hot_path
def drift_magnitude(values: list[float]) -> float:
    # HOT PATH: drift metrics are per-window core signals; use vector math primitives at runtime.
    return float(sum(abs(value) for value in values))


@hot_path
def build_phase_windows(
    signatures_df: "DataFrame",
    tau_near_q: float,
    tau_far_q: float,
    persistence_q: float,
) -> "DataFrame":
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    quantiles = signatures_df.groupBy("tail_id", "flight_id").agg(
        F.percentile_approx("drift_mag", F.array(F.lit(float(tau_near_q)), F.lit(float(tau_far_q))), 1000).alias(
            "drift_quantiles"
        )
    )

    with_thresholds = (
        signatures_df.alias("s")
        .join(quantiles.alias("q"), on=["tail_id", "flight_id"], how="left")
        .withColumn("tau_near", F.coalesce(F.col("drift_quantiles").getItem(0), F.lit(0.0)).cast("double"))
        .withColumn("tau_far", F.coalesce(F.col("drift_quantiles").getItem(1), F.lit(0.0)).cast("double"))
        .withColumn(
            "phase_state",
            F.when((F.col("drift_mag") <= F.col("tau_near")) & (F.col("breadth") <= F.lit(0.5)), F.lit("stable"))
            .when((F.col("drift_mag") <= F.col("tau_far")) & (F.col("breadth") <= F.lit(0.7)), F.lit("entering_phase"))
            .when((F.col("drift_mag") > F.col("tau_far")) & (F.col("breadth") >= F.lit(0.7)), F.lit("transition_region"))
            .otherwise(F.lit("leaving_phase")),
        )
        .withColumn(
            "phase_id",
            F.when(F.col("phase_state") == F.lit("stable"), F.lit(0))
            .when(F.col("phase_state") == F.lit("entering_phase"), F.lit(1))
            .when(F.col("phase_state") == F.lit("leaving_phase"), F.lit(2))
            .otherwise(F.lit(3)),
        )
        .withColumn(
            "phase_confidence",
            F.greatest(
                F.lit(0.0),
                F.lit(1.0)
                - (
                    F.col("drift_mag")
                    / F.greatest(F.col("tau_far"), F.lit(1e-6))
                ),
            ),
        )
        .withColumn("distance_to_centroid", F.col("drift_mag").cast("double"))
        .withColumn("persistence_step", (F.col("drift_mag") * F.col("breadth")).cast("double"))
    )

    persist_window = Window.partitionBy("tail_id", "flight_id").orderBy("win_id").rowsBetween(Window.unboundedPreceding, 0)
    with_persistence = with_thresholds.withColumn("persistence", F.sum("persistence_step").over(persist_window))

    persist_quantiles = with_persistence.groupBy("tail_id", "flight_id").agg(
        F.percentile_approx("persistence", F.lit(float(persistence_q)), 1000).alias("persistence_threshold")
    )

    return (
        with_persistence.alias("p")
        .join(persist_quantiles.alias("pq"), on=["tail_id", "flight_id"], how="left")
        .withColumn("is_stable", F.col("phase_state") == F.lit("stable"))
        .withColumn("phase_persistent", F.col("persistence") >= F.coalesce(F.col("persistence_threshold"), F.lit(0.0)))
        .select(
            "tail_id",
            "flight_id",
            "win_id",
            "phase_id",
            "phase_state",
            F.col("phase_confidence").cast("double").alias("phase_confidence"),
            F.col("distance_to_centroid").cast("double").alias("distance_to_centroid"),
            F.col("drift_mag").cast("double").alias("drift_magnitude"),
            F.col("breadth").cast("double").alias("breadth"),
            F.col("persistence").cast("double").alias("persistence"),
            F.col("is_stable").cast("boolean").alias("is_stable"),
            F.col("phase_persistent").cast("boolean").alias("phase_persistent"),
            "date_utc",
        )
    )


def build_phase_centroids(phase_windows_df: "DataFrame", version: int = 1) -> "DataFrame":
    from pyspark.sql import functions as F

    return (
        phase_windows_df.groupBy("tail_id", "phase_id")
        .agg(
            F.avg("distance_to_centroid").alias("mean_distance"),
            F.avg("breadth").alias("mean_breadth"),
            F.avg("phase_confidence").alias("mean_confidence"),
            F.stddev_pop("distance_to_centroid").alias("std_distance"),
            F.stddev_pop("breadth").alias("std_breadth"),
            F.stddev_pop("phase_confidence").alias("std_confidence"),
        )
        .withColumn(
            "name",
            F.when(F.col("phase_id") == F.lit(0), F.lit("Stable"))
            .when(F.col("phase_id") == F.lit(1), F.lit("Entering"))
            .when(F.col("phase_id") == F.lit(2), F.lit("Leaving"))
            .otherwise(F.lit("Transition")),
        )
        .withColumn(
            "centroid",
            F.array(
                F.coalesce(F.col("mean_distance"), F.lit(0.0)),
                F.coalesce(F.col("mean_breadth"), F.lit(0.0)),
                F.coalesce(F.col("mean_confidence"), F.lit(0.0)),
            ),
        )
        .withColumn(
            "var_envelope",
            F.array(
                F.coalesce(F.col("std_distance"), F.lit(0.0)),
                F.coalesce(F.col("std_breadth"), F.lit(0.0)),
                F.coalesce(F.col("std_confidence"), F.lit(0.0)),
            ),
        )
        .withColumn("version", F.lit(int(version)).cast("int"))
        .select("tail_id", "phase_id", "name", "centroid", "var_envelope", "version")
    )


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
