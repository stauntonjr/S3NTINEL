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

    per_flight_stats = signatures_df.groupBy("tail_id", "flight_id").agg(
        F.percentile_approx("drift_mag", F.array(F.lit(float(tau_near_q)), F.lit(float(tau_far_q))), 1000).alias(
            "drift_quantiles"
        ),
        F.stddev_pop("drift_mag").alias("drift_std"),
        F.avg("breadth").alias("breadth_avg"),
        F.stddev_pop("breadth").alias("breadth_std"),
    )

    order_window = Window.partitionBy("tail_id", "flight_id").orderBy("win_id")
    persistence_window = Window.partitionBy("tail_id", "flight_id", "reversal_segment").orderBy("win_id").rowsBetween(
        Window.unboundedPreceding, 0
    )

    thresholded = (
        signatures_df.alias("s")
        .join(per_flight_stats.alias("q"), on=["tail_id", "flight_id"], how="left")
        .withColumn("drift_mag", F.coalesce(F.col("drift_mag").cast("double"), F.lit(0.0)))
        .withColumn("breadth", F.coalesce(F.col("breadth").cast("double"), F.lit(0.0)))
        .withColumn(
            "drift_dir_scalar",
            F.coalesce(
                F.col("drift_dir").getItem(0).cast("double"),
                (F.col("drift_mag") - F.coalesce(F.lag("drift_mag").over(order_window), F.col("drift_mag"))).cast("double"),
            ),
        )
        .withColumn("tau_near_raw", F.coalesce(F.col("drift_quantiles").getItem(0).cast("double"), F.lit(0.0)))
        .withColumn("tau_far_raw", F.coalesce(F.col("drift_quantiles").getItem(1).cast("double"), F.lit(0.0)))
        .withColumn("drift_std", F.coalesce(F.col("drift_std").cast("double"), F.lit(0.0)))
        .withColumn("breadth_avg", F.coalesce(F.col("breadth_avg").cast("double"), F.lit(0.0)))
        .withColumn("breadth_std", F.coalesce(F.col("breadth_std").cast("double"), F.lit(0.0)))
        .withColumn("tau_near", F.greatest(F.col("tau_near_raw"), F.col("drift_std") * F.lit(0.5), F.lit(0.05)))
        .withColumn(
            "tau_far",
            F.greatest(F.col("tau_far_raw"), F.col("tau_near") + F.col("drift_std") * F.lit(0.5), F.col("tau_near") + F.lit(1e-6)),
        )
        .withColumn(
            "transition_breadth_min",
            F.greatest(F.lit(0.7), F.col("breadth_avg") + (F.col("breadth_std") * F.lit(0.5))),
        )
        .withColumn(
            "is_transition_candidate",
            (F.col("drift_mag") > F.col("tau_far")) & (F.col("breadth") >= F.col("transition_breadth_min")),
        )
        .withColumn("is_stable_candidate", (F.col("drift_mag") <= F.col("tau_near")) & (F.col("breadth") <= F.lit(0.5)))
        .withColumn(
            "phase_state_base",
            F.when(F.col("is_stable_candidate"), F.lit("stable"))
            .when((F.col("drift_mag") <= F.col("tau_far")) & (F.col("breadth") < F.col("transition_breadth_min")), F.lit("entering_phase"))
            .otherwise(F.lit("leaving_phase")),
        )
        .withColumn(
            "delta_t",
            F.when(F.size(F.col("cur_block")) >= F.lit(3), F.abs(F.col("cur_block").getItem(2).cast("double")) / F.lit(1000.0)).otherwise(F.lit(1.0)),
        )
        .withColumn("delta_t", F.when(F.col("delta_t") > F.lit(0.0), F.col("delta_t")).otherwise(F.lit(1.0)))
        .withColumn("drift_sign", F.when(F.col("drift_dir_scalar") > F.lit(0.0), F.lit(1)).when(F.col("drift_dir_scalar") < F.lit(0.0), F.lit(-1)).otherwise(F.lit(0)))
    )

    with_reversal = (
        thresholded.withColumn("prev_drift_sign", F.coalesce(F.lag("drift_sign").over(order_window), F.lit(0)))
        .withColumn(
            "drift_reversal",
            (F.col("drift_sign") != F.lit(0))
            & (F.col("prev_drift_sign") != F.lit(0))
            & (F.col("drift_sign") != F.col("prev_drift_sign")),
        )
        .withColumn(
            "reversal_segment",
            F.sum(F.when(F.col("drift_reversal"), F.lit(1)).otherwise(F.lit(0))).over(order_window),
        )
        .withColumn("persistence_step", (F.col("drift_mag") * F.col("breadth") * F.col("delta_t")).cast("double"))
        .withColumn("persistence", F.sum("persistence_step").over(persistence_window))
    )

    persistence_thresholds = with_reversal.groupBy("tail_id", "flight_id").agg(
        F.percentile_approx("persistence", F.lit(float(persistence_q)), 1000).alias("persistence_threshold")
    )

    with_state = (
        with_reversal.alias("w")
        .join(persistence_thresholds.alias("p"), on=["tail_id", "flight_id"], how="left")
        .withColumn("persistence_threshold", F.coalesce(F.col("p.persistence_threshold"), F.lit(0.0)).cast("double"))
        .withColumn(
            "phase_state",
            F.when(F.col("is_stable_candidate"), F.lit("stable"))
            .when(F.col("is_transition_candidate") & (F.col("persistence") >= F.col("persistence_threshold")), F.lit("transition_region"))
            .otherwise(F.col("phase_state_base")),
        )
        .withColumn("is_stable", F.col("phase_state") == F.lit("stable"))
        .withColumn("phase_persistent", F.col("persistence") >= F.col("persistence_threshold"))
        .withColumn("stable_start", F.when(F.col("is_stable") & (~F.coalesce(F.lag("is_stable").over(order_window), F.lit(False))), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("stable_phase_counter", F.sum("stable_start").over(order_window))
        .withColumn(
            "phase_id",
            F.coalesce(
                F.last(F.when(F.col("is_stable"), F.col("stable_phase_counter")), ignorenulls=True).over(order_window),
                F.col("stable_phase_counter"),
                F.lit(0),
            ).cast("int"),
        )
    )

    centroid_window = Window.partitionBy("tail_id", "flight_id", "phase_id").orderBy("win_id").rowsBetween(
        Window.unboundedPreceding, -1
    )
    stable_for_centroid = F.when(F.col("is_stable"), F.col("drift_mag"))
    stable_breadth_for_centroid = F.when(F.col("is_stable"), F.col("breadth"))
    stable_dir_for_centroid = F.when(F.col("is_stable"), F.col("drift_dir_scalar"))

    with_centroid_distance = (
        with_state
        .withColumn("centroid_drift", F.avg(stable_for_centroid).over(centroid_window))
        .withColumn("centroid_breadth", F.avg(stable_breadth_for_centroid).over(centroid_window))
        .withColumn("centroid_dir", F.avg(stable_dir_for_centroid).over(centroid_window))
        .withColumn(
            "distance_to_centroid",
            F.sqrt(
                F.pow(F.col("drift_mag") - F.coalesce(F.col("centroid_drift"), F.col("drift_mag")), 2)
                + F.pow(F.col("breadth") - F.coalesce(F.col("centroid_breadth"), F.col("breadth")), 2)
                + F.pow(F.col("drift_dir_scalar") - F.coalesce(F.col("centroid_dir"), F.col("drift_dir_scalar")), 2)
            ),
        )
        .withColumn(
            "phase_confidence",
            F.greatest(
                F.lit(0.0),
                F.lit(1.0) - (F.col("distance_to_centroid") / F.greatest(F.col("tau_far"), F.lit(1e-6))),
            ),
        )
    )

    return with_centroid_distance.select(
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


@hot_path
def build_phase_centroids(phase_windows_df: "DataFrame", version: int = 1) -> "DataFrame":
    from pyspark.sql import functions as F

    return (
        phase_windows_df.where(F.col("is_stable") == F.lit(True)).groupBy("tail_id", "phase_id")
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
