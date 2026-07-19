"""Phase fit logic: per-flight scaling, centroid fitting, and distance scales."""

from __future__ import annotations

from typing import TYPE_CHECKING

from libs.perf.annotations import hot_path
from libs.phase.frames import PhaseFeatureFrame, PhaseObservationFrame
from libs.phase.types import PhaseClusterModel, PhasePlanConfig, PhaseTransitionModel
from libs.phase.utils import array_distance, with_progress_mass_coordinates


def build_flight_stats(
    observation_frame: PhaseObservationFrame,
    *,
    feature_count: int,
    config: PhasePlanConfig,
) -> "DataFrame":
    from pyspark.sql import functions as F

    observation_df = observation_frame.dataframe
    median_exprs = [
        F.expr(f"percentile(element_at(s_w, {idx + 1}), 0.5D)").cast("double").alias(f"phase_median_{idx}")
        for idx in range(feature_count)
    ]
    medians_df = observation_df.groupBy("tail_id", "flight_id").agg(
        F.count(F.lit(1)).cast("int").alias("flight_window_count"),
        F.expr(f"percentile(drift_magnitude_profiled, {float(config.phase_stable_drift_quantile)}D)")
        .cast("double")
        .alias("drift_threshold"),
        *median_exprs,
    )
    medians_df = medians_df.withColumn(
        "phase_feature_medians",
        F.array(*[F.coalesce(F.col(f"phase_median_{idx}"), F.lit(0.0)) for idx in range(feature_count)]),
    ).drop(*[f"phase_median_{idx}" for idx in range(feature_count)])

    with_medians_df = (
        observation_df.join(medians_df, on=["tail_id", "flight_id"], how="inner")
        .withColumn(
            "phase_feature_abs_dev",
            F.zip_with("s_w", "phase_feature_medians", lambda value, median: F.abs(value - median)),
        )
        .withColumn(
            "phase_is_stable_raw",
            F.coalesce(F.col("drift_magnitude_profiled"), F.lit(0.0))
            <= F.coalesce(F.col("drift_threshold"), F.lit(0.0)),
        )
    )
    mad_exprs = [
        F.expr(f"percentile(element_at(phase_feature_abs_dev, {idx + 1}), 0.5D)")
        .cast("double")
        .alias(f"phase_mad_{idx}")
        for idx in range(feature_count)
    ]
    mad_df = with_medians_df.groupBy("tail_id", "flight_id").agg(
        F.sum(F.when(F.col("phase_is_stable_raw"), F.lit(1)).otherwise(F.lit(0)))
        .cast("int")
        .alias("stable_window_count_raw"),
        *mad_exprs,
    )
    return (
        medians_df.join(mad_df, on=["tail_id", "flight_id"], how="inner")
        .withColumn(
            "phase_feature_scales",
            F.array(
                *[
                    F.when(F.coalesce(F.col(f"phase_mad_{idx}"), F.lit(0.0)) > F.lit(1e-6), F.col(f"phase_mad_{idx}"))
                    .otherwise(F.lit(1.0))
                    .cast("double")
                    for idx in range(feature_count)
                ]
            ),
        )
        .withColumn(
            "stable_window_count_effective",
            F.when(F.col("stable_window_count_raw") > F.lit(0), F.col("stable_window_count_raw"))
            .otherwise(F.col("flight_window_count"))
            .cast("int"),
        )
        .withColumn(
            "effective_phase_count",
            F.least(
                F.lit(max(int(config.phase_count), 1)),
                F.greatest(F.col("flight_window_count"), F.lit(1)),
            ).cast("int"),
        )
        .withColumn(
            "dwell_limit",
            F.least(
                F.lit(max(int(config.phase_min_dwell_windows), 1)),
                F.greatest(
                    F.floor(
                        F.col("flight_window_count")
                        / F.greatest(F.col("effective_phase_count") * F.lit(2), F.lit(1))
                    ).cast("int"),
                    F.lit(1),
                ),
            ).cast("int"),
        )
        .withColumn(
            "can_refine_centroids",
            (F.col("effective_phase_count") > F.lit(1))
            & (F.col("flight_window_count") > F.col("effective_phase_count")),
        )
        .drop(*[f"phase_mad_{idx}" for idx in range(feature_count)])
    )


def build_scaled_phase_observations(
    *,
    feature_frame: PhaseFeatureFrame,
    feature_count: int,
    config: PhasePlanConfig,
) -> tuple[PhaseObservationFrame, "DataFrame", "DataFrame"]:
    observation_frame = PhaseObservationFrame.from_feature_frame(feature_frame)
    stats_df = build_flight_stats(observation_frame, feature_count=feature_count, config=config)
    scaled_df = scale_phase_observations(observation_frame.dataframe, stats_df)
    return observation_frame, stats_df, scaled_df


def scale_phase_observations(observation_df: "DataFrame", stats_df: "DataFrame") -> "DataFrame":
    """Apply persisted phase feature scaling statistics to observation rows."""
    from pyspark.sql import functions as F

    return (
        observation_df.join(
            stats_df.select(
                "tail_id",
                "flight_id",
                "flight_window_count",
                "drift_threshold",
                "phase_feature_medians",
                "phase_feature_scales",
                "effective_phase_count",
                "stable_window_count_raw",
                "stable_window_count_effective",
                "dwell_limit",
                "can_refine_centroids",
            ),
            on=["tail_id", "flight_id"],
            how="inner",
        )
        .withColumn(
            "phase_is_stable_raw",
            F.coalesce(F.col("drift_magnitude_profiled"), F.lit(0.0))
            <= F.coalesce(F.col("drift_threshold"), F.lit(0.0)),
        )
        .withColumn(
            "s_w_scaled",
            F.zip_with(
                F.zip_with("s_w", "phase_feature_medians", lambda value, median: value - median),
                "phase_feature_scales",
                lambda value, scale: value / scale,
            ),
        )
        .drop("phase_feature_medians", "phase_feature_scales")
    )


def build_fit_source(scaled_df: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    fit_window = Window.partitionBy("tail_id", "flight_id").orderBy("phase_row_number")
    fit_source_df = (
        with_progress_mass_coordinates(scaled_df)
        .withColumn("fit_rank", F.row_number().over(fit_window).cast("int"))
        .withColumn(
            "count_seed_phase_id",
            F.floor(
                ((F.col("fit_rank") - F.lit(1)) * F.col("effective_phase_count"))
                / F.greatest(F.col("flight_window_count"), F.lit(1))
            ).cast("int"),
        )
        .withColumn(
            "progress_mass_seed_phase_id",
            F.least(
                F.floor(
                    F.col("progress_mass_position") * F.col("effective_phase_count")
                ),
                F.col("effective_phase_count") - F.lit(1),
            ).cast("int"),
        )
    )
    progress_mass_counts_df = fit_source_df.groupBy("tail_id", "flight_id").agg(
        F.countDistinct("progress_mass_seed_phase_id").cast("int").alias("progress_mass_seed_phase_count"),
        F.first("effective_phase_count").cast("int").alias("effective_phase_count_check"),
    )
    return (
        fit_source_df.join(progress_mass_counts_df, on=["tail_id", "flight_id"], how="inner")
        .withColumn(
            "seed_phase_id",
            F.when(
                F.col("progress_mass_seed_phase_count") == F.col("effective_phase_count_check"),
                F.col("progress_mass_seed_phase_id"),
            )
            .otherwise(F.col("count_seed_phase_id"))
            .cast("int"),
        )
        .drop(
            "progress_mass_seed_phase_id",
            "progress_mass_seed_phase_count",
            "effective_phase_count_check",
        )
    )


def build_seed_centroids(
    fit_source_df: "DataFrame",
    *,
    feature_count: int,
) -> tuple["DataFrame", "DataFrame"]:
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    bucket_means_df = fit_source_df.groupBy("tail_id", "flight_id", "seed_phase_id").agg(
        F.array(
            *[F.avg(F.element_at("s_w_scaled", F.lit(idx + 1)).cast("double")).cast("double") for idx in range(feature_count)]
        ).alias("seed_bucket_mean"),
        F.count(F.lit(1)).cast("int").alias("seed_bucket_count"),
    )
    seed_bucket_counts_df = bucket_means_df.select(
        "tail_id",
        "flight_id",
        F.col("seed_phase_id").cast("int").alias("phase_id_detected"),
        F.col("seed_bucket_count").cast("int").alias("seed_bucket_count"),
    )
    seed_pick_window = Window.partitionBy("tail_id", "flight_id", "seed_phase_id").orderBy(
        F.col("seed_bucket_distance").asc(),
        F.coalesce(F.abs(F.col("drift_magnitude_profiled")), F.lit(1e12)).asc(),
        F.col("phase_row_number").asc(),
        F.col("win_id").asc(),
    )
    centroids_df = (
        fit_source_df.join(bucket_means_df, on=["tail_id", "flight_id", "seed_phase_id"], how="inner")
        .withColumn("seed_bucket_distance", array_distance(F.col("s_w_scaled"), F.col("seed_bucket_mean")))
        .withColumn("seed_pick_rank", F.row_number().over(seed_pick_window).cast("int"))
        .where(F.col("seed_pick_rank") == F.lit(1))
        .select(
            "tail_id",
            "flight_id",
            F.col("seed_phase_id").cast("int").alias("phase_id_detected"),
            F.col("s_w_scaled").alias("s_w_centroid"),
        )
    ).localCheckpoint(eager=True)
    return centroids_df, seed_bucket_counts_df


def build_fit_assignments(fit_source_df: "DataFrame", centroids_df: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    fit_candidates_df = (
        fit_source_df.join(centroids_df, on=["tail_id", "flight_id"], how="inner")
        .withColumn("phase_fit_distance", array_distance(F.col("s_w_scaled"), F.col("s_w_centroid")))
    )
    assignment_window = Window.partitionBy("tail_id", "flight_id", "win_id").orderBy(
        F.col("phase_fit_distance").asc(),
        F.col("phase_id_detected").asc(),
    )
    return (
        fit_candidates_df.withColumn("phase_fit_rank", F.row_number().over(assignment_window).cast("int"))
        .where(F.col("phase_fit_rank") == F.lit(1))
    )


def refine_centroids(
    *,
    stats_df: "DataFrame",
    fit_source_df: "DataFrame",
    centroids_df: "DataFrame",
    feature_count: int,
    config: PhasePlanConfig,
) -> "DataFrame":
    from pyspark.sql import functions as F

    refine_keys_df = stats_df.where(F.col("can_refine_centroids")).select("tail_id", "flight_id")
    if not int(refine_keys_df.limit(1).count()):
        return centroids_df

    fixed_centroids_df = centroids_df.join(refine_keys_df, on=["tail_id", "flight_id"], how="left_anti")
    refine_source_df = fit_source_df.join(refine_keys_df, on=["tail_id", "flight_id"], how="inner")
    refine_centroids_df = centroids_df.join(refine_keys_df, on=["tail_id", "flight_id"], how="inner").localCheckpoint(
        eager=True
    )
    for _ in range(max(int(config.max_iter), 1)):
        fit_assignments_df = build_fit_assignments(refine_source_df, refine_centroids_df)
        updated_centroids_df = fit_assignments_df.groupBy("tail_id", "flight_id", "phase_id_detected").agg(
            F.array(
                *[
                    F.avg(F.element_at("s_w_scaled", F.lit(idx + 1)).cast("double")).cast("double")
                    for idx in range(feature_count)
                ]
            ).alias("s_w_centroid")
        )
        next_refine_centroids_df = (
            refine_centroids_df.alias("current")
            .join(
                updated_centroids_df.alias("updated"),
                on=["tail_id", "flight_id", "phase_id_detected"],
                how="left",
            )
            .select(
                "tail_id",
                "flight_id",
                "phase_id_detected",
                F.coalesce(F.col("updated.s_w_centroid"), F.col("current.s_w_centroid")).alias("s_w_centroid"),
            )
        ).localCheckpoint(eager=True)
        centroid_shift_df = refine_centroids_df.alias("current").join(
            next_refine_centroids_df.alias("next"),
            on=["tail_id", "flight_id", "phase_id_detected"],
            how="inner",
        ).select(
            array_distance(F.col("current.s_w_centroid"), F.col("next.s_w_centroid")).alias("centroid_shift")
        )
        refine_centroids_df = next_refine_centroids_df
        if not int(centroid_shift_df.where(F.col("centroid_shift") > F.lit(1e-9)).limit(1).count()):
            break
    return fixed_centroids_df.unionByName(refine_centroids_df).localCheckpoint(eager=True)


def build_cluster_outputs(
    centroids_df: "DataFrame",
    fit_source_df: "DataFrame",
    fit_assignments_df: "DataFrame",
) -> tuple["DataFrame", "DataFrame", "DataFrame"]:
    from pyspark.sql import functions as F

    fit_window_counts_df = (
        fit_assignments_df.groupBy("tail_id", "flight_id", "phase_id_detected")
        .agg(
            F.count(F.lit(1)).cast("int").alias("fit_window_count"),
        )
    )
    ordered_centroids_df = (
        centroids_df.join(fit_window_counts_df, on=["tail_id", "flight_id", "phase_id_detected"], how="inner")
        .select(
            "tail_id",
            "flight_id",
            F.col("phase_id_detected").cast("int").alias("phase_id_detected"),
            "s_w_centroid",
            F.col("fit_window_count").cast("int").alias("fit_window_count"),
        )
    )
    flight_scale_df = fit_assignments_df.groupBy("tail_id", "flight_id").agg(
        F.expr("percentile(phase_fit_distance, 0.75D)").cast("double").alias("flight_distance_scale")
    )
    distance_scales_df = (
        fit_assignments_df.groupBy("tail_id", "flight_id", "phase_id_detected")
        .agg(F.expr("percentile(phase_fit_distance, 0.9D)").cast("double").alias("distance_scale"))
        .join(
            fit_window_counts_df.select(
                "tail_id",
                "flight_id",
                F.col("phase_id_detected").cast("int").alias("phase_id_detected"),
                F.col("fit_window_count").cast("int").alias("fit_window_count"),
            ),
            on=["tail_id", "flight_id", "phase_id_detected"],
            how="inner",
        )
        .join(flight_scale_df, on=["tail_id", "flight_id"], how="left")
        .withColumn(
            "flight_distance_scale",
            F.greatest(F.coalesce(F.col("flight_distance_scale"), F.lit(1.0)), F.lit(1e-3)),
        )
        .withColumn(
            "distance_scale_floor",
            F.greatest(F.col("flight_distance_scale") * F.lit(0.5), F.lit(1e-3)),
        )
        .select(
            "tail_id",
            "flight_id",
            "phase_id_detected",
            F.when(
                F.col("fit_window_count") <= F.lit(1),
                F.col("flight_distance_scale"),
            )
            .otherwise(
                F.greatest(
                    F.coalesce(F.col("distance_scale"), F.col("flight_distance_scale")),
                    F.col("distance_scale_floor"),
                )
            )
            .cast("double")
            .alias("distance_scale"),
        )
    )
    transition_support_df = (
        fit_source_df.groupBy("tail_id", "flight_id", "seed_phase_id")
        .agg(
            F.first("effective_phase_count").cast("int").alias("effective_phase_count"),
            F.first("flight_window_count").cast("int").alias("flight_window_count"),
            F.min("progress_mass_position").cast("double").alias("phase_progress_start"),
            F.max("progress_mass_position").cast("double").alias("phase_progress_end"),
        )
        .withColumn(
            "phase_progress_center",
            ((F.col("phase_progress_start") + F.col("phase_progress_end")) / F.lit(2.0)).cast("double"),
        )
        .withColumn(
            "phase_progress_half_width",
            F.greatest(
                ((F.col("phase_progress_end") - F.col("phase_progress_start")) / F.lit(2.0)).cast("double"),
                (F.lit(1.0) / F.greatest((F.col("flight_window_count") - F.lit(1)).cast("double"), F.lit(1.0))).cast("double"),
            ).cast("double"),
        )
        .select(
            "tail_id",
            "flight_id",
            F.col("seed_phase_id").cast("int").alias("phase_id_detected"),
            F.col("phase_progress_start").cast("double").alias("phase_progress_start"),
            F.col("phase_progress_end").cast("double").alias("phase_progress_end"),
            F.col("phase_progress_center").cast("double").alias("phase_progress_center"),
            F.col("phase_progress_half_width").cast("double").alias("phase_progress_half_width"),
        )
    )
    return ordered_centroids_df, distance_scales_df, transition_support_df


@hot_path
def fit_cluster_model(
    feature_frame: PhaseFeatureFrame,
    *,
    config: PhasePlanConfig,
) -> tuple["DataFrame", PhaseClusterModel]:
    from pyspark.sql import functions as F

    feature_count = len(feature_frame.feature_names)
    _, stats_df, scaled_df = build_scaled_phase_observations(
        feature_frame=feature_frame,
        feature_count=feature_count,
        config=config,
    )
    fit_source_df = build_fit_source(scaled_df).localCheckpoint(eager=True)
    fit_source_stats_df = fit_source_df.groupBy("tail_id", "flight_id").agg(
        F.count(F.lit(1)).cast("int").alias("fit_source_window_count")
    )
    centroids_df, seed_bucket_counts_df = build_seed_centroids(
        fit_source_df,
        feature_count=feature_count,
    )
    centroids_df = refine_centroids(
        stats_df=stats_df,
        fit_source_df=fit_source_df,
        centroids_df=centroids_df,
        feature_count=feature_count,
        config=config,
    )
    fit_assignments_df = build_fit_assignments(fit_source_df, centroids_df)
    ordered_centroids_df, distance_scales_df, transition_support_df = build_cluster_outputs(
        centroids_df,
        fit_source_df,
        fit_assignments_df,
    )
    return scaled_df, PhaseClusterModel(
        feature_stats_df=stats_df,
        centroids_df=ordered_centroids_df,
        distance_scales_df=distance_scales_df,
        transition_model=PhaseTransitionModel(support_df=transition_support_df),
        fit_source_stats_df=fit_source_stats_df,
        seed_bucket_counts_df=seed_bucket_counts_df,
    )


if TYPE_CHECKING:
    from pyspark.sql import DataFrame
