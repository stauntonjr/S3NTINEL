"""Phase artifact assembly from decoded assignments."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from libs.common import empty_array
from libs.phase.feature_config import PhaseFeatureConfig
from libs.phase.frames import PhaseFeatureFrame
from libs.phase.utils import double_matrix_literal, string_array_literal


def phase_output_literals(phase_config: PhaseFeatureConfig) -> dict[str, "Column"]:
    return {
        "feature_names": string_array_literal(phase_config.feature_names),
        "selected_sensors_c": string_array_literal(list(phase_config.backbone_model.selected_sensors_c)),
        "selected_event_types": string_array_literal(list(phase_config.phase_selected_event_types)),
        "selected_categorical_state_pairs": string_array_literal(list(phase_config.categorical_state_labels)),
        "selected_window_cooccurrence_pairs": string_array_literal(list(phase_config.window_cooccurrence_labels)),
        "backbone_all_sensors": string_array_literal(list(phase_config.backbone_model.all_sensors)),
    }


def join_phase_payload(assigned_df: "DataFrame", *, payload_df: "DataFrame") -> "DataFrame":
    return assigned_df.drop("t_end").join(payload_df, on=["tail_id", "flight_id", "win_id"], how="inner")


def select_phase_windows(
    *,
    enriched_df: "DataFrame",
    phase_literals: dict[str, "Column"],
) -> "DataFrame":
    from pyspark.sql import functions as F

    assigned_distance_expr = F.element_at("phase_raw_distances", F.col("phase_id_detected") + F.lit(1)).cast("double")
    assigned_scale_expr = F.greatest(
        F.element_at("phase_distance_scales", F.col("phase_id_detected") + F.lit(1)).cast("double"),
        F.lit(1e-6),
    )
    assigned_cost_expr = F.element_at("phase_costs", F.col("phase_id_detected") + F.lit(1)).cast("double")
    return enriched_df.select(
        F.col("tail_id").cast("string").alias("tail_id"),
        F.col("flight_id").cast("string").alias("flight_id"),
        F.col("win_id").cast("int").alias("win_id"),
        F.col("t_start").alias("t_start"),
        F.col("t_end").alias("t_end"),
        F.col("duration_ms").cast("int").alias("duration_ms"),
        F.col("event_count").cast("int").alias("event_count"),
        F.col("phase_id_detected").cast("int").alias("phase_id_detected"),
        F.when(
            (F.coalesce(F.col("drift_magnitude_profiled"), F.lit(0.0)) <= F.coalesce(F.col("drift_threshold"), F.lit(0.0)))
            & (assigned_distance_expr <= assigned_scale_expr),
            F.lit("stable"),
        )
        .otherwise(F.lit("transition_region"))
        .alias("phase_state_detected"),
        F.greatest(F.lit(0.0), F.lit(1.0) - assigned_cost_expr).cast("double").alias("phase_confidence_detected"),
        assigned_distance_expr.alias("distance_to_centroid_detected"),
        F.coalesce(F.col("drift_magnitude_profiled"), F.lit(0.0)).cast("double").alias("drift_magnitude"),
        F.coalesce(F.col("breadth"), F.lit(0.0)).cast("double").alias("breadth"),
        F.col("backbone_reconstruction_error").cast("double").alias("backbone_reconstruction_error"),
        F.col("backbone_residual_by_parameter").alias("backbone_residual_by_parameter"),
        F.col("x_c").alias("x_c"),
        F.col("s_w").alias("s_w"),
        F.col("date_utc").alias("date_utc"),
        phase_literals["feature_names"].alias("feature_names"),
        phase_literals["selected_sensors_c"].alias("selected_sensors_c"),
        phase_literals["selected_event_types"].alias("selected_event_types"),
        phase_literals["selected_categorical_state_pairs"].alias("selected_categorical_state_pairs"),
        phase_literals["selected_window_cooccurrence_pairs"].alias("selected_window_cooccurrence_pairs"),
        phase_literals["backbone_all_sensors"].alias("backbone_all_sensors"),
    )


def build_phase_windows_from_assignments(
    assigned_df: "DataFrame",
    *,
    feature_frame: PhaseFeatureFrame,
    phase_config: PhaseFeatureConfig,
) -> "DataFrame":
    return select_phase_windows(
        enriched_df=join_phase_payload(assigned_df, payload_df=feature_frame.payload_dataframe()),
        phase_literals=phase_output_literals(phase_config),
    )


def build_phase_baselines(
    phase_windows_df: "DataFrame",
    *,
    phase_config: "PhaseFeatureConfig | dict[str, Any]",
) -> "DataFrame":
    from pyspark.sql import functions as F

    config = PhaseFeatureConfig.coerce(phase_config)
    feature_count = len(config.feature_names)
    feature_names_lit = string_array_literal(config.feature_names)
    selected_sensors_lit = string_array_literal(list(config.backbone_model.selected_sensors_c))
    selected_event_types_lit = string_array_literal(list(config.phase_selected_event_types))
    categorical_pairs_lit = string_array_literal(list(config.categorical_state_labels))
    cooccurrence_pairs_lit = string_array_literal(list(config.window_cooccurrence_labels))
    backbone_all_sensors_lit = string_array_literal(list(config.backbone_model.all_sensors))
    backbone_weights_lit = double_matrix_literal(config.backbone_weights_rows)

    stable_windows = phase_windows_df.where(
        (F.col("phase_state_detected") == F.lit("stable"))
        & (F.col("phase_confidence_detected") >= F.lit(0.5))
    )
    centroid_expr = (
        F.array(
            *[
                F.avg(F.element_at("s_w", F.lit(index + 1)).cast("double")).cast("double")
                for index in range(feature_count)
            ]
        )
        if feature_count > 0
        else empty_array("double")
    )
    medians = stable_windows.groupBy("tail_id", "phase_id_detected").agg(
        centroid_expr.alias("s_w_centroid"),
        F.expr("percentile(backbone_reconstruction_error, 0.5D)").cast("double").alias("reconstruction_median"),
        F.expr("percentile(distance_to_centroid_detected, 0.5D)").cast("double").alias("distance_median"),
        F.count(F.lit(1)).cast("int").alias("stable_window_count"),
    )
    deviations = (
        stable_windows.join(
            medians.select(
                "tail_id",
                "phase_id_detected",
                "reconstruction_median",
                "distance_median",
            ),
            on=["tail_id", "phase_id_detected"],
            how="inner",
        )
        .withColumn(
            "reconstruction_abs_dev",
            F.abs(F.col("backbone_reconstruction_error") - F.col("reconstruction_median")),
        )
        .withColumn(
            "distance_abs_dev",
            F.abs(F.col("distance_to_centroid_detected") - F.col("distance_median")),
        )
    )
    mads = deviations.groupBy("tail_id", "phase_id_detected").agg(
        F.expr("percentile(reconstruction_abs_dev, 0.5D)").cast("double").alias("reconstruction_mad"),
        F.expr("percentile(distance_abs_dev, 0.5D)").cast("double").alias("distance_mad"),
    )
    return medians.join(mads, on=["tail_id", "phase_id_detected"], how="left").select(
        F.col("tail_id").cast("string").alias("tail_id"),
        F.col("phase_id_detected").cast("int").alias("phase_id_detected"),
        F.concat(F.lit("phase_"), F.col("phase_id_detected").cast("string")).alias("phase_name_detected"),
        F.col("s_w_centroid").alias("s_w_centroid"),
        F.col("reconstruction_median").cast("double").alias("reconstruction_median"),
        F.coalesce(F.col("reconstruction_mad"), F.lit(0.0)).cast("double").alias("reconstruction_mad"),
        F.col("distance_median").cast("double").alias("distance_median"),
        F.coalesce(F.col("distance_mad"), F.lit(0.0)).cast("double").alias("distance_mad"),
        F.col("stable_window_count").cast("int").alias("stable_window_count"),
        feature_names_lit.alias("feature_names"),
        selected_sensors_lit.alias("selected_sensors_c"),
        selected_event_types_lit.alias("selected_event_types"),
        categorical_pairs_lit.alias("selected_categorical_state_pairs"),
        cooccurrence_pairs_lit.alias("selected_window_cooccurrence_pairs"),
        backbone_all_sensors_lit.alias("backbone_all_sensors"),
        backbone_weights_lit.alias("backbone_weights_b"),
        F.lit(2).cast("int").alias("version"),
    )


if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame
