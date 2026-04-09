"""Phase artifact assembly from decoded assignments."""

from __future__ import annotations

from typing import TYPE_CHECKING

from libs.phase.feature_config import PhaseFeatureConfig
from libs.phase.utils import string_array_literal


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
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    assigned_phase_index_expr = F.col("phase_id_detected").cast("int") + F.lit(1)
    assigned_distance_expr = F.element_at("phase_raw_distances", assigned_phase_index_expr).cast("double")
    assigned_scale_expr = F.greatest(
        F.element_at("phase_distance_scales", assigned_phase_index_expr).cast("double"),
        F.lit(1e-6),
    )
    assigned_cost_expr = F.element_at("phase_costs", assigned_phase_index_expr).cast("double")
    assigned_vs_alt_costs = F.zip_with(
        F.sequence(F.lit(0), F.size("phase_costs") - F.lit(1)),
        F.col("phase_costs"),
        lambda pos, cost: F.when(pos != F.col("phase_id_detected"), cost.cast("double")),
    )
    assigned_second_best_cost_expr = F.array_min(
        F.filter(
            assigned_vs_alt_costs,
            lambda cost: cost.isNotNull(),
        )
    ).cast("double")
    assigned_margin_confidence_expr = F.when(
        F.size("phase_costs") <= F.lit(1),
        F.lit(1.0),
    ).otherwise(
        F.greatest(
            F.lit(0.0),
            (assigned_second_best_cost_expr - assigned_cost_expr)
            / F.greatest(assigned_second_best_cost_expr, F.lit(1e-6)),
        )
    )
    local_stable_expr = (
        (F.coalesce(F.col("drift_magnitude_profiled"), F.lit(0.0)) <= F.coalesce(F.col("drift_threshold"), F.lit(0.0)))
        & (assigned_distance_expr <= assigned_scale_expr)
    )
    order_window = Window.partitionBy("tail_id", "flight_id").orderBy(
        F.col("t_start").asc_nulls_last(),
        F.col("win_id").asc(),
    )
    stable_phase_id_expr = F.when(local_stable_expr, F.col("phase_id_detected").cast("int"))
    transition_context_df = (
        enriched_df.withColumn(
            "_previous_stable_phase_id_detected",
            F.last(stable_phase_id_expr, ignorenulls=True).over(
                order_window.rowsBetween(Window.unboundedPreceding, -1)
            ),
        )
        .withColumn(
            "_next_stable_phase_id_detected",
            F.first(stable_phase_id_expr, ignorenulls=True).over(
                order_window.rowsBetween(1, Window.unboundedFollowing)
            ),
        )
        .withColumn(
            "_is_transition_boundary",
            (~local_stable_expr)
            & F.col("_previous_stable_phase_id_detected").isNotNull()
            & F.col("_next_stable_phase_id_detected").isNotNull()
            & (F.col("_previous_stable_phase_id_detected") != F.col("_next_stable_phase_id_detected")),
        )
        .withColumn(
            "phase_state_detected",
            F.when(F.col("_is_transition_boundary"), F.lit("transition_region")).otherwise(F.lit("stable")),
        )
        .withColumn(
            "transition_from_phase_id_detected",
            F.when(
                F.col("_is_transition_boundary"),
                F.col("_previous_stable_phase_id_detected").cast("int"),
            ),
        )
        .withColumn(
            "transition_to_phase_id_detected",
            F.when(
                F.col("_is_transition_boundary"),
                F.col("_next_stable_phase_id_detected").cast("int"),
            ),
        )
    )
    return transition_context_df.select(
        F.col("tail_id").cast("string").alias("tail_id"),
        F.col("flight_id").cast("string").alias("flight_id"),
        F.col("win_id").cast("int").alias("win_id"),
        F.col("t_start").alias("t_start"),
        F.col("t_end").alias("t_end"),
        F.col("duration_ms").cast("int").alias("duration_ms"),
        F.col("event_count").cast("int").alias("event_count"),
        F.col("phase_id_detected").cast("int").alias("phase_id_detected"),
        F.col("phase_state_detected").cast("string").alias("phase_state_detected"),
        F.col("transition_from_phase_id_detected").cast("int").alias("transition_from_phase_id_detected"),
        F.col("transition_to_phase_id_detected").cast("int").alias("transition_to_phase_id_detected"),
        assigned_margin_confidence_expr.cast("double").alias("phase_confidence_detected"),
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
if TYPE_CHECKING:
    from pyspark.sql import Column
