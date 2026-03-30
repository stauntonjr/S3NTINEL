"""Typed Spark tables for persisted phase artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from libs.io.schemas.phase import PHASE_BASELINES_SCHEMA, PHASE_WINDOWS_SCHEMA
from libs.pyspark import Table


@dataclass(frozen=True)
class PhaseWindowsTable(Table):
    partition_by: tuple[str, ...] = ("tail_id",)

    @classmethod
    def spark_schema(cls):
        return PHASE_WINDOWS_SCHEMA()

    @classmethod
    def from_assignments(
        cls,
        assigned_df: "DataFrame",
        *,
        feature_frame: "PhaseFeatureFrame",
        phase_config: "PhaseFeatureConfig",
    ) -> "PhaseWindowsTable":
        from libs.phase.artifacts import phase_output_literals, join_phase_payload, select_phase_windows

        return cls(
            dataframe=select_phase_windows(
                enriched_df=join_phase_payload(assigned_df, payload_df=feature_frame.payload_dataframe()),
                phase_literals=phase_output_literals(phase_config),
            )
        )


@dataclass(frozen=True)
class PhaseBaselinesTable(Table):
    partition_by: tuple[str, ...] = ("tail_id",)

    @classmethod
    def spark_schema(cls):
        return PHASE_BASELINES_SCHEMA()

    @classmethod
    def from_phase_windows(
        cls,
        phase_windows_df: "DataFrame",
        *,
        phase_config: "PhaseFeatureConfig | dict[str, Any]",
    ) -> "PhaseBaselinesTable":
        from pyspark.sql import functions as F
        from libs.common import empty_array
        from libs.phase.feature_config import PhaseFeatureConfig
        from libs.phase.utils import double_matrix_literal, string_array_literal

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
                medians.select("tail_id", "phase_id_detected", "reconstruction_median", "distance_median"),
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
        return cls(
            dataframe=medians.join(mads, on=["tail_id", "phase_id_detected"], how="left").select(
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
            ),
        )


if TYPE_CHECKING:
    from typing import Any

    from pyspark.sql import DataFrame

    from libs.phase.feature_config import PhaseFeatureConfig
    from libs.phase.frames import PhaseFeatureFrame
