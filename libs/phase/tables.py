"""Typed Spark tables for persisted phase artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from libs.io.schemas.phase import (
    PHASE_BASELINES_SCHEMA,
    PHASE_LABEL_CENTROIDS_SCHEMA,
    PHASE_REFERENCE_MODEL_SCHEMA,
    PHASE_WINDOWS_SCHEMA,
)
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
        from pyspark.sql import Window
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
        phase_window = Window.partitionBy("tail_id", "phase_id_detected")
        baseline_candidates = (
            phase_windows_df.withColumn(
                "baseline_source_tier",
                F.when(
                    (F.col("phase_state_detected") == F.lit("stable"))
                    & (F.col("phase_confidence_detected") >= F.lit(0.5)),
                    F.lit(0),
                )
                .when(F.col("phase_state_detected") == F.lit("stable"), F.lit(1))
                .when(F.col("phase_confidence_detected") >= F.lit(0.5), F.lit(2))
                .otherwise(F.lit(3))
                .cast("int"),
            )
            .withColumn(
                "baseline_source_mode",
                F.when(F.col("baseline_source_tier") == F.lit(0), F.lit("stable_high_confidence"))
                .when(F.col("baseline_source_tier") == F.lit(1), F.lit("stable"))
                .when(F.col("baseline_source_tier") == F.lit(2), F.lit("confident_transition"))
                .otherwise(F.lit("fallback_all")),
            )
            .withColumn("baseline_source_tier_min", F.min("baseline_source_tier").over(phase_window))
            .where(F.col("baseline_source_tier") == F.col("baseline_source_tier_min"))
            .drop("baseline_source_tier_min")
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
        medians = baseline_candidates.groupBy("tail_id", "phase_id_detected").agg(
            centroid_expr.alias("s_w_centroid"),
            F.expr("percentile(backbone_reconstruction_error, 0.5D)").cast("double").alias("reconstruction_median"),
            F.expr("percentile(distance_to_centroid_detected, 0.5D)").cast("double").alias("distance_median"),
            F.first("baseline_source_mode", ignorenulls=True).alias("baseline_source_mode"),
            F.count(F.lit(1)).cast("int").alias("baseline_window_count"),
            F.sum(F.when(F.col("phase_state_detected") == F.lit("stable"), F.lit(1)).otherwise(F.lit(0)))
            .cast("int")
            .alias("stable_window_count"),
        )
        deviations = (
            baseline_candidates.join(
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
                F.col("baseline_source_mode").cast("string").alias("baseline_source_mode"),
                F.col("baseline_window_count").cast("int").alias("baseline_window_count"),
                F.col("stable_window_count").cast("int").alias("stable_window_count"),
                feature_names_lit.alias("feature_names"),
                selected_sensors_lit.alias("selected_sensors_c"),
                selected_event_types_lit.alias("selected_event_types"),
                categorical_pairs_lit.alias("selected_categorical_state_pairs"),
                cooccurrence_pairs_lit.alias("selected_window_cooccurrence_pairs"),
                backbone_all_sensors_lit.alias("backbone_all_sensors"),
                backbone_weights_lit.alias("backbone_weights_b"),
                F.lit(3).cast("int").alias("version"),
            ),
        )


@dataclass(frozen=True)
class PhaseReferenceModelTable(Table):
    """Reusable fitted phase model for applying phase detection to new windows."""

    partition_by: tuple[str, ...] = ("tail_id",)

    @classmethod
    def spark_schema(cls):
        return PHASE_REFERENCE_MODEL_SCHEMA()

    @classmethod
    def from_detection_run(
        cls,
        detection_run: "PhaseDetectionRun",
    ) -> "PhaseReferenceModelTable":
        from pyspark.sql import functions as F

        config = detection_run.phase_config
        model = detection_run.cluster_model
        pair_type = "array<array<string>>"
        categorical_pairs = [list(item) for item in config.phase_selected_categorical_state_pairs]
        cooccurrence_pairs = [list(item) for item in config.phase_selected_window_cooccurrence_pairs]
        return cls(
            dataframe=(
                model.centroids_df.join(
                    model.feature_stats_df.select(
                        "tail_id",
                        "flight_id",
                        "phase_feature_medians",
                        "phase_feature_scales",
                        "drift_threshold",
                        "flight_window_count",
                        "stable_window_count_raw",
                        "stable_window_count_effective",
                        "effective_phase_count",
                        "dwell_limit",
                        "can_refine_centroids",
                    ),
                    on=["tail_id", "flight_id"],
                    how="inner",
                )
                .join(
                    model.distance_scales_df.select(
                        "tail_id", "flight_id", "phase_id_detected", "distance_scale"
                    ),
                    on=["tail_id", "flight_id", "phase_id_detected"],
                    how="inner",
                )
                .join(
                    model.transition_model.support_df,
                    on=["tail_id", "flight_id", "phase_id_detected"],
                    how="inner",
                )
                .select(
                    "tail_id",
                    "flight_id",
                    "phase_id_detected",
                    "phase_feature_medians",
                    "phase_feature_scales",
                    "drift_threshold",
                    "flight_window_count",
                    "stable_window_count_raw",
                    "stable_window_count_effective",
                    "effective_phase_count",
                    "dwell_limit",
                    "can_refine_centroids",
                    "s_w_centroid",
                    "distance_scale",
                    "phase_progress_start",
                    "phase_progress_end",
                    "phase_progress_center",
                    "phase_progress_half_width",
                    F.lit(list(config.phase_selected_sensors)).cast("array<string>").alias(
                        "phase_selected_sensors"
                    ),
                    F.lit(list(config.phase_selected_event_types)).cast("array<string>").alias(
                        "phase_selected_event_types"
                    ),
                    F.lit(categorical_pairs).cast(pair_type).alias("phase_selected_categorical_state_pairs"),
                    F.lit(cooccurrence_pairs).cast(pair_type).alias(
                        "phase_selected_window_cooccurrence_pairs"
                    ),
                    F.lit(list(config.backbone_model.selected_sensors_c)).cast("array<string>").alias(
                        "selected_sensors_c"
                    ),
                    F.lit(list(config.backbone_model.all_sensors)).cast("array<string>").alias(
                        "backbone_all_sensors"
                    ),
                    F.lit(config.backbone_weights_rows).cast("array<array<double>>").alias(
                        "backbone_weights_b"
                    ),
                    F.lit(float(config.backbone_model.lambda_ridge)).alias("backbone_lambda_ridge"),
                    F.lit(int(config.backbone_model.training_window_count)).cast("int").alias(
                        "backbone_training_window_count"
                    ),
                    F.lit(int(config.backbone_model.backbone_version)).cast("int").alias("backbone_version"),
                    F.lit(1).cast("int").alias("version"),
                )
            )
        )


@dataclass(frozen=True)
class PhaseLabelCentroidsTable(Table):
    partition_by: tuple[str, ...] = ("tail_id",)

    @classmethod
    def spark_schema(cls):
        return PHASE_LABEL_CENTROIDS_SCHEMA()

    @classmethod
    def from_phase_windows_and_labels(
        cls,
        phase_windows_df: "DataFrame",
        phase_labels_df: "DataFrame",
    ) -> "PhaseLabelCentroidsTable":
        from pyspark.sql import Window
        from pyspark.sql import functions as F
        from libs.common import empty_array

        join_keys = ["tail_id", "flight_id", "win_id"]
        overlapping_labels = (
            phase_windows_df.select("tail_id", "flight_id", "win_id", "t_start", "t_end")
            .join(
                phase_labels_df.select("tail_id", "flight_id", "timestamp_utc", "phase_label"),
                on=["tail_id", "flight_id"],
                how="inner",
            )
            .where(
                (F.col("timestamp_utc") >= F.col("t_start"))
                & (F.col("timestamp_utc") <= F.col("t_end"))
                & F.col("phase_label").isNotNull()
            )
            .groupBy(*join_keys, "phase_label")
            .agg(F.count(F.lit(1)).cast("int").alias("label_vote_count"))
        )
        majority_window = Window.partitionBy(*join_keys).orderBy(
            F.desc("label_vote_count"),
            F.asc("phase_label"),
        )
        window_labels = (
            overlapping_labels.withColumn("label_rank", F.row_number().over(majority_window))
            .where(F.col("label_rank") == F.lit(1))
            .select(*join_keys, F.col("phase_label").cast("string").alias("phase_label"))
        )
        labeled_windows = phase_windows_df.join(window_labels, on=join_keys, how="inner")

        feature_count_rows = (
            labeled_windows.select(F.size("s_w").cast("int").alias("feature_count"))
            .where(F.col("feature_count").isNotNull())
            .limit(1)
            .collect()
        )
        feature_count = int(feature_count_rows[0]["feature_count"]) if feature_count_rows else 0
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
        return cls(
            dataframe=labeled_windows.groupBy("tail_id", "phase_label").agg(
                centroid_expr.alias("s_w_centroid"),
                F.count(F.lit(1)).cast("int").alias("labeled_window_count"),
                F.countDistinct("flight_id").cast("int").alias("flight_count"),
                F.first("feature_names", ignorenulls=True).alias("feature_names"),
                F.first("selected_sensors_c", ignorenulls=True).alias("selected_sensors_c"),
                F.first("selected_event_types", ignorenulls=True).alias("selected_event_types"),
                F.first("selected_categorical_state_pairs", ignorenulls=True).alias("selected_categorical_state_pairs"),
                F.first("selected_window_cooccurrence_pairs", ignorenulls=True).alias("selected_window_cooccurrence_pairs"),
                F.first("backbone_all_sensors", ignorenulls=True).alias("backbone_all_sensors"),
            ).select(
                F.col("tail_id").cast("string").alias("tail_id"),
                F.col("phase_label").cast("string").alias("phase_label"),
                F.col("s_w_centroid").alias("s_w_centroid"),
                F.col("labeled_window_count").cast("int").alias("labeled_window_count"),
                F.col("flight_count").cast("int").alias("flight_count"),
                F.col("feature_names").alias("feature_names"),
                F.col("selected_sensors_c").alias("selected_sensors_c"),
                F.col("selected_event_types").alias("selected_event_types"),
                F.col("selected_categorical_state_pairs").alias("selected_categorical_state_pairs"),
                F.col("selected_window_cooccurrence_pairs").alias("selected_window_cooccurrence_pairs"),
                F.col("backbone_all_sensors").alias("backbone_all_sensors"),
                F.lit(1).cast("int").alias("version"),
            ),
        )


if TYPE_CHECKING:
    from typing import Any

    from pyspark.sql import DataFrame

    from libs.phase.feature_config import PhaseFeatureConfig
    from libs.phase.frames import PhaseFeatureFrame
    from libs.phase.types import PhaseDetectionRun
