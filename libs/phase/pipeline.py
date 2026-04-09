"""Spark-first orchestration for phase fitting and artifact assembly."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from libs.perf.annotations import hot_path
from libs.phase.config_fit import (
    fit_phase_feature_config_from_window_features_spark,
    fit_phase_feature_config_with_diagnostics_from_window_features_spark,
)
from libs.phase.decode import build_assignment_input, enforce_min_dwell, assign_phases_segmented
from libs.phase.feature_config import PhaseFeatureConfig
from libs.phase.fit import fit_cluster_model
from libs.phase.frames import PhaseFeatureFrame, PhaseObservationFrame
from libs.phase.tables import PhaseBaselinesTable, PhaseWindowsTable
from libs.phase.types import (
    PhaseArtifactSet,
    PhaseClusterModel,
    PhaseDetectionRun,
    PhaseFeatureSelectionDiagnostics,
    PhaseFeatureSelectionPolicy,
    PhasePlanConfig,
)


def _collect_grouped_phase_counts(
    df: "DataFrame",
    *,
    phase_column: str,
) -> dict[tuple[str, str], list[dict[str, int]]]:
    from pyspark.sql import functions as F

    rows = (
        df.groupBy("tail_id", "flight_id", F.col(phase_column).cast("int").alias("phase_id_detected"))
        .agg(F.count(F.lit(1)).cast("int").alias("window_count"))
        .orderBy("tail_id", "flight_id", "phase_id_detected")
        .collect()
    )
    grouped: dict[tuple[str, str], list[dict[str, int]]] = {}
    for row in rows:
        key = (str(row["tail_id"]), str(row["flight_id"]))
        grouped.setdefault(key, []).append(
            {
                "phase_id_detected": int(row["phase_id_detected"]),
                "window_count": int(row["window_count"]),
            }
        )
    return grouped


def _collect_phase_fit_diagnostics(
    *,
    cluster_model: PhaseClusterModel,
    assignment_input_df: "DataFrame",
    assigned_df: "DataFrame",
    merged_df: "DataFrame",
) -> dict[str, object]:
    flight_rows = (
        cluster_model.feature_stats_df.select(
            "tail_id",
            "flight_id",
            "flight_window_count",
            "stable_window_count_raw",
            "effective_phase_count",
        )
        .orderBy("tail_id", "flight_id")
        .collect()
    )
    fit_source_counts: dict[tuple[str, str], int] = {}
    if cluster_model.fit_source_stats_df is not None:
        for row in cluster_model.fit_source_stats_df.orderBy("tail_id", "flight_id").collect():
            fit_source_counts[(str(row["tail_id"]), str(row["flight_id"]))] = int(row["fit_source_window_count"])
    seed_bucket_counts: dict[tuple[str, str], list[dict[str, int]]] = {}
    if cluster_model.seed_bucket_counts_df is not None:
        for row in cluster_model.seed_bucket_counts_df.orderBy("tail_id", "flight_id", "phase_id_detected").collect():
            key = (str(row["tail_id"]), str(row["flight_id"]))
            seed_bucket_counts.setdefault(key, []).append(
                {
                    "phase_id_detected": int(row["phase_id_detected"]),
                    "seed_bucket_count": int(row["seed_bucket_count"]),
                }
            )
    phase_progress_support: dict[tuple[str, str], list[dict[str, float | int]]] = {}
    for row in cluster_model.transition_model.support_df.orderBy("tail_id", "flight_id", "phase_id_detected").collect():
        key = (str(row["tail_id"]), str(row["flight_id"]))
        phase_progress_support.setdefault(key, []).append(
            {
                "phase_id_detected": int(row["phase_id_detected"]),
                "phase_progress_start": float(row["phase_progress_start"]),
                "phase_progress_end": float(row["phase_progress_end"]),
                "phase_progress_center": float(row["phase_progress_center"]),
                "phase_progress_half_width": float(row["phase_progress_half_width"]),
            }
        )
    raw_assignment_counts = _collect_grouped_phase_counts(assignment_input_df, phase_column="raw_phase_id")
    post_decode_counts = _collect_grouped_phase_counts(assigned_df, phase_column="phase_id_detected")
    post_dwell_counts = _collect_grouped_phase_counts(merged_df, phase_column="phase_id_detected")

    return {
        "phase_fit_flights": [
            {
                "tail_id": str(row["tail_id"]),
                "flight_id": str(row["flight_id"]),
                "flight_window_count": int(row["flight_window_count"]),
                "fit_source_window_count": fit_source_counts.get((str(row["tail_id"]), str(row["flight_id"]))),
                "stable_window_count_raw": int(row["stable_window_count_raw"]),
                "effective_phase_count": int(row["effective_phase_count"]),
                "canonical_phase_order_source": cluster_model.transition_model.canonical_order_source,
                "transition_policy_name": cluster_model.transition_model.policy_name,
                "progress_support_source": cluster_model.transition_model.progress_support_source,
                "seed_bucket_counts": seed_bucket_counts.get((str(row["tail_id"]), str(row["flight_id"])), []),
                "phase_progress_support_by_phase_id": phase_progress_support.get(
                    (str(row["tail_id"]), str(row["flight_id"])),
                    [],
                ),
                "raw_assignment_counts_by_phase_id": raw_assignment_counts.get(
                    (str(row["tail_id"]), str(row["flight_id"])),
                    [],
                ),
                "post_decode_assignment_counts_by_phase_id": post_decode_counts.get(
                    (str(row["tail_id"]), str(row["flight_id"])),
                    [],
                ),
                "post_dwell_assignment_counts_by_phase_id": post_dwell_counts.get(
                    (str(row["tail_id"]), str(row["flight_id"])),
                    [],
                ),
            }
            for row in flight_rows
        ]
    }


@dataclass(frozen=True)
class PhaseDetectionPlan(PhasePlanConfig):
    @staticmethod
    def _phase_config(value: "PhaseFeatureConfig | dict[str, Any]") -> PhaseFeatureConfig:
        return PhaseFeatureConfig.coerce(value)

    def feature_selection_policy(self) -> PhaseFeatureSelectionPolicy:
        return PhaseFeatureSelectionPolicy(
            sensor_count=self.phase_detect_sensor_count,
            event_type_count=self.phase_detect_event_type_count,
            categorical_state_count=self.phase_detect_categorical_state_count,
        )

    @hot_path
    def fit_phase_feature_config(
        self,
        window_features_df: "DataFrame",
        *,
        backbone_df: "DataFrame",
    ) -> dict[str, object]:
        backbone_row = backbone_df.first()
        backbone_payload = backbone_row.asDict(recursive=True) if backbone_row is not None else {}
        if not backbone_payload:
            raise ValueError("fit_phase_feature_config_from_spark requires a non-empty backbone dataframe")
        return fit_phase_feature_config_from_window_features_spark(
            window_features_df,
            backbone_row=backbone_payload,
            selection_policy=self.feature_selection_policy(),
        ).to_dict()

    @hot_path
    def fit_phase_feature_config_with_diagnostics(
        self,
        window_features_df: "DataFrame",
        *,
        backbone_df: "DataFrame",
    ) -> tuple[dict[str, object], PhaseFeatureSelectionDiagnostics]:
        backbone_row = backbone_df.first()
        backbone_payload = backbone_row.asDict(recursive=True) if backbone_row is not None else {}
        if not backbone_payload:
            raise ValueError("fit_phase_feature_config_from_spark requires a non-empty backbone dataframe")
        phase_config, diagnostics = fit_phase_feature_config_with_diagnostics_from_window_features_spark(
            window_features_df,
            backbone_row=backbone_payload,
            selection_policy=self.feature_selection_policy(),
        )
        return phase_config.to_dict(), diagnostics

    def build_feature_frame(
        self,
        window_features_df: "DataFrame",
        *,
        phase_config: "PhaseFeatureConfig | dict[str, Any]",
    ) -> PhaseFeatureFrame:
        return PhaseFeatureFrame.from_window_features_df(
            window_features_df,
            phase_config=self._phase_config(phase_config),
        )

    def build_observation_frame(self, feature_frame: PhaseFeatureFrame) -> PhaseObservationFrame:
        return PhaseObservationFrame.from_feature_frame(feature_frame)

    @staticmethod
    def _checkpoint(df: "DataFrame") -> "DataFrame":
        return df.localCheckpoint(eager=True)

    def _fit_cluster_model(self, feature_frame: PhaseFeatureFrame) -> tuple["DataFrame", PhaseClusterModel]:
        return fit_cluster_model(feature_frame, config=self)

    @hot_path
    def run_detection(
        self,
        window_features_df: "DataFrame",
        *,
        phase_config: "PhaseFeatureConfig | dict[str, Any]",
    ) -> PhaseDetectionRun:
        return self._run_detection(
            window_features_df,
            phase_config=self._phase_config(phase_config),
        )

    @hot_path
    def _run_detection(
        self,
        window_features_df: "DataFrame",
        *,
        phase_config: PhaseFeatureConfig,
    ) -> PhaseDetectionRun:
        feature_frame = self.build_feature_frame(window_features_df, phase_config=phase_config)
        scaled_df, cluster_model = fit_cluster_model(feature_frame, config=self)
        scaled_df = self._checkpoint(scaled_df)
        assignment_input_df = build_assignment_input(scaled_df, cluster_model=cluster_model)
        assignment_input_df = self._checkpoint(assignment_input_df)
        assigned_df = assign_phases_segmented(assignment_input_df, cluster_model=cluster_model, config=self)
        assigned_df = self._checkpoint(assigned_df)
        merged_df = enforce_min_dwell(assigned_df, config=self)
        merged_df = self._checkpoint(merged_df)
        diagnostics = _collect_phase_fit_diagnostics(
            cluster_model=cluster_model,
            assignment_input_df=assignment_input_df,
            assigned_df=assigned_df,
            merged_df=merged_df,
        )
        phase_windows = PhaseWindowsTable.from_assignments(
            merged_df,
            feature_frame=feature_frame,
            phase_config=phase_config,
        )
        phase_windows_df = self._checkpoint(phase_windows.to_dataframe())
        return PhaseDetectionRun(
            phase_config=phase_config,
            feature_frame=feature_frame,
            cluster_model=cluster_model,
            phase_windows=PhaseWindowsTable(dataframe=phase_windows_df),
            diagnostics=diagnostics,
        )

    def build_phase_baselines(
        self,
        phase_windows: "PhaseWindowsTable | DataFrame",
        *,
        phase_config: "PhaseFeatureConfig | dict[str, Any]",
    ) -> PhaseBaselinesTable:
        phase_windows_df = phase_windows.to_dataframe() if isinstance(phase_windows, PhaseWindowsTable) else phase_windows
        return PhaseBaselinesTable.from_phase_windows(phase_windows_df, phase_config=phase_config)

    @hot_path
    def build_phase_windows(
        self,
        window_features_df: "DataFrame",
        *,
        phase_config: "PhaseFeatureConfig | dict[str, Any]",
    ) -> PhaseWindowsTable:
        return self.run_detection(window_features_df, phase_config=phase_config).phase_windows

    @hot_path
    def build(
        self,
        window_features_df: "DataFrame",
        *,
        backbone_df: "DataFrame",
    ) -> PhaseArtifactSet:
        phase_config = self._phase_config(self.fit_phase_feature_config(window_features_df, backbone_df=backbone_df))
        detection_run = self._run_detection(
            window_features_df,
            phase_config=phase_config,
        )
        phase_baselines = self.build_phase_baselines(detection_run.phase_windows, phase_config=phase_config)
        return PhaseArtifactSet(
            phase_windows=detection_run.phase_windows,
            phase_baselines=phase_baselines,
            phase_config=phase_config,
            feature_frame=detection_run.feature_frame,
            cluster_model=detection_run.cluster_model,
        )


@hot_path
def fit_phase_feature_config_from_spark(
    window_features_df: "DataFrame",
    *,
    backbone_df: "DataFrame",
    phase_detect_sensor_count: int = 8,
    phase_detect_event_type_count: int = 6,
    phase_detect_categorical_state_count: int = 6,
) -> dict[str, object]:
    return PhaseDetectionPlan(
        phase_count=1,
        phase_detect_sensor_count=phase_detect_sensor_count,
        phase_detect_event_type_count=phase_detect_event_type_count,
        phase_detect_categorical_state_count=phase_detect_categorical_state_count,
    ).fit_phase_feature_config(window_features_df, backbone_df=backbone_df)


@hot_path
def fit_phase_feature_config_with_diagnostics_from_spark(
    window_features_df: "DataFrame",
    *,
    backbone_df: "DataFrame",
    phase_detect_sensor_count: int = 8,
    phase_detect_event_type_count: int = 6,
    phase_detect_categorical_state_count: int = 6,
) -> tuple[dict[str, object], PhaseFeatureSelectionDiagnostics]:
    return PhaseDetectionPlan(
        phase_count=1,
        phase_detect_sensor_count=phase_detect_sensor_count,
        phase_detect_event_type_count=phase_detect_event_type_count,
        phase_detect_categorical_state_count=phase_detect_categorical_state_count,
    ).fit_phase_feature_config_with_diagnostics(window_features_df, backbone_df=backbone_df)


if TYPE_CHECKING:
    from pyspark.sql import DataFrame
