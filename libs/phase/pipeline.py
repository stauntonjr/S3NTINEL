"""Spark-first orchestration for phase fitting and artifact assembly."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from libs.perf.annotations import hot_path
from libs.phase.artifacts import build_phase_baselines, build_phase_windows_from_assignments
from libs.phase.config_fit import (
    fit_phase_feature_config_from_window_features_spark,
    fit_phase_feature_config_with_diagnostics_from_window_features_spark,
)
from libs.phase.decode import build_assignment_input, enforce_min_dwell, assign_phases_segmented
from libs.phase.feature_config import PhaseFeatureConfig
from libs.phase.fit import fit_cluster_model
from libs.phase.frames import PhaseFeatureFrame, PhaseObservationFrame
from libs.phase.types import (
    PhaseArtifactSet,
    PhaseClusterModel,
    PhaseDetectionRun,
    PhaseFeatureSelectionDiagnostics,
    PhaseFeatureSelectionPolicy,
    PhasePlanConfig,
)


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
    def _run_detection(
        self,
        window_features_df: "DataFrame",
        *,
        phase_config: PhaseFeatureConfig,
    ) -> PhaseDetectionRun:
        feature_frame = self.build_feature_frame(window_features_df, phase_config=phase_config)
        scaled_df, cluster_model = fit_cluster_model(feature_frame, config=self)
        scaled_df = self._checkpoint(scaled_df)
        assignment_input_df = build_assignment_input(scaled_df, cluster_model=cluster_model, config=self)
        assignment_input_df = self._checkpoint(assignment_input_df)
        assigned_df = assign_phases_segmented(assignment_input_df, config=self)
        assigned_df = self._checkpoint(assigned_df)
        merged_df = enforce_min_dwell(assigned_df, config=self)
        merged_df = self._checkpoint(merged_df)
        phase_windows_df = build_phase_windows_from_assignments(
            merged_df,
            feature_frame=feature_frame,
            phase_config=phase_config,
        )
        phase_windows_df = self._checkpoint(phase_windows_df)
        return PhaseDetectionRun(
            phase_config=phase_config,
            feature_frame=feature_frame,
            cluster_model=cluster_model,
            phase_windows_df=phase_windows_df,
        )

    def build_phase_baselines(
        self,
        phase_windows_df: "DataFrame",
        *,
        phase_config: "PhaseFeatureConfig | dict[str, Any]",
    ) -> "DataFrame":
        return build_phase_baselines(phase_windows_df, phase_config=phase_config)

    @hot_path
    def build_phase_windows(
        self,
        window_features_df: "DataFrame",
        *,
        phase_config: "PhaseFeatureConfig | dict[str, Any]",
    ) -> "DataFrame":
        return self._run_detection(
            window_features_df,
            phase_config=self._phase_config(phase_config),
        ).phase_windows_df

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
        phase_baselines_df = self.build_phase_baselines(detection_run.phase_windows_df, phase_config=phase_config)
        return PhaseArtifactSet(
            phase_windows_df=detection_run.phase_windows_df,
            phase_baselines_df=phase_baselines_df,
            phase_config=phase_config,
            feature_frame=detection_run.feature_frame,
            cluster_model=detection_run.cluster_model,
        )


@hot_path
def build_phase_windows_spark_table(
    window_features_df: "DataFrame",
    *,
    phase_config: dict[str, object],
    phase_count: int,
    phase_stable_drift_quantile: float = 0.35,
    phase_smoothing_radius: int = 2,
    phase_transition_penalty: float = 1.5,
    phase_min_dwell_windows: int = 8,
) -> "DataFrame":
    return PhaseDetectionPlan(
        phase_count=max(int(phase_count), 1),
        phase_stable_drift_quantile=float(phase_stable_drift_quantile),
        phase_smoothing_radius=max(int(phase_smoothing_radius), 0),
        phase_transition_penalty=float(phase_transition_penalty),
        phase_min_dwell_windows=max(int(phase_min_dwell_windows), 1),
    ).build_phase_windows(window_features_df, phase_config=phase_config)


@hot_path
def build_phase_baselines_spark_table(
    phase_windows_df: "DataFrame",
    *,
    phase_config: dict[str, object],
) -> "DataFrame":
    return PhaseDetectionPlan(phase_count=1).build_phase_baselines(phase_windows_df, phase_config=phase_config)


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
