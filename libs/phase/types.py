"""Phase artifact and plan dataclasses."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from libs.phase.tables import PhaseBaselinesTable, PhaseReferenceModelTable, PhaseWindowsTable
from libs.phase.utils import default_phase_segment_policy
from libs.spark_sequence import SequenceSegmentPolicy


@dataclass(frozen=True)
class PhaseTransitionModel:
    support_df: "DataFrame"
    policy_name: str = "monotone_progress_band"
    canonical_order_source: str = "seed_bucket"
    progress_support_source: str = "seed_progress_mass_position_span"
    allowed_transition_offsets: tuple[int, ...] = (0, 1)

    def transition_penalty_for_offset(self, offset: int, *, base_penalty: float) -> float:
        normalized_offset = abs(int(offset))
        if normalized_offset == 0:
            return 0.0
        return float(base_penalty) * float(normalized_offset)


@dataclass(frozen=True)
class PhaseClusterModel:
    feature_stats_df: "DataFrame"
    centroids_df: "DataFrame"
    distance_scales_df: "DataFrame"
    transition_model: PhaseTransitionModel
    fit_source_stats_df: "DataFrame | None" = None
    seed_bucket_counts_df: "DataFrame | None" = None


@dataclass(frozen=True)
class PhaseArtifactSet:
    phase_windows: PhaseWindowsTable
    phase_baselines: PhaseBaselinesTable
    phase_config: "PhaseFeatureConfig"
    feature_frame: "PhaseFeatureFrame | None" = None
    cluster_model: PhaseClusterModel | None = None
    reference_model: PhaseReferenceModelTable | None = None


@dataclass(frozen=True)
class PhaseDetectionRun:
    phase_config: "PhaseFeatureConfig"
    feature_frame: "PhaseFeatureFrame"
    cluster_model: PhaseClusterModel
    phase_windows: PhaseWindowsTable
    diagnostics: dict[str, Any] | None = None


@dataclass(frozen=True)
class PhaseFeatureSelectionPolicy:
    sensor_count: int = 8
    event_type_count: int = 6
    categorical_state_count: int = 6


@dataclass(frozen=True)
class PhaseSelectorDiagnostics:
    selector_name: str
    selected_count: int
    timing_ms: float
    candidate_count: int | None = None
    fallback_used: bool = False


@dataclass(frozen=True)
class PhaseFeatureSelectionDiagnostics:
    sensors: PhaseSelectorDiagnostics
    event_types: PhaseSelectorDiagnostics
    categorical_state_pairs: PhaseSelectorDiagnostics
    selected_event_types: list[str]
    selected_categorical_state_pairs: list[tuple[str, str]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "sensors": {
                "selector_name": self.sensors.selector_name,
                "selected_count": int(self.sensors.selected_count),
                "timing_ms": float(self.sensors.timing_ms),
                "candidate_count": None if self.sensors.candidate_count is None else int(self.sensors.candidate_count),
                "fallback_used": bool(self.sensors.fallback_used),
            },
            "event_types": {
                "selector_name": self.event_types.selector_name,
                "selected_count": int(self.event_types.selected_count),
                "timing_ms": float(self.event_types.timing_ms),
                "candidate_count": None
                if self.event_types.candidate_count is None
                else int(self.event_types.candidate_count),
                "fallback_used": bool(self.event_types.fallback_used),
            },
            "categorical_state_pairs": {
                "selector_name": self.categorical_state_pairs.selector_name,
                "selected_count": int(self.categorical_state_pairs.selected_count),
                "timing_ms": float(self.categorical_state_pairs.timing_ms),
                "candidate_count": None
                if self.categorical_state_pairs.candidate_count is None
                else int(self.categorical_state_pairs.candidate_count),
                "fallback_used": bool(self.categorical_state_pairs.fallback_used),
            },
            "selected_event_types": list(self.selected_event_types),
            "selected_categorical_state_pairs": [
                [str(parameter_name), str(state)] for parameter_name, state in self.selected_categorical_state_pairs
            ],
        }


@dataclass(frozen=True)
class PhasePlanConfig:
    phase_count: int
    phase_stable_drift_quantile: float = 0.35
    phase_transition_penalty: float = 1.5
    phase_min_dwell_windows: int = 8
    phase_detect_sensor_count: int = 8
    phase_detect_event_type_count: int = 6
    phase_detect_categorical_state_count: int = 6
    max_iter: int = 12
    segment_policy: SequenceSegmentPolicy = field(default_factory=default_phase_segment_policy)


if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from libs.phase.feature_config import PhaseFeatureConfig
    from libs.phase.frames import PhaseFeatureFrame
