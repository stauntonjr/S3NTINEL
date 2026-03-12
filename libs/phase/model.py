from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from libs.backbone import BackboneModel, BackboneSpec
from libs.common.event_types import CATEGORICAL_EVENT_TYPES, CONTINUOUS_EVENT_TYPES
from libs.io.contracts import PhaseBaselineRow, PhaseWindowRow
from libs.phase.runtime import PhaseDetectionPolicy, PhaseStream
from libs.scoring.window_scores import build_phase_score_baselines
from libs.windows.features import WindowFeatureSelection, WindowFeatures


@dataclass(frozen=True)
class PhaseFeatureConfig:
    backbone_model: BackboneModel
    phase_selected_sensors: list[str]
    phase_selected_event_types: list[str]
    phase_selected_categorical_state_pairs: list[tuple[str, str]]
    phase_selected_window_cooccurrence_pairs: list[tuple[str, str]]

    @staticmethod
    def select_event_types_from_counts(counts: Counter[str], *, k: int) -> list[str]:
        limit = max(int(k), 0)
        if limit <= 0:
            return []
        continuous = [(event_type, count) for event_type, count in counts.most_common() if event_type in CONTINUOUS_EVENT_TYPES]
        categorical = [(event_type, count) for event_type, count in counts.most_common() if event_type in CATEGORICAL_EVENT_TYPES]
        continuous_k = max(limit // 2, 1) if continuous else 0
        categorical_k = max(limit - continuous_k, 0) if categorical else 0
        selected = [event_type for event_type, _ in continuous[:continuous_k]]
        selected.extend(event_type for event_type, _ in categorical[:categorical_k] if event_type not in selected)
        if len(selected) < limit:
            for event_type, _ in counts.most_common():
                if event_type in selected:
                    continue
                selected.append(event_type)
                if len(selected) >= limit:
                    break
        return selected

    @staticmethod
    def build_structure_vectors(
        windows: list[PhaseWindowRow],
        *,
        selected_sensors: list[str],
        selected_event_types: list[str] | None = None,
        selected_categorical_state_pairs: list[tuple[str, str]] | None = None,
    ) -> tuple[list[PhaseWindowRow], list[str]]:
        event_types = [str(item) for item in (selected_event_types or []) if str(item)]
        state_pairs = [
            (str(parameter_name), str(state))
            for parameter_name, state in (selected_categorical_state_pairs or [])
            if str(parameter_name) and str(state)
        ]
        feature_names = [f"parameter_name::{parameter_name}" for parameter_name in selected_sensors] + [
            f"event_type::{event_type}" for event_type in event_types
        ] + [
            f"categorical::{parameter_name}={state}" for parameter_name, state in state_pairs
        ] + [
            "summary::event_density_hz",
            "summary::continuous_event_fraction",
            "summary::categorical_event_fraction",
            "summary::active_sensor_fraction",
        ]

        structured: list[PhaseWindowRow] = []
        for window in windows:
            scaled = window.get("continuous_vector_t_end_scaled")
            if not isinstance(scaled, dict):
                scaled = {}
            event_counts = window.get("event_type_counts")
            if not isinstance(event_counts, dict):
                event_counts = {}
            categorical_t_end = window.get("categorical_state_t_end")
            if not isinstance(categorical_t_end, dict):
                categorical_t_end = {}
            event_total = max(int(window.get("event_count", 0) or 0), 0)
            duration_ms = max(int(window.get("duration_ms", 0) or 0), 1)
            duration_s = float(duration_ms) / 1000.0

            vector: list[float] = []
            for parameter_name in selected_sensors:
                vector.append(float(scaled.get(parameter_name, 0.0) or 0.0))
            for event_type in event_types:
                count = float(event_counts.get(event_type, 0) or 0.0)
                vector.append(count / float(max(event_total, 1)))
            for parameter_name, state in state_pairs:
                vector.append(1.0 if str(categorical_t_end.get(parameter_name, "")) == state else 0.0)
            continuous_count = float(sum(int(event_counts.get(item, 0) or 0) for item in CONTINUOUS_EVENT_TYPES))
            categorical_count = float(sum(int(event_counts.get(item, 0) or 0) for item in CATEGORICAL_EVENT_TYPES))
            active_sensor_fraction = float(len(scaled)) / float(max(len(selected_sensors), 1))
            vector.extend(
                [
                    float(event_total) / float(max(duration_s, 1e-6)),
                    continuous_count / float(max(event_total, 1)),
                    categorical_count / float(max(event_total, 1)),
                    active_sensor_fraction,
                ]
            )

            enriched = dict(window)
            enriched["s_w"] = vector
            structured.append(enriched)
        return structured, feature_names

    @classmethod
    def from_window_feature_rows(
        cls,
        window_feature_rows: list[dict[str, Any]],
        *,
        backbone_sensor_count: int = 8,
        backbone_ridge_lambda: float = 1.0,
        phase_detect_sensor_count: int = 8,
        phase_detect_event_type_count: int = 6,
        phase_detect_categorical_state_count: int = 6,
        phase_detect_window_cooccurrence_count: int = 0,
    ) -> "PhaseFeatureConfig":
        backbone_model, _ = BackboneModel.from_window_x_rows(
            window_feature_rows,
            spec=BackboneSpec(
                sensor_count=backbone_sensor_count,
                ridge_lambda=backbone_ridge_lambda,
            ),
        )
        return cls(
            backbone_model=backbone_model,
            phase_selected_sensors=backbone_model.selected_sensors_c[: max(int(phase_detect_sensor_count), 1)],
            phase_selected_event_types=WindowFeatures.top_phase_event_types(
                window_feature_rows, k=max(int(phase_detect_event_type_count), 0)
            ),
            phase_selected_categorical_state_pairs=WindowFeatures.top_categorical_state_pairs(
                window_feature_rows,
                k=max(int(phase_detect_categorical_state_count), 0),
            ),
            phase_selected_window_cooccurrence_pairs=WindowFeatures.top_cooccurrence_sensor_pairs(
                window_feature_rows,
                k=max(int(phase_detect_window_cooccurrence_count), 0),
            ),
        )

    @classmethod
    def from_dict(cls, row: dict[str, Any]) -> "PhaseFeatureConfig":
        return cls(
            backbone_model=BackboneModel(
                selected_sensors_c=[str(item) for item in row.get("selected_sensors_c", [])],
                all_sensors=[str(item) for item in row.get("all_sensors", [])],
                weights_b=np.asarray(row.get("weights_b", []), dtype=float),
                lambda_ridge=float(row.get("lambda_ridge", 1.0) or 1.0),
                training_window_count=int(row.get("training_window_count", 0) or 0),
                backbone_version=int(row.get("backbone_version", 2) or 2),
            ),
            phase_selected_sensors=[str(item) for item in row.get("phase_selected_sensors", [])],
            phase_selected_event_types=[str(item) for item in row.get("phase_selected_event_types", [])],
            phase_selected_categorical_state_pairs=[
                (str(parameter_name), str(state))
                for parameter_name, state in row.get("phase_selected_categorical_state_pairs", [])
            ],
            phase_selected_window_cooccurrence_pairs=[
                (str(left), str(right))
                for left, right in row.get("phase_selected_window_cooccurrence_pairs", [])
            ],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "selected_sensors_c": list(self.backbone_model.selected_sensors_c),
            "all_sensors": list(self.backbone_model.all_sensors),
            "weights_b": [[float(value) for value in row] for row in self.backbone_model.weights_b.tolist()],
            "lambda_ridge": float(self.backbone_model.lambda_ridge),
            "training_window_count": int(self.backbone_model.training_window_count),
            "backbone_version": int(self.backbone_model.backbone_version),
            "phase_selected_sensors": list(self.phase_selected_sensors),
            "phase_selected_event_types": list(self.phase_selected_event_types),
            "phase_selected_categorical_state_pairs": list(self.phase_selected_categorical_state_pairs),
            "phase_selected_window_cooccurrence_pairs": list(self.phase_selected_window_cooccurrence_pairs),
        }

    def enrich_window_feature_rows(self, window_feature_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        enriched_rows: list[dict[str, Any]] = []
        for row in window_feature_rows:
            enriched = dict(row)
            x_true = dict(enriched.get("continuous_vector_t_end_scaled", {}))
            x_hat = self.backbone_model.reconstruct_window_vector(x_true)
            error, residuals = self.backbone_model.reconstruction_error(x_true, x_hat)
            enriched["backbone_reconstruction_error"] = float(error)
            enriched["backbone_x_c"] = [
                float(x_true.get(parameter_name, 0.0) or 0.0)
                for parameter_name in self.backbone_model.selected_sensors_c
            ]
            enriched["backbone_residual_by_parameter"] = {
                str(parameter_name): float(residual)
                for parameter_name, residual in residuals.items()
            }
            enriched_rows.append(enriched)
        return enriched_rows

    def feature_selection(self) -> WindowFeatureSelection:
        return WindowFeatureSelection(
            selected_sensors_c=self.phase_selected_sensors,
            selected_event_types=self.phase_selected_event_types,
            selected_categorical_state_pairs=self.phase_selected_categorical_state_pairs,
            selected_cooccurrence_sensor_pairs=self.phase_selected_window_cooccurrence_pairs,
        )

    def encode_window_feature_rows(self, window_feature_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
        return self.feature_selection().encode_rows(self.enrich_window_feature_rows(window_feature_rows))


@dataclass(frozen=True)
class PhaseFeatures:
    phase_windows: list[PhaseWindowRow]
    phase_baselines: list[PhaseBaselineRow]

    @classmethod
    def from_window_feature_rows(
        cls,
        window_feature_rows: list[dict[str, Any]],
        *,
        config: PhaseFeatureConfig,
        phase_count: int,
        phase_stable_drift_quantile: float = 0.35,
        phase_smoothing_radius: int = 2,
        phase_transition_penalty: float = 1.5,
        phase_min_dwell_windows: int = 8,
    ) -> "PhaseFeatures":
        window_s_rows, feature_names = config.encode_window_feature_rows(window_feature_rows)
        phase_assignments, _ = PhaseStream.from_windows(
            window_s_rows,
            policy=PhaseDetectionPolicy(
                phase_count=max(int(phase_count), 1),
                stable_drift_quantile=float(phase_stable_drift_quantile),
                smoothing_radius=max(int(phase_smoothing_radius), 0),
                transition_penalty=float(phase_transition_penalty),
                min_dwell_windows=max(int(phase_min_dwell_windows), 1),
                ordered_phase_progression=True,
            ),
        ).detect()
        phase_score_baselines = build_phase_score_baselines(window_s_rows, phase_assignments)
        assignment_by_key = {
            (str(item.get("tail_id", "")), str(item.get("flight_id", "")), int(item.get("win_id", 0))): item
            for item in phase_assignments
        }

        return cls(
            phase_windows=cls.build_phase_window_rows(
                window_s_rows=window_s_rows,
                feature_names=feature_names,
                config=config,
                assignment_by_key=assignment_by_key,
            ),
            phase_baselines=cls.build_phase_baseline_rows(
                phase_score_baselines=phase_score_baselines,
                feature_names=feature_names,
                config=config,
            ),
        )

    @staticmethod
    def build_phase_window_rows(
        *,
        window_s_rows: list[dict[str, Any]],
        feature_names: list[str],
        config: PhaseFeatureConfig,
        assignment_by_key: dict[tuple[str, str, int], dict[str, Any]],
    ) -> list[PhaseWindowRow]:
        phase_windows: list[PhaseWindowRow] = []
        for row in window_s_rows:
            key = (str(row.get("tail_id", "")), str(row.get("flight_id", "")), int(row.get("win_id", 0)))
            assignment = assignment_by_key.get(key, {})
            phase_windows.append(
                {
                    "tail_id": str(row.get("tail_id", "")),
                    "flight_id": str(row.get("flight_id", "")),
                    "win_id": int(row.get("win_id", 0)),
                    "t_start": row.get("t_start"),
                    "t_end": row.get("t_end"),
                    "duration_ms": int(row.get("duration_ms", 0) or 0),
                    "event_count": int(row.get("event_count", 0) or 0),
                    "phase_id_detected": int(assignment.get("phase_id_detected", 0) or 0),
                    "phase_state_detected": str(assignment.get("phase_state_detected", "unknown")),
                    "phase_confidence_detected": float(assignment.get("phase_confidence_detected", 0.0) or 0.0),
                    "distance_to_centroid_detected": float(assignment.get("distance_to_centroid_detected", 0.0) or 0.0),
                    "drift_magnitude": float(row.get("drift_magnitude_profiled", 0.0) or 0.0),
                    "breadth": float(row.get("s_w", [0.0])[-1]) if row.get("s_w") else 0.0,
                    "backbone_reconstruction_error": float(row.get("backbone_reconstruction_error", 0.0) or 0.0),
                    "backbone_residual_by_parameter": {
                        str(parameter_name): float(residual)
                        for parameter_name, residual in dict(row.get("backbone_residual_by_parameter", {})).items()
                    },
                    "x_c": [float(item) for item in row.get("x_c", [])],
                    "s_w": [float(item) for item in row.get("s_w", [])],
                    "date_utc": row.get("date_utc"),
                    "feature_names": list(feature_names),
                    "selected_sensors_c": list(config.backbone_model.selected_sensors_c),
                    "selected_event_types": list(config.phase_selected_event_types),
                    "selected_categorical_state_pairs": [
                        f"{parameter_name}={state}" for parameter_name, state in config.phase_selected_categorical_state_pairs
                    ],
                    "selected_window_cooccurrence_pairs": [
                        f"{left}&{right}" for left, right in config.phase_selected_window_cooccurrence_pairs
                    ],
                    "backbone_all_sensors": list(config.backbone_model.all_sensors),
                }
            )
        return phase_windows

    @staticmethod
    def build_phase_baseline_rows(
        *,
        phase_score_baselines: list[dict[str, Any]],
        feature_names: list[str],
        config: PhaseFeatureConfig,
    ) -> list[PhaseBaselineRow]:
        phase_baselines: list[PhaseBaselineRow] = []
        for baseline in phase_score_baselines:
            phase_baselines.append(
                {
                    "tail_id": str(baseline.get("tail_id", "")),
                    "phase_id_detected": int(baseline.get("phase_id_detected", 0) or 0),
                    "phase_name_detected": f"phase_{int(baseline.get('phase_id_detected', 0) or 0)}",
                    "s_w_centroid": [float(item) for item in baseline.get("s_w_centroid", [])],
                    "reconstruction_median": float(baseline.get("reconstruction_median", 0.0) or 0.0),
                    "reconstruction_mad": float(baseline.get("reconstruction_mad", 0.0) or 0.0),
                    "distance_median": float(baseline.get("distance_median", 0.0) or 0.0),
                    "distance_mad": float(baseline.get("distance_mad", 0.0) or 0.0),
                    "stable_window_count": int(baseline.get("stable_window_count", 0) or 0),
                    "feature_names": list(feature_names),
                    "selected_sensors_c": list(config.backbone_model.selected_sensors_c),
                    "selected_event_types": list(config.phase_selected_event_types),
                    "selected_categorical_state_pairs": [
                        f"{parameter_name}={state}" for parameter_name, state in config.phase_selected_categorical_state_pairs
                    ],
                    "selected_window_cooccurrence_pairs": [
                        f"{left}&{right}" for left, right in config.phase_selected_window_cooccurrence_pairs
                    ],
                    "backbone_all_sensors": list(config.backbone_model.all_sensors),
                    "backbone_weights_b": [[float(value) for value in row] for row in config.backbone_model.weights_b.tolist()],
                    "version": 2,
                }
            )
        return phase_baselines

    def phase_windows_df(self) -> pd.DataFrame:
        return pd.DataFrame(self.phase_windows)

    def phase_baselines_df(self) -> pd.DataFrame:
        return pd.DataFrame(self.phase_baselines)
