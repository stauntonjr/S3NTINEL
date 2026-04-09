from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

from libs.backbone import BackboneModel

TEMPORAL_HISTORY_SCALES: tuple[int, ...] = (2, 4, 8)


@dataclass(frozen=True)
class PhaseFeatureConfig:
    backbone_model: BackboneModel
    phase_selected_sensors: list[str]
    phase_selected_event_types: list[str]
    phase_selected_categorical_state_pairs: list[tuple[str, str]]
    phase_selected_window_cooccurrence_pairs: list[tuple[str, str]]

    @property
    def level_feature_names(self) -> list[str]:
        return [f"parameter_name::{parameter_name}" for parameter_name in self.phase_selected_sensors]

    @property
    def delta_feature_names(self) -> list[str]:
        return [f"parameter_delta::{parameter_name}" for parameter_name in self.phase_selected_sensors] + [
            "summary::delta_energy_per_sensor",
            "summary::delta_directionality",
            "summary::sensor_change_fraction",
        ]

    @property
    def event_feature_names(self) -> list[str]:
        return [
            f"event_type::{event_type}" for event_type in self.phase_selected_event_types
        ]

    @property
    def categorical_feature_names(self) -> list[str]:
        return [f"categorical_start::{parameter_name}={state}" for parameter_name, state in self.phase_selected_categorical_state_pairs] + [
            f"categorical_end::{parameter_name}={state}" for parameter_name, state in self.phase_selected_categorical_state_pairs
        ] + [
            f"categorical_changed::{parameter_name}={state}" for parameter_name, state in self.phase_selected_categorical_state_pairs
        ]

    @property
    def summary_feature_names(self) -> list[str]:
        return [
            "summary::event_density_hz",
            "summary::continuous_event_fraction",
            "summary::categorical_event_fraction",
            "summary::active_sensor_fraction",
            "summary::drift_rate",
            "summary::reconstruction_error_per_sensor",
            "summary::slope_reinforcement_fraction",
            "summary::slope_directionality",
            "summary::slope_peak_concentration",
            "summary::switch_fraction",
            "summary::threshold_fraction",
            "summary::oscillation_fraction",
            "summary::drift_guard_fraction",
            "summary::categorical_change_count",
            "summary::categorical_change_fraction",
        ]

    @property
    def temporal_history_scales(self) -> tuple[int, ...]:
        return TEMPORAL_HISTORY_SCALES

    @property
    def temporal_sensor_feature_names(self) -> list[str]:
        return [
            f"temporal_sensor::{parameter_name}:delta_mean:w{scale}"
            for scale in self.temporal_history_scales
            for parameter_name in self.phase_selected_sensors
        ]

    @property
    def temporal_event_feature_names(self) -> list[str]:
        return [
            f"temporal_event::{event_type}:rate_mean:w{scale}"
            for scale in self.temporal_history_scales
            for event_type in self.phase_selected_event_types
        ]

    @property
    def temporal_categorical_feature_names(self) -> list[str]:
        return [
            f"temporal_categorical::dwell:{parameter_name}={state}:w{scale}"
            for scale in self.temporal_history_scales
            for parameter_name, state in self.phase_selected_categorical_state_pairs
        ]

    @property
    def temporal_summary_feature_names(self) -> list[str]:
        summary_names = (
            "history_coverage",
            "delta_abs_mean",
            "delta_energy_mean",
            "delta_directionality_mean",
            "delta_continuation_fraction",
            "delta_reversal_fraction",
            "event_shift",
            "categorical_transition_rate",
            "drift_rate_mean",
            "reconstruction_error_mean",
            "active_sensor_fraction_mean",
            "slope_directionality_mean",
        )
        return [
            f"temporal_summary::{summary_name}:w{scale}"
            for scale in self.temporal_history_scales
            for summary_name in summary_names
        ] + [
            "temporal_summary::delta_energy_short_long_contrast",
            "temporal_summary::drift_rate_short_long_contrast",
            "temporal_summary::event_shift_short_long_contrast",
        ]

    @property
    def temporal_feature_names(self) -> list[str]:
        return (
            self.temporal_sensor_feature_names
            + self.temporal_event_feature_names
            + self.temporal_categorical_feature_names
            + self.temporal_summary_feature_names
        )

    @property
    def feature_names(self) -> list[str]:
        return (
            self.level_feature_names
            + self.delta_feature_names
            + self.event_feature_names
            + self.categorical_feature_names
            + self.summary_feature_names
            + self.temporal_feature_names
        )

    @property
    def categorical_state_labels(self) -> list[str]:
        return [f"{parameter_name}={state}" for parameter_name, state in self.phase_selected_categorical_state_pairs]

    @property
    def window_cooccurrence_labels(self) -> list[str]:
        return [f"{left}&{right}" for left, right in self.phase_selected_window_cooccurrence_pairs]

    @property
    def backbone_weights_rows(self) -> list[list[float]]:
        weights = self.backbone_model.weights_b
        if hasattr(weights, "tolist"):
            weights = weights.tolist()
        return [[float(value) for value in row] for row in weights]

    @classmethod
    def coerce(cls, value: "PhaseFeatureConfig | dict[str, Any]") -> "PhaseFeatureConfig":
        if isinstance(value, cls):
            return value
        return cls.from_dict(value)

    @classmethod
    def from_backbone_row(
        cls,
        backbone_row: dict[str, Any],
        *,
        phase_selected_sensors: list[str],
        phase_selected_event_types: list[str],
        phase_selected_categorical_state_pairs: list[tuple[str, str]],
    ) -> "PhaseFeatureConfig":
        backbone_payload = {
            "selected_sensors_c": list(backbone_row.get("selected_sensors_c") or []),
            "all_sensors": list(backbone_row.get("all_sensors") or []),
            "weights_b": list(backbone_row.get("weights_b") or []),
            "lambda_ridge": float(backbone_row.get("lambda_ridge", 1.0) or 1.0),
            "training_window_count": int(backbone_row.get("training_window_count", 0) or 0),
            "backbone_version": int(backbone_row.get("backbone_version", 2) or 2),
            "phase_selected_sensors": list(phase_selected_sensors),
            "phase_selected_event_types": list(phase_selected_event_types),
            "phase_selected_categorical_state_pairs": list(phase_selected_categorical_state_pairs),
            "phase_selected_window_cooccurrence_pairs": [],
        }
        return cls.from_dict(backbone_payload)

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
        weights = self.backbone_model.weights_b
        if hasattr(weights, "tolist"):
            weights = weights.tolist()
        return {
            "selected_sensors_c": list(self.backbone_model.selected_sensors_c),
            "all_sensors": list(self.backbone_model.all_sensors),
            "weights_b": [[float(value) for value in row] for row in weights],
            "lambda_ridge": float(self.backbone_model.lambda_ridge),
            "training_window_count": int(self.backbone_model.training_window_count),
            "backbone_version": int(self.backbone_model.backbone_version),
            "phase_selected_sensors": list(self.phase_selected_sensors),
            "phase_selected_event_types": list(self.phase_selected_event_types),
            "phase_selected_categorical_state_pairs": list(self.phase_selected_categorical_state_pairs),
            "phase_selected_window_cooccurrence_pairs": list(self.phase_selected_window_cooccurrence_pairs),
        }


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
