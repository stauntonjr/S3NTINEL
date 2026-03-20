from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

from libs.backbone.energy import EVENT_PRIOR_DEFAULT_ALPHA, compute_window_sensor_energy
from libs.backbone.fit import (
    aggregate_backbone_gh,
    compute_backbone_gh_by_flight,
    reconstruct_window_vector,
    reconstruction_error,
    select_backbone_sensors_by_energy,
    solve_backbone_weights,
)


@dataclass(frozen=True)
class BackboneSpec:
    sensor_count: int = 8
    ridge_lambda: float = 1.0
    event_prior_alpha: float = 0.35
    backbone_version: int = 2

    @property
    def selected_sensor_limit(self) -> int:
        return max(int(self.sensor_count), 1)


@dataclass(frozen=True)
class BackboneSensorEnergy:
    parameter_name: str
    energy: float
    support_count: int
    event_prior: float = 0.0
    selection_score: float = 0.0
    selected_backbone: bool = False
    backbone_version: int = 2

    @classmethod
    def from_window_feature_rows(
        cls,
        window_feature_rows: list[dict[str, Any]],
        *,
        spec: BackboneSpec | None = None,
        selected_sensors: set[str] | None = None,
        backbone_version: int = 2,
    ) -> list["BackboneSensorEnergy"]:
        selected = selected_sensors or set()
        event_prior_alpha = float(spec.event_prior_alpha) if spec is not None else EVENT_PRIOR_DEFAULT_ALPHA
        return [
            cls(
                parameter_name=str(row["parameter_name"]),
                energy=float(row["energy"]),
                support_count=int(row["support_count"]),
                event_prior=float(row.get("event_prior", 0.0) or 0.0),
                selection_score=float(row.get("selection_score", row["energy"]) or 0.0),
                selected_backbone=str(row["parameter_name"]) in selected,
                backbone_version=int(backbone_version),
            )
            for row in compute_window_sensor_energy(
                window_feature_rows,
                event_prior_alpha=event_prior_alpha,
            )
        ]

    def to_row(self) -> dict[str, Any]:
        return {
            "parameter_name": self.parameter_name,
            "energy": float(self.energy),
            "support_count": int(self.support_count),
            "event_prior": float(self.event_prior),
            "selection_score": float(self.selection_score),
            "selected_backbone": bool(self.selected_backbone),
            "backbone_version": int(self.backbone_version),
        }


@dataclass(frozen=True)
class BackboneModel:
    selected_sensors_c: list[str]
    all_sensors: list[str]
    weights_b: np.ndarray
    lambda_ridge: float
    training_window_count: int
    backbone_version: int = 2

    @classmethod
    def from_window_feature_rows(
        cls,
        window_feature_rows: list[dict[str, Any]],
        *,
        spec: BackboneSpec,
    ) -> tuple["BackboneModel", list[BackboneSensorEnergy]]:
        sensor_energies = BackboneSensorEnergy.from_window_feature_rows(
            window_feature_rows,
            backbone_version=spec.backbone_version,
            spec=spec,
        )
        selected_sensors_c = select_backbone_sensors_by_energy(
            [item.to_row() for item in sensor_energies],
            k=spec.selected_sensor_limit,
        )
        gh_rows, all_sensors = compute_backbone_gh_by_flight(
            window_feature_rows,
            selected_sensors=selected_sensors_c,
        )
        g, h, total_window_count = aggregate_backbone_gh(gh_rows)
        weights_b = solve_backbone_weights(g, h, ridge_lambda=float(spec.ridge_lambda))
        selected_sensor_set = set(selected_sensors_c)
        selected_energy_rows = [
            BackboneSensorEnergy(
                parameter_name=item.parameter_name,
                energy=item.energy,
                support_count=item.support_count,
                event_prior=item.event_prior,
                selection_score=item.selection_score,
                selected_backbone=item.parameter_name in selected_sensor_set,
                backbone_version=item.backbone_version,
            )
            for item in sensor_energies
        ]
        return (
            cls(
                selected_sensors_c=list(selected_sensors_c),
                all_sensors=list(all_sensors),
                weights_b=np.asarray(weights_b, dtype=float),
                lambda_ridge=float(spec.ridge_lambda),
                training_window_count=int(total_window_count),
                backbone_version=int(spec.backbone_version),
            ),
            selected_energy_rows,
        )

    def reconstruct_window_vector(self, x_c: dict[str, float]) -> dict[str, float]:
        return reconstruct_window_vector(
            x_c,
            selected_sensors=self.selected_sensors_c,
            all_sensors=self.all_sensors,
            weights_b=self.weights_b,
        )

    def reconstruction_error(
        self,
        x_true: dict[str, float],
        x_hat: dict[str, float],
    ) -> tuple[float, dict[str, float]]:
        return reconstruction_error(x_true, x_hat, sensor_order=self.all_sensors)

    def to_row(self) -> dict[str, Any]:
        return {
            "backbone_version": int(self.backbone_version),
            "selected_sensors_c": list(self.selected_sensors_c),
            "all_sensors": list(self.all_sensors),
            "weights_b": [[float(value) for value in row] for row in self.weights_b.tolist()],
            "lambda_ridge": float(self.lambda_ridge),
            "training_window_count": int(self.training_window_count),
        }
