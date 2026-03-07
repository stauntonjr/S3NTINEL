"""Backbone fitting from provisional window vectors using additive sufficient statistics."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import numpy as np


def select_backbone_sensors_by_energy(
    sensor_energy_rows: list[dict[str, Any]],
    *,
    k: int,
) -> list[str]:
    limit = max(int(k), 1)
    return [str(item.get("parameter_name", "")) for item in sensor_energy_rows[:limit] if str(item.get("parameter_name", ""))][:limit]


def compute_backbone_gh_by_flight(
    windows: list[dict[str, Any]],
    *,
    selected_sensors: list[str],
    all_sensors: list[str] | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Compute per-flight G_f and H_f from robust-scaled window vectors."""
    backbone_sensors = [str(item) for item in selected_sensors if str(item)]
    if all_sensors is None:
        sensor_union: set[str] = set()
        for item in windows:
            vector = item.get("continuous_vector_t_end_scaled")
            if isinstance(vector, dict):
                sensor_union.update(str(sensor) for sensor in vector.keys() if str(sensor))
        all_sensor_order = sorted(sensor_union)
    else:
        all_sensor_order = [str(item) for item in all_sensors if str(item)]

    by_flight: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for item in windows:
        key = (str(item.get("tail_id", "")), str(item.get("flight_id", "")))
        by_flight[key].append(item)

    rows: list[dict[str, Any]] = []
    for (tail_id, flight_id), items in sorted(by_flight.items(), key=lambda item: (item[0][0], item[0][1])):
        g = np.zeros((len(backbone_sensors), len(backbone_sensors)), dtype=float)
        h = np.zeros((len(backbone_sensors), len(all_sensor_order)), dtype=float)
        window_count = 0
        for item in items:
            vector = item.get("continuous_vector_t_end_scaled")
            if not isinstance(vector, dict):
                continue
            x_c = np.asarray([float(vector.get(sensor, 0.0) or 0.0) for sensor in backbone_sensors], dtype=float)
            x_all = np.asarray([float(vector.get(sensor, 0.0) or 0.0) for sensor in all_sensor_order], dtype=float)
            g += np.outer(x_c, x_c)
            h += np.outer(x_c, x_all)
            window_count += 1
        rows.append(
            {
                "tail_id": tail_id,
                "flight_id": flight_id,
                "window_count": int(window_count),
                "g_f": g,
                "h_f": h,
            }
        )
    return rows, all_sensor_order


def aggregate_backbone_gh(
    gh_rows: list[dict[str, Any]],
) -> tuple[np.ndarray, np.ndarray, int]:
    if not gh_rows:
        return np.zeros((0, 0), dtype=float), np.zeros((0, 0), dtype=float), 0
    g = np.zeros_like(np.asarray(gh_rows[0]["g_f"], dtype=float))
    h = np.zeros_like(np.asarray(gh_rows[0]["h_f"], dtype=float))
    total_windows = 0
    for item in gh_rows:
        g += np.asarray(item["g_f"], dtype=float)
        h += np.asarray(item["h_f"], dtype=float)
        total_windows += int(item.get("window_count", 0))
    return g, h, int(total_windows)


def solve_backbone_weights(
    g: np.ndarray,
    h: np.ndarray,
    *,
    ridge_lambda: float,
) -> np.ndarray:
    if g.size == 0 or h.size == 0:
        return np.zeros((0, 0), dtype=float)
    lam = max(float(ridge_lambda), 0.0)
    system = np.asarray(g + (lam * np.eye(g.shape[0], dtype=float)), dtype=float)
    rhs = np.asarray(h, dtype=float).copy()

    n = int(system.shape[0])
    m = int(rhs.shape[1])

    # Small ridge systems are expected here; use explicit elimination to avoid BLAS/OpenMP runtime issues.
    aug = np.concatenate([system.copy(), rhs], axis=1)

    for pivot_idx in range(n):
        best_row = pivot_idx
        best_abs = abs(float(aug[pivot_idx, pivot_idx]))
        for row_idx in range(pivot_idx + 1, n):
            cand = abs(float(aug[row_idx, pivot_idx]))
            if cand > best_abs:
                best_row = row_idx
                best_abs = cand
        if best_row != pivot_idx:
            aug[[pivot_idx, best_row], :] = aug[[best_row, pivot_idx], :]

        pivot = float(aug[pivot_idx, pivot_idx])
        if abs(pivot) <= 1e-12:
            pivot = 1e-12
            aug[pivot_idx, pivot_idx] = pivot

        aug[pivot_idx, :] = aug[pivot_idx, :] / pivot
        for row_idx in range(n):
            if row_idx == pivot_idx:
                continue
            factor = float(aug[row_idx, pivot_idx])
            if abs(factor) <= 1e-18:
                continue
            aug[row_idx, :] = aug[row_idx, :] - (factor * aug[pivot_idx, :])

    return aug[:, n : n + m]


def reconstruct_window_vector(
    x_c: dict[str, float],
    *,
    selected_sensors: list[str],
    all_sensors: list[str],
    weights_b: np.ndarray,
) -> dict[str, float]:
    if weights_b.size == 0:
        return {}
    x = np.asarray([float(x_c.get(sensor, 0.0) or 0.0) for sensor in selected_sensors], dtype=float)
    x_hat = x @ weights_b
    return {
        sensor: float(value)
        for sensor, value in zip(all_sensors, x_hat.tolist(), strict=False)
    }


def reconstruction_error(
    x_true: dict[str, float],
    x_hat: dict[str, float],
    *,
    sensor_order: list[str],
) -> tuple[float, dict[str, float]]:
    residuals: dict[str, float] = {}
    error_sq = 0.0
    for sensor in sensor_order:
        label_value = float(x_true.get(sensor, 0.0) or 0.0)
        pred_value = float(x_hat.get(sensor, 0.0) or 0.0)
        residual = label_value - pred_value
        residuals[sensor] = residual
        error_sq += residual * residual
    return error_sq ** 0.5, residuals
