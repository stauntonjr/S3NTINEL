"""Window-vector energy helpers for backbone sensor selection."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
import math
from typing import Any, Iterable

EVENT_PRIOR_DEFAULT_ALPHA = 0.35


def _summary_map(summary: Any, field_name: str) -> dict[str, Any]:
    if hasattr(summary, "asDict"):
        summary = summary.asDict(recursive=True)
    if not isinstance(summary, Mapping):
        return {}
    metric_map = summary.get(field_name)
    if hasattr(metric_map, "asDict"):
        metric_map = metric_map.asDict(recursive=True)
    return dict(metric_map) if isinstance(metric_map, Mapping) else {}


def _safe_number(value: Any) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def _zscore(values: list[float]) -> list[float]:
    if not values:
        return []
    mean = sum(values) / float(len(values))
    variance = sum((value - mean) * (value - mean) for value in values) / float(len(values))
    if variance <= 1e-12:
        return [0.0 for _ in values]
    stddev = math.sqrt(variance)
    return [(value - mean) / stddev for value in values]


def _event_prior_from_totals(*, slope_abs_impulse: float, switch_count: float, threshold_count: float, oscillation_count: float, drift_guard_count: float, slope_reinforcement_count: float) -> float:
    return (
        math.log1p(max(float(slope_abs_impulse), 0.0))
        + 0.75 * math.log1p(max(float(switch_count), 0.0))
        + 0.5 * math.log1p(max(float(threshold_count) + float(oscillation_count), 0.0))
        + 0.25 * math.log1p(max(float(drift_guard_count), 0.0))
        + 0.25 * math.log1p(max(float(slope_reinforcement_count), 0.0))
    )


def compute_window_sensor_energy(
    sampled_windows: Iterable[dict[str, Any]],
    *,
    vector_field: str = "continuous_vector_t_end_scaled",
    event_prior_alpha: float = EVENT_PRIOR_DEFAULT_ALPHA,
) -> list[dict[str, float | int | str]]:
    """Compute per-parameter energy over sampled windows."""
    energy_by_sensor: dict[str, float] = defaultdict(float)
    support_by_sensor: dict[str, int] = defaultdict(int)
    slope_abs_impulse_by_sensor: dict[str, float] = defaultdict(float)
    switch_count_by_sensor: dict[str, float] = defaultdict(float)
    threshold_count_by_sensor: dict[str, float] = defaultdict(float)
    oscillation_count_by_sensor: dict[str, float] = defaultdict(float)
    drift_guard_count_by_sensor: dict[str, float] = defaultdict(float)
    slope_reinforcement_count_by_sensor: dict[str, float] = defaultdict(float)

    for window in sampled_windows:
        vector = window.get(vector_field)
        if not isinstance(vector, dict):
            continue
        summary = window.get("continuous_event_summary")
        slope_abs_impulse_map = _summary_map(summary, "slope_abs_impulse_by_parameter")
        switch_count_map = _summary_map(summary, "switch_count_by_parameter")
        threshold_count_map = _summary_map(summary, "threshold_count_by_parameter")
        oscillation_count_map = _summary_map(summary, "oscillation_count_by_parameter")
        drift_guard_count_map = _summary_map(summary, "drift_guard_count_by_parameter")
        slope_reinforcement_count_map = _summary_map(summary, "slope_reinforcement_count_by_parameter")
        for sensor, value in vector.items():
            sensor_name = str(sensor)
            if not sensor_name:
                continue
            try:
                x = float(value)
            except Exception:
                continue
            energy_by_sensor[sensor_name] += x * x
            support_by_sensor[sensor_name] += 1
            slope_abs_impulse_by_sensor[sensor_name] += _safe_number(slope_abs_impulse_map.get(sensor_name))
            switch_count_by_sensor[sensor_name] += _safe_number(switch_count_map.get(sensor_name))
            threshold_count_by_sensor[sensor_name] += _safe_number(threshold_count_map.get(sensor_name))
            oscillation_count_by_sensor[sensor_name] += _safe_number(oscillation_count_map.get(sensor_name))
            drift_guard_count_by_sensor[sensor_name] += _safe_number(drift_guard_count_map.get(sensor_name))
            slope_reinforcement_count_by_sensor[sensor_name] += _safe_number(
                slope_reinforcement_count_map.get(sensor_name)
            )

    rows = [
        {
            "parameter_name": sensor,
            "energy": float(energy_by_sensor[sensor]),
            "support_count": int(support_by_sensor[sensor]),
            "event_prior": _event_prior_from_totals(
                slope_abs_impulse=slope_abs_impulse_by_sensor[sensor],
                switch_count=switch_count_by_sensor[sensor],
                threshold_count=threshold_count_by_sensor[sensor],
                oscillation_count=oscillation_count_by_sensor[sensor],
                drift_guard_count=drift_guard_count_by_sensor[sensor],
                slope_reinforcement_count=slope_reinforcement_count_by_sensor[sensor],
            ),
        }
        for sensor in energy_by_sensor
    ]
    energy_zscores = _zscore([math.log1p(float(item["energy"])) for item in rows])
    event_prior_zscores = _zscore([float(item["event_prior"]) for item in rows])
    for index, item in enumerate(rows):
        item["selection_score"] = float(energy_zscores[index]) + (
            float(event_prior_alpha) * float(event_prior_zscores[index])
        )
    rows.sort(
        key=lambda item: (
            -float(item.get("selection_score", 0.0)),
            -float(item["energy"]),
            str(item["parameter_name"]),
        )
    )
    return rows


def aggregate_sensor_energy_over_corpus(
    per_flight_sensor_energy: Iterable[dict[str, Any]],
) -> list[dict[str, float | int | str]]:
    """Aggregate per-flight sensor energies into corpus totals."""
    energy_by_sensor: dict[str, float] = defaultdict(float)
    flight_support_by_sensor: dict[str, int] = defaultdict(int)

    for item in per_flight_sensor_energy:
        sensor = str(item.get("parameter_name", "")).strip()
        if not sensor:
            continue
        try:
            energy = float(item.get("energy", 0.0) or 0.0)
        except Exception:
            continue
        energy_by_sensor[sensor] += energy
        flight_support_by_sensor[sensor] += 1

    rows = [
        {
            "parameter_name": sensor,
            "energy": float(energy_by_sensor[sensor]),
            "flight_support_count": int(flight_support_by_sensor[sensor]),
        }
        for sensor in energy_by_sensor
    ]
    rows.sort(key=lambda item: (-float(item["energy"]), str(item["parameter_name"])))
    return rows
