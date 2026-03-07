"""Window-vector energy helpers for CUR-style feature scoring."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable


def compute_window_sensor_energy(
    sampled_windows: Iterable[dict[str, Any]],
    *,
    vector_field: str = "continuous_vector_t_end_scaled",
) -> list[dict[str, float | int | str]]:
    """Compute per-sensor energy E_f(j) = sum_w x_w[j]^2 over sampled windows."""
    energy_by_sensor: dict[str, float] = defaultdict(float)
    support_by_sensor: dict[str, int] = defaultdict(int)

    for window in sampled_windows:
        vector = window.get(vector_field)
        if not isinstance(vector, dict):
            continue
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

    rows = [
        {
            "parameter_name": sensor,
            "energy": float(energy_by_sensor[sensor]),
            "support_count": int(support_by_sensor[sensor]),
        }
        for sensor in energy_by_sensor
    ]
    rows.sort(key=lambda item: (-float(item["energy"]), str(item["parameter_name"])))
    return rows


def aggregate_sensor_energy_over_corpus(
    per_flight_sensor_energy: Iterable[dict[str, Any]],
) -> list[dict[str, float | int | str]]:
    """Aggregate per-flight sensor energies into corpus energies E(j)=sum_f E_f(j)."""
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
