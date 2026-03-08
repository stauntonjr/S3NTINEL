"""Reusable V2-style window representations."""

from __future__ import annotations

from collections import Counter
from typing import Any

import pandas as pd

from libs.common import AdaptiveWindowRow, DetectedEventRow, PhaseWindowRow, WindowXRow
from libs.common.event_types import CATEGORICAL_EVENT_TYPES, CONTINUOUS_EVENT_TYPES


def build_continuous_robust_scaler(telemetry_df: pd.DataFrame) -> dict[str, dict[str, float]]:
    values = telemetry_df.copy()
    if "parameter_name" not in values.columns and "sensor" in values.columns:
        values["parameter_name"] = values["sensor"]
    values["parameter_name"] = values.get("parameter_name", "").astype(str)
    value_source = values.get("parameter_value")
    if value_source is None:
        value_source = values.get("parameter_value_clean")
    values["value_num"] = pd.to_numeric(value_source, errors="coerce")
    values = values.dropna(subset=["parameter_name", "value_num"])
    if values.empty:
        return {}

    scaler: dict[str, dict[str, float]] = {}
    grouped = values.groupby("parameter_name")["value_num"]
    for parameter_name, series in grouped:
        median = float(series.median())
        q25 = float(series.quantile(0.25))
        q75 = float(series.quantile(0.75))
        iqr = max(q75 - q25, 1e-6)
        scaler[str(parameter_name)] = {
            "median": median,
            "iqr": iqr,
        }
    return scaler


def window_continuous_vectors(
    window_events: list[DetectedEventRow],
    scaler_by_sensor: dict[str, dict[str, float]],
) -> tuple[dict[str, float], dict[str, float]]:
    raw_by_parameter: dict[str, float] = {}
    for event in window_events:
        parameter_name = str(event.get("parameter_name") or event.get("sensor") or "")
        if not parameter_name:
            continue
        payload = event.get("payload")
        if not isinstance(payload, dict):
            continue
        value = payload.get("value")
        if value is None:
            continue
        try:
            value_num = float(value)
        except Exception:
            continue
        raw_by_parameter[parameter_name] = value_num

    scaled_by_parameter: dict[str, float] = {}
    for parameter_name, value in raw_by_parameter.items():
        scaler = scaler_by_sensor.get(parameter_name)
        if scaler is None:
            continue
        median = float(scaler.get("median", 0.0))
        iqr = max(float(scaler.get("iqr", 1.0)), 1e-6)
        scaled_by_parameter[parameter_name] = (float(value) - median) / iqr
    return raw_by_parameter, scaled_by_parameter


def window_categorical_state_t_end(window: AdaptiveWindowRow) -> dict[str, str]:
    snapshot = window.get("zoh_snapshot")
    if not isinstance(snapshot, dict):
        return {}
    out: dict[str, str] = {}
    for parameter_name, value in snapshot.items():
        parameter_name_text = str(parameter_name)
        if not parameter_name_text:
            continue
        value_text = "" if value is None else str(value).strip()
        if not value_text:
            continue
        try:
            float(value_text)
            continue
        except Exception:
            pass
        out[parameter_name_text] = value_text
    return out


def window_vector_drift_magnitude(previous_scaled: dict[str, float], current_scaled: dict[str, float]) -> float:
    parameter_union = set(previous_scaled.keys()) | set(current_scaled.keys())
    if not parameter_union:
        return 0.0
    drift_sq = 0.0
    for parameter_name in parameter_union:
        prev = float(previous_scaled.get(parameter_name, 0.0))
        curr = float(current_scaled.get(parameter_name, 0.0))
        delta = curr - prev
        drift_sq += delta * delta
    return drift_sq ** 0.5


def build_window_x_row(
    *,
    window: AdaptiveWindowRow,
    window_events: list[DetectedEventRow],
    scaler_by_sensor: dict[str, dict[str, float]],
    previous_scaled_by_flight: dict[tuple[str, str], dict[str, float]],
    phase_label: str | None = None,
) -> WindowXRow:
    # provisional_window_vector: robust-scaled continuous end-of-window snapshot
    raw_vector, scaled_vector = window_continuous_vectors(window_events=window_events, scaler_by_sensor=scaler_by_sensor)
    if not raw_vector:
        snapshot = window.get("zoh_snapshot")
        if isinstance(snapshot, dict):
            for parameter_name, value in snapshot.items():
                parameter_name_text = str(parameter_name)
                if not parameter_name_text:
                    continue
                try:
                    value_num = float(value)
                except Exception:
                    continue
                raw_vector[parameter_name_text] = value_num
                scaler = scaler_by_sensor.get(parameter_name_text)
                if scaler is None:
                    continue
                median = float(scaler.get("median", 0.0))
                iqr = max(float(scaler.get("iqr", 1.0)), 1e-6)
                scaled_vector[parameter_name_text] = (value_num - median) / iqr
    flight_key = (str(window.get("tail_id", "")), str(window.get("flight_id", "")))
    prev_vector = previous_scaled_by_flight.get(flight_key, {})
    drift_magnitude_profiled = window_vector_drift_magnitude(prev_vector, scaled_vector)
    previous_scaled_by_flight[flight_key] = dict(scaled_vector)

    event_type_counts = window.get("event_type_counts")
    if not isinstance(event_type_counts, dict):
        event_type_counts = {}

    return {
        "tail_id": str(window.get("tail_id", "")),
        "flight_id": str(window.get("flight_id", "")),
        "win_id": int(window.get("win_id", 0)),
        "t_start": window.get("t_start"),
        "t_end": window.get("t_end"),
        "duration_ms": int(window.get("duration_ms", 0)),
        "event_count": int(window.get("event_count", 0)),
        "date_utc": window.get("date_utc"),
        "event_type_counts": dict(event_type_counts),
        "continuous_vector_t_end": {key: float(value) for key, value in sorted(raw_vector.items(), key=lambda item: item[0])},
        "continuous_vector_t_end_scaled": {key: float(value) for key, value in sorted(scaled_vector.items(), key=lambda item: item[0])},
        "categorical_state_t_end": window_categorical_state_t_end(window),
        "drift_magnitude_profiled": float(drift_magnitude_profiled),
        "phase_label": phase_label,
    }


def top_phase_event_types(window_x_rows: list[WindowXRow], *, k: int) -> list[str]:
    counts: Counter[str] = Counter()
    for item in window_x_rows:
        event_type_counts = item.get("event_type_counts")
        if not isinstance(event_type_counts, dict):
            continue
        for event_type, count in event_type_counts.items():
            counts[str(event_type)] += int(count)
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


def top_categorical_state_pairs(window_x_rows: list[WindowXRow], *, k: int) -> list[tuple[str, str]]:
    counts: Counter[tuple[str, str]] = Counter()
    for item in window_x_rows:
        categorical_state_t_end = item.get("categorical_state_t_end")
        if not isinstance(categorical_state_t_end, dict):
            continue
        for parameter_name, state in categorical_state_t_end.items():
            counts[(str(parameter_name), str(state))] += 1
    return [pair for pair, _ in counts.most_common(max(int(k), 0))]


def top_window_cooccurrence_sensor_pairs(
    window_x_rows: list[WindowXRow],
    *,
    k: int,
) -> list[tuple[str, str]]:
    counts: Counter[tuple[str, str]] = Counter()
    for item in window_x_rows:
        parameter_names = sorted(str(parameter_name) for parameter_name in item.get("continuous_vector_t_end_scaled", {}).keys())
        parameter_names.extend(
            sorted(
                str(parameter_name)
                for parameter_name in item.get("categorical_state_t_end", {}).keys()
                if str(parameter_name) not in set(parameter_names)
            )
        )
        distinct = sorted(set(parameter_names))
        for idx, left in enumerate(distinct):
            for right in distinct[idx + 1 :]:
                counts[(left, right)] += 1
    return [pair for pair, _ in counts.most_common(max(int(k), 0))]


def build_window_s_rows(
    window_x_rows: list[WindowXRow],
    *,
    selected_sensors_c: list[str],
    selected_event_types: list[str],
    selected_categorical_state_pairs: list[tuple[str, str]],
    selected_cooccurrence_sensor_pairs: list[tuple[str, str]] | None = None,
) -> tuple[list[PhaseWindowRow], list[str]]:
    # structure_vector: compact phase/scoring representation built from x_c + event/categorical summaries
    feature_names = [f"parameter_name::{parameter_name}" for parameter_name in selected_sensors_c]
    feature_names.extend(f"event_type::{event_type}" for event_type in selected_event_types)
    feature_names.extend(f"categorical::{parameter_name}={state}" for parameter_name, state in selected_categorical_state_pairs)
    cooccurrence_pairs = [(str(left), str(right)) for left, right in (selected_cooccurrence_sensor_pairs or []) if str(left) and str(right)]
    feature_names.extend(f"cooccur::{left}&{right}" for left, right in cooccurrence_pairs)
    feature_names.extend(
        [
            "summary::event_density_hz",
            "summary::continuous_event_fraction",
            "summary::categorical_event_fraction",
            "summary::active_sensor_fraction",
        ]
    )

    structured: list[PhaseWindowRow] = []
    for window in window_x_rows:
        scaled = window.get("continuous_vector_t_end_scaled")
        if not isinstance(scaled, dict):
            scaled = {}
        event_counts = window.get("event_type_counts")
        if not isinstance(event_counts, dict):
            event_counts = {}
        categorical_t_end = window.get("categorical_state_t_end")
        if not isinstance(categorical_t_end, dict):
            categorical_t_end = {}
        active_parameters = set(str(parameter_name) for parameter_name in scaled.keys()) | set(str(parameter_name) for parameter_name in categorical_t_end.keys())
        event_total = max(int(window.get("event_count", 0) or 0), 0)
        duration_ms = max(int(window.get("duration_ms", 0) or 0), 1)
        duration_s = float(duration_ms) / 1000.0

        vector: list[float] = []
        x_c: list[float] = []
        for parameter_name in selected_sensors_c:
            value = float(scaled.get(parameter_name, 0.0) or 0.0)
            vector.append(value)
            x_c.append(value)
        for event_type in selected_event_types:
            vector.append(float(event_counts.get(event_type, 0) or 0.0) / float(max(event_total, 1)))
        for parameter_name, state in selected_categorical_state_pairs:
            vector.append(1.0 if str(categorical_t_end.get(parameter_name, "")) == state else 0.0)
        for left, right in cooccurrence_pairs:
            vector.append(1.0 if left in active_parameters and right in active_parameters else 0.0)
        continuous_count = float(sum(int(event_counts.get(item, 0) or 0) for item in CONTINUOUS_EVENT_TYPES))
        categorical_count = float(sum(int(event_counts.get(item, 0) or 0) for item in CATEGORICAL_EVENT_TYPES))
        active_sensor_fraction = float(len(scaled)) / float(max(len(selected_sensors_c), 1))
        vector.extend(
            [
                float(event_total) / float(max(duration_s, 1e-6)),
                continuous_count / float(max(event_total, 1)),
                categorical_count / float(max(event_total, 1)),
                active_sensor_fraction,
            ]
        )

        enriched = dict(window)
        enriched["x_c"] = x_c
        enriched["s_w"] = vector
        structured.append(enriched)
    return structured, feature_names
