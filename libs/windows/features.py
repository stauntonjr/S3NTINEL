"""Window feature-domain objects."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any

import pandas as pd

from libs.common.event_types import CATEGORICAL_EVENT_TYPES, CONTINUOUS_EVENT_TYPES
from libs.io.contracts import AdaptiveWindowRow, DetectedEventRow, PhaseWindowRow, WindowXRow
from libs.windows.context import WindowContext


@dataclass(frozen=True)
class WindowScaler:
    by_parameter: dict[str, dict[str, float]]

    @classmethod
    def from_telemetry_df(cls, telemetry_df: pd.DataFrame) -> "WindowScaler":
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
            return cls(by_parameter={})

        scaler: dict[str, dict[str, float]] = {}
        for parameter_name, series in values.groupby("parameter_name")["value_num"]:
            median = float(series.median())
            q25 = float(series.quantile(0.25))
            q75 = float(series.quantile(0.75))
            scaler[str(parameter_name)] = {"median": median, "iqr": max(q75 - q25, 1e-6)}
        return cls(by_parameter=scaler)

    def scale(self, parameter_name: str, value: float) -> float | None:
        spec = self.by_parameter.get(str(parameter_name))
        if spec is None:
            return None
        median = float(spec.get("median", 0.0))
        iqr = max(float(spec.get("iqr", 1.0)), 1e-6)
        return (float(value) - median) / iqr

    def vectors_for_window(
        self,
        *,
        window_events: list[DetectedEventRow],
        snapshot: dict[str, Any],
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
            scaled = self.scale(parameter_name, value)
            if scaled is not None:
                scaled_by_parameter[parameter_name] = float(scaled)

        if raw_by_parameter:
            return raw_by_parameter, scaled_by_parameter

        for parameter_name, value in snapshot.items():
            parameter_name_text = str(parameter_name)
            if not parameter_name_text:
                continue
            try:
                value_num = float(value)
            except Exception:
                continue
            raw_by_parameter[parameter_name_text] = value_num
            scaled = self.scale(parameter_name_text, value_num)
            if scaled is not None:
                scaled_by_parameter[parameter_name_text] = float(scaled)
        return raw_by_parameter, scaled_by_parameter


@dataclass(frozen=True)
class WindowFeatures:
    row: WindowXRow

    @staticmethod
    def categorical_state_from_snapshot(snapshot: dict[str, Any]) -> dict[str, str]:
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

    @staticmethod
    def drift_magnitude(previous_scaled: dict[str, float], current_scaled: dict[str, float]) -> float:
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

    @classmethod
    def from_window_row(
        cls,
        *,
        window: AdaptiveWindowRow,
        scaler: WindowScaler,
        previous_scaled_by_flight: dict[tuple[str, str], dict[str, float]],
        phase_label: str | None = None,
    ) -> "WindowFeatures":
        snapshot = dict(window.get("zoh_snapshot", {})) if isinstance(window.get("zoh_snapshot"), dict) else {}
        window_events = list(window.get("window_events", []))
        raw_vector, scaled_vector = scaler.vectors_for_window(window_events=window_events, snapshot=snapshot)
        flight_key = (str(window.get("tail_id", "")), str(window.get("flight_id", "")))
        prev_vector = previous_scaled_by_flight.get(flight_key, {})
        drift_magnitude_profiled = cls.drift_magnitude(prev_vector, scaled_vector)
        previous_scaled_by_flight[flight_key] = dict(scaled_vector)

        event_type_counts = window.get("event_type_counts")
        if not isinstance(event_type_counts, dict):
            event_type_counts = {}

        return cls(
            row={
                "tail_id": str(window.get("tail_id", "")),
                "flight_id": str(window.get("flight_id", "")),
                "win_id": int(window.get("win_id", 0)),
                "t_start": window.get("t_start"),
                "t_end": window.get("t_end"),
                "duration_ms": int(window.get("duration_ms", 0)),
                "event_count": int(window.get("event_count", 0)),
                "date_utc": window.get("date_utc"),
                "event_type_counts": dict(event_type_counts),
                "continuous_vector_t_end": {
                    key: float(value) for key, value in sorted(raw_vector.items(), key=lambda item: item[0])
                },
                "continuous_vector_t_end_scaled": {
                    key: float(value) for key, value in sorted(scaled_vector.items(), key=lambda item: item[0])
                },
                "categorical_state_t_end": cls.categorical_state_from_snapshot(snapshot),
                "drift_magnitude_profiled": float(drift_magnitude_profiled),
                "phase_label": phase_label,
            }
        )

    @classmethod
    def from_context(
        cls,
        *,
        context: WindowContext,
        scaler: WindowScaler,
        previous_scaled_by_flight: dict[tuple[str, str], dict[str, float]],
        phase_label: str | None = None,
    ) -> "WindowFeatures":
        return cls.from_window_row(
            window=context.row,
            scaler=scaler,
            previous_scaled_by_flight=previous_scaled_by_flight,
            phase_label=phase_label,
        )

    @staticmethod
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

    @staticmethod
    def top_categorical_state_pairs(window_x_rows: list[WindowXRow], *, k: int) -> list[tuple[str, str]]:
        counts: Counter[tuple[str, str]] = Counter()
        for item in window_x_rows:
            categorical_state_t_end = item.get("categorical_state_t_end")
            if not isinstance(categorical_state_t_end, dict):
                continue
            for parameter_name, state in categorical_state_t_end.items():
                counts[(str(parameter_name), str(state))] += 1
        return [pair for pair, _ in counts.most_common(max(int(k), 0))]

    @staticmethod
    def top_cooccurrence_sensor_pairs(window_x_rows: list[WindowXRow], *, k: int) -> list[tuple[str, str]]:
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


@dataclass(frozen=True)
class WindowFeatureSelection:
    selected_sensors_c: list[str]
    selected_event_types: list[str]
    selected_categorical_state_pairs: list[tuple[str, str]]
    selected_cooccurrence_sensor_pairs: list[tuple[str, str]] | None = None

    @property
    def feature_names(self) -> list[str]:
        names = [f"parameter_name::{parameter_name}" for parameter_name in self.selected_sensors_c]
        names.extend(f"event_type::{event_type}" for event_type in self.selected_event_types)
        names.extend(
            f"categorical::{parameter_name}={state}"
            for parameter_name, state in self.selected_categorical_state_pairs
        )
        for left, right in self.cooccurrence_pairs:
            names.append(f"cooccur::{left}&{right}")
        names.extend(
            [
                "summary::event_density_hz",
                "summary::continuous_event_fraction",
                "summary::categorical_event_fraction",
                "summary::active_sensor_fraction",
            ]
        )
        return names

    @property
    def cooccurrence_pairs(self) -> list[tuple[str, str]]:
        return [
            (str(left), str(right))
            for left, right in (self.selected_cooccurrence_sensor_pairs or [])
            if str(left) and str(right)
        ]

    def encode_row(self, window: WindowXRow) -> PhaseWindowRow:
        scaled = window.get("continuous_vector_t_end_scaled")
        if not isinstance(scaled, dict):
            scaled = {}
        event_counts = window.get("event_type_counts")
        if not isinstance(event_counts, dict):
            event_counts = {}
        categorical_t_end = window.get("categorical_state_t_end")
        if not isinstance(categorical_t_end, dict):
            categorical_t_end = {}
        active_parameters = set(str(parameter_name) for parameter_name in scaled.keys()) | set(
            str(parameter_name) for parameter_name in categorical_t_end.keys()
        )
        event_total = max(int(window.get("event_count", 0) or 0), 0)
        duration_ms = max(int(window.get("duration_ms", 0) or 0), 1)
        duration_s = float(duration_ms) / 1000.0

        vector: list[float] = []
        x_c: list[float] = []
        for parameter_name in self.selected_sensors_c:
            value = float(scaled.get(parameter_name, 0.0) or 0.0)
            vector.append(value)
            x_c.append(value)
        for event_type in self.selected_event_types:
            vector.append(float(event_counts.get(event_type, 0) or 0.0) / float(max(event_total, 1)))
        for parameter_name, state in self.selected_categorical_state_pairs:
            vector.append(1.0 if str(categorical_t_end.get(parameter_name, "")) == state else 0.0)
        for left, right in self.cooccurrence_pairs:
            vector.append(1.0 if left in active_parameters and right in active_parameters else 0.0)
        continuous_count = float(sum(int(event_counts.get(item, 0) or 0) for item in CONTINUOUS_EVENT_TYPES))
        categorical_count = float(sum(int(event_counts.get(item, 0) or 0) for item in CATEGORICAL_EVENT_TYPES))
        active_sensor_fraction = float(len(scaled)) / float(max(len(self.selected_sensors_c), 1))
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
        return enriched

    def encode_rows(self, window_x_rows: list[WindowXRow]) -> tuple[list[PhaseWindowRow], list[str]]:
        return [self.encode_row(window) for window in window_x_rows], list(self.feature_names)
