"""Simulation-side event-truth labeling over canonical telemetry rows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(slots=True)
class _ContinuousRunState:
    sign: int = 0
    length: int = 0
    peak_abs_delta: float = 0.0


def _coerce_float(value: object | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _label_continuous_rows(rows: list[dict[str, Any]]) -> None:
    values = [_coerce_float(row.get("parameter_value_clean")) for row in rows]
    diffs = [
        abs(current - previous)
        for previous, current in zip(values[:-1], values[1:], strict=False)
        if previous is not None and current is not None
    ]
    if not diffs:
        return
    abs_diff_series = pd.Series(diffs, dtype="float64")
    threshold = max(float(abs_diff_series.quantile(0.75)) * 0.5, 1e-6)
    min_persistence = 2
    state = _ContinuousRunState()
    previous_value = values[0]
    rows[0]["event_type_label"] = str(rows[0].get("event_type_label", "") or "")

    for row, current_value in zip(rows[1:], values[1:], strict=False):
        row["event_type_label"] = str(row.get("event_type_label", "") or "")
        if previous_value is None or current_value is None:
            state = _ContinuousRunState()
            previous_value = current_value
            continue
        delta = float(current_value - previous_value)
        previous_value = current_value
        if abs(delta) < threshold:
            state = _ContinuousRunState()
            continue

        sign = 1 if delta > 0.0 else -1
        abs_delta = abs(delta)
        if sign != state.sign:
            state = _ContinuousRunState(sign=sign, length=1, peak_abs_delta=abs_delta)
            continue

        state.length += 1
        state.peak_abs_delta = max(state.peak_abs_delta, abs_delta)
        if state.length == min_persistence:
            row["event_type_label"] = "slope_pos" if sign > 0 else "slope_neg"


def _label_discrete_rows(rows: list[dict[str, Any]]) -> None:
    previous_value = str(rows[0].get("parameter_value_clean") or rows[0].get("parameter_value") or "")
    rows[0]["event_type_label"] = str(rows[0].get("event_type_label", "") or "")
    for row in rows[1:]:
        current_value = str(row.get("parameter_value_clean") or row.get("parameter_value") or "")
        row["event_type_label"] = str(row.get("event_type_label", "") or "")
        if previous_value and current_value and current_value != previous_value:
            row["event_type_label"] = "transition"
        elif (not previous_value) and current_value:
            row["event_type_label"] = "state_enter"
        elif previous_value and (not current_value):
            row["event_type_label"] = "dropped"
        previous_value = current_value


def annotate_event_type_labels(telemetry_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Annotate simulator telemetry rows with evaluation-only `event_type_label` truth."""
    if not telemetry_rows:
        return telemetry_rows
    for row in telemetry_rows:
        row["event_type_label"] = str(row.get("event_type_label", "") or "")

    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in telemetry_rows:
        key = (
            str(row.get("tail_id", "")),
            str(row.get("flight_id", "")),
            str(row.get("parameter_name", row.get("sensor", ""))),
        )
        groups.setdefault(key, []).append(row)

    for rows in groups.values():
        rows.sort(
            key=lambda row: (
                pd.to_datetime(row.get("timestamp_utc"), utc=True, errors="coerce"),
                int(row.get("step_index", 0) or 0),
            )
        )
        datatype = str(rows[0].get("parameter_datatype_label", "") or "").lower()
        if datatype in {"numeric", "constant"}:
            _label_continuous_rows(rows)
        elif datatype in {"binary", "categorical", "high_cardinality"}:
            _label_discrete_rows(rows)
    return telemetry_rows
