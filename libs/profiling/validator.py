"""Streaming validator for profiled datatype rows against simulator datatype labels."""

from __future__ import annotations

from datetime import datetime
from typing import Generator, Iterable

import pandas as pd

from libs.common import DatatypeLabelRow, DatatypeProfiledRow, ProfilerValidatorSnapshot, TelemetryRow, normalize_sensor_datatype


def _row_ts(row: TelemetryRow | DatatypeLabelRow | DatatypeProfiledRow, *, field: str = "timestamp_utc") -> datetime:
    value = row.get(field)
    if isinstance(value, datetime):
        return value
    return pd.to_datetime(value, utc=True).to_pydatetime()


def _row_key(row: TelemetryRow | DatatypeLabelRow | DatatypeProfiledRow, *, ts_field: str = "timestamp_utc") -> tuple[str, str, str, datetime]:
    return (
        str(row.get("tail_id", "")),
        str(row.get("flight_id", "")),
        str(row.get("parameter_name", "")),
        _row_ts(row, field=ts_field),
    )


def _dtype_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() in {"none", "null"}:
        return ""
    return normalize_sensor_datatype(text)


def simulator_datatype_label_rows(
    simulator_rows: Iterable[TelemetryRow],
) -> Generator[DatatypeLabelRow, None, None]:
    for row in simulator_rows:
        if "parameter_datatype" in row:
            raise ValueError("simulator rows must not include legacy 'parameter_datatype'; use 'parameter_datatype_label'")
        if "parameter_datatype_label" not in row:
            raise ValueError("simulator rows must include 'parameter_datatype_label'")
        yield {
            "tail_id": str(row.get("tail_id", "")),
            "flight_id": str(row.get("flight_id", "")),
            "parameter_name": str(row.get("parameter_name", "")),
            "timestamp_utc": _row_ts(row, field="timestamp_utc"),
            "parameter_datatype_label": _dtype_text(row.get("parameter_datatype_label")),
        }


def profiler_datatype_rows(
    profiler_rows: Iterable[DatatypeProfiledRow],
) -> Generator[DatatypeProfiledRow, None, None]:
    for row in profiler_rows:
        if "detected_type" in row:
            raise ValueError("profiler rows must not include legacy 'detected_type'; use 'parameter_datatype_profiled'")
        if "parameter_datatype_profiled" not in row:
            raise ValueError("profiler rows must include 'parameter_datatype_profiled'")
        yield {
            "tail_id": str(row.get("tail_id", "")),
            "flight_id": str(row.get("flight_id", "")),
            "parameter_name": str(row.get("parameter_name", "")),
            "timestamp_utc": _row_ts(row, field="timestamp_utc"),
            "parameter_datatype_profiled": _dtype_text(row.get("parameter_datatype_profiled")),
        }


def stream_profiler_validation(
    *,
    simulator_rows: Iterable[TelemetryRow],
    profiler_rows: Iterable[DatatypeProfiledRow],
    emit_orphan_fp: bool = True,
) -> Generator[ProfilerValidatorSnapshot, None, None]:
    """Yield cumulative TP/FP/FN/TN snapshots for profiled vs label datatype rows."""
    labels = list(simulator_datatype_label_rows(simulator_rows))
    profiled = list(profiler_datatype_rows(profiler_rows))

    profiled_by_key: dict[tuple[str, str, str, datetime], str] = {}
    for row in profiled:
        profiled_by_key[_row_key(row)] = str(row.get("parameter_datatype_profiled", ""))

    labels_sorted = sorted(labels, key=_row_key)

    tp = 0
    fp = 0
    fn = 0
    tn = 0

    for label_row in labels_sorted:
        key = _row_key(label_row)
        label_value = str(label_row.get("parameter_datatype_label", ""))
        profiled_value = str(profiled_by_key.pop(key, ""))

        label_present = bool(label_value)
        profiled_present = bool(profiled_value)

        if label_present and profiled_present:
            if label_value == profiled_value:
                tp += 1
            else:
                fp += 1
                fn += 1
        elif label_present and not profiled_present:
            fn += 1
        elif profiled_present and not label_present:
            fp += 1
        else:
            tn += 1

        yield {
            "tail_id": key[0],
            "flight_id": key[1],
            "parameter_name": key[2],
            "timestamp_utc": key[3],
            "parameter_datatype_label": label_value,
            "parameter_datatype_profiled": profiled_value,
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "tn": tn,
        }

    if not emit_orphan_fp:
        return

    for key in sorted(profiled_by_key.keys(), key=lambda item: (item[0], item[1], item[2], item[3])):
        fp += 1
        yield {
            "tail_id": key[0],
            "flight_id": key[1],
            "parameter_name": key[2],
            "timestamp_utc": key[3],
            "parameter_datatype_label": "",
            "parameter_datatype_profiled": str(profiled_by_key[key]),
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "tn": tn,
        }
