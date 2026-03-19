"""Streaming and summary validators for profile artifacts against simulator labels."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Generator, Iterable

import pandas as pd

from libs.common import normalize_parameter_datatype
from libs.io.contracts import DatatypeLabelRow, DatatypeProfiledRow, ProfilerValidatorSnapshot, TelemetryRow


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
    return normalize_parameter_datatype(text)


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


def iter_profile_validation_snapshots(
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


def build_profile_validation_summary(
    *,
    raw_telemetry_df: pd.DataFrame,
    parameter_datatype_profile_df: pd.DataFrame,
    parameter_behavior_profile_df: pd.DataFrame,
) -> dict[str, Any]:
    if raw_telemetry_df is None or raw_telemetry_df.empty:
        return {
            "status": "ok",
            "parameter_count": 0,
            "datatype_labeled_parameter_count": 0,
            "datatype_profiled_parameter_count": 0,
            "behavior_labeled_parameter_count": 0,
            "behavior_profiled_parameter_count": 0,
        }

    raw_df = raw_telemetry_df.copy()
    raw_df["parameter_name"] = raw_df.get("parameter_name", "").fillna("").astype(str)
    raw_df["parameter_datatype_label"] = raw_df.get("parameter_datatype_label", "").fillna("").astype(str)
    raw_df["behavior_family_label"] = raw_df.get("behavior_family_label", "").fillna("").astype(str)
    label_df = (
        raw_df.groupby("parameter_name", dropna=False)
        .agg(
            parameter_datatype_label=("parameter_datatype_label", lambda values: next((value for value in values if value), "")),
            behavior_family_label=("behavior_family_label", lambda values: next((value for value in values if value), "")),
        )
        .reset_index()
    )

    merged = label_df.merge(
        parameter_datatype_profile_df[["parameter_name", "parameter_datatype_profiled"]]
        if parameter_datatype_profile_df is not None and not parameter_datatype_profile_df.empty
        else pd.DataFrame(columns=["parameter_name", "parameter_datatype_profiled"]),
        on="parameter_name",
        how="left",
    ).merge(
        parameter_behavior_profile_df[["parameter_name", "behavior_family_profiled"]]
        if parameter_behavior_profile_df is not None and not parameter_behavior_profile_df.empty
        else pd.DataFrame(columns=["parameter_name", "behavior_family_profiled"]),
        on="parameter_name",
        how="left",
    )

    datatype_mask = merged["parameter_datatype_label"].fillna("").astype(str) != ""
    behavior_mask = merged["behavior_family_label"].fillna("").astype(str) != ""
    datatype_profiled_mask = merged.get("parameter_datatype_profiled", pd.Series(dtype="object")).fillna("").astype(str) != ""
    behavior_profiled_mask = merged.get("behavior_family_profiled", pd.Series(dtype="object")).fillna("").astype(str) != ""
    datatype_match_mask = datatype_mask & datatype_profiled_mask & (
        merged["parameter_datatype_label"].astype(str) == merged["parameter_datatype_profiled"].fillna("").astype(str)
    )
    behavior_match_mask = behavior_mask & behavior_profiled_mask & (
        merged["behavior_family_label"].astype(str) == merged["behavior_family_profiled"].fillna("").astype(str)
    )
    datatype_labeled_parameter_count = int(datatype_mask.sum())
    datatype_profiled_parameter_count = int(datatype_profiled_mask.sum())
    datatype_exact_match_count = int(datatype_match_mask.sum())
    behavior_labeled_parameter_count = int(behavior_mask.sum())
    behavior_profiled_parameter_count = int(behavior_profiled_mask.sum())
    behavior_exact_match_count = int(behavior_match_mask.sum())

    return {
        "status": "ok",
        "parameter_count": int(len(merged)),
        "datatype_labeled_parameter_count": datatype_labeled_parameter_count,
        "datatype_profiled_parameter_count": datatype_profiled_parameter_count,
        "datatype_exact_match_count": datatype_exact_match_count,
        "datatype_error_count": max(datatype_labeled_parameter_count - datatype_exact_match_count, 0),
        "datatype_accuracy": (
            float(datatype_exact_match_count / datatype_labeled_parameter_count)
            if datatype_labeled_parameter_count > 0
            else None
        ),
        "datatype_profile_coverage": (
            float(datatype_profiled_parameter_count / datatype_labeled_parameter_count)
            if datatype_labeled_parameter_count > 0
            else None
        ),
        "behavior_labeled_parameter_count": behavior_labeled_parameter_count,
        "behavior_profiled_parameter_count": behavior_profiled_parameter_count,
        "behavior_exact_match_count": behavior_exact_match_count,
        "behavior_error_count": max(behavior_labeled_parameter_count - behavior_exact_match_count, 0),
        "behavior_accuracy": (
            float(behavior_exact_match_count / behavior_labeled_parameter_count)
            if behavior_labeled_parameter_count > 0
            else None
        ),
        "behavior_profile_coverage": (
            float(behavior_profiled_parameter_count / behavior_labeled_parameter_count)
            if behavior_labeled_parameter_count > 0
            else None
        ),
    }
