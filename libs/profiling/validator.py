"""Streaming and summary validators for profile artifacts against simulator labels."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Generator, Iterable

import pandas as pd

from libs.common import normalize_parameter_datatype
from libs.io.contracts import DatatypeLabelRow, DatatypeProfiledRow, ProfilerValidatorSnapshot, TelemetryRow
from libs.io.schemas.profiling import PARAMETER_BEHAVIOR_PRIMITIVE_PROFILE_COLUMNS


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
    parameter_behavior_primitive_profile_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    primitive_columns = tuple(
        column
        for column in PARAMETER_BEHAVIOR_PRIMITIVE_PROFILE_COLUMNS
        if column
        not in {
            "parameter_name",
            "parameter_datatype_profiled",
            "sample_count",
            "profile_window_start_utc",
            "profile_window_end_utc",
            "sign_flip_rate_profiled",
            "discrete_low_cardinality_score_profiled",
            "discrete_low_transition_score_profiled",
            "discrete_dwell_score_profiled",
            "transition_balance_score_profiled",
        }
    )

    def _first_nonempty_string(values: pd.Series) -> str:
        for value in values:
            if pd.notna(value):
                text = str(value).strip()
                if text:
                    return text
        return ""

    def _build_match_details(
        *,
        merged_df: pd.DataFrame,
        label_column: str,
        predicted_column: str,
        confidence_column: str | None = None,
        extra_columns: tuple[str, ...] = (),
        mismatch_limit: int = 25,
    ) -> dict[str, Any]:
        working = merged_df.copy()
        working[label_column] = working.get(label_column, "").fillna("").astype(str)
        working[predicted_column] = working.get(predicted_column, "").fillna("").astype(str)
        match_mask = working[label_column] == working[predicted_column]

        confusion = (
            working.groupby([label_column, predicted_column], dropna=False)
            .size()
            .reset_index(name="count")
            .sort_values(["count", label_column, predicted_column], ascending=[False, True, True], kind="stable")
        )
        errors_by_label = (
            working.loc[~match_mask]
            .groupby(label_column, dropna=False)
            .size()
            .reset_index(name="error_count")
            .sort_values(["error_count", label_column], ascending=[False, True], kind="stable")
        )
        prediction_counts = (
            working.groupby(predicted_column, dropna=False)
            .size()
            .reset_index(name="count")
            .sort_values(["count", predicted_column], ascending=[False, True], kind="stable")
        )

        details: dict[str, Any] = {
            "confusion_matrix": confusion.to_dict(orient="records"),
            "errors_by_label": errors_by_label.to_dict(orient="records"),
            "prediction_counts": prediction_counts.to_dict(orient="records"),
            "mismatch_examples": (
                working.loc[~match_mask, ["parameter_name", *extra_columns, label_column, predicted_column]]
                .sort_values(["parameter_name"], kind="stable")
                .head(mismatch_limit)
                .to_dict(orient="records")
            ),
        }
        if confidence_column is not None and confidence_column in working.columns:
            confidence_summary = (
                working.groupby(predicted_column, dropna=False)[confidence_column]
                .mean()
                .reset_index(name="mean_confidence")
                .sort_values(["mean_confidence", predicted_column], ascending=[False, True], kind="stable")
            )
            mismatch_with_confidence = (
                working.loc[~match_mask, ["parameter_name", *extra_columns, label_column, predicted_column, confidence_column]]
                .sort_values(["parameter_name"], kind="stable")
                .head(mismatch_limit)
                .to_dict(orient="records")
            )
            details["confidence_by_predicted_family"] = confidence_summary.to_dict(orient="records")
            details["mismatch_examples"] = mismatch_with_confidence
        return details

    if raw_telemetry_df is None or raw_telemetry_df.empty:
        return {
            "status": "ok",
            "parameter_count": 0,
            "datatype_labeled_parameter_count": 0,
            "datatype_profiled_parameter_count": 0,
            "behavior_labeled_parameter_count": 0,
            "behavior_profiled_parameter_count": 0,
            "datatype_details": {
                "confusion_matrix": [],
                "errors_by_label": [],
                "prediction_counts": [],
                "mismatch_examples": [],
            },
            "behavior_details": {
                "confusion_matrix": [],
                "errors_by_label": [],
                "prediction_counts": [],
                "confidence_by_predicted_family": [],
                "mismatch_examples": [],
                "primitive_evidence_by_label": [],
            },
        }

    raw_df = raw_telemetry_df.copy()
    raw_df["parameter_name"] = raw_df.get("parameter_name", "").fillna("").astype(str)
    raw_df["parameter_datatype_label"] = raw_df.get("parameter_datatype_label", "").fillna("").astype(str)
    raw_df["behavior_family_label"] = raw_df.get("behavior_family_label", "").fillna("").astype(str)
    label_df = (
        raw_df.groupby("parameter_name", dropna=False)
        .agg(
            parameter_datatype_label=("parameter_datatype_label", _first_nonempty_string),
            behavior_family_label=("behavior_family_label", _first_nonempty_string),
            system_id=("system_id", _first_nonempty_string),
            subsystem_id=("subsystem_id", _first_nonempty_string),
            module_id=("module_id", _first_nonempty_string),
        )
        .reset_index()
    )

    if parameter_datatype_profile_df is not None and not parameter_datatype_profile_df.empty:
        datatype_profile_df = parameter_datatype_profile_df[
            ["parameter_name", "parameter_datatype_profiled", "sampling_rate_profiled_hz"]
        ]
    else:
        datatype_profile_df = pd.DataFrame(
            columns=["parameter_name", "parameter_datatype_profiled", "sampling_rate_profiled_hz"]
        )
    if parameter_behavior_profile_df is not None and not parameter_behavior_profile_df.empty:
        behavior_profile_df = parameter_behavior_profile_df[
            [
                "parameter_name",
                "behavior_family_profiled",
                "behavior_profile_confidence",
                *[column for column in primitive_columns if column in parameter_behavior_profile_df.columns],
            ]
        ]
    else:
        behavior_profile_df = pd.DataFrame(
            columns=["parameter_name", "behavior_family_profiled", "behavior_profile_confidence"]
        )
    if parameter_behavior_primitive_profile_df is not None and not parameter_behavior_primitive_profile_df.empty:
        primitive_profile_df = parameter_behavior_primitive_profile_df[
            ["parameter_name", *[column for column in primitive_columns if column in parameter_behavior_primitive_profile_df.columns]]
        ]
    else:
        primitive_profile_df = pd.DataFrame(columns=["parameter_name", *primitive_columns])

    merged = label_df.merge(
        datatype_profile_df,
        on="parameter_name",
        how="left",
    ).merge(
        behavior_profile_df,
        on="parameter_name",
        how="left",
    ).merge(
        primitive_profile_df,
        on="parameter_name",
        how="left",
        suffixes=("", "_primitive"),
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

    datatype_detail_df = merged.loc[
        datatype_mask | datatype_profiled_mask,
        [
            "parameter_name",
            "system_id",
            "subsystem_id",
            "module_id",
            "parameter_datatype_label",
            "parameter_datatype_profiled",
            "sampling_rate_profiled_hz",
        ],
    ].copy()
    behavior_detail_df = merged.loc[
        behavior_mask | behavior_profiled_mask,
        [
            "parameter_name",
            "system_id",
            "subsystem_id",
            "module_id",
            "parameter_datatype_label",
            "sampling_rate_profiled_hz",
            "behavior_family_label",
            "behavior_family_profiled",
            "behavior_profile_confidence",
            *[column for column in primitive_columns if column in merged.columns],
        ],
    ].copy()

    primitive_by_label: list[dict[str, Any]] = []
    available_primitive_columns = [column for column in primitive_columns if column in behavior_detail_df.columns]
    if available_primitive_columns:
        primitive_by_label = (
            behavior_detail_df.loc[behavior_mask]
            .groupby("behavior_family_label", dropna=False)[available_primitive_columns]
            .mean(numeric_only=True)
            .reset_index()
            .sort_values(["behavior_family_label"], kind="stable")
            .to_dict(orient="records")
        )

    behavior_details = _build_match_details(
        merged_df=behavior_detail_df,
        label_column="behavior_family_label",
        predicted_column="behavior_family_profiled",
        confidence_column="behavior_profile_confidence",
        extra_columns=("parameter_datatype_label", "sampling_rate_profiled_hz", "system_id", "subsystem_id", "module_id", *available_primitive_columns),
    )
    behavior_details["primitive_evidence_by_label"] = primitive_by_label

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
        "datatype_details": _build_match_details(
            merged_df=datatype_detail_df,
            label_column="parameter_datatype_label",
            predicted_column="parameter_datatype_profiled",
            extra_columns=("sampling_rate_profiled_hz", "system_id", "subsystem_id", "module_id"),
        ),
        "behavior_details": behavior_details,
    }
