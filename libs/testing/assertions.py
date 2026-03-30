"""Helpers to enforce canonical label schema contracts."""

from __future__ import annotations

from collections.abc import Iterable

BANNED_COLUMNS = {
    "sim_event_type",
    "truth_event_label_types_json",
    "truth_event_label_primary_type",
    "truth_event_label_count",
    "truth_anomaly_label_type",
    "truth_anomaly_label_score",
}

REQUIRED_LABEL_COLUMNS = {
    "event_type_label",
    "event_misbehavior_label",
    "anomaly_type_label",
    "anomaly_score_label",
}

REQUIRED_DETECTED_COLUMNS = {
    "event_type_detected",
    "anomaly_type_detected",
    "anomaly_score_detected",
}

REQUIRED_PROFILER_VALIDATOR_LABEL_COLUMNS = {
    "parameter_datatype_label",
}

REQUIRED_PROFILER_VALIDATOR_PROFILED_COLUMNS = {
    "parameter_datatype_profiled",
}


def assert_no_banned_columns(columns: Iterable[str]) -> None:
    col_set = {str(col) for col in columns}
    prefixed = sorted(col for col in col_set if col.startswith("truth_"))
    banned = sorted(col for col in col_set if col in BANNED_COLUMNS)
    failures = prefixed + banned
    if failures:
        raise AssertionError(f"Banned columns present: {failures}")


def assert_required_columns(columns: Iterable[str], required: set[str]) -> None:
    col_set = {str(col) for col in columns}
    missing = sorted(item for item in required if item not in col_set)
    if missing:
        raise AssertionError(f"Missing required columns: {missing}")


def assert_no_bare_detector_event_type(columns: Iterable[str]) -> None:
    col_set = {str(col) for col in columns}
    if "event_type" in col_set:
        raise AssertionError("Detector output must use 'event_type_detected' instead of bare 'event_type'")


def assert_profiler_validator_canonical_columns(columns: Iterable[str]) -> None:
    col_set = {str(col) for col in columns}
    if "parameter_datatype" in col_set:
        raise AssertionError("Profiler validator input must use 'parameter_datatype_label', not 'parameter_datatype'")
    if "detected_type" in col_set:
        raise AssertionError("Profiler validator input must use 'parameter_datatype_profiled', not 'detected_type'")
