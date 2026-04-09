"""Validation report generation for simulation pipeline runs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from libs.anomaly.validator import (
    build_fault_attribution_summary_from_misbehavior_summary,
    validate_attribution_against_misbehavior_truth,
)
from libs.events.validator import build_event_validation_summary
from libs.graph import build_coupling_validation_summary, build_graph_validation_summary
from libs.io.schemas.profiling import PARAMETER_BEHAVIOR_PROFILE_COLUMNS, PARAMETER_BEHAVIOR_PRIMITIVE_PROFILE_COLUMNS
from libs.phase import validate_detected_phases_from_tables
from libs.profiling.validator import build_profile_validation_summary
from libs.scoring.validator import validate_scores_against_misbehavior_windows
from libs.simulation.report_tables import ArtifactView, RunArtifactBundle
from libs.simulation.run_context import RunPaths, write_manifest
from libs.testing.assertions import (
    REQUIRED_DETECTED_COLUMNS,
    REQUIRED_LABEL_COLUMNS,
    REQUIRED_PROFILER_VALIDATOR_LABEL_COLUMNS,
    assert_no_banned_columns,
    assert_no_bare_detector_event_type,
    assert_required_columns,
)

PROFILE_VALIDATION_PRIMITIVE_COLUMNS = tuple(
    column
    for column in PARAMETER_BEHAVIOR_PRIMITIVE_PROFILE_COLUMNS
    if column
    not in {
        "parameter_name",
        "parameter_datatype_profiled",
        "sample_count",
        "profile_window_start_utc",
        "profile_window_end_utc",
        "discrete_low_cardinality_score_profiled",
        "discrete_low_transition_score_profiled",
        "discrete_dwell_score_profiled",
        "transition_balance_score_profiled",
    }
)
PROFILE_BEHAVIOR_SCORE_COLUMNS = tuple(
    column
    for column in PARAMETER_BEHAVIOR_PROFILE_COLUMNS
    if column.endswith("_score_profiled")
)

RAW_TELEMETRY_REPORT_COLUMNS = (
    "tail_id",
    "flight_id",
    "timestamp_utc",
    "parameter_name",
    "system_id",
    "subsystem_id",
    "module_id",
    "behavior_family_label",
    "parameter_datatype_label",
    "misbehavior_active",
    "misbehavior_applied",
    "misbehavior_family_label",
    "misbehavior_detail_label",
    "misbehavior_window_id",
    "event_type_label",
    "event_misbehavior_label",
    "anomaly_type_label",
    "anomaly_score_label",
    "fault_active",
    "fault_applied",
    "fault_family_label",
    "fault_type",
    "fault_window_id",
    "coupling_id_label",
    "unit",
    "rate_hz",
)

RAW_TELEMETRY_REPORT_VIEW = ArtifactView("raw_telemetry", RAW_TELEMETRY_REPORT_COLUMNS)
RAW_TELEMETRY_PROFILE_VIEW = ArtifactView(
    "raw_telemetry",
    ("parameter_name", "parameter_datatype_label", "behavior_family_label", "system_id", "subsystem_id", "module_id"),
)
RAW_TELEMETRY_EVENT_LABEL_VIEW = ArtifactView(
    "raw_telemetry",
    (
        "tail_id",
        "flight_id",
        "parameter_name",
        "timestamp_utc",
        "step_index",
        "parameter_value",
        "parameter_value_clean",
        "event_type_label",
    ),
    ("tail_id", "flight_id", "parameter_name", "timestamp_utc", "step_index"),
)
RAW_TELEMETRY_SCORE_VIEW = ArtifactView(
    "raw_telemetry",
    RAW_TELEMETRY_REPORT_COLUMNS,
    ("tail_id", "flight_id", "timestamp_utc"),
)

DATATYPE_PROFILE_VIEW = ArtifactView(
    "parameter_datatype_profile",
    ("parameter_name", "parameter_datatype_profiled", "sampling_rate_profiled_hz"),
)
BEHAVIOR_PRIMITIVE_PROFILE_VIEW = ArtifactView(
    "parameter_behavior_primitive_profile",
    ("parameter_name", *PROFILE_VALIDATION_PRIMITIVE_COLUMNS),
)
BEHAVIOR_PROFILE_VIEW = ArtifactView(
    "parameter_behavior_profile",
    (
        "parameter_name",
        "behavior_family_profiled",
        "behavior_profile_confidence",
        *PROFILE_BEHAVIOR_SCORE_COLUMNS,
        *PROFILE_VALIDATION_PRIMITIVE_COLUMNS,
    ),
)
EVENTS_VIEW = ArtifactView(
    "events",
    (
        "tail_id",
        "flight_id",
        "parameter_name",
        "timestamp_utc",
        "event_type_detected",
        "anomaly_type_detected",
        "anomaly_score_detected",
        "payload",
        "date_utc",
    ),
)
DETECTED_EVENT_VIEW = ArtifactView(
    "events",
    ("tail_id", "flight_id", "parameter_name", "timestamp_utc", "event_type_detected"),
    ("tail_id", "flight_id", "parameter_name", "timestamp_utc"),
)
WINDOWS_VIEW = ArtifactView(
    "windows",
    ("tail_id", "flight_id", "win_id", "t_start", "t_end", "date_utc"),
    ("tail_id", "flight_id", "win_id"),
)
WINDOWS_SCORE_VIEW = ArtifactView(
    "windows",
    (
        "tail_id",
        "flight_id",
        "win_id",
        "t_start",
        "t_end",
        "event_count",
        "real_event_count",
        "close_reason",
        "date_utc",
    ),
    ("tail_id", "flight_id", "win_id"),
)
PHASE_LABELS_VIEW = ArtifactView(
    "phase_labels",
    ("tail_id", "flight_id", "timestamp_utc", "phase_label"),
    ("tail_id", "flight_id", "timestamp_utc"),
)
PHASE_WINDOWS_VIEW = ArtifactView(
    "phase_windows",
    (
        "tail_id",
        "flight_id",
        "win_id",
        "t_start",
        "t_end",
        "phase_id_detected",
        "phase_state_detected",
        "transition_from_phase_id_detected",
        "transition_to_phase_id_detected",
        "phase_confidence_detected",
        "distance_to_centroid_detected",
    ),
    ("tail_id", "flight_id", "win_id"),
)
HIERARCHY_SENSOR_MAP_VIEW = ArtifactView(
    "hierarchy_sensor_map",
    ("parameter_name", "system_id", "subsystem_id", "module_id"),
    ("parameter_name",),
)
HIERARCHY_LABEL_VIEW = ArtifactView(
    "hierarchy_sensor_map_label",
    ("parameter_name", "system_id", "subsystem_id", "module_id"),
    ("parameter_name",),
)
HIERARCHY_SUBSYSTEM_VIEW = ArtifactView(
    "hierarchy_sensor_map",
    ("parameter_name", "subsystem_id"),
    ("parameter_name",),
)
HIERARCHY_LABEL_SUBSYSTEM_VIEW = ArtifactView(
    "hierarchy_sensor_map_label",
    ("parameter_name", "subsystem_id"),
    ("parameter_name",),
)
COUPLING_TRUTH_VIEW = ArtifactView(
    "coupling_misbehavior_windows",
    (
        "coupling_id",
        "start_step",
        "end_step_exclusive",
        "misbehavior_window_id",
        "misbehavior_family_label",
        "misbehavior_detail_label",
        "fault_window_id",
        "fault_family_label",
    ),
    ("coupling_id", "start_step", "misbehavior_window_id"),
)
LAG_GRAPH_VIEW = ArtifactView(
    "lag_graph",
    ("parameter_name_u", "parameter_name_v", "lag_weight", "mean_lag_seconds"),
    ("parameter_name_u", "parameter_name_v"),
)
PRECISION_GRAPH_VIEW = ArtifactView(
    "precision_graph",
    ("parameter_name_u", "parameter_name_v", "partial_corr", "precision_weight"),
    ("parameter_name_u", "parameter_name_v"),
)
FUSED_GRAPH_VIEW = ArtifactView(
    "fused_graph",
    ("parameter_name_u", "parameter_name_v", "fused_weight"),
    ("parameter_name_u", "parameter_name_v"),
)
CALIBRATED_SCORES_VIEW = ArtifactView(
    "window_scores_calibrated",
    (
        "tail_id",
        "flight_id",
        "win_id",
        "date_utc",
        "global_score",
        "p_value",
        "severity",
        "emit_ready",
    ),
    ("tail_id", "flight_id", "win_id"),
)
RAW_SCORES_VIEW = ArtifactView(
    "window_scores_raw",
    (
        "tail_id",
        "flight_id",
        "win_id",
        "date_utc",
        "phase_id_detected",
        "phase_state_detected",
        "phase_confidence_detected",
        "distance_to_centroid_detected",
        "global_score",
        "severity",
        "dominant_subsystem_id",
        "dominant_score_component",
    ),
    ("tail_id", "flight_id", "win_id"),
)
ANOMALY_WINDOW_VIEW = ArtifactView(
    "anomaly_window_attribution",
    ("tail_id", "flight_id", "win_id", "dominant_subsystem_id"),
    ("tail_id", "flight_id", "win_id"),
)
ANOMALY_TELEMETRY_VIEW = ArtifactView(
    "anomaly_telemetry_attribution",
    ("tail_id", "flight_id", "win_id", "parameter_name"),
    ("tail_id", "flight_id", "win_id"),
)
ANOMALY_EVENT_VIEW = ArtifactView(
    "anomaly_event_attribution",
    ("tail_id", "flight_id", "win_id", "parameter_name"),
    ("tail_id", "flight_id", "win_id"),
)

VALIDATION_ARTIFACT_VIEWS = (
    RAW_TELEMETRY_REPORT_VIEW,
    RAW_TELEMETRY_PROFILE_VIEW,
    RAW_TELEMETRY_EVENT_LABEL_VIEW,
    RAW_TELEMETRY_SCORE_VIEW,
    DATATYPE_PROFILE_VIEW,
    BEHAVIOR_PRIMITIVE_PROFILE_VIEW,
    BEHAVIOR_PROFILE_VIEW,
    EVENTS_VIEW,
    DETECTED_EVENT_VIEW,
    WINDOWS_VIEW,
    WINDOWS_SCORE_VIEW,
    PHASE_LABELS_VIEW,
    PHASE_WINDOWS_VIEW,
    HIERARCHY_SENSOR_MAP_VIEW,
    HIERARCHY_LABEL_VIEW,
    HIERARCHY_SUBSYSTEM_VIEW,
    HIERARCHY_LABEL_SUBSYSTEM_VIEW,
    COUPLING_TRUTH_VIEW,
    LAG_GRAPH_VIEW,
    PRECISION_GRAPH_VIEW,
    FUSED_GRAPH_VIEW,
    RAW_SCORES_VIEW,
    CALIBRATED_SCORES_VIEW,
    ANOMALY_WINDOW_VIEW,
    ANOMALY_TELEMETRY_VIEW,
    ANOMALY_EVENT_VIEW,
)


@dataclass(frozen=True)
class ValidationReportSet:
    payloads: dict[str, dict[str, Any]]

    def write(self, *, paths: RunPaths) -> None:
        for filename, payload in self.payloads.items():
            write_manifest(paths.run_dir / "reports" / filename, payload)

    def to_payloads(self) -> dict[str, dict[str, Any]]:
        return dict(self.payloads)


def _records_frame(tables: RunArtifactBundle, view: ArtifactView) -> pd.DataFrame:
    return tables.report_frame(view).to_pandas()


def _skipped_report(*, reason: str) -> dict[str, Any]:
    return {
        "status": "skipped",
        "reason": reason,
    }


def _has_artifact(tables: RunArtifactBundle, artifact_name: str) -> bool:
    return tables.table(artifact_name) is not None


def _build_label_contract_summary(
    *,
    raw_telemetry_sdf: Any | None,
    events_sdf: Any | None,
) -> dict[str, Any]:
    from pyspark.sql import functions as F

    raw_columns = list(raw_telemetry_sdf.columns) if raw_telemetry_sdf is not None else []
    event_columns = list(events_sdf.columns) if events_sdf is not None else []
    failures: list[str] = []

    try:
        assert_no_banned_columns(raw_columns)
        assert_required_columns(raw_columns, REQUIRED_LABEL_COLUMNS | REQUIRED_PROFILER_VALIDATOR_LABEL_COLUMNS)
    except AssertionError as exc:
        failures.append(str(exc))

    if event_columns:
        try:
            assert_no_bare_detector_event_type(event_columns)
            assert_required_columns(event_columns, REQUIRED_DETECTED_COLUMNS)
        except AssertionError as exc:
            failures.append(str(exc))

    def _non_empty_count(df: Any | None, column: str) -> int:
        if df is None or column not in df.columns:
            return 0
        if column == "anomaly_score_label":
            count_row = df.agg(F.sum(F.when(F.col(column).isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias("count")).first()
        else:
            text_value = F.trim(F.coalesce(F.col(column).cast("string"), F.lit("")))
            count_row = df.agg(F.sum(F.when(text_value != "", F.lit(1)).otherwise(F.lit(0))).alias("count")).first()
        return int(count_row["count"] or 0)

    return {
        "status": "ok" if not failures else "failed",
        "failures": failures,
        "raw_telemetry_columns": raw_columns,
        "events_columns": event_columns,
        "raw_label_non_null_counts": {
            "event_type_label": _non_empty_count(raw_telemetry_sdf, "event_type_label"),
            "event_misbehavior_label": _non_empty_count(raw_telemetry_sdf, "event_misbehavior_label"),
            "anomaly_type_label": _non_empty_count(raw_telemetry_sdf, "anomaly_type_label"),
            "anomaly_score_label": _non_empty_count(raw_telemetry_sdf, "anomaly_score_label"),
            "misbehavior_family_label": _non_empty_count(raw_telemetry_sdf, "misbehavior_family_label"),
            "coupling_id_label": _non_empty_count(raw_telemetry_sdf, "coupling_id_label"),
            "unit": _non_empty_count(raw_telemetry_sdf, "unit"),
            "rate_hz": _non_empty_count(raw_telemetry_sdf, "rate_hz"),
        },
    }


def _build_profile_validation_summary(tables: RunArtifactBundle) -> dict[str, Any]:
    return build_profile_validation_summary(
        raw_telemetry_df=tables.pandas(RAW_TELEMETRY_PROFILE_VIEW),
        parameter_datatype_profile_df=tables.pandas(DATATYPE_PROFILE_VIEW),
        parameter_behavior_profile_df=tables.pandas(BEHAVIOR_PROFILE_VIEW),
        parameter_behavior_primitive_profile_df=tables.pandas(BEHAVIOR_PRIMITIVE_PROFILE_VIEW),
    )


def _build_event_validation_summary(
    tables: RunArtifactBundle,
    *,
    tolerance_seconds: float = 0.5,
) -> dict[str, Any]:
    return build_event_validation_summary(
        simulator_rows=tables.records(RAW_TELEMETRY_EVENT_LABEL_VIEW),
        detected_events=tables.records(DETECTED_EVENT_VIEW),
        tolerance_seconds=tolerance_seconds,
        label_field="event_type_label",
    )


def _build_phase_validation_summary(tables: RunArtifactBundle) -> dict[str, Any]:
    return validate_detected_phases_from_tables(
        phase_windows_df=tables.pandas(PHASE_WINDOWS_VIEW),
        phase_labels_df=tables.pandas(PHASE_LABELS_VIEW),
        windows_df=tables.pandas(WINDOWS_VIEW),
    )


def _build_graph_validation_summary(
    tables: RunArtifactBundle,
    *,
    expected_lag_edges: tuple[dict[str, str], ...],
    expected_fused_edges: tuple[dict[str, str], ...],
) -> dict[str, Any]:
    return build_graph_validation_summary(
        hierarchy_sensor_map_df=tables.pandas(HIERARCHY_SENSOR_MAP_VIEW),
        hierarchy_label_df=tables.pandas(HIERARCHY_LABEL_VIEW),
        lag_graph_df=tables.pandas(LAG_GRAPH_VIEW),
        fused_graph_df=tables.pandas(FUSED_GRAPH_VIEW),
        expected_lag_edges=expected_lag_edges,
        expected_fused_edges=expected_fused_edges,
    )


def _build_coupling_validation_summary(
    tables: RunArtifactBundle,
    *,
    expected_coupling_signatures: tuple[dict[str, Any], ...],
) -> dict[str, Any]:
    return build_coupling_validation_summary(
        coupling_truth_df=_records_frame(tables, COUPLING_TRUTH_VIEW),
        lag_graph_df=_records_frame(tables, LAG_GRAPH_VIEW),
        precision_graph_df=_records_frame(tables, PRECISION_GRAPH_VIEW),
        fused_graph_df=_records_frame(tables, FUSED_GRAPH_VIEW),
        expected_coupling_signatures=expected_coupling_signatures,
    )


def _build_misbehavior_score_summary(tables: RunArtifactBundle) -> dict[str, Any]:
    return validate_scores_against_misbehavior_windows(
        raw_telemetry_df=tables.pandas(RAW_TELEMETRY_SCORE_VIEW),
        windows_df=tables.pandas(WINDOWS_SCORE_VIEW),
        raw_scores_df=tables.pandas(RAW_SCORES_VIEW),
        calibrated_scores_df=tables.pandas(CALIBRATED_SCORES_VIEW),
    )


def _build_misbehavior_window_summary(summary: dict[str, Any]) -> dict[str, Any]:
    if summary.get("status") != "ok":
        return summary
    return {
        "status": "ok",
        "misbehavior_window_count": int(summary.get("misbehavior_window_count", 0)),
        "detected_misbehavior_window_count": int(summary.get("detected_misbehavior_window_count", 0)),
        "emit_ready_misbehavior_window_count": int(summary.get("emit_ready_misbehavior_window_count", 0)),
        "misbehavior_windows": summary.get("misbehavior_windows", []),
    }


def _remap_windows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            **row,
            "fault_window_id": row.get("fault_window_id", row.get("misbehavior_window_id", "")),
            "fault_family_label": row.get("fault_family_label", ""),
            "fault_type": row.get("fault_type", ""),
        }
        for row in rows
    ]


def build_fault_score_summary_from_misbehavior(summary: dict[str, Any]) -> dict[str, Any]:
    if summary.get("status") != "ok":
        return summary
    return {
        "status": "ok",
        "fault_window_count": int(summary.get("misbehavior_window_count", 0)),
        "detected_fault_window_count": int(summary.get("detected_misbehavior_window_count", 0)),
        "emit_ready_fault_window_count": int(summary.get("emit_ready_misbehavior_window_count", 0)),
        "detected_fault_window_rate": summary.get("detected_misbehavior_window_rate"),
        "emit_ready_fault_window_rate": summary.get("emit_ready_misbehavior_window_rate"),
        "median_fault_window_score": summary.get("median_misbehavior_window_score"),
        "median_detection_latency_seconds": summary.get("median_detection_latency_seconds"),
        "median_emit_ready_latency_seconds": summary.get("median_emit_ready_latency_seconds"),
        "raw_score_validation": summary.get("raw_score_validation"),
        "calibrated_score_validation": summary.get("calibrated_score_validation"),
        "emission_validation": summary.get("emission_validation"),
        "score_window_diagnostics": summary.get("score_window_diagnostics", []),
        "fault_windows": _remap_windows(summary.get("misbehavior_windows", [])),
    }


def _build_fault_window_summary(summary: dict[str, Any]) -> dict[str, Any]:
    if summary.get("status") != "ok":
        return summary
    return {
        "status": "ok",
        "fault_window_count": int(summary.get("misbehavior_window_count", 0)),
        "detected_fault_window_count": int(summary.get("detected_misbehavior_window_count", 0)),
        "emit_ready_fault_window_count": int(summary.get("emit_ready_misbehavior_window_count", 0)),
        "fault_windows": _remap_windows(summary.get("misbehavior_windows", [])),
    }


def _build_misbehavior_attribution_summary(tables: RunArtifactBundle) -> dict[str, Any]:
    return validate_attribution_against_misbehavior_truth(
        raw_telemetry_df=tables.pandas(RAW_TELEMETRY_SCORE_VIEW),
        windows_df=tables.pandas(WINDOWS_VIEW),
        anomaly_window_attribution_df=tables.pandas(ANOMALY_WINDOW_VIEW),
        anomaly_telemetry_attribution_df=tables.pandas(ANOMALY_TELEMETRY_VIEW),
        anomaly_event_attribution_df=tables.pandas(ANOMALY_EVENT_VIEW),
        hierarchy_sensor_map_df=tables.pandas(HIERARCHY_SUBSYSTEM_VIEW),
        hierarchy_label_df=tables.pandas(HIERARCHY_LABEL_SUBSYSTEM_VIEW),
    )


def build_fault_attribution_summary_from_misbehavior(summary: dict[str, Any]) -> dict[str, Any]:
    return build_fault_attribution_summary_from_misbehavior_summary(summary)


def write_validation_reports(
    *,
    spark: Any,
    paths: RunPaths,
    flight: Any,
    table_format: str,
) -> dict[str, Any]:
    tables = RunArtifactBundle.load(
        spark=spark,
        paths=paths,
        table_format=table_format,
        views=VALIDATION_ARTIFACT_VIEWS,
    )
    validation_expectations = dict(flight.metadata.get("validation", {}) or {})
    can_validate_phase = _has_artifact(tables, "phase_windows") and _has_artifact(tables, "phase_labels")
    can_validate_hierarchy = _has_artifact(tables, "hierarchy_sensor_map")
    can_validate_couplings = any(
        _has_artifact(tables, artifact_name)
        for artifact_name in ("lag_graph", "precision_graph", "fused_graph")
    )
    can_validate_scores = _has_artifact(tables, "windows") and _has_artifact(tables, "window_scores_calibrated")
    can_validate_attribution = (
        _has_artifact(tables, "windows")
        and _has_artifact(tables, "hierarchy_sensor_map")
        and any(
            _has_artifact(tables, artifact_name)
            for artifact_name in ("anomaly_window_attribution", "anomaly_telemetry_attribution", "anomaly_event_attribution")
        )
    )

    misbehavior_score_summary = (
        _build_misbehavior_score_summary(tables)
        if can_validate_scores
        else _skipped_report(reason="missing window or calibrated score artifacts")
    )
    misbehavior_window_summary = _build_misbehavior_window_summary(misbehavior_score_summary)
    misbehavior_attribution_summary = (
        _build_misbehavior_attribution_summary(tables)
        if can_validate_attribution
        else _skipped_report(reason="missing window, hierarchy, or anomaly attribution artifacts")
    )
    report_set = ValidationReportSet(
        payloads={
            "profile_validation_summary.json": _build_profile_validation_summary(tables),
            "event_validation_summary.json": _build_event_validation_summary(tables),
            "label_contract_summary.json": _build_label_contract_summary(
                raw_telemetry_sdf=tables.table("raw_telemetry"),
                events_sdf=tables.table("events"),
            ),
            "phase_validation_summary.json": (
                _build_phase_validation_summary(tables)
                if can_validate_phase
                else _skipped_report(reason="missing phase window artifacts")
            ),
            "hierarchy_validation_summary.json": (
                _build_graph_validation_summary(
                    tables,
                    expected_lag_edges=tuple(validation_expectations.get("expected_lag_edges", ()) or ()),
                    expected_fused_edges=tuple(validation_expectations.get("expected_fused_edges", ()) or ()),
                )
                if can_validate_hierarchy
                else _skipped_report(reason="missing hierarchy sensor map artifact")
            ),
            "coupling_validation_summary.json": (
                _build_coupling_validation_summary(
                    tables,
                    expected_coupling_signatures=tuple(validation_expectations.get("expected_coupling_signatures", ()) or ()),
                )
                if can_validate_couplings
                else _skipped_report(reason="missing graph artifacts")
            ),
            "score_validation_summary.json": build_fault_score_summary_from_misbehavior(misbehavior_score_summary),
            "misbehavior_score_validation_summary.json": misbehavior_score_summary,
            "misbehavior_window_validation_summary.json": misbehavior_window_summary,
            "misbehavior_attribution_validation_summary.json": misbehavior_attribution_summary,
            "fault_window_validation_summary.json": _build_fault_window_summary(misbehavior_window_summary),
            "attribution_validation_summary.json": build_fault_attribution_summary_from_misbehavior(
                misbehavior_attribution_summary
            ),
        }
    )
    report_set.write(paths=paths)
    return report_set.to_payloads()
