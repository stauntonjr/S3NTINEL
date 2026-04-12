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
from libs.simulation.fault.spec import (
    BENCHMARK_RECOVERABILITY_PHASES,
    BENCHMARK_RECOVERABILITY_TARGETS,
    OBSERVED_RECOVERABILITY_STRENGTH_TIERS,
    recoverability_target_alignment_status,
    resolve_window_benchmark_recoverability_target,
    resolve_window_fault_window_id,
)
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
        "dominant_module_id",
        "dominant_score_component",
        "score_component_scores",
    ),
    ("tail_id", "flight_id", "win_id"),
)
ANOMALY_WINDOW_VIEW = ArtifactView(
    "anomaly_window_attribution",
    (
        "tail_id",
        "flight_id",
        "win_id",
        "dominant_subsystem_id",
        "dominant_module_id",
        "top_subsystem_candidates",
        "top_module_candidates",
        "dominant_score_component",
    ),
    ("tail_id", "flight_id", "win_id"),
)
ANOMALY_TELEMETRY_VIEW = ArtifactView(
    "anomaly_telemetry_attribution",
    (
        "tail_id",
        "flight_id",
        "win_id",
        "parameter_name",
        "parameter_localization_support",
        "parameter_support_rank_in_window",
        "parameter_localization_selected",
    ),
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


_BENCHMARK_REVIEW_PRIORITY_BY_TIER = {
    "module_recoverable": "low",
    "subsystem_recoverable": "medium",
    "parameter_visible_only": "high",
    "detection_only": "critical",
    "undetected": "critical",
}
_BENCHMARK_REVIEW_ACTION_BY_TIER = {
    "module_recoverable": "keep_as_module_localization_benchmark",
    "subsystem_recoverable": "use_as_subsystem_benchmark_or_improve_module_separation",
    "parameter_visible_only": "review_truth_granularity_and_structural_observability",
    "detection_only": "review_signal_observability_and_fault_design",
    "undetected": "review_detection_signal_and_scenario_validity",
}
_BENCHMARK_REVIEW_PRIORITY_ORDER = {
    "critical": 0,
    "high": 1,
    "medium": 2,
    "low": 3,
}
_DECLARED_TARGET_ALIGNMENT_ORDER = {
    "missed_target": 0,
    "undeclared": 1,
    "met_target": 2,
    "exceeded_target": 3,
}


def _bool_value(value: Any) -> bool:
    return False if pd.isna(value) else bool(value)


def _int_value(value: Any) -> int:
    if pd.isna(value):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _float_value(value: Any) -> float | None:
    if pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _text_value(value: Any, *, default: str = "") -> str:
    if pd.isna(value):
        return default
    text = str(value)
    return text if text else default


def _observed_recoverability_strength_tier(row: dict[str, Any]) -> str:
    if _bool_value(row.get("dominant_module_match")) or _bool_value(row.get("top_module_candidate_present")):
        return "module_recoverable"
    if _bool_value(row.get("dominant_subsystem_match")) or _bool_value(row.get("top_subsystem_candidate_present")):
        return "subsystem_recoverable"
    if (
        _bool_value(row.get("telemetry_parameter_match"))
        or _bool_value(row.get("telemetry_selected_parameter_match"))
        or _bool_value(row.get("event_parameter_match"))
    ):
        return "parameter_visible_only"
    if _bool_value(row.get("detected")) or _bool_value(row.get("emit_ready")):
        return "detection_only"
    return "undetected"


def _declared_target_alignment_status(*, observed_tier: str, declared_target: str) -> str:
    return recoverability_target_alignment_status(
        observed_tier=str(observed_tier),
        declared_target=str(declared_target),
    )


def _group_review_priority(group: pd.DataFrame) -> str:
    priorities = {
        _BENCHMARK_REVIEW_PRIORITY_BY_TIER.get(str(value), "critical")
        for value in group.get("observed_recoverability_strength_tier", pd.Series(dtype="object")).fillna("").astype(str).tolist()
        if str(value)
    }
    if not priorities:
        return "critical"
    return sorted(priorities, key=lambda value: _BENCHMARK_REVIEW_PRIORITY_ORDER.get(value, 99))[0]


def _recoverability_summary_by_field(cases_df: pd.DataFrame, field: str) -> list[dict[str, Any]]:
    if cases_df.empty or field not in cases_df.columns:
        return []

    rows: list[dict[str, Any]] = []
    for raw_value, group in cases_df.groupby(field, dropna=False):
        label = _text_value(raw_value, default="unspecified")
        tier_counts = {
            tier: int(
                (group.get("observed_recoverability_strength_tier", pd.Series(dtype="object")).fillna("").astype(str) == tier).sum()
            )
            for tier in OBSERVED_RECOVERABILITY_STRENGTH_TIERS
        }
        declared_target_counts = {
            target: int(
                (group.get("declared_benchmark_phase", pd.Series(dtype="object")).fillna("").astype(str) == target).sum()
            )
            for target in BENCHMARK_RECOVERABILITY_TARGETS
            if int(
                (group.get("declared_benchmark_phase", pd.Series(dtype="object")).fillna("").astype(str) == target).sum()
            )
            > 0
        }
        alignment_counts = {
            status: int(
                (group.get("declared_target_alignment_status", pd.Series(dtype="object")).fillna("").astype(str) == status).sum()
            )
            for status in _DECLARED_TARGET_ALIGNMENT_ORDER
            if int(
                (group.get("declared_target_alignment_status", pd.Series(dtype="object")).fillna("").astype(str) == status).sum()
            )
            > 0
        }
        total = int(len(group))
        declared_total = int(sum(declared_target_counts.values()))
        rows.append(
            {
                field: label,
                "fault_window_count": total,
                "detected_fault_window_rate": float(group["detected"].mean()) if total > 0 else None,
                "emit_ready_fault_window_rate": float(group["emit_ready"].mean()) if total > 0 else None,
                "module_recoverable_exact_rate": float(
                    (group["observed_recoverability_strength_tier"] == "module_recoverable").mean()
                )
                if total > 0
                else None,
                "subsystem_or_better_rate": float(
                    group["observed_recoverability_strength_tier"]
                    .isin(("module_recoverable", "subsystem_recoverable"))
                    .mean()
                )
                if total > 0
                else None,
                "parameter_or_better_rate": float(
                    group["observed_recoverability_strength_tier"].isin(
                        ("module_recoverable", "subsystem_recoverable", "parameter_visible_only")
                    ).mean()
                )
                if total > 0
                else None,
                "recoverability_strength_tier_count": tier_counts,
                "declared_benchmark_phase_count": declared_target_counts,
                "benchmark_phase_alignment_status_count": alignment_counts,
                "declared_target_coverage_rate": (float(declared_total / total) if total > 0 else None),
                "declared_target_met_or_exceeded_rate": (
                    float(
                        group["declared_target_alignment_status"].isin(("met_target", "exceeded_target")).mean()
                    )
                    if declared_total > 0
                    else None
                ),
                "benchmark_review_priority": _group_review_priority(group),
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            _BENCHMARK_REVIEW_PRIORITY_ORDER.get(str(row.get("benchmark_review_priority")), 99),
            -int(row.get("fault_window_count", 0) or 0),
            str(row.get(field, "")),
        ),
    )


def _build_simulation_benchmark_audit_summary(
    *,
    flight: Any,
    fault_score_summary: dict[str, Any],
    fault_attribution_summary: dict[str, Any],
) -> dict[str, Any]:
    flight_metadata = dict(getattr(flight, "metadata", {}) or {})
    if fault_score_summary.get("status") != "ok" or fault_attribution_summary.get("status") != "ok":
        return _skipped_report(reason="missing score or attribution validation summary")

    score_cases = pd.DataFrame.from_records(fault_score_summary.get("fault_windows", []))
    attribution_cases = pd.DataFrame.from_records(fault_attribution_summary.get("fault_windows", []))
    join_keys = [
        key
        for key in ("tail_id", "flight_id", "fault_window_id")
        if key in score_cases.columns or key in attribution_cases.columns
    ]
    if not join_keys:
        return {
            "status": "ok",
            "flight_name": str(flight_metadata.get("flight_name", "")),
            "fault_window_count": 0,
            "observed_recoverability_strength_tier_count": {tier: 0 for tier in OBSERVED_RECOVERABILITY_STRENGTH_TIERS},
            "observed_recoverability_strength_tier_rate": {tier: None for tier in OBSERVED_RECOVERABILITY_STRENGTH_TIERS},
            "declared_benchmark_phase_count": {},
            "benchmark_phase_alignment_status_count": {},
            "benchmark_review_priority_count": {},
            "dominant_score_component_count": {},
            "summary_by_fault_family": [],
            "summary_by_fault_type": [],
            "summary_by_source_subsystem": [],
            "summary_by_source_module": [],
            "top_review_candidates": [],
            "fault_window_audit_cases": [],
            "methodology": {
                "interpretation": "observed recoverability under the current anomaly stack, not theoretical identifiability",
            },
        }
    for key in join_keys:
        if key not in score_cases.columns:
            score_cases[key] = pd.Series(dtype="object")
        if key not in attribution_cases.columns:
            attribution_cases[key] = pd.Series(dtype="object")

    merged = score_cases.merge(
        attribution_cases,
        on=join_keys,
        how="outer",
        suffixes=("_score", "_attr"),
    )
    declared_windows = []
    for window in tuple(getattr(getattr(flight, "misbehavior_program_spec", None), "windows", ()) or ()):
        fault_window_id = resolve_window_fault_window_id(window)
        if not fault_window_id:
            continue
        declared_windows.append(
            {
                "fault_window_id": str(fault_window_id),
                "declared_benchmark_phase": str(resolve_window_benchmark_recoverability_target(window) or ""),
                "declared_subject_kind": str(getattr(window, "subject_kind", "parameter")),
                "declared_fault_family_label": _text_value(
                    dict(getattr(window, "metadata", {}) or {}).get("fault_family_label")
                    or dict(getattr(window, "context", {}) or {}).get("fault_family_label")
                    or dict(getattr(window, "metadata", {}) or {}).get("misbehavior_family_label")
                    or dict(getattr(window, "context", {}) or {}).get("misbehavior_family_label")
                ),
                "declared_fault_type": _text_value(
                    dict(getattr(window, "metadata", {}) or {}).get("fault_type")
                    or dict(getattr(window, "metadata", {}) or {}).get("misbehavior_detail_label")
                    or dict(getattr(window, "context", {}) or {}).get("misbehavior_detail_label")
                    or dict(getattr(window, "context", {}) or {}).get("violation_type")
                ),
            }
        )
    declared_windows_df = pd.DataFrame.from_records(declared_windows)
    if not declared_windows_df.empty and "fault_window_id" in merged.columns:
        merged = merged.merge(declared_windows_df, on="fault_window_id", how="left")
    else:
        merged["declared_benchmark_phase"] = pd.Series("", index=merged.index)
        merged["declared_subject_kind"] = pd.Series("", index=merged.index)
        merged["declared_fault_family_label"] = pd.Series("", index=merged.index)
        merged["declared_fault_type"] = pd.Series("", index=merged.index)
    merged["fault_family_label"] = merged.apply(
        lambda row: (
            _text_value(row.get("fault_family_label"))
            or _text_value(row.get("fault_family_label_score"))
            or _text_value(row.get("fault_family_label_attr"))
            or _text_value(row.get("declared_fault_family_label"))
        ),
        axis=1,
    )
    merged["fault_type"] = merged.apply(
        lambda row: (
            _text_value(row.get("fault_type"))
            or _text_value(row.get("fault_type_score"))
            or _text_value(row.get("fault_type_attr"))
            or _text_value(row.get("declared_fault_type"))
        ),
        axis=1,
    )
    merged["truth_subsystem_id"] = merged.apply(
        lambda row: (
            _text_value(row.get("subsystem_id"))
            or _text_value(row.get("subsystem_id_score"))
            or _text_value(row.get("subsystem_id_attr"))
        ),
        axis=1,
    )
    merged["truth_module_id"] = merged.apply(
        lambda row: (
            _text_value(row.get("module_id"))
            or _text_value(row.get("module_id_score"))
            or _text_value(row.get("module_id_attr"))
        ),
        axis=1,
    )
    merged["truth_parameter_name"] = merged.apply(
        lambda row: (
            _text_value(row.get("parameter_name"))
            or _text_value(row.get("parameter_name_score"))
            or _text_value(row.get("parameter_name_attr"))
        ),
        axis=1,
    )
    merged["detected"] = merged.get("detected_window_count", pd.Series(0, index=merged.index)).fillna(0).astype(float) > 0.0
    merged["emit_ready"] = merged.get("emit_ready_window_count", pd.Series(0, index=merged.index)).fillna(0).astype(float) > 0.0
    merged["dominant_score_component"] = (
        merged.get("dominant_score_component", pd.Series("", index=merged.index))
        .fillna("")
        .astype(str)
        .replace({"": "unassigned"})
    )
    for column_name in (
        "telemetry_parameter_match",
        "telemetry_selected_parameter_match",
        "event_parameter_match",
        "dominant_subsystem_match",
        "dominant_module_match",
        "top_subsystem_candidate_present",
        "top_module_candidate_present",
    ):
        if column_name not in merged.columns:
            merged[column_name] = False
        else:
            merged[column_name] = merged[column_name].fillna(False).astype(bool)

    merged["observed_recoverability_strength_tier"] = [
        _observed_recoverability_strength_tier(row) for row in merged.to_dict(orient="records")
    ]
    merged["declared_benchmark_phase"] = (
        merged.get("declared_benchmark_phase", pd.Series("", index=merged.index)).fillna("").astype(str)
    )
    merged["declared_target_alignment_status"] = [
        _declared_target_alignment_status(
            observed_tier=str(observed_tier),
            declared_target=str(declared_target),
        )
        for observed_tier, declared_target in zip(
            merged["observed_recoverability_strength_tier"].tolist(),
            merged["declared_benchmark_phase"].tolist(),
            strict=False,
        )
    ]
    merged["benchmark_review_priority"] = merged["observed_recoverability_strength_tier"].map(
        lambda tier: _BENCHMARK_REVIEW_PRIORITY_BY_TIER.get(str(tier), "critical")
    )
    merged["recommended_review_action"] = merged["observed_recoverability_strength_tier"].map(
        lambda tier: _BENCHMARK_REVIEW_ACTION_BY_TIER.get(str(tier), "review_scenario")
    )

    total = int(len(merged))
    tier_count = {
        tier: int((merged["observed_recoverability_strength_tier"] == tier).sum())
        for tier in OBSERVED_RECOVERABILITY_STRENGTH_TIERS
    }
    tier_rate = {
        tier: (float(count / total) if total > 0 else None)
        for tier, count in tier_count.items()
    }
    declared_target_count = {
        target: int((merged["declared_benchmark_phase"] == target).sum())
        for target in BENCHMARK_RECOVERABILITY_TARGETS
        if int((merged["declared_benchmark_phase"] == target).sum()) > 0
    }
    benchmark_phase_alignment_status_count = {
        status: int((merged["declared_target_alignment_status"] == status).sum())
        for status in _DECLARED_TARGET_ALIGNMENT_ORDER
        if int((merged["declared_target_alignment_status"] == status).sum()) > 0
    }
    review_priority_count = {
        priority: int((merged["benchmark_review_priority"] == priority).sum())
        for priority in ("critical", "high", "medium", "low")
        if int((merged["benchmark_review_priority"] == priority).sum()) > 0
    }
    dominant_score_component_count = {
        str(label): int(count)
        for label, count in merged["dominant_score_component"].value_counts(dropna=False).sort_index().items()
        if str(label)
    }
    fault_window_cases = [
        {
            "tail_id": _text_value(row.get("tail_id")),
            "flight_id": _text_value(row.get("flight_id")),
            "fault_window_id": _text_value(row.get("fault_window_id")),
            "fault_family_label": _text_value(row.get("fault_family_label")),
            "fault_type": _text_value(row.get("fault_type")),
            "truth_subsystem_id": _text_value(row.get("truth_subsystem_id")),
            "truth_module_id": _text_value(row.get("truth_module_id")),
            "truth_parameter_name": _text_value(row.get("truth_parameter_name")),
            "declared_subject_kind": _text_value(row.get("declared_subject_kind")),
            "declared_benchmark_phase": _text_value(row.get("declared_benchmark_phase")),
            "declared_target_alignment_status": _text_value(row.get("declared_target_alignment_status")),
            "detected": _bool_value(row.get("detected")),
            "emit_ready": _bool_value(row.get("emit_ready")),
            "detected_window_count": _int_value(row.get("detected_window_count")),
            "emit_ready_window_count": _int_value(row.get("emit_ready_window_count")),
            "detection_latency_seconds": _float_value(row.get("detection_latency_seconds")),
            "emit_ready_latency_seconds": _float_value(row.get("emit_ready_latency_seconds")),
            "dominant_score_component": _text_value(row.get("dominant_score_component"), default="unassigned"),
            "telemetry_parameter_match": _bool_value(row.get("telemetry_parameter_match")),
            "telemetry_selected_parameter_match": _bool_value(row.get("telemetry_selected_parameter_match")),
            "event_parameter_match": _bool_value(row.get("event_parameter_match")),
            "dominant_subsystem_match": _bool_value(row.get("dominant_subsystem_match")),
            "dominant_module_match": _bool_value(row.get("dominant_module_match")),
            "top_subsystem_candidate_present": _bool_value(row.get("top_subsystem_candidate_present")),
            "top_module_candidate_present": _bool_value(row.get("top_module_candidate_present")),
            "observed_recoverability_strength_tier": _text_value(row.get("observed_recoverability_strength_tier")),
            "benchmark_review_priority": _text_value(row.get("benchmark_review_priority")),
            "recommended_review_action": _text_value(row.get("recommended_review_action")),
        }
        for row in merged.sort_values(
            ["declared_target_alignment_status", "benchmark_review_priority", "fault_family_label", "fault_type", "fault_window_id"],
            key=lambda series: (
                series.map(lambda value: _DECLARED_TARGET_ALIGNMENT_ORDER.get(str(value), 99))
                if series.name == "declared_target_alignment_status"
                else series.map(lambda value: _BENCHMARK_REVIEW_PRIORITY_ORDER.get(str(value), 99))
                if series.name == "benchmark_review_priority"
                else series
            ),
            kind="mergesort",
        ).to_dict(orient="records")
    ]
    summary_by_fault_family = _recoverability_summary_by_field(merged, "fault_family_label")
    summary_by_fault_type = _recoverability_summary_by_field(merged, "fault_type")
    summary_by_source_subsystem = _recoverability_summary_by_field(merged, "truth_subsystem_id")
    summary_by_source_module = _recoverability_summary_by_field(merged, "truth_module_id")
    top_review_candidates = [
        {
            "fault_type": row.get("fault_type", ""),
            "fault_window_count": row.get("fault_window_count", 0),
            "benchmark_review_priority": row.get("benchmark_review_priority", ""),
            "declared_benchmark_phase_count": row.get("declared_benchmark_phase_count", {}),
            "benchmark_phase_alignment_status_count": row.get("benchmark_phase_alignment_status_count", {}),
            "declared_target_met_or_exceeded_rate": row.get("declared_target_met_or_exceeded_rate"),
            "module_recoverable_exact_rate": row.get("module_recoverable_exact_rate"),
            "subsystem_or_better_rate": row.get("subsystem_or_better_rate"),
            "parameter_or_better_rate": row.get("parameter_or_better_rate"),
        }
        for row in summary_by_fault_type
        if (
            str(row.get("benchmark_review_priority", "")) in {"critical", "high"}
            or int((row.get("benchmark_phase_alignment_status_count", {}) or {}).get("missed_target", 0)) > 0
        )
    ][:8]
    return {
        "status": "ok",
        "flight_name": str(flight_metadata.get("flight_name", "")),
        "fault_window_count": total,
        "detected_fault_window_count": int(merged["detected"].sum()) if total > 0 else 0,
        "emit_ready_fault_window_count": int(merged["emit_ready"].sum()) if total > 0 else 0,
        "observed_recoverability_strength_tier_count": tier_count,
        "observed_recoverability_strength_tier_rate": tier_rate,
        "declared_benchmark_phase_count": declared_target_count,
        "benchmark_phase_alignment_status_count": benchmark_phase_alignment_status_count,
        "benchmark_review_priority_count": review_priority_count,
        "dominant_score_component_count": dominant_score_component_count,
        "summary_by_fault_family": summary_by_fault_family,
        "summary_by_fault_type": summary_by_fault_type,
        "summary_by_source_subsystem": summary_by_source_subsystem,
        "summary_by_source_module": summary_by_source_module,
        "top_review_candidates": top_review_candidates,
        "fault_window_audit_cases": fault_window_cases,
        "methodology": {
            "interpretation": "observed recoverability under the current anomaly stack, not theoretical identifiability",
            "development_phase_order": list(BENCHMARK_RECOVERABILITY_PHASES),
            "observed_strength_order": list(OBSERVED_RECOVERABILITY_STRENGTH_TIERS),
            "declared_target_order": list(BENCHMARK_RECOVERABILITY_TARGETS),
            "tier_definitions": {
                "module_recoverable": "truth module matched or present in top module candidates",
                "subsystem_recoverable": "truth subsystem matched or present in top subsystem candidates without module recovery",
                "parameter_visible_only": "truth parameter surfaced but no structural candidate recovery",
                "detection_only": "fault detected without truth parameter or structure recovery",
                "undetected": "fault not detected in the current run",
            },
            "declared_target_alignment_definitions": {
                "missed_target": "observed recoverability fell below the declared benchmark target",
                "undeclared": "no declared benchmark target was attached to the truth fault window",
                "met_target": "observed recoverability matched the declared benchmark target",
                "exceeded_target": "observed recoverability exceeded the declared benchmark target",
            },
        },
    }


def _build_misbehavior_attribution_summary(tables: RunArtifactBundle) -> dict[str, Any]:
    return validate_attribution_against_misbehavior_truth(
        raw_telemetry_df=tables.pandas(RAW_TELEMETRY_SCORE_VIEW),
        windows_df=tables.pandas(WINDOWS_VIEW),
        anomaly_window_attribution_df=tables.pandas(ANOMALY_WINDOW_VIEW),
        anomaly_telemetry_attribution_df=tables.pandas(ANOMALY_TELEMETRY_VIEW),
        anomaly_event_attribution_df=tables.pandas(ANOMALY_EVENT_VIEW),
        hierarchy_sensor_map_df=tables.pandas(HIERARCHY_SENSOR_MAP_VIEW),
        hierarchy_label_df=tables.pandas(HIERARCHY_LABEL_VIEW),
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
    fault_score_summary = build_fault_score_summary_from_misbehavior(misbehavior_score_summary)
    fault_attribution_summary = build_fault_attribution_summary_from_misbehavior(
        misbehavior_attribution_summary
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
            "score_validation_summary.json": fault_score_summary,
            "misbehavior_score_validation_summary.json": misbehavior_score_summary,
            "misbehavior_window_validation_summary.json": misbehavior_window_summary,
            "misbehavior_attribution_validation_summary.json": misbehavior_attribution_summary,
            "fault_window_validation_summary.json": _build_fault_window_summary(misbehavior_window_summary),
            "attribution_validation_summary.json": fault_attribution_summary,
            "simulation_benchmark_audit_summary.json": _build_simulation_benchmark_audit_summary(
                flight=flight,
                fault_score_summary=fault_score_summary,
                fault_attribution_summary=fault_attribution_summary,
            ),
        }
    )
    report_set.write(paths=paths)
    return report_set.to_payloads()
