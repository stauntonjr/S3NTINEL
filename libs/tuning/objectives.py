"""Objective specifications and evaluation over validation harness reports."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal


MetricCategory = Literal["validation", "compute"]
ObjectiveDirection = Literal["maximize", "minimize"]
ConstraintOperator = Literal["<", "<=", ">", ">=", "=="]

DEFAULT_COMPARE_BY = (
    "workload_signature.source.flight_name",
    "workload_signature.simulation.n_steps",
    "workload_signature.simulation.dt_seconds",
    "workload_signature.pipeline.mode",
    "workload_signature.stochasticity.profile_name",
    "workload_signature.stochasticity.profile_version",
    "workload_signature.stochasticity.seed",
)
DEFAULT_OBJECTIVE_NAME_BY_MODE = {
    "profile": "sim_profile_default_v1",
    "event": "sim_event_default_v1",
    "structural": "sim_structural_default_v1",
    "full": "sim_full_default_v1",
}


def _finite_number(value: Any) -> float | int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _lookup_path(payload: dict[str, Any], path: str) -> Any:
    current: Any = payload
    for segment in str(path).split("."):
        if not isinstance(current, dict) or segment not in current:
            return None
        current = current[segment]
    return current


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


@dataclass(frozen=True)
class ObjectiveMetricRef:
    category: MetricCategory
    scope_name: str
    subscope_name: str
    metric_path: str

    def key(self) -> tuple[str, str, str, str]:
        return (self.category, self.scope_name, self.subscope_name, self.metric_path)

    def label(self) -> str:
        return f"{self.category}:{self.scope_name}:{self.subscope_name}:{self.metric_path}"

    def to_payload(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "scope_name": self.scope_name,
            "subscope_name": self.subscope_name,
            "metric_path": self.metric_path,
        }


@dataclass(frozen=True)
class ObjectiveTerm:
    metric: ObjectiveMetricRef
    direction: ObjectiveDirection
    weight: float = 1.0
    required: bool = True
    lower_bound: float | None = None
    upper_bound: float | None = None
    label: str | None = None

    def resolved_label(self) -> str:
        return str(self.label or self.metric.label())

    def to_payload(self) -> dict[str, Any]:
        payload = {
            "metric": self.metric.to_payload(),
            "direction": self.direction,
            "weight": float(self.weight),
            "required": bool(self.required),
        }
        if self.label is not None:
            payload["label"] = self.label
        if self.lower_bound is not None:
            payload["lower_bound"] = float(self.lower_bound)
        if self.upper_bound is not None:
            payload["upper_bound"] = float(self.upper_bound)
        return payload


@dataclass(frozen=True)
class ObjectiveConstraint:
    metric: ObjectiveMetricRef
    op: ConstraintOperator
    threshold: float
    required: bool = True
    label: str | None = None

    def resolved_label(self) -> str:
        return str(self.label or self.metric.label())

    def to_payload(self) -> dict[str, Any]:
        return {
            "metric": self.metric.to_payload(),
            "op": self.op,
            "threshold": float(self.threshold),
            "required": bool(self.required),
            "label": self.resolved_label(),
        }


@dataclass(frozen=True)
class ObjectiveSpec:
    name: str
    primary_terms: tuple[ObjectiveTerm, ...]
    constraints: tuple[ObjectiveConstraint, ...] = ()
    tie_break_terms: tuple[ObjectiveTerm, ...] = ()
    compare_by: tuple[str, ...] = DEFAULT_COMPARE_BY
    evaluation_tier: str = "full"
    required_end_stage_script: str = "95_emit_explorer_bundle.py"
    description: str = ""

    def to_payload(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "compare_by": list(self.compare_by),
            "evaluation_tier": self.evaluation_tier,
            "required_end_stage_script": self.required_end_stage_script,
            "primary_terms": [term.to_payload() for term in self.primary_terms],
            "constraints": [constraint.to_payload() for constraint in self.constraints],
            "tie_break_terms": [term.to_payload() for term in self.tie_break_terms],
        }


@dataclass(frozen=True)
class ObjectiveTermEvaluation:
    label: str
    metric: ObjectiveMetricRef
    direction: ObjectiveDirection
    weight: float
    required: bool
    status: str
    actual_value: float | int | None
    preference_value: float | None
    normalized_score: float | None
    weighted_score: float | None
    notes: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        payload = {
            "label": self.label,
            "metric": self.metric.to_payload(),
            "direction": self.direction,
            "weight": float(self.weight),
            "required": bool(self.required),
            "status": self.status,
            "actual_value": self.actual_value,
            "preference_value": self.preference_value,
            "normalized_score": self.normalized_score,
            "weighted_score": self.weighted_score,
            "notes": list(self.notes),
        }
        return payload


@dataclass(frozen=True)
class ObjectiveConstraintEvaluation:
    label: str
    metric: ObjectiveMetricRef
    op: ConstraintOperator
    threshold: float
    required: bool
    status: str
    actual_value: float | int | None
    passed: bool | None
    notes: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "metric": self.metric.to_payload(),
            "op": self.op,
            "threshold": float(self.threshold),
            "required": bool(self.required),
            "status": self.status,
            "actual_value": self.actual_value,
            "passed": self.passed,
            "notes": list(self.notes),
        }


@dataclass(frozen=True)
class ObjectiveEvaluation:
    spec: ObjectiveSpec
    harness_status: str | None
    comparison_signature: dict[str, Any]
    comparable: bool
    constraint_results: tuple[ObjectiveConstraintEvaluation, ...]
    primary_term_results: tuple[ObjectiveTermEvaluation, ...]
    tie_break_term_results: tuple[ObjectiveTermEvaluation, ...]
    constraint_pass: bool
    required_primary_term_coverage_pass: bool
    objective_score: float | None
    tie_break_score: float | None
    combined_score: float | None
    ready_for_search: bool
    overall_status: str
    notes: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        return {
            "objective_spec": self.spec.to_payload(),
            "harness_status": self.harness_status,
            "comparison_signature": self.comparison_signature,
            "comparable": self.comparable,
            "constraint_results": [result.to_payload() for result in self.constraint_results],
            "primary_term_results": [result.to_payload() for result in self.primary_term_results],
            "tie_break_term_results": [result.to_payload() for result in self.tie_break_term_results],
            "constraint_pass": self.constraint_pass,
            "required_primary_term_coverage_pass": self.required_primary_term_coverage_pass,
            "objective_score": self.objective_score,
            "tie_break_score": self.tie_break_score,
            "combined_score": self.combined_score,
            "ready_for_search": self.ready_for_search,
            "overall_status": self.overall_status,
            "notes": list(self.notes),
        }


def _metric(
    category: MetricCategory,
    scope_name: str,
    subscope_name: str,
    metric_path: str,
) -> ObjectiveMetricRef:
    return ObjectiveMetricRef(
        category=category,
        scope_name=scope_name,
        subscope_name=subscope_name,
        metric_path=metric_path,
    )


def _objective_metric_ref_from_payload(payload: dict[str, Any]) -> ObjectiveMetricRef:
    return ObjectiveMetricRef(
        category=str(payload.get("category") or ""),
        scope_name=str(payload.get("scope_name") or ""),
        subscope_name=str(payload.get("subscope_name") or ""),
        metric_path=str(payload.get("metric_path") or ""),
    )


def _objective_term_from_payload(payload: dict[str, Any]) -> ObjectiveTerm:
    return ObjectiveTerm(
        metric=_objective_metric_ref_from_payload(dict(payload.get("metric") or {})),
        direction=str(payload.get("direction") or "maximize"),
        weight=float(payload.get("weight", 1.0)),
        required=bool(payload.get("required", True)),
        lower_bound=(
            None if payload.get("lower_bound") is None else float(payload.get("lower_bound"))
        ),
        upper_bound=(
            None if payload.get("upper_bound") is None else float(payload.get("upper_bound"))
        ),
        label=(None if payload.get("label") is None else str(payload.get("label"))),
    )


def _objective_constraint_from_payload(payload: dict[str, Any]) -> ObjectiveConstraint:
    return ObjectiveConstraint(
        metric=_objective_metric_ref_from_payload(dict(payload.get("metric") or {})),
        op=str(payload.get("op") or "<="),
        threshold=float(payload.get("threshold", 0.0)),
        required=bool(payload.get("required", True)),
        label=(None if payload.get("label") is None else str(payload.get("label"))),
    )


def _profile_objective_spec() -> ObjectiveSpec:
    return ObjectiveSpec(
        name="sim_profile_default_v1",
        evaluation_tier="profile",
        required_end_stage_script="15_event_profiles_fit.py",
        description="Profile-mode default objective emphasizing datatype and behavior fidelity with lightweight compute tie-breaks.",
        primary_terms=(
            ObjectiveTerm(
                metric=_metric("validation", "overall", "profile_validation", "datatype_accuracy"),
                direction="maximize",
                weight=1.0,
                label="profile datatype accuracy",
            ),
            ObjectiveTerm(
                metric=_metric("validation", "overall", "profile_validation", "behavior_accuracy"),
                direction="maximize",
                weight=1.5,
                label="profile behavior accuracy",
            ),
            ObjectiveTerm(
                metric=_metric("validation", "overall", "profile_validation", "behavior_profile_coverage"),
                direction="maximize",
                weight=0.75,
                label="profile behavior coverage",
            ),
        ),
        tie_break_terms=(
            ObjectiveTerm(
                metric=_metric("compute", "overall", "overall", "pipeline_summary.total_elapsed_ms"),
                direction="minimize",
                weight=0.35,
                label="total elapsed ms",
            ),
            ObjectiveTerm(
                metric=_metric("compute", "overall", "overall", "artifact_disk_bytes_total"),
                direction="minimize",
                weight=0.15,
                label="artifact disk bytes",
            ),
        ),
    )


def _windowing_objective_spec() -> ObjectiveSpec:
    return ObjectiveSpec(
        name="sim_windowing_default_v1",
        evaluation_tier="structural",
        required_end_stage_script="60_fit_hierarchy.py",
        description="Windowing-stage default objective emphasizing stage-25 policy quality, boundary stability, bounded downstream cost, and downstream hierarchy fit.",
        primary_terms=(
            ObjectiveTerm(
                metric=_metric(
                    "validation",
                    "overall",
                    "window_policy_profile",
                    "edge_stability.mean_boundary_jaccard",
                ),
                direction="maximize",
                weight=1.5,
                label="window boundary stability",
            ),
            ObjectiveTerm(
                metric=_metric(
                    "validation",
                    "overall",
                    "window_policy_profile",
                    "selected_balance_penalty",
                ),
                direction="minimize",
                weight=1.0,
                label="window policy penalty",
            ),
            ObjectiveTerm(
                metric=_metric(
                    "validation",
                    "overall",
                    "window_policy_profile",
                    "downstream_cost_proxy.pair_cost_proxy",
                ),
                direction="minimize",
                weight=1.0,
                label="window pair cost proxy",
            ),
            ObjectiveTerm(
                metric=_metric(
                    "validation",
                    "overall",
                    "window_policy_profile",
                    "downstream_cost_proxy.same_window_pair_expansion_proxy",
                ),
                direction="minimize",
                weight=0.75,
                label="same-window pair expansion proxy",
            ),
            ObjectiveTerm(
                metric=_metric(
                    "validation",
                    "overall",
                    "hierarchy_validation",
                    "module_exact_match",
                ),
                direction="maximize",
                weight=0.75,
                label="hierarchy module exact match",
            ),
            ObjectiveTerm(
                metric=_metric(
                    "validation",
                    "overall",
                    "hierarchy_validation",
                    "subsystem_exact_match",
                ),
                direction="maximize",
                weight=0.5,
                label="hierarchy subsystem exact match",
            ),
        ),
        constraints=(
            ObjectiveConstraint(
                metric=_metric(
                    "validation",
                    "overall",
                    "window_policy_profile",
                    "edge_stability.mean_boundary_jaccard",
                ),
                op=">=",
                threshold=0.5,
                label="window boundary stability",
            ),
        ),
        tie_break_terms=(
            ObjectiveTerm(
                metric=_metric(
                    "compute",
                    "25_window_policy_profile.py",
                    "engineering_performance",
                    "elapsed_ms",
                ),
                direction="minimize",
                weight=0.25,
                label="window policy profile elapsed ms",
            ),
            ObjectiveTerm(
                metric=_metric(
                    "compute",
                    "30_windows_adaptive.py",
                    "engineering_performance",
                    "elapsed_ms",
                ),
                direction="minimize",
                weight=0.25,
                label="window build elapsed ms",
            ),
            ObjectiveTerm(
                metric=_metric("compute", "overall", "overall", "pipeline_summary.total_elapsed_ms"),
                direction="minimize",
                weight=0.25,
                label="total elapsed ms",
            ),
        ),
    )


def _structural_objective_spec() -> ObjectiveSpec:
    return ObjectiveSpec(
        name="sim_structural_default_v1",
        evaluation_tier="structural",
        required_end_stage_script="60_fit_hierarchy.py",
        description="Structural-mode default objective emphasizing profile, event, backbone, graph, and hierarchy quality before compute tie-breaks.",
        primary_terms=(
            ObjectiveTerm(
                metric=_metric("validation", "overall", "profile_validation", "behavior_accuracy"),
                direction="maximize",
                weight=1.0,
                label="profile behavior accuracy",
            ),
            ObjectiveTerm(
                metric=_metric("validation", "overall", "event_validation", "f1"),
                direction="maximize",
                weight=1.5,
                label="event validation f1",
            ),
            ObjectiveTerm(
                metric=_metric("validation", "overall", "hierarchy_validation", "module_exact_match"),
                direction="maximize",
                weight=1.5,
                label="hierarchy module exact match",
            ),
            ObjectiveTerm(
                metric=_metric("validation", "overall", "hierarchy_validation", "subsystem_exact_match"),
                direction="maximize",
                weight=1.0,
                label="hierarchy subsystem exact match",
            ),
            ObjectiveTerm(
                metric=_metric("validation", "overall", "coupling_validation", "signature_hit_rate"),
                direction="maximize",
                weight=0.75,
                required=False,
                label="coupling signature hit rate",
            ),
        ),
        tie_break_terms=(
            ObjectiveTerm(
                metric=_metric("compute", "overall", "overall", "pipeline_summary.total_elapsed_ms"),
                direction="minimize",
                weight=0.35,
                label="total elapsed ms",
            ),
            ObjectiveTerm(
                metric=_metric("compute", "overall", "overall", "artifact_disk_bytes_total"),
                direction="minimize",
                weight=0.15,
                label="artifact disk bytes",
            ),
        ),
    )


def _event_objective_spec() -> ObjectiveSpec:
    return ObjectiveSpec(
        name="sim_event_default_v1",
        evaluation_tier="event",
        required_end_stage_script="20_events_extract.py",
        description="Event-mode default objective emphasizing run-level numeric slope capture, event-level slope quality, and precision guardrails for the 15/20 tuning loop.",
        primary_terms=(
            ObjectiveTerm(
                metric=_metric("validation", "overall", "event_validation", "slope_run_capture_metrics.slope_pos.run_recall"),
                direction="maximize",
                weight=1.5,
                label="slope_pos run recall",
            ),
            ObjectiveTerm(
                metric=_metric("validation", "overall", "event_validation", "slope_run_capture_metrics.slope_neg.run_recall"),
                direction="maximize",
                weight=1.5,
                label="slope_neg run recall",
            ),
            ObjectiveTerm(
                metric=_metric("validation", "overall", "event_validation", "event_family_metrics.slope_pos.f1"),
                direction="maximize",
                weight=1.0,
                label="slope_pos f1",
            ),
            ObjectiveTerm(
                metric=_metric("validation", "overall", "event_validation", "event_family_metrics.slope_neg.f1"),
                direction="maximize",
                weight=1.0,
                label="slope_neg f1",
            ),
            ObjectiveTerm(
                metric=_metric("validation", "overall", "event_validation", "precision"),
                direction="maximize",
                weight=0.5,
                label="event validation precision",
            ),
            ObjectiveTerm(
                metric=_metric("validation", "overall", "event_validation", "event_family_metrics.transition.f1"),
                direction="maximize",
                weight=0.25,
                required=False,
                label="transition f1",
            ),
        ),
        constraints=(
            ObjectiveConstraint(
                metric=_metric("validation", "overall", "event_validation", "slope_run_capture_metrics.slope_pos.run_recall"),
                op=">=",
                threshold=0.40,
                label="slope_pos run recall",
            ),
            ObjectiveConstraint(
                metric=_metric("validation", "overall", "event_validation", "slope_run_capture_metrics.slope_neg.run_recall"),
                op=">=",
                threshold=0.40,
                label="slope_neg run recall",
            ),
            ObjectiveConstraint(
                metric=_metric("validation", "overall", "event_validation", "precision"),
                op=">=",
                threshold=0.10,
                label="event validation precision",
            ),
        ),
        tie_break_terms=(
            ObjectiveTerm(
                metric=_metric("compute", "20_events_extract.py", "engineering_performance", "elapsed_ms"),
                direction="minimize",
                weight=0.35,
                label="event extraction elapsed ms",
            ),
            ObjectiveTerm(
                metric=_metric("compute", "15_event_profiles_fit.py", "engineering_performance", "elapsed_ms"),
                direction="minimize",
                weight=0.15,
                label="event profile fit elapsed ms",
            ),
            ObjectiveTerm(
                metric=_metric("compute", "overall", "overall", "pipeline_summary.total_elapsed_ms"),
                direction="minimize",
                weight=0.25,
                label="total elapsed ms",
            ),
        ),
    )


def _full_objective_spec() -> ObjectiveSpec:
    return ObjectiveSpec(
        name="sim_full_default_v1",
        evaluation_tier="full",
        required_end_stage_script="95_emit_explorer_bundle.py",
        description="Full-mode default objective emphasizing end-to-end anomaly detection and attribution quality with compute tie-breaks.",
        primary_terms=(
            ObjectiveTerm(
                metric=_metric("validation", "overall", "profile_validation", "behavior_accuracy"),
                direction="maximize",
                weight=0.75,
                label="profile behavior accuracy",
            ),
            ObjectiveTerm(
                metric=_metric("validation", "overall", "event_validation", "f1"),
                direction="maximize",
                weight=1.0,
                label="event validation f1",
            ),
            ObjectiveTerm(
                metric=_metric("validation", "overall", "hierarchy_validation", "module_exact_match"),
                direction="maximize",
                weight=1.0,
                label="hierarchy module exact match",
            ),
            ObjectiveTerm(
                metric=_metric("validation", "overall", "phase_validation", "macro_f1"),
                direction="maximize",
                weight=1.5,
                label="phase macro f1",
            ),
            ObjectiveTerm(
                metric=_metric(
                    "validation",
                    "overall",
                    "benchmark_scope_validation",
                    "score_validation_by_benchmark_scope.detection.detected_fault_window_rate",
                ),
                direction="maximize",
                weight=2.5,
                label="detected fault window rate",
            ),
            ObjectiveTerm(
                metric=_metric(
                    "validation",
                    "overall",
                    "benchmark_scope_validation",
                    "score_validation_by_benchmark_scope.detection.emit_ready_fault_window_rate",
                ),
                direction="maximize",
                weight=1.5,
                label="emit-ready fault window rate",
            ),
            ObjectiveTerm(
                metric=_metric(
                    "validation",
                    "overall",
                    "benchmark_scope_validation",
                    "attribution_validation_by_benchmark_scope.subsystem.dominant_subsystem_match_rate",
                ),
                direction="maximize",
                weight=2.0,
                label="dominant subsystem match rate",
            ),
            ObjectiveTerm(
                metric=_metric(
                    "validation",
                    "overall",
                    "benchmark_scope_validation",
                    "attribution_validation_by_benchmark_scope.parameter.telemetry_parameter_match_rate",
                ),
                direction="maximize",
                weight=1.5,
                label="telemetry parameter match rate",
            ),
            ObjectiveTerm(
                metric=_metric(
                    "validation",
                    "overall",
                    "benchmark_scope_validation",
                    "attribution_validation_by_benchmark_scope.parameter.event_parameter_match_rate",
                ),
                direction="maximize",
                weight=1.0,
                label="event parameter match rate",
            ),
        ),
        tie_break_terms=(
            ObjectiveTerm(
                metric=_metric("compute", "overall", "overall", "pipeline_summary.total_elapsed_ms"),
                direction="minimize",
                weight=0.35,
                label="total elapsed ms",
            ),
            ObjectiveTerm(
                metric=_metric("compute", "overall", "overall", "artifact_disk_bytes_total"),
                direction="minimize",
                weight=0.15,
                label="artifact disk bytes",
            ),
        ),
    )


def build_default_objective_spec(*, harness_report: dict[str, Any]) -> ObjectiveSpec:
    mode = str(_lookup_path(harness_report, "workload_signature.pipeline.mode") or "").strip().lower()
    if mode == "profile":
        return _profile_objective_spec()
    if mode == "event":
        return _event_objective_spec()
    if mode == "structural":
        return _structural_objective_spec()
    return _full_objective_spec()


def resolve_default_objective_name(*, mode: str) -> str:
    normalized_mode = str(mode).strip().lower()
    if normalized_mode not in DEFAULT_OBJECTIVE_NAME_BY_MODE:
        raise ValueError(f"unsupported default objective mode {mode!r}")
    return DEFAULT_OBJECTIVE_NAME_BY_MODE[normalized_mode]


def objective_spec_from_payload(payload: dict[str, Any]) -> ObjectiveSpec:
    return ObjectiveSpec(
        name=str(payload.get("name") or ""),
        description=str(payload.get("description") or ""),
        compare_by=tuple(str(path) for path in list(payload.get("compare_by") or DEFAULT_COMPARE_BY)),
        evaluation_tier=str(payload.get("evaluation_tier") or "full"),
        required_end_stage_script=str(payload.get("required_end_stage_script") or "95_emit_explorer_bundle.py"),
        primary_terms=tuple(
            _objective_term_from_payload(dict(term_payload))
            for term_payload in list(payload.get("primary_terms") or [])
            if isinstance(term_payload, dict)
        ),
        constraints=tuple(
            _objective_constraint_from_payload(dict(constraint_payload))
            for constraint_payload in list(payload.get("constraints") or [])
            if isinstance(constraint_payload, dict)
        ),
        tie_break_terms=tuple(
            _objective_term_from_payload(dict(term_payload))
            for term_payload in list(payload.get("tie_break_terms") or [])
            if isinstance(term_payload, dict)
        ),
    )


def resolve_objective_spec(*, objective_name: str) -> ObjectiveSpec:
    normalized_name = str(objective_name).strip()
    if normalized_name == "sim_profile_default_v1":
        return _profile_objective_spec()
    if normalized_name == "sim_event_default_v1":
        return _event_objective_spec()
    if normalized_name == "sim_windowing_default_v1":
        return _windowing_objective_spec()
    if normalized_name == "sim_structural_default_v1":
        return _structural_objective_spec()
    if normalized_name == "sim_full_default_v1":
        return _full_objective_spec()
    raise ValueError(f"unknown objective spec name {objective_name!r}")


def load_objective_spec(path: str | Path) -> ObjectiveSpec:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        if isinstance(payload.get("evaluation"), dict):
            evaluation_payload = dict(payload.get("evaluation") or {})
            objective_payload = dict(evaluation_payload.get("objective_spec") or {})
            if objective_payload:
                return objective_spec_from_payload(objective_payload)
        if isinstance(payload.get("objective_spec"), dict):
            return objective_spec_from_payload(dict(payload.get("objective_spec") or {}))
        return objective_spec_from_payload(payload)
    raise ValueError(f"objective spec payload at {path!r} must be a JSON object")


def resolve_objective_evaluation_tier(*, objective_name: str) -> str:
    return resolve_objective_spec(objective_name=objective_name).evaluation_tier


def resolve_objective_required_end_stage(*, objective_name: str) -> str:
    return resolve_objective_spec(objective_name=objective_name).required_end_stage_script


KNOWN_OBJECTIVE_SPEC_NAMES = tuple(
    sorted(
        (
            _profile_objective_spec().name,
            _event_objective_spec().name,
            _windowing_objective_spec().name,
            _structural_objective_spec().name,
            _full_objective_spec().name,
        )
    )
)


def _metric_index(harness_report: dict[str, Any]) -> dict[tuple[str, str, str, str], float | int]:
    index: dict[tuple[str, str, str, str], float | int] = {}
    for group_key in ("validation_metrics", "compute_performance"):
        for record in list((harness_report.get(group_key) or {}).get("metric_records") or []):
            category = str(record.get("category", "") or "")
            scope_name = str(record.get("scope_name", "") or "")
            subscope_name = str(record.get("subscope_name", "") or "")
            metric_path = str(record.get("metric_path", "") or "")
            metric_value = _finite_number(record.get("value"))
            if not category or not scope_name or not subscope_name or not metric_path or metric_value is None:
                continue
            index[(category, scope_name, subscope_name, metric_path)] = metric_value
    return index


def _normalized_score(term: ObjectiveTerm, *, value: float) -> float | None:
    lower_bound = term.lower_bound
    upper_bound = term.upper_bound
    if lower_bound is not None and upper_bound is not None and float(upper_bound) > float(lower_bound):
        span = float(upper_bound) - float(lower_bound)
        if term.direction == "maximize":
            return _clamp01((float(value) - float(lower_bound)) / span)
        return _clamp01((float(upper_bound) - float(value)) / span)
    if 0.0 <= float(value) <= 1.0:
        return float(value) if term.direction == "maximize" else float(1.0 - float(value))
    if float(value) < 0.0:
        return None
    if term.direction == "maximize":
        return float(float(value) / (1.0 + float(value)))
    return float(1.0 / (1.0 + float(value)))


def _evaluate_term(
    term: ObjectiveTerm,
    *,
    metric_index: dict[tuple[str, str, str, str], float | int],
) -> ObjectiveTermEvaluation:
    actual_value = metric_index.get(term.metric.key())
    if actual_value is None:
        return ObjectiveTermEvaluation(
            label=term.resolved_label(),
            metric=term.metric,
            direction=term.direction,
            weight=float(term.weight),
            required=bool(term.required),
            status="missing",
            actual_value=None,
            preference_value=None,
            normalized_score=None,
            weighted_score=None,
            notes=("missing metric record",),
        )
    resolved_value = float(actual_value)
    normalized_score = _normalized_score(term, value=resolved_value)
    preference_value = resolved_value if term.direction == "maximize" else (-resolved_value)
    return ObjectiveTermEvaluation(
        label=term.resolved_label(),
        metric=term.metric,
        direction=term.direction,
        weight=float(term.weight),
        required=bool(term.required),
        status="ok",
        actual_value=actual_value,
        preference_value=float(preference_value),
        normalized_score=normalized_score,
        weighted_score=(None if normalized_score is None else float(normalized_score * float(term.weight))),
        notes=(() if normalized_score is not None else ("could not normalize metric for scalar scoring",)),
    )


def _evaluate_constraint(
    constraint: ObjectiveConstraint,
    *,
    metric_index: dict[tuple[str, str, str, str], float | int],
) -> ObjectiveConstraintEvaluation:
    actual_value = metric_index.get(constraint.metric.key())
    if actual_value is None:
        return ObjectiveConstraintEvaluation(
            label=constraint.resolved_label(),
            metric=constraint.metric,
            op=constraint.op,
            threshold=float(constraint.threshold),
            required=bool(constraint.required),
            status="missing",
            actual_value=None,
            passed=None,
            notes=("missing metric record",),
        )

    value = float(actual_value)
    threshold = float(constraint.threshold)
    if constraint.op == "<":
        passed = value < threshold
    elif constraint.op == "<=":
        passed = value <= threshold
    elif constraint.op == ">":
        passed = value > threshold
    elif constraint.op == ">=":
        passed = value >= threshold
    else:
        passed = value == threshold
    return ObjectiveConstraintEvaluation(
        label=constraint.resolved_label(),
        metric=constraint.metric,
        op=constraint.op,
        threshold=threshold,
        required=bool(constraint.required),
        status="ok",
        actual_value=actual_value,
        passed=bool(passed),
    )


def evaluate_objective_spec(
    *,
    harness_report: dict[str, Any],
    objective_spec: ObjectiveSpec,
) -> ObjectiveEvaluation:
    metric_index = _metric_index(harness_report)
    harness_status = str(harness_report.get("status") or "")
    comparison_signature = {
        path: _lookup_path(harness_report, path)
        for path in objective_spec.compare_by
    }
    comparable = all(value is not None for value in comparison_signature.values())

    constraint_results = tuple(
        _evaluate_constraint(constraint, metric_index=metric_index)
        for constraint in objective_spec.constraints
    )
    primary_term_results = tuple(
        _evaluate_term(term, metric_index=metric_index)
        for term in objective_spec.primary_terms
    )
    tie_break_term_results = tuple(
        _evaluate_term(term, metric_index=metric_index)
        for term in objective_spec.tie_break_terms
    )

    constraint_pass = all(
        (result.passed is True)
        if constraint.required
        else (result.passed is not False)
        for constraint, result in zip(objective_spec.constraints, constraint_results, strict=False)
    )
    required_primary_term_coverage_pass = all(
        result.status == "ok"
        for term, result in zip(objective_spec.primary_terms, primary_term_results, strict=False)
        if term.required
    )
    primary_scores = [result.weighted_score for result in primary_term_results if result.weighted_score is not None]
    tie_break_scores = [result.weighted_score for result in tie_break_term_results if result.weighted_score is not None]
    objective_score = (
        float(sum(primary_scores))
        if required_primary_term_coverage_pass and primary_scores
        else None
    )
    tie_break_score = float(sum(tie_break_scores)) if tie_break_scores else None
    combined_score = (
        None
        if objective_score is None
        else float(objective_score + (tie_break_score or 0.0))
    )

    notes: list[str] = []
    if not comparable:
        missing_compare_by = [path for path, value in comparison_signature.items() if value is None]
        notes.append(f"missing comparison signature fields: {', '.join(missing_compare_by)}")
    if not required_primary_term_coverage_pass:
        missing_terms = [
            result.label
            for term, result in zip(objective_spec.primary_terms, primary_term_results, strict=False)
            if term.required and result.status != "ok"
        ]
        notes.append(f"missing required primary terms: {', '.join(missing_terms)}")
    if not constraint_pass:
        failed_constraints = [
            result.label
            for constraint, result in zip(objective_spec.constraints, constraint_results, strict=False)
            if constraint.required and result.passed is not True
        ]
        notes.append(f"failed required constraints: {', '.join(failed_constraints)}")

    ready_for_search = bool(
        harness_status in {"success", "ok"}
        and comparable
        and constraint_pass
        and required_primary_term_coverage_pass
        and objective_score is not None
    )
    overall_status = (
        "ok"
        if ready_for_search
        else ("failed" if harness_status not in {"success", "ok"} else "incomplete")
    )

    return ObjectiveEvaluation(
        spec=objective_spec,
        harness_status=(harness_status or None),
        comparison_signature=comparison_signature,
        comparable=comparable,
        constraint_results=constraint_results,
        primary_term_results=primary_term_results,
        tie_break_term_results=tie_break_term_results,
        constraint_pass=constraint_pass,
        required_primary_term_coverage_pass=required_primary_term_coverage_pass,
        objective_score=objective_score,
        tie_break_score=tie_break_score,
        combined_score=combined_score,
        ready_for_search=ready_for_search,
        overall_status=overall_status,
        notes=tuple(notes),
    )
