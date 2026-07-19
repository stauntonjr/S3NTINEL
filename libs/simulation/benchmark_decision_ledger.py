"""Cross-run benchmark decision ledger for composite simulation review."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


_SUCCESSFUL_ALIGNMENT_STATUSES = frozenset({"met_target", "exceeded_target"})


def _text_value(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text or None


def _bool_value(value: Any) -> bool:
    return bool(value)


@dataclass(frozen=True, slots=True)
class BenchmarkDecisionReference:
    """Outcome of one named clean gate relevant to a composite fault family."""

    fault_type: str
    gate_name: str
    flight_name: str
    declared_benchmark_tier: str
    run_status: str
    declared_target_alignment_status: str | None
    observed_recoverability_strength_tier: str | None
    recommended_review_action: str | None
    run_dir: str | None

    @property
    def met_or_exceeded_target(self) -> bool:
        return (
            self.run_status == "success"
            and self.declared_target_alignment_status in _SUCCESSFUL_ALIGNMENT_STATUSES
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "fault_type": self.fault_type,
            "gate_name": self.gate_name,
            "flight_name": self.flight_name,
            "declared_benchmark_tier": self.declared_benchmark_tier,
            "run_status": self.run_status,
            "declared_target_alignment_status": self.declared_target_alignment_status,
            "observed_recoverability_strength_tier": self.observed_recoverability_strength_tier,
            "recommended_review_action": self.recommended_review_action,
            "run_dir": self.run_dir,
            "met_or_exceeded_target": self.met_or_exceeded_target,
        }


@dataclass(frozen=True, slots=True)
class BenchmarkDecisionLedgerEntry:
    """One composite fault-window decision, with linked clean gate evidence."""

    fault_window_id: str
    fault_type: str | None
    fault_family_label: str | None
    declared_benchmark_tier: str | None
    observed_recoverability_strength_tier: str | None
    declared_target_alignment_status: str | None
    first_failed_benchmark_scope: str | None
    dominant_score_component: str | None
    truth_parameter_name: str | None
    truth_module_id: str | None
    truth_subsystem_id: str | None
    telemetry_parameter_match: bool
    telemetry_selected_parameter_match: bool
    event_parameter_match: bool
    top_subsystem_candidate_present: bool
    top_module_candidate_present: bool
    dedicated_references: tuple[BenchmarkDecisionReference, ...]
    recommended_decision: str
    decision_rationale: str
    requires_human_review: bool

    def to_payload(self) -> dict[str, Any]:
        return {
            "fault_window_id": self.fault_window_id,
            "fault_type": self.fault_type,
            "fault_family_label": self.fault_family_label,
            "declared_benchmark_tier": self.declared_benchmark_tier,
            "observed_recoverability_strength_tier": self.observed_recoverability_strength_tier,
            "declared_target_alignment_status": self.declared_target_alignment_status,
            "first_failed_benchmark_scope": self.first_failed_benchmark_scope,
            "dominant_score_component": self.dominant_score_component,
            "truth_parameter_name": self.truth_parameter_name,
            "truth_module_id": self.truth_module_id,
            "truth_subsystem_id": self.truth_subsystem_id,
            "telemetry_parameter_match": self.telemetry_parameter_match,
            "telemetry_selected_parameter_match": self.telemetry_selected_parameter_match,
            "event_parameter_match": self.event_parameter_match,
            "top_subsystem_candidate_present": self.top_subsystem_candidate_present,
            "top_module_candidate_present": self.top_module_candidate_present,
            "dedicated_references": [reference.to_payload() for reference in self.dedicated_references],
            "recommended_decision": self.recommended_decision,
            "decision_rationale": self.decision_rationale,
            "requires_human_review": self.requires_human_review,
        }


@dataclass(frozen=True, slots=True)
class BenchmarkDecisionLedger:
    """Durable cross-run artifact for selecting the next benchmark review action."""

    composite_run_dir: str
    reference_suite_dir: str
    entries: tuple[BenchmarkDecisionLedgerEntry, ...]

    def to_payload(self) -> dict[str, Any]:
        decision_counts = {
            decision: sum(1 for entry in self.entries if entry.recommended_decision == decision)
            for decision in sorted({entry.recommended_decision for entry in self.entries})
        }
        return {
            "status": "ok",
            "report_version": 1,
            "composite_run_dir": self.composite_run_dir,
            "reference_suite_dir": self.reference_suite_dir,
            "fault_window_count": len(self.entries),
            "requires_human_review_count": sum(entry.requires_human_review for entry in self.entries),
            "recommended_decision_count": decision_counts,
            "entries": [entry.to_payload() for entry in self.entries],
            "methodology": {
                "interpretation": (
                    "joins the canonical composite benchmark audit and eligible failure ledger to named clean gate outcomes; "
                    "recommendations identify the next review path and never modify declared benchmark truth"
                ),
                "successful_reference_rule": (
                    "a named clean gate is successful only when its run succeeded and its declared target alignment is met_target or exceeded_target"
                ),
            },
        }


def _decision_for_case(
    case: dict[str, Any], references: tuple[BenchmarkDecisionReference, ...]
) -> tuple[str, str, bool]:
    alignment = _text_value(case.get("declared_target_alignment_status"))
    if alignment in _SUCCESSFUL_ALIGNMENT_STATUSES:
        return (
            "retain_target",
            "The composite window met or exceeded its declared benchmark target.",
            False,
        )

    successful_references = tuple(reference for reference in references if reference.met_or_exceeded_target)
    declared_tier = _text_value(case.get("declared_benchmark_tier"))
    if successful_references:
        if any(reference.declared_benchmark_tier != declared_tier for reference in successful_references):
            return (
                "review_lower_target_or_module_separation",
                "A named clean gate succeeds for this fault type at a different declared tier; review target scope and hierarchy separation before changing the anomaly model.",
                True,
            )
        return (
            "formulate_downstream_model_hypothesis",
            "A named clean gate succeeds at the same declared tier; use the composite failure scope to formulate one downstream model hypothesis.",
            True,
        )

    observed_tier = _text_value(case.get("observed_recoverability_strength_tier"))
    if observed_tier in {"undetected", "detection_only"}:
        return (
            "review_signal_observability_and_fault_design",
            "The composite window does not reach parameter visibility and has no successful dedicated reference; review signal observability and fault design first.",
            True,
        )
    return (
        "review_truth_scope_or_structural_observability",
        "The composite window misses its target without a successful dedicated reference; review truth scope and structural observability before tuning the anomaly model.",
        True,
    )


def build_benchmark_decision_ledger(
    *,
    composite_run_dir: str | Path,
    reference_suite_dir: str | Path,
    simulation_benchmark_audit_summary: dict[str, Any],
    benchmark_tier_validation_summary: dict[str, Any],
    references: tuple[BenchmarkDecisionReference, ...],
) -> BenchmarkDecisionLedger:
    """Join canonical composite cases to the clean gate references for their fault type."""
    failure_scope_by_window_id = {
        str(row.get("fault_window_id")): row
        for row in benchmark_tier_validation_summary.get("eligible_composite_window_failure_ledger", [])
        if _text_value(row.get("fault_window_id"))
    }
    references_by_fault_type: dict[str, list[BenchmarkDecisionReference]] = {}
    for reference in references:
        references_by_fault_type.setdefault(reference.fault_type, []).append(reference)

    entries: list[BenchmarkDecisionLedgerEntry] = []
    for case in simulation_benchmark_audit_summary.get("fault_window_audit_cases", []):
        fault_window_id = _text_value(case.get("fault_window_id"))
        if not fault_window_id:
            continue
        fault_type = _text_value(case.get("fault_type"))
        scope_row = failure_scope_by_window_id.get(fault_window_id, {})
        case_references = tuple(
            sorted(references_by_fault_type.get(str(fault_type or ""), []), key=lambda reference: reference.gate_name)
        )
        recommended_decision, decision_rationale, requires_human_review = _decision_for_case(case, case_references)
        entries.append(
            BenchmarkDecisionLedgerEntry(
                fault_window_id=fault_window_id,
                fault_type=fault_type,
                fault_family_label=_text_value(case.get("fault_family_label")),
                declared_benchmark_tier=_text_value(case.get("declared_benchmark_tier")),
                observed_recoverability_strength_tier=_text_value(case.get("observed_recoverability_strength_tier")),
                declared_target_alignment_status=_text_value(case.get("declared_target_alignment_status")),
                first_failed_benchmark_scope=_text_value(scope_row.get("first_failed_benchmark_scope")),
                dominant_score_component=_text_value(scope_row.get("dominant_score_component")),
                truth_parameter_name=_text_value(case.get("truth_parameter_name")),
                truth_module_id=_text_value(case.get("truth_module_id")),
                truth_subsystem_id=_text_value(case.get("truth_subsystem_id")),
                telemetry_parameter_match=_bool_value(scope_row.get("telemetry_parameter_match", case.get("telemetry_parameter_match"))),
                telemetry_selected_parameter_match=_bool_value(
                    scope_row.get("telemetry_selected_parameter_match", case.get("telemetry_selected_parameter_match"))
                ),
                event_parameter_match=_bool_value(scope_row.get("event_parameter_match", case.get("event_parameter_match"))),
                top_subsystem_candidate_present=_bool_value(
                    scope_row.get("top_subsystem_candidate_present", case.get("top_subsystem_candidate_present"))
                ),
                top_module_candidate_present=_bool_value(
                    scope_row.get("top_module_candidate_present", case.get("top_module_candidate_present"))
                ),
                dedicated_references=case_references,
                recommended_decision=recommended_decision,
                decision_rationale=decision_rationale,
                requires_human_review=requires_human_review,
            )
        )
    return BenchmarkDecisionLedger(
        composite_run_dir=str(composite_run_dir),
        reference_suite_dir=str(reference_suite_dir),
        entries=tuple(sorted(entries, key=lambda entry: entry.fault_window_id)),
    )


def render_benchmark_decision_ledger_markdown(ledger: BenchmarkDecisionLedger) -> str:
    """Render a concise human review view alongside the JSON payload."""
    payload = ledger.to_payload()
    lines = [
        "# Benchmark Decision Ledger",
        "",
        f"- composite_run_dir: `{payload['composite_run_dir']}`",
        f"- reference_suite_dir: `{payload['reference_suite_dir']}`",
        f"- fault_window_count: `{payload['fault_window_count']}`",
        f"- requires_human_review_count: `{payload['requires_human_review_count']}`",
        "",
        "## Decisions",
    ]
    for entry in ledger.entries:
        lines.extend(
            (
                "",
                f"### {entry.fault_window_id}",
                f"- fault_type: `{entry.fault_type}`",
                f"- declared_benchmark_tier: `{entry.declared_benchmark_tier}`",
                f"- observed_recoverability_strength_tier: `{entry.observed_recoverability_strength_tier}`",
                f"- first_failed_benchmark_scope: `{entry.first_failed_benchmark_scope}`",
                f"- recommended_decision: `{entry.recommended_decision}`",
                f"- requires_human_review: `{entry.requires_human_review}`",
                f"- rationale: {entry.decision_rationale}",
            )
        )
    return "\n".join(lines) + "\n"
