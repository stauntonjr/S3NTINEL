"""Objective-evaluation report writing over validation harness payloads."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from libs.tuning.objectives import (
    ObjectiveEvaluation,
    ObjectiveSpec,
    build_default_objective_spec,
    evaluate_objective_spec,
)


@dataclass(frozen=True)
class ObjectiveEvaluationReport:
    report_version: str
    status: str
    run_dir: str
    source_artifacts: dict[str, str]
    workload_signature: dict[str, Any]
    evaluation: ObjectiveEvaluation

    def to_payload(self) -> dict[str, Any]:
        return {
            "report_version": self.report_version,
            "status": self.status,
            "run_dir": self.run_dir,
            "source_artifacts": self.source_artifacts,
            "workload_signature": self.workload_signature,
            "evaluation": self.evaluation.to_payload(),
        }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")


def _render_markdown(report: dict[str, Any]) -> str:
    evaluation = dict(report.get("evaluation") or {})
    lines = [
        "# Objective Evaluation Report",
        "",
        "## Summary",
        "```json",
        json.dumps(
            {
                "status": report.get("status"),
                "objective_name": (((evaluation.get("objective_spec") or {}).get("name"))),
                "comparable": evaluation.get("comparable"),
                "constraint_pass": evaluation.get("constraint_pass"),
                "required_primary_term_coverage_pass": evaluation.get("required_primary_term_coverage_pass"),
                "ready_for_search": evaluation.get("ready_for_search"),
                "objective_score": evaluation.get("objective_score"),
                "tie_break_score": evaluation.get("tie_break_score"),
                "combined_score": evaluation.get("combined_score"),
                "notes": evaluation.get("notes"),
            },
            indent=2,
            sort_keys=True,
            default=str,
        ),
        "```",
        "",
        "## Workload Signature",
        "```json",
        json.dumps(report.get("workload_signature", {}), indent=2, sort_keys=True, default=str),
        "```",
        "",
        "## Objective Spec",
        "```json",
        json.dumps(evaluation.get("objective_spec", {}), indent=2, sort_keys=True, default=str),
        "```",
        "",
        "## Constraint Results",
        "```json",
        json.dumps(evaluation.get("constraint_results", []), indent=2, sort_keys=True, default=str),
        "```",
        "",
        "## Primary Terms",
        "```json",
        json.dumps(evaluation.get("primary_term_results", []), indent=2, sort_keys=True, default=str),
        "```",
        "",
        "## Tie Break Terms",
        "```json",
        json.dumps(evaluation.get("tie_break_term_results", []), indent=2, sort_keys=True, default=str),
        "```",
        "",
    ]
    return "\n".join(lines)


def write_objective_evaluation_report(
    *,
    run_dir: Path,
    harness_report: dict[str, Any],
    objective_spec: ObjectiveSpec | None = None,
) -> dict[str, Any]:
    resolved_spec = objective_spec or build_default_objective_spec(harness_report=harness_report)
    evaluation = evaluate_objective_spec(
        harness_report=harness_report,
        objective_spec=resolved_spec,
    )
    report = ObjectiveEvaluationReport(
        report_version="v1",
        status=evaluation.overall_status,
        run_dir=str(run_dir),
        source_artifacts={
            "validation_harness_report_path": str(run_dir / "reports" / "validation_harness_report.json"),
        },
        workload_signature=dict(harness_report.get("workload_signature") or {}),
        evaluation=evaluation,
    )
    report_payload = report.to_payload()
    reports_dir = run_dir / "reports"
    _write_json(reports_dir / "objective_evaluation_report.json", report_payload)
    (reports_dir / "objective_evaluation_report.md").write_text(_render_markdown(report_payload), encoding="utf-8")
    return report_payload
