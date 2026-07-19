"""Canonical benchmark-tier gate suite for simulation-backed anomaly evaluation."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from libs.perf import get_logger
from libs.simulation.benchmark_decision_ledger import (
    BenchmarkDecisionLedger,
    BenchmarkDecisionReference,
    build_benchmark_decision_ledger,
    render_benchmark_decision_ledger_markdown,
)
from libs.simulation.cli import add_backbone_args, add_event_args, add_profile_args, add_source_args, add_window_args
from libs.simulation.fault.spec import BENCHMARK_RECOVERABILITY_LADDER
from libs.simulation.run_bundle import load_json_if_exists
from libs.simulation.run_context import PipelineRunConfig
from libs.simulation.runner import run_pipeline


LOGGER_NAME = "s3ntinel.run_sim_benchmark_tier_gates"
DEFAULT_BENCHMARK_TIER_GATE_SUITE_KEY = "localization"
BENCHMARK_TIER_GATE_SUITE_NAME = "localization_benchmark_tier_gates"
BENCHMARK_TIER_GATE_SUMMARY_FILENAME = "benchmark_tier_gate_suite_summary.json"
BENCHMARK_TIER_GATE_MARKDOWN_FILENAME = "benchmark_tier_gate_suite_summary.md"
BENCHMARK_DECISION_LEDGER_SUMMARY_FILENAME = "benchmark_decision_ledger_summary.json"
BENCHMARK_DECISION_LEDGER_MARKDOWN_FILENAME = "benchmark_decision_ledger.md"


@dataclass(frozen=True, slots=True)
class BenchmarkTierGateSpec:
    gate_name: str
    flight_name: str
    declared_benchmark_tier: str
    description: str
    fault_types: tuple[str, ...]

    def to_payload(self) -> dict[str, Any]:
        return {
            "gate_name": self.gate_name,
            "flight_name": self.flight_name,
            "declared_benchmark_tier": self.declared_benchmark_tier,
            "description": self.description,
            "fault_types": list(self.fault_types),
        }


@dataclass(frozen=True, slots=True)
class BenchmarkTierGateSuiteSpec:
    suite_key: str
    suite_name: str
    suite_interpretation: str
    gate_specs: tuple[BenchmarkTierGateSpec, ...]


@dataclass(frozen=True, slots=True)
class BenchmarkTierGateRunSummary:
    gate_name: str
    flight_name: str
    declared_benchmark_tier: str
    run_dir: str | None
    run_status: str
    error_message: str | None
    declared_target_alignment_status: str | None
    observed_recoverability_strength_tier: str | None
    recommended_review_action: str | None
    detected_fault_window_rate: float | None
    emit_ready_fault_window_rate: float | None
    telemetry_parameter_match_rate: float | None
    telemetry_selected_parameter_match_rate: float | None
    dominant_subsystem_match_rate: float | None
    dominant_module_match_rate: float | None
    top_subsystem_candidate_present_rate: float | None
    top_module_candidate_present_rate: float | None

    def to_payload(self) -> dict[str, Any]:
        return {
            "gate_name": self.gate_name,
            "flight_name": self.flight_name,
            "declared_benchmark_tier": self.declared_benchmark_tier,
            "run_dir": self.run_dir,
            "run_status": self.run_status,
            "error_message": self.error_message,
            "declared_target_alignment_status": self.declared_target_alignment_status,
            "observed_recoverability_strength_tier": self.observed_recoverability_strength_tier,
            "recommended_review_action": self.recommended_review_action,
            "detected_fault_window_rate": self.detected_fault_window_rate,
            "emit_ready_fault_window_rate": self.emit_ready_fault_window_rate,
            "telemetry_parameter_match_rate": self.telemetry_parameter_match_rate,
            "telemetry_selected_parameter_match_rate": self.telemetry_selected_parameter_match_rate,
            "dominant_subsystem_match_rate": self.dominant_subsystem_match_rate,
            "dominant_module_match_rate": self.dominant_module_match_rate,
            "top_subsystem_candidate_present_rate": self.top_subsystem_candidate_present_rate,
            "top_module_candidate_present_rate": self.top_module_candidate_present_rate,
        }


@dataclass(frozen=True, slots=True)
class BenchmarkTierGateSuiteSummary:
    suite_key: str
    suite_name: str
    suite_interpretation: str
    generated_at_utc: str
    suite_dir: str
    gate_specs: tuple[BenchmarkTierGateSpec, ...]
    gate_results: tuple[BenchmarkTierGateRunSummary, ...]

    def to_payload(self) -> dict[str, Any]:
        gate_alignment_status_count = {
            status: sum(1 for result in self.gate_results if result.declared_target_alignment_status == status)
            for status in ("met_target", "exceeded_target", "missed_target", "undeclared")
            if any(result.declared_target_alignment_status == status for result in self.gate_results)
        }
        met_or_exceeded = sum(
            1
            for result in self.gate_results
            if result.declared_target_alignment_status in {"met_target", "exceeded_target"}
        )
        return {
            "suite_key": self.suite_key,
            "suite_name": self.suite_name,
            "generated_at_utc": self.generated_at_utc,
            "suite_dir": self.suite_dir,
            "gate_specs": [spec.to_payload() for spec in self.gate_specs],
            "gate_results": [result.to_payload() for result in self.gate_results],
            "gate_count": len(self.gate_results),
            "gate_alignment_status_count": gate_alignment_status_count,
            "met_or_exceeded_gate_count": met_or_exceeded,
            "all_gates_met_or_exceeded": bool(self.gate_results) and met_or_exceeded == len(self.gate_results),
            "suite_interpretation": self.suite_interpretation,
        }


LOCALIZATION_BENCHMARK_TIER_GATE_SPECS = (
    BenchmarkTierGateSpec(
        gate_name="subsystem_tier_bias",
        flight_name="power_pressurization_hierarchy_smoke_localization_focus_bias",
        declared_benchmark_tier="subsystem_recoverable",
        description="Clean subsystem-tier acceptance gate for the regulated bias family on the smoke topology.",
        fault_types=("bias",),
    ),
    BenchmarkTierGateSpec(
        gate_name="module_tier_drift",
        flight_name="power_pressurization_hierarchy_smoke_localization_focus_drift",
        declared_benchmark_tier="module_recoverable",
        description="Clean module-tier acceptance gate for the accumulative drift family on the smoke topology.",
        fault_types=("drift",),
    ),
)

PARAMETER_BENCHMARK_TIER_GATE_SPECS = (
    BenchmarkTierGateSpec(
        gate_name="parameter_tier_regulated_saturation",
        flight_name="power_pressurization_hierarchy_smoke_parameter_focus_regulated",
        declared_benchmark_tier="parameter_visible_only",
        description="Clean parameter-tier gate for the regulated saturation family on the smoke topology.",
        fault_types=("saturation",),
    ),
    BenchmarkTierGateSpec(
        gate_name="parameter_tier_accumulative_drift",
        flight_name="power_pressurization_hierarchy_smoke_parameter_focus_accumulative",
        declared_benchmark_tier="parameter_visible_only",
        description="Clean parameter-tier gate for the accumulative drift family on the smoke topology.",
        fault_types=("drift",),
    ),
    BenchmarkTierGateSpec(
        gate_name="parameter_tier_discrete_state_chatter",
        flight_name="power_pressurization_hierarchy_smoke_parameter_focus_discrete",
        declared_benchmark_tier="parameter_visible_only",
        description="Clean parameter-tier gate for the discrete state-chatter family on the smoke topology.",
        fault_types=("state_chatter",),
    ),
    BenchmarkTierGateSpec(
        gate_name="parameter_tier_coupling_timing_jitter",
        flight_name="power_pressurization_hierarchy_smoke_parameter_focus_coupling",
        declared_benchmark_tier="parameter_visible_only",
        description="Clean parameter-tier gate for the coupling timing-jitter family on the smoke topology.",
        fault_types=("timing_jitter",),
    ),
)

BENCHMARK_TIER_GATE_SUITE_SPECS = {
    "localization": BenchmarkTierGateSuiteSpec(
        suite_key="localization",
        suite_name="localization_benchmark_tier_gates",
        suite_interpretation=(
            "use this suite before the mixed composite bundle when evaluating anomaly changes "
            "against the clean subsystem and module benchmark tiers"
        ),
        gate_specs=LOCALIZATION_BENCHMARK_TIER_GATE_SPECS,
    ),
    "parameter": BenchmarkTierGateSuiteSpec(
        suite_key="parameter",
        suite_name="parameter_benchmark_tier_gates",
        suite_interpretation=(
            "use this suite before parameter-level anomaly tuning so regulated, accumulative, discrete, "
            "and coupling/timing parameter visibility are screened on dedicated lower-tier smoke packs"
        ),
        gate_specs=PARAMETER_BENCHMARK_TIER_GATE_SPECS,
    ),
}


def resolve_benchmark_tier_gate_suite_spec(suite_key: str | None = None) -> BenchmarkTierGateSuiteSpec:
    return BENCHMARK_TIER_GATE_SUITE_SPECS.get(
        str(suite_key or DEFAULT_BENCHMARK_TIER_GATE_SUITE_KEY),
        BENCHMARK_TIER_GATE_SUITE_SPECS[DEFAULT_BENCHMARK_TIER_GATE_SUITE_KEY],
    )


def ordered_benchmark_tier_gate_specs(suite_key: str | None = None) -> tuple[BenchmarkTierGateSpec, ...]:
    suite_spec = resolve_benchmark_tier_gate_suite_spec(suite_key)
    ordered_specs: list[BenchmarkTierGateSpec] = []
    for tier in BENCHMARK_RECOVERABILITY_LADDER:
        ordered_specs.extend(spec for spec in suite_spec.gate_specs if spec.declared_benchmark_tier == tier)
    return tuple(ordered_specs)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the canonical benchmark-tier gate suite for simulation-backed anomaly evaluation"
    )
    add_source_args(parser)
    add_profile_args(parser)
    add_event_args(parser)
    add_window_args(parser)
    add_backbone_args(parser)
    parser.add_argument("--base-dir", default="data/simulation_gate_runs", help="Base directory for gate suite bundles")
    parser.add_argument(
        "--composite-run-dir",
        help=(
            "Completed canonical composite run bundle to join with clean gate outcomes; "
            "writes a cross-run benchmark decision ledger in this suite's reports directory"
        ),
    )
    parser.add_argument("--mode", default="full", choices=("full",), help="Gate suites always run the full persisted pipeline")
    parser.add_argument("--format", default="parquet", choices=("parquet", "delta"), help="Persisted table format")
    parser.add_argument("--write-mode", default="overwrite", choices=("overwrite", "append", "merge"))
    parser.add_argument("--min-warm", default=1, type=int, help="Conformal minimum warm size")
    parser.add_argument("--phase-count", type=int, default=4, help="Detected phase count")
    parser.add_argument(
        "--suite",
        default=DEFAULT_BENCHMARK_TIER_GATE_SUITE_KEY,
        choices=tuple(sorted(BENCHMARK_TIER_GATE_SUITE_SPECS)),
        help="Benchmark-tier gate suite to run",
    )
    parser.set_defaults(start_stage=None, end_stage=None, replay_run_dir=None)
    return parser.parse_args()


def _timestamped_suite_dir(base_dir: str | Path, *, suite_spec: BenchmarkTierGateSuiteSpec) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path(base_dir) / f"{timestamp}_{suite_spec.suite_name}"


def _text_value(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text or None


def _float_value(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _latest_new_run_dir(*, runs_dir: Path, flight_name: str, before: set[Path]) -> Path | None:
    candidates = [path for path in runs_dir.glob(f"*_{flight_name}") if path not in before]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def build_benchmark_tier_gate_run_summary(
    *,
    gate_spec: BenchmarkTierGateSpec,
    run_dir: Path | None,
    run_status: str,
    error_message: str | None = None,
) -> BenchmarkTierGateRunSummary:
    reports_dir = None if run_dir is None else run_dir / "reports"
    audit_payload = None if reports_dir is None else load_json_if_exists(reports_dir / "simulation_benchmark_audit_summary.json")
    attribution_payload = None if reports_dir is None else load_json_if_exists(reports_dir / "attribution_validation_summary.json")
    score_payload = None if reports_dir is None else load_json_if_exists(reports_dir / "score_validation_summary.json")
    case_payload = dict(((audit_payload or {}).get("fault_window_audit_cases") or [{}])[0])
    return BenchmarkTierGateRunSummary(
        gate_name=gate_spec.gate_name,
        flight_name=gate_spec.flight_name,
        declared_benchmark_tier=gate_spec.declared_benchmark_tier,
        run_dir=(None if run_dir is None else str(run_dir)),
        run_status=str(run_status),
        error_message=_text_value(error_message),
        declared_target_alignment_status=_text_value(case_payload.get("declared_target_alignment_status")),
        observed_recoverability_strength_tier=_text_value(case_payload.get("observed_recoverability_strength_tier")),
        recommended_review_action=_text_value(case_payload.get("recommended_review_action")),
        detected_fault_window_rate=_float_value((score_payload or {}).get("detected_fault_window_rate")),
        emit_ready_fault_window_rate=_float_value((score_payload or {}).get("emit_ready_fault_window_rate")),
        telemetry_parameter_match_rate=_float_value((attribution_payload or {}).get("telemetry_parameter_match_rate")),
        telemetry_selected_parameter_match_rate=_float_value(
            (attribution_payload or {}).get("telemetry_selected_parameter_match_rate")
        ),
        dominant_subsystem_match_rate=_float_value((attribution_payload or {}).get("dominant_subsystem_match_rate")),
        dominant_module_match_rate=_float_value((attribution_payload or {}).get("dominant_module_match_rate")),
        top_subsystem_candidate_present_rate=_float_value(
            (attribution_payload or {}).get("top_subsystem_candidate_present_rate")
        ),
        top_module_candidate_present_rate=_float_value((attribution_payload or {}).get("top_module_candidate_present_rate")),
    )


def build_benchmark_tier_gate_suite_summary(
    *,
    suite_dir: Path,
    gate_results: tuple[BenchmarkTierGateRunSummary, ...],
    suite_key: str | None = None,
    gate_specs: tuple[BenchmarkTierGateSpec, ...] | None = None,
) -> BenchmarkTierGateSuiteSummary:
    suite_spec = resolve_benchmark_tier_gate_suite_spec(suite_key)
    return BenchmarkTierGateSuiteSummary(
        suite_key=suite_spec.suite_key,
        suite_name=suite_spec.suite_name,
        suite_interpretation=suite_spec.suite_interpretation,
        generated_at_utc=datetime.now(timezone.utc).isoformat(),
        suite_dir=str(suite_dir),
        gate_specs=ordered_benchmark_tier_gate_specs(suite_spec.suite_key) if gate_specs is None else gate_specs,
        gate_results=gate_results,
    )


def render_benchmark_tier_gate_suite_markdown(summary: BenchmarkTierGateSuiteSummary) -> str:
    payload = summary.to_payload()
    lines = [
        "# Benchmark Tier Gate Suite",
        "",
        f"- suite_name: `{payload['suite_name']}`",
        f"- suite_dir: `{payload['suite_dir']}`",
        f"- all_gates_met_or_exceeded: `{payload['all_gates_met_or_exceeded']}`",
        f"- met_or_exceeded_gate_count: `{payload['met_or_exceeded_gate_count']}` / `{payload['gate_count']}`",
        "",
        "## Gate Results",
    ]
    for result in summary.gate_results:
        lines.extend(
            (
                "",
                f"### {result.gate_name}",
                f"- flight_name: `{result.flight_name}`",
                f"- declared_benchmark_tier: `{result.declared_benchmark_tier}`",
                f"- declared_target_alignment_status: `{result.declared_target_alignment_status}`",
                f"- observed_recoverability_strength_tier: `{result.observed_recoverability_strength_tier}`",
                f"- run_status: `{result.run_status}`",
                f"- detected_fault_window_rate: `{result.detected_fault_window_rate}`",
                f"- emit_ready_fault_window_rate: `{result.emit_ready_fault_window_rate}`",
                f"- telemetry_parameter_match_rate: `{result.telemetry_parameter_match_rate}`",
                f"- dominant_subsystem_match_rate: `{result.dominant_subsystem_match_rate}`",
                f"- dominant_module_match_rate: `{result.dominant_module_match_rate}`",
                f"- top_subsystem_candidate_present_rate: `{result.top_subsystem_candidate_present_rate}`",
                f"- top_module_candidate_present_rate: `{result.top_module_candidate_present_rate}`",
            )
        )
    return "\n".join(lines) + "\n"


def write_benchmark_tier_gate_suite_report(
    *,
    suite_dir: Path,
    summary: BenchmarkTierGateSuiteSummary,
) -> dict[str, Any]:
    reports_dir = suite_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    payload = summary.to_payload()
    (reports_dir / BENCHMARK_TIER_GATE_SUMMARY_FILENAME).write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / BENCHMARK_TIER_GATE_MARKDOWN_FILENAME).write_text(
        render_benchmark_tier_gate_suite_markdown(summary),
        encoding="utf-8",
    )
    return payload


def build_benchmark_decision_references(summary: BenchmarkTierGateSuiteSummary) -> tuple[BenchmarkDecisionReference, ...]:
    """Associate clean gate outcomes with the fault types their packs exercise."""
    result_by_gate_name = {result.gate_name: result for result in summary.gate_results}
    references: list[BenchmarkDecisionReference] = []
    for spec in summary.gate_specs:
        result = result_by_gate_name.get(spec.gate_name)
        if result is None:
            continue
        for fault_type in spec.fault_types:
            references.append(
                BenchmarkDecisionReference(
                    fault_type=fault_type,
                    gate_name=spec.gate_name,
                    flight_name=spec.flight_name,
                    declared_benchmark_tier=spec.declared_benchmark_tier,
                    run_status=result.run_status,
                    declared_target_alignment_status=result.declared_target_alignment_status,
                    observed_recoverability_strength_tier=result.observed_recoverability_strength_tier,
                    recommended_review_action=result.recommended_review_action,
                    run_dir=result.run_dir,
                )
            )
    return tuple(sorted(references, key=lambda reference: (reference.fault_type, reference.gate_name)))


def write_benchmark_decision_ledger_report(
    *,
    suite_dir: Path,
    summary: BenchmarkTierGateSuiteSummary,
    composite_run_dir: str | Path,
) -> dict[str, Any]:
    """Write the cross-run ledger after all named clean gates have completed."""
    composite_reports_dir = Path(composite_run_dir) / "reports"
    audit_payload = load_json_if_exists(composite_reports_dir / "simulation_benchmark_audit_summary.json")
    tier_payload = load_json_if_exists(composite_reports_dir / "benchmark_tier_validation_summary.json")
    if audit_payload is None or tier_payload is None:
        raise FileNotFoundError(
            "composite run must contain reports/simulation_benchmark_audit_summary.json and "
            "reports/benchmark_tier_validation_summary.json"
        )
    ledger: BenchmarkDecisionLedger = build_benchmark_decision_ledger(
        composite_run_dir=composite_run_dir,
        reference_suite_dir=suite_dir,
        simulation_benchmark_audit_summary=audit_payload,
        benchmark_tier_validation_summary=tier_payload,
        references=build_benchmark_decision_references(summary),
    )
    reports_dir = suite_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    payload = ledger.to_payload()
    (reports_dir / BENCHMARK_DECISION_LEDGER_SUMMARY_FILENAME).write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / BENCHMARK_DECISION_LEDGER_MARKDOWN_FILENAME).write_text(
        render_benchmark_decision_ledger_markdown(ledger),
        encoding="utf-8",
    )
    return payload


def run_benchmark_tier_gate_suite(
    base_config: PipelineRunConfig,
    *,
    suite_key: str | None = None,
    composite_run_dir: str | Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    logger = get_logger(LOGGER_NAME)
    suite_spec = resolve_benchmark_tier_gate_suite_spec(suite_key)
    ordered_gate_specs = ordered_benchmark_tier_gate_specs(suite_spec.suite_key)
    suite_dir = _timestamped_suite_dir(base_config.base_dir, suite_spec=suite_spec)
    runs_dir = suite_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    gate_results: list[BenchmarkTierGateRunSummary] = []

    for gate_spec in ordered_gate_specs:
        gate_config = replace(
            base_config,
            base_dir=str(runs_dir),
            mode="full",
            flight_name=gate_spec.flight_name,
            flight_id=f"{base_config.flight_id}_{gate_spec.gate_name.upper()}",
            start_stage=None,
            end_stage=None,
            replay_run_dir=None,
        )
        existing_run_dirs = set(runs_dir.glob(f"*_{gate_spec.flight_name}"))
        logger.info(
            "benchmark_tier_gate_start suite=%s gate=%s flight=%s declared_benchmark_tier=%s suite_dir=%s",
            suite_spec.suite_key,
            gate_spec.gate_name,
            gate_spec.flight_name,
            gate_spec.declared_benchmark_tier,
            suite_dir,
        )
        run_dir: Path | None = None
        run_status = "failed"
        error_message: str | None = None
        try:
            run_result = run_pipeline(gate_config)
            run_dir = run_result.paths.run_dir
            run_status = run_result.status
        except Exception as exc:  # pragma: no cover - exercised through integration runs
            run_dir = _latest_new_run_dir(runs_dir=runs_dir, flight_name=gate_spec.flight_name, before=existing_run_dirs)
            error_message = f"{exc.__class__.__name__}: {exc}"
            logger.exception("benchmark_tier_gate_failed gate=%s flight=%s", gate_spec.gate_name, gate_spec.flight_name)
        gate_results.append(
            build_benchmark_tier_gate_run_summary(
                gate_spec=gate_spec,
                run_dir=run_dir,
                run_status=run_status,
                error_message=error_message,
            )
        )

    summary = build_benchmark_tier_gate_suite_summary(
        suite_dir=suite_dir,
        gate_results=tuple(gate_results),
        suite_key=suite_spec.suite_key,
        gate_specs=ordered_gate_specs,
    )
    payload = write_benchmark_tier_gate_suite_report(suite_dir=suite_dir, summary=summary)
    if composite_run_dir is not None:
        ledger_payload = write_benchmark_decision_ledger_report(
            suite_dir=suite_dir,
            summary=summary,
            composite_run_dir=composite_run_dir,
        )
        payload["benchmark_decision_ledger_path"] = str(suite_dir / "reports" / BENCHMARK_DECISION_LEDGER_SUMMARY_FILENAME)
        payload["benchmark_decision_ledger_requires_human_review_count"] = ledger_payload["requires_human_review_count"]
    logger.info(
        "benchmark_tier_gate_suite_complete suite=%s suite_dir=%s all_gates_met_or_exceeded=%s",
        suite_spec.suite_key,
        suite_dir,
        payload.get("all_gates_met_or_exceeded"),
    )
    return suite_dir, payload


def main() -> None:
    args = parse_args()
    config = PipelineRunConfig.from_args(args)
    suite_dir, payload = run_benchmark_tier_gate_suite(
        config,
        suite_key=str(args.suite),
        composite_run_dir=args.composite_run_dir,
    )
    print(
        json.dumps(
            {
                "suite_dir": str(suite_dir),
                "suite_key": payload.get("suite_key"),
                "summary_path": str(Path(suite_dir) / "reports" / BENCHMARK_TIER_GATE_SUMMARY_FILENAME),
                "all_gates_met_or_exceeded": payload.get("all_gates_met_or_exceeded"),
                "gate_alignment_status_count": payload.get("gate_alignment_status_count"),
                "benchmark_decision_ledger_path": payload.get("benchmark_decision_ledger_path"),
            },
            indent=2,
            sort_keys=True,
        )
    )
