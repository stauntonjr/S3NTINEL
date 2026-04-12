"""Canonical benchmark-phase gate suite for simulation-backed anomaly evaluation."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from libs.perf import get_logger
from libs.simulation.cli import add_backbone_args, add_event_args, add_profile_args, add_source_args, add_window_args
from libs.simulation.fault.spec import BENCHMARK_RECOVERABILITY_PHASES
from libs.simulation.run_bundle import load_json_if_exists
from libs.simulation.run_context import PipelineRunConfig
from libs.simulation.runner import run_pipeline


LOGGER_NAME = "s3ntinel.run_sim_benchmark_phase_gates"
BENCHMARK_PHASE_GATE_SUITE_NAME = "localization_benchmark_phase_gates"
BENCHMARK_PHASE_GATE_SUMMARY_FILENAME = "benchmark_phase_gate_suite_summary.json"
BENCHMARK_PHASE_GATE_MARKDOWN_FILENAME = "benchmark_phase_gate_suite_summary.md"


@dataclass(frozen=True, slots=True)
class BenchmarkPhaseGateSpec:
    gate_name: str
    flight_name: str
    declared_benchmark_phase: str
    description: str

    def to_payload(self) -> dict[str, Any]:
        return {
            "gate_name": self.gate_name,
            "flight_name": self.flight_name,
            "declared_benchmark_phase": self.declared_benchmark_phase,
            "description": self.description,
        }


@dataclass(frozen=True, slots=True)
class BenchmarkPhaseGateRunSummary:
    gate_name: str
    flight_name: str
    declared_benchmark_phase: str
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
            "declared_benchmark_phase": self.declared_benchmark_phase,
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
class BenchmarkPhaseGateSuiteSummary:
    suite_name: str
    generated_at_utc: str
    suite_dir: str
    gate_specs: tuple[BenchmarkPhaseGateSpec, ...]
    gate_results: tuple[BenchmarkPhaseGateRunSummary, ...]

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
            "suite_name": self.suite_name,
            "generated_at_utc": self.generated_at_utc,
            "suite_dir": self.suite_dir,
            "gate_specs": [spec.to_payload() for spec in self.gate_specs],
            "gate_results": [result.to_payload() for result in self.gate_results],
            "gate_count": len(self.gate_results),
            "gate_alignment_status_count": gate_alignment_status_count,
            "met_or_exceeded_gate_count": met_or_exceeded,
            "all_gates_met_or_exceeded": bool(self.gate_results) and met_or_exceeded == len(self.gate_results),
            "suite_interpretation": (
                "use this suite before the mixed composite bundle when evaluating anomaly changes against the clean subsystem and module benchmark phases"
            ),
        }


BENCHMARK_PHASE_GATE_SPECS = (
    BenchmarkPhaseGateSpec(
        gate_name="subsystem_phase_bias",
        flight_name="power_pressurization_hierarchy_smoke_localization_focus_bias",
        declared_benchmark_phase="subsystem_recoverable",
        description="Clean subsystem-phase acceptance gate for the regulated bias family on the smoke topology.",
    ),
    BenchmarkPhaseGateSpec(
        gate_name="module_phase_drift",
        flight_name="power_pressurization_hierarchy_smoke_localization_focus_drift",
        declared_benchmark_phase="module_recoverable",
        description="Clean module-phase acceptance gate for the accumulative drift family on the smoke topology.",
    ),
)


def ordered_benchmark_phase_gate_specs() -> tuple[BenchmarkPhaseGateSpec, ...]:
    ordered_specs: list[BenchmarkPhaseGateSpec] = []
    for phase in BENCHMARK_RECOVERABILITY_PHASES:
        ordered_specs.extend(spec for spec in BENCHMARK_PHASE_GATE_SPECS if spec.declared_benchmark_phase == phase)
    return tuple(ordered_specs)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the canonical benchmark-phase gate suite for simulation-backed anomaly evaluation"
    )
    add_source_args(parser)
    add_profile_args(parser)
    add_event_args(parser)
    add_window_args(parser)
    add_backbone_args(parser)
    parser.add_argument("--base-dir", default="data/simulation_gate_runs", help="Base directory for gate suite bundles")
    parser.add_argument("--mode", default="full", choices=("full",), help="Gate suites always run the full persisted pipeline")
    parser.add_argument("--format", default="parquet", choices=("parquet", "delta"), help="Persisted table format")
    parser.add_argument("--write-mode", default="overwrite", choices=("overwrite", "append", "merge"))
    parser.add_argument("--min-warm", default=1, type=int, help="Conformal minimum warm size")
    parser.add_argument("--phase-count", type=int, default=4, help="Detected phase count")
    parser.set_defaults(start_stage=None, end_stage=None, replay_run_dir=None)
    return parser.parse_args()


def _timestamped_suite_dir(base_dir: str | Path) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path(base_dir) / f"{timestamp}_{BENCHMARK_PHASE_GATE_SUITE_NAME}"


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


def build_benchmark_phase_gate_run_summary(
    *,
    gate_spec: BenchmarkPhaseGateSpec,
    run_dir: Path | None,
    run_status: str,
    error_message: str | None = None,
) -> BenchmarkPhaseGateRunSummary:
    reports_dir = None if run_dir is None else run_dir / "reports"
    audit_payload = None if reports_dir is None else load_json_if_exists(reports_dir / "simulation_benchmark_audit_summary.json")
    attribution_payload = None if reports_dir is None else load_json_if_exists(reports_dir / "attribution_validation_summary.json")
    score_payload = None if reports_dir is None else load_json_if_exists(reports_dir / "score_validation_summary.json")
    case_payload = dict(((audit_payload or {}).get("fault_window_audit_cases") or [{}])[0])
    return BenchmarkPhaseGateRunSummary(
        gate_name=gate_spec.gate_name,
        flight_name=gate_spec.flight_name,
        declared_benchmark_phase=gate_spec.declared_benchmark_phase,
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


def build_benchmark_phase_gate_suite_summary(
    *,
    suite_dir: Path,
    gate_results: tuple[BenchmarkPhaseGateRunSummary, ...],
    gate_specs: tuple[BenchmarkPhaseGateSpec, ...] | None = None,
) -> BenchmarkPhaseGateSuiteSummary:
    return BenchmarkPhaseGateSuiteSummary(
        suite_name=BENCHMARK_PHASE_GATE_SUITE_NAME,
        generated_at_utc=datetime.now(timezone.utc).isoformat(),
        suite_dir=str(suite_dir),
        gate_specs=ordered_benchmark_phase_gate_specs() if gate_specs is None else gate_specs,
        gate_results=gate_results,
    )


def render_benchmark_phase_gate_suite_markdown(summary: BenchmarkPhaseGateSuiteSummary) -> str:
    payload = summary.to_payload()
    lines = [
        "# Benchmark Phase Gate Suite",
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
                f"- declared_benchmark_phase: `{result.declared_benchmark_phase}`",
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


def write_benchmark_phase_gate_suite_report(
    *,
    suite_dir: Path,
    summary: BenchmarkPhaseGateSuiteSummary,
) -> dict[str, Any]:
    reports_dir = suite_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    payload = summary.to_payload()
    (reports_dir / BENCHMARK_PHASE_GATE_SUMMARY_FILENAME).write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / BENCHMARK_PHASE_GATE_MARKDOWN_FILENAME).write_text(
        render_benchmark_phase_gate_suite_markdown(summary),
        encoding="utf-8",
    )
    return payload


def run_benchmark_phase_gate_suite(base_config: PipelineRunConfig) -> tuple[Path, dict[str, Any]]:
    logger = get_logger(LOGGER_NAME)
    suite_dir = _timestamped_suite_dir(base_config.base_dir)
    runs_dir = suite_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    gate_results: list[BenchmarkPhaseGateRunSummary] = []

    for gate_spec in ordered_benchmark_phase_gate_specs():
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
            "benchmark_phase_gate_start gate=%s flight=%s declared_benchmark_phase=%s suite_dir=%s",
            gate_spec.gate_name,
            gate_spec.flight_name,
            gate_spec.declared_benchmark_phase,
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
            logger.exception("benchmark_phase_gate_failed gate=%s flight=%s", gate_spec.gate_name, gate_spec.flight_name)
        gate_results.append(
            build_benchmark_phase_gate_run_summary(
                gate_spec=gate_spec,
                run_dir=run_dir,
                run_status=run_status,
                error_message=error_message,
            )
        )

    summary = build_benchmark_phase_gate_suite_summary(
        suite_dir=suite_dir,
        gate_results=tuple(gate_results),
        gate_specs=ordered_benchmark_phase_gate_specs(),
    )
    payload = write_benchmark_phase_gate_suite_report(suite_dir=suite_dir, summary=summary)
    logger.info(
        "benchmark_phase_gate_suite_complete suite_dir=%s all_gates_met_or_exceeded=%s",
        suite_dir,
        payload.get("all_gates_met_or_exceeded"),
    )
    return suite_dir, payload


def main() -> None:
    args = parse_args()
    config = PipelineRunConfig.from_args(args)
    suite_dir, payload = run_benchmark_phase_gate_suite(config)
    print(
        json.dumps(
            {
                "suite_dir": str(suite_dir),
                "summary_path": str(Path(suite_dir) / "reports" / BENCHMARK_PHASE_GATE_SUMMARY_FILENAME),
                "all_gates_met_or_exceeded": payload.get("all_gates_met_or_exceeded"),
                "gate_alignment_status_count": payload.get("gate_alignment_status_count"),
            },
            indent=2,
            sort_keys=True,
        )
    )
