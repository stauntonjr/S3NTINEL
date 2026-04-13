"""Full-run engineering and modeling report rendering for simulation bundles."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from libs.simulation.report_tables import ArtifactView, RunArtifactBundle
from libs.simulation.run_bundle import load_json_if_exists, path_size_bytes
from libs.simulation.run_context import RunPaths, write_manifest
from libs.windows import build_window_truth_phase_coverage_summary


@dataclass(frozen=True)
class StageModelingSection:
    stage_script: str
    report_keys: tuple[str, ...]


@dataclass(frozen=True)
class StageRunReport:
    stage_script: str
    status: str | None
    engineering_performance: dict[str, Any]
    modeling_performance: dict[str, Any]

    def to_payload(self) -> dict[str, Any]:
        return {
            "stage_script": self.stage_script,
            "status": self.status,
            "engineering_performance": self.engineering_performance,
            "modeling_performance": self.modeling_performance,
        }


@dataclass(frozen=True)
class EngineeringPerformanceReport:
    overall: dict[str, Any]
    stages: tuple[StageRunReport, ...]
    scale_signature: dict[str, Any]

    def to_payload(self) -> dict[str, Any]:
        return {
            "overall": self.overall,
            "stages": [stage.to_payload() for stage in self.stages],
            "scale_signature": self.scale_signature,
        }


@dataclass(frozen=True)
class FullRunReport:
    report_version: str
    status: str | None
    run_dir: str
    modeling_performance: dict[str, Any]
    window_policy_profile: dict[str, Any]
    engineering_performance: EngineeringPerformanceReport

    def to_payload(self) -> dict[str, Any]:
        return {
            "report_version": self.report_version,
            "status": self.status,
            "run_dir": self.run_dir,
            "modeling_performance": self.modeling_performance,
            "window_policy_profile": self.window_policy_profile,
            "engineering_performance": self.engineering_performance.to_payload(),
        }


VALIDATION_REPORT_BY_KEY = {
    "profile_validation": "profile_validation_summary.json",
    "event_validation": "event_validation_summary.json",
    "label_contract": "label_contract_summary.json",
    "phase_validation": "phase_validation_summary.json",
    "hierarchy_validation": "hierarchy_validation_summary.json",
    "coupling_validation": "coupling_validation_summary.json",
    "score_validation": "score_validation_summary.json",
    "misbehavior_score_validation": "misbehavior_score_validation_summary.json",
    "fault_window_validation": "fault_window_validation_summary.json",
    "misbehavior_window_validation": "misbehavior_window_validation_summary.json",
    "attribution_validation": "attribution_validation_summary.json",
    "misbehavior_attribution_validation": "misbehavior_attribution_validation_summary.json",
    "simulation_benchmark_audit": "simulation_benchmark_audit_summary.json",
    "benchmark_scope_validation": "benchmark_scope_validation_summary.json",
    "benchmark_tier_validation": "benchmark_tier_validation_summary.json",
}

MODELING_SUMMARY_KEYS = tuple(VALIDATION_REPORT_BY_KEY.keys())
MARKDOWN_MODELING_KEYS = (
    "profile_validation",
    "event_validation",
    "phase_validation",
    "hierarchy_validation",
    "coupling_validation",
    "score_validation",
    "attribution_validation",
    "simulation_benchmark_audit",
    "benchmark_scope_validation",
    "benchmark_tier_validation",
)
STAGE_MODELING_SECTIONS = (
    StageModelingSection("12_behavior_profiles_fit.py", ("profile_validation",)),
    StageModelingSection("20_events_extract.py", ("event_validation", "label_contract")),
    StageModelingSection("60_fit_hierarchy.py", ("hierarchy_validation", "coupling_validation")),
    StageModelingSection("70_phase_fit.py", ("phase_validation",)),
    StageModelingSection(
        "85_window_scores_calibrate.py",
        ("score_validation", "misbehavior_score_validation", "fault_window_validation", "misbehavior_window_validation"),
    ),
    StageModelingSection(
        "90_anomaly_attribution.py",
        (
            "attribution_validation",
            "misbehavior_attribution_validation",
            "simulation_benchmark_audit",
            "benchmark_scope_validation",
            "benchmark_tier_validation",
        ),
    ),
)

WINDOW_POLICY_WINDOWS_VIEW = ArtifactView(
    "windows",
    ("tail_id", "flight_id", "win_id", "t_start", "t_end", "duration_ms", "event_count", "real_event_count", "close_reason"),
    ("tail_id", "flight_id", "win_id"),
)
WINDOW_POLICY_PHASE_LABELS_VIEW = ArtifactView(
    "phase_labels",
    ("tail_id", "flight_id", "timestamp_utc", "phase_label"),
    ("tail_id", "flight_id", "timestamp_utc"),
)


def _payload(validation_payloads: dict[str, Any] | None, report_key: str) -> dict[str, Any] | None:
    filename = VALIDATION_REPORT_BY_KEY[report_key]
    return (validation_payloads or {}).get(filename)


def _stage_modeling_sections(validation_payloads: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    return {
        section.stage_script: {
            report_key: _payload(validation_payloads, report_key)
            for report_key in section.report_keys
        }
        for section in STAGE_MODELING_SECTIONS
    }


def _build_modeling_performance_summary(validation_payloads: dict[str, Any] | None) -> dict[str, Any]:
    summary = {report_key: _payload(validation_payloads, report_key) for report_key in MODELING_SUMMARY_KEYS}
    hierarchy_payload = summary.get("hierarchy_validation") or {}
    summary["hierarchy_validation"] = hierarchy_payload.get("hierarchy", {})
    summary["graph_signatures"] = hierarchy_payload.get("graph_signatures", {})
    return summary


def _build_stage_engineering_sections(
    *,
    paths: RunPaths,
    pipeline_summary: dict[str, Any],
    validation_payloads: dict[str, Any] | None,
) -> tuple[StageRunReport, ...]:
    total_elapsed_ms = float(pipeline_summary.get("total_elapsed_ms") or 0.0)
    modeling_by_stage = _stage_modeling_sections(validation_payloads)
    stage_sections: list[StageRunReport] = []
    for stage in pipeline_summary.get("stages", []) or []:
        stage_script = str(stage.get("stage_script", ""))
        stage_id = stage_script.removesuffix(".py")
        stage_summary_path = paths.run_dir / "reports" / "stages" / f"{stage_id}_summary.json"
        stage_manifest_path = paths.run_dir / "reports" / "stages" / f"{stage_id}_manifest.json"
        stage_summary = load_json_if_exists(stage_summary_path) or {}
        output_paths = {
            str(value)
            for key, value in stage_summary.items()
            if key.endswith("_path") and isinstance(value, str)
        }
        elapsed_ms = float(stage.get("elapsed_ms") or 0.0)
        stage_sections.append(
            StageRunReport(
                stage_script=stage_script,
                status=stage.get("status"),
                engineering_performance={
                    "elapsed_ms": elapsed_ms,
                    "elapsed_seconds": elapsed_ms / 1000.0,
                    "share_of_total_elapsed": (elapsed_ms / total_elapsed_ms) if total_elapsed_ms > 0.0 else None,
                    "summary_path": str(stage_summary_path),
                    "manifest_path": str(stage_manifest_path),
                    "stage_summary": stage_summary,
                    "output_artifact_size_bytes": sum(path_size_bytes(Path(path)) for path in output_paths),
                },
                modeling_performance=modeling_by_stage.get(stage_script, {}),
            )
        )
    return tuple(stage_sections)


def _build_scale_signature(
    *,
    manifest: dict[str, Any],
    validation_payloads: dict[str, Any] | None,
    stage_sections: tuple[StageRunReport, ...],
) -> dict[str, Any]:
    stage_by_script = {section.stage_script: section for section in stage_sections}
    graph_stage_summary = (
        (stage_by_script.get("50_build_graph.py").engineering_performance if stage_by_script.get("50_build_graph.py") else {})
        .get("stage_summary")
        or {}
    )
    phase_summary = _payload(validation_payloads, "phase_validation") or {}
    score_summary = _payload(validation_payloads, "score_validation") or {}
    event_summary = _payload(validation_payloads, "event_validation") or {}
    profile_summary = _payload(validation_payloads, "profile_validation") or {}
    return {
        "seed_counts": dict(manifest.get("seed_counts", {}) or {}),
        "validation_counts": {
            "labeled_event_count": event_summary.get("label_event_count"),
            "detected_event_count": event_summary.get("detected_event_count"),
            "parameter_count": profile_summary.get("parameter_count"),
            "phase_assignment_count": phase_summary.get("assignment_count"),
            "fault_window_count": score_summary.get("fault_window_count"),
        },
        "graph_counts": {
            "lag_edge_count": graph_stage_summary.get("lag_edge_count"),
            "event_edge_count": graph_stage_summary.get("event_edge_count"),
            "transition_edge_count": graph_stage_summary.get("transition_edge_count"),
            "fused_edge_count": graph_stage_summary.get("fused_edge_count"),
            "graph_parameter_universe_count": graph_stage_summary.get("graph_parameter_universe_count"),
        },
        "current_scale_visibility": {
            "size_proxies_present_in_run": True,
            "variant_benchmarking_script": "scripts/profile_pipeline_performance.py",
            "dataset_size_sweep_available": False,
            "recommendation": "set up an explicit scale-sweep experiment; current tooling compares tuning variants on a fixed workload and only exposes size proxies within single runs",
        },
    }


def _build_engineering_performance_summary(
    *,
    paths: RunPaths,
    manifest: dict[str, Any],
    summary_artifact_path: str | None,
    validation_payloads: dict[str, Any] | None,
) -> EngineeringPerformanceReport:
    pipeline_summary = (load_json_if_exists(paths.run_dir / summary_artifact_path) if summary_artifact_path is not None else None) or {}
    stage_sections = _build_stage_engineering_sections(
        paths=paths,
        pipeline_summary=pipeline_summary,
        validation_payloads=validation_payloads,
    )
    artifact_sizes = {
        name: path_size_bytes(Path(payload.get("path", "")))
        for name, payload in (manifest.get("artifacts", {}) or {}).items()
        if isinstance(payload, dict) and payload.get("exists")
    }
    return EngineeringPerformanceReport(
        overall={
            "pipeline_summary": pipeline_summary,
            "manifest_timing": manifest.get("timing"),
            "environment": manifest.get("environment"),
            "memory_snapshot_end": pipeline_summary.get("memory_snapshot_end"),
            "artifact_disk_bytes_total": int(sum(artifact_sizes.values())),
            "artifact_disk_bytes_by_name": artifact_sizes,
        },
        stages=stage_sections,
        scale_signature=_build_scale_signature(
            manifest=manifest,
            validation_payloads=validation_payloads,
            stage_sections=stage_sections,
        ),
    )


def _build_truth_phase_window_supply_summary(
    *,
    spark: Any | None,
    paths: RunPaths,
    table_format: str | None,
) -> dict[str, Any]:
    if spark is None or table_format is None:
        return {
            "status": "skipped",
            "reason": "spark or table_format unavailable",
        }
    try:
        bundle = RunArtifactBundle.load(
            spark=spark,
            paths=paths,
            table_format=str(table_format),
            views=(WINDOW_POLICY_WINDOWS_VIEW, WINDOW_POLICY_PHASE_LABELS_VIEW),
        )
        return build_window_truth_phase_coverage_summary(
            windows_df=bundle.pandas(WINDOW_POLICY_WINDOWS_VIEW),
            phase_labels_df=bundle.pandas(WINDOW_POLICY_PHASE_LABELS_VIEW),
        )
    except Exception as exc:
        return {
            "status": "skipped",
            "reason": f"window_truth_phase_coverage_unavailable: {exc!r}",
        }


def _build_window_policy_profile_summary_without_stage25(
    *,
    spark: Any | None,
    paths: RunPaths,
    table_format: str | None,
    reason: str,
) -> dict[str, Any]:
    stage30_summary_path = paths.run_dir / "reports" / "stages" / "30_windows_adaptive_summary.json"
    stage30_summary = load_json_if_exists(stage30_summary_path) or {}
    return {
        "status": "skipped",
        "reason": str(reason),
        "source_report_path": str(stage30_summary_path if stage30_summary else (paths.run_dir / "reports" / "stages" / "25_window_policy_profile_evaluation.json")),
        "policy_source": stage30_summary.get("policy_source"),
        "selected_max_ms": stage30_summary.get("max_ms"),
        "selected_event_threshold": stage30_summary.get("event_threshold"),
        "selected_candidate_rank": None,
        "selected_objective_score": None,
        "selected_balance_penalty": None,
        "closure_mix": {
            "event_threshold_rate": None,
            "budget_threshold_rate": None,
            "end_of_stream_rate": None,
            "mean_quiet_credit_end": None,
            "p95_quiet_credit_end": None,
            "mean_closure_budget_end": None,
            "p95_closure_budget_end": None,
        },
        "downstream_cost_proxy": {
            "window_count": None,
            "pair_cost_proxy": None,
            "same_window_pair_expansion_proxy": None,
            "p95_event_count": None,
            "p95_sensor_count": None,
        },
        "edge_stability": {
            "status": "skipped",
            "mean_boundary_jaccard": None,
        },
        "truth_phase_window_supply": _build_truth_phase_window_supply_summary(
            spark=spark,
            paths=paths,
            table_format=table_format,
        ),
        "warnings": [],
    }


def _build_window_policy_profile_summary(
    *,
    spark: Any | None,
    paths: RunPaths,
    table_format: str | None,
) -> dict[str, Any]:
    evaluation_report_path = paths.run_dir / "reports" / "stages" / "25_window_policy_profile_evaluation.json"
    evaluation_payload = load_json_if_exists(evaluation_report_path)
    if evaluation_payload is None:
        return _build_window_policy_profile_summary_without_stage25(
            spark=spark,
            paths=paths,
            table_format=table_format,
            reason="missing stage-25 evaluation report",
        )
    selected_policy = dict(evaluation_payload.get("selected_policy") or {})
    resolved_policy = dict(selected_policy.get("resolved_policy") or {})
    closure_rates = dict((evaluation_payload.get("closure_mix") or {}).get("rates") or {})
    downstream_cost = dict(evaluation_payload.get("downstream_cost_proxy") or {})
    edge_stability = dict(evaluation_payload.get("edge_stability") or {})
    return {
        "status": evaluation_payload.get("status", "skipped"),
        "source_report_path": str(evaluation_report_path),
        "policy_source": selected_policy.get("policy_source"),
        "selected_max_ms": resolved_policy.get("max_ms"),
        "selected_event_threshold": resolved_policy.get("event_threshold"),
        "selected_candidate_rank": ((selected_policy.get("profile_row") or {}) or {}).get("candidate_rank"),
        "selected_objective_score": ((selected_policy.get("profile_row") or {}) or {}).get("objective_score"),
        "selected_balance_penalty": ((selected_policy.get("profile_row") or {}) or {}).get("balance_penalty"),
        "closure_mix": {
            "event_threshold_rate": closure_rates.get("event_threshold"),
            "budget_threshold_rate": closure_rates.get("budget_threshold"),
            "end_of_stream_rate": closure_rates.get("end_of_stream"),
            "mean_quiet_credit_end": (evaluation_payload.get("closure_mix") or {}).get("mean_quiet_credit_end"),
            "p95_quiet_credit_end": (evaluation_payload.get("closure_mix") or {}).get("p95_quiet_credit_end"),
            "mean_closure_budget_end": (evaluation_payload.get("closure_mix") or {}).get("mean_closure_budget_end"),
            "p95_closure_budget_end": (evaluation_payload.get("closure_mix") or {}).get("p95_closure_budget_end"),
        },
        "downstream_cost_proxy": {
            "window_count": downstream_cost.get("window_count"),
            "pair_cost_proxy": downstream_cost.get("pair_cost_proxy"),
            "same_window_pair_expansion_proxy": downstream_cost.get("same_window_pair_expansion_proxy"),
            "p95_event_count": downstream_cost.get("p95_event_count"),
            "p95_sensor_count": downstream_cost.get("p95_sensor_count"),
        },
        "edge_stability": {
            "status": edge_stability.get("status"),
            "mean_boundary_jaccard": edge_stability.get("mean_boundary_jaccard"),
        },
        "truth_phase_window_supply": _build_truth_phase_window_supply_summary(
            spark=spark,
            paths=paths,
            table_format=table_format,
        ),
        "warnings": list(evaluation_payload.get("warnings") or []),
    }


def _append_json_section(lines: list[str], heading: str, payload: dict[str, Any] | None) -> None:
    if payload is None:
        return
    lines.extend([heading, "```json", json.dumps(payload, indent=2, sort_keys=True, default=str), "```"])


def _render_markdown(report: dict[str, Any]) -> str:
    engineering = report.get("engineering_performance", {})
    overall = engineering.get("overall", {})
    pipeline_summary = overall.get("pipeline_summary", {})
    lines = ["# Full Run Report", "", "## Modeling Performance"]
    for report_key in MARKDOWN_MODELING_KEYS:
        _append_json_section(lines, f"### {report_key}", report.get("modeling_performance", {}).get(report_key))
    lines.extend(
        [
            "",
            "## Engineering Performance",
            "",
            "### Overall",
            "```json",
            json.dumps(
                {
                    "status": report.get("status"),
                    "total_elapsed_ms": pipeline_summary.get("total_elapsed_ms"),
                    "stage_count": pipeline_summary.get("stage_count"),
                    "artifact_disk_bytes_total": overall.get("artifact_disk_bytes_total"),
                    "memory_snapshot_end": overall.get("memory_snapshot_end"),
                    "scale_signature": engineering.get("scale_signature"),
                },
                indent=2,
                sort_keys=True,
                default=str,
            ),
            "```",
            "",
            "## Window Policy Profile",
            "```json",
            json.dumps(report.get("window_policy_profile", {}), indent=2, sort_keys=True, default=str),
            "```",
            "",
            "### Stages",
        ]
    )
    for stage in engineering.get("stages", []) or []:
        _append_json_section(lines, f"#### {stage.get('stage_script')}", stage)
    lines.append("")
    return "\n".join(lines)


def write_full_run_report(
    *,
    spark: Any | None = None,
    paths: RunPaths,
    manifest: dict[str, Any],
    summary_artifact_path: str | None,
    validation_payloads: dict[str, Any] | None,
    table_format: str | None = None,
) -> dict[str, Any]:
    report = FullRunReport(
        report_version="v1",
        status=manifest.get("status"),
        run_dir=str(paths.run_dir),
        modeling_performance=_build_modeling_performance_summary(validation_payloads),
        window_policy_profile=_build_window_policy_profile_summary(
            spark=spark,
            paths=paths,
            table_format=table_format,
        ),
        engineering_performance=_build_engineering_performance_summary(
            paths=paths,
            manifest=manifest,
            summary_artifact_path=summary_artifact_path,
            validation_payloads=validation_payloads,
        ),
    )
    report_payload = report.to_payload()
    write_manifest(paths.run_dir / "reports" / "full_run_report.json", report_payload)
    (paths.run_dir / "reports" / "full_run_report.md").write_text(_render_markdown(report_payload), encoding="utf-8")
    return report_payload
