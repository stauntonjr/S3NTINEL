"""Benchmark runtime helpers for persisted child-run result extraction."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from libs.tuning.benchmark_reporting import BenchmarkResult


def load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def find_child_run_dir(run_base_dir: Path) -> Path | None:
    candidates = [path for path in run_base_dir.iterdir() if path.is_dir()]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def stage_elapsed_ms(summary_payload: dict[str, object]) -> dict[str, float]:
    stages = summary_payload.get("stages", [])
    if not isinstance(stages, list):
        return {}
    stage_timings: dict[str, float] = {}
    for stage_entry in stages:
        if not isinstance(stage_entry, dict):
            continue
        stage_script = stage_entry.get("stage_script")
        elapsed_ms = stage_entry.get("elapsed_ms")
        if isinstance(stage_script, str) and isinstance(elapsed_ms, (int, float)):
            stage_timings[stage_script] = float(elapsed_ms)
    return stage_timings


def replay_drift_status(
    *,
    planned_start_stage: str | None,
    actual_start_stage: str | None,
) -> str | None:
    if planned_start_stage is None and actual_start_stage is None:
        return None
    if planned_start_stage is None and actual_start_stage is not None:
        return "unplanned"
    if planned_start_stage is not None and actual_start_stage is None:
        return "missing_actual"
    if planned_start_stage == actual_start_stage:
        return "matched"
    return "drifted"


def validation_metric_index(harness_payload: dict[str, Any]) -> dict[tuple[str, str, str], float | int]:
    index: dict[tuple[str, str, str], float | int] = {}
    for record in list(((harness_payload.get("validation_metrics") or {}).get("metric_records") or [])):
        if not isinstance(record, dict):
            continue
        scope_name = str(record.get("scope_name") or "")
        subscope_name = str(record.get("subscope_name") or "")
        metric_path = str(record.get("metric_path") or "")
        value = record.get("value")
        if not scope_name or not subscope_name or not metric_path or not isinstance(value, (int, float)) or isinstance(value, bool):
            continue
        selected = index.get((scope_name, subscope_name, metric_path))
        if selected is None:
            index[(scope_name, subscope_name, metric_path)] = value
    return index


def all_validation_metrics_from_harness(harness_payload: dict[str, Any]) -> dict[str, float | int]:
    all_metrics: dict[str, float | int] = {}
    for (scope_name, subscope_name, metric_path), value in validation_metric_index(harness_payload).items():
        all_metrics[f"{scope_name}:{subscope_name}:{metric_path}"] = value
    return dict(sorted(all_metrics.items()))


def selected_validation_metrics_for_objective(
    *,
    objective_spec: Any,
    harness_payload: dict[str, Any],
) -> dict[str, float | int]:
    metric_index = validation_metric_index(harness_payload)
    selected: dict[str, float | int] = {}
    for item in [*objective_spec.primary_terms, *objective_spec.constraints]:
        metric = item.metric
        if str(metric.category) != "validation":
            continue
        key = (str(metric.scope_name), str(metric.subscope_name), str(metric.metric_path))
        value = metric_index.get(key)
        if value is None:
            continue
        label = item.resolved_label()
        selected[str(label)] = value
    return selected


def build_benchmark_result(
    *,
    variant_name: str,
    variant_description: str,
    variant_env_overrides: dict[str, str],
    variant_arg_overrides: dict[str, Any] | None,
    variant_objective_preset_name: str | None,
    repeat_index: int,
    completed_return_code: int,
    replay_source_run_dir: str | None,
    replay_target_stage: str | None,
    replay_start_stage: str | None,
    replay_end_stage: str | None,
    planned_replay_start_stage: str | None,
    planned_replay_stage_count: int | None,
    evaluation_tier: str | None,
    objective_name: str | None,
    objective_spec_path: str | None,
    objective_overrides: tuple[dict[str, Any], ...],
    run_dir: Path | None,
    summary_name: str,
    objective_spec: Any | None,
) -> BenchmarkResult:
    manifest_path = (run_dir / "reports" / "run_manifest.json") if run_dir is not None else None
    summary_path = (run_dir / "reports" / summary_name) if run_dir is not None else None
    manifest_payload = load_json(manifest_path) if manifest_path is not None and manifest_path.exists() else {}
    summary_payload = load_json(summary_path) if summary_path is not None and summary_path.exists() else {}
    harness_path = (run_dir / "reports" / "validation_harness_report.json") if run_dir is not None else None
    harness_payload = load_json(harness_path) if harness_path is not None and harness_path.exists() else {}
    objective_report_path = (run_dir / "reports" / "objective_evaluation_report.json") if run_dir is not None else None
    objective_report_payload = (
        load_json(objective_report_path)
        if objective_report_path is not None and objective_report_path.exists()
        else {}
    )
    objective_evaluation_payload = dict(objective_report_payload.get("evaluation") or {})
    status = str(manifest_payload.get("status", "failed" if completed_return_code else "success"))
    error = manifest_payload.get("error")
    if error is not None:
        error = str(error)
    elif completed_return_code:
        error = f"benchmark child exited with code {completed_return_code}"
    return BenchmarkResult(
        name=variant_name,
        description=variant_description,
        repeat_index=repeat_index,
        status=status,
        env_overrides=dict(variant_env_overrides),
        arg_overrides=dict(variant_arg_overrides or {}),
        run_dir=(str(run_dir) if run_dir is not None else None),
        manifest_path=(str(manifest_path) if manifest_path is not None else None),
        elapsed_ms=(
            float(manifest_payload["timing"]["elapsed_ms"])
            if isinstance(manifest_payload.get("timing"), dict)
            and isinstance(manifest_payload["timing"].get("elapsed_ms"), (int, float))
            else None
        ),
        stage_elapsed_ms=stage_elapsed_ms(summary_payload),
        return_code=int(completed_return_code),
        error=error,
        replay_source_run_dir=replay_source_run_dir,
        replay_target_stage=replay_target_stage,
        planned_replay_start_stage=planned_replay_start_stage,
        planned_replay_stage_count=planned_replay_stage_count,
        replay_start_stage=replay_start_stage,
        replay_end_stage=replay_end_stage,
        replay_drift_status=replay_drift_status(
            planned_start_stage=planned_replay_start_stage,
            actual_start_stage=replay_start_stage,
        ),
        evaluation_tier=evaluation_tier,
        objective_name=objective_name,
        objective_preset=variant_objective_preset_name,
        objective_spec_path=objective_spec_path,
        objective_overrides=objective_overrides,
        objective_status=(
            str(objective_evaluation_payload.get("overall_status"))
            if objective_evaluation_payload.get("overall_status") is not None
            else None
        ),
        objective_ready_for_search=(
            bool(objective_evaluation_payload.get("ready_for_search"))
            if isinstance(objective_evaluation_payload.get("ready_for_search"), bool)
            else None
        ),
        objective_combined_score=(
            float(objective_evaluation_payload.get("combined_score"))
            if isinstance(objective_evaluation_payload.get("combined_score"), (int, float))
            and not isinstance(objective_evaluation_payload.get("combined_score"), bool)
            else None
        ),
        selected_validation_metrics=(
            selected_validation_metrics_for_objective(
                objective_spec=objective_spec,
                harness_payload=harness_payload,
            )
            if harness_payload and objective_spec is not None
            else None
        ),
        all_validation_metrics=(
            all_validation_metrics_from_harness(harness_payload)
            if harness_payload
            else None
        ),
    )
