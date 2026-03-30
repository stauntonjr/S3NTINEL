"""Shared runner for benchmark variants."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Callable

from libs.perf import get_logger
from libs.tuning.benchmark_execution import build_replay_run_command, build_run_command
from libs.tuning.benchmark_reporting import BenchmarkResult
from libs.tuning.benchmark_runtime import build_benchmark_result, find_child_run_dir
from libs.tuning.benchmark_variants import BenchmarkVariant


def run_benchmark_variant(
    *,
    args: Any,
    benchmark_dir: Path,
    variant: BenchmarkVariant,
    repeat_index: int,
    logger_name: str,
    summary_name: str,
    planned_variant_replay: dict[str, Any],
    resolved_objective_payload: dict[str, Any] | None,
    resolve_effective_objective_spec: Callable[..., Any],
    resolve_effective_objective_name: Callable[..., str],
    resolve_effective_evaluation_tier: Callable[..., str],
    infer_replay_target_stage: Callable[..., str | None],
    resolve_replay_end_stage: Callable[..., str],
    merged_objective_overrides: Callable[..., tuple[tuple[str, Any], ...]],
    process_runner: Callable[..., Any],
    script_cwd: Path,
) -> BenchmarkResult:
    logger = get_logger(logger_name)
    effective_args = argparse.Namespace(**vars(args))
    for key, value in dict(variant.arg_overrides or {}).items():
        setattr(effective_args, str(key), value)
    run_base_dir = benchmark_dir / "runs" / variant.name / f"repeat_{repeat_index}"
    run_base_dir.mkdir(parents=True, exist_ok=True)
    replay_source_run_dir = (
        Path(str(effective_args.replay_source_run_dir)).resolve()
        if effective_args.replay_source_run_dir is not None
        else None
    )
    replay_resume_plan = None
    objective_spec = resolve_effective_objective_spec(effective_args, variant=variant)
    resolved_evaluation_tier = resolve_effective_evaluation_tier(effective_args, variant=variant)
    resolved_objective_name = resolve_effective_objective_name(effective_args, variant=variant)
    persisted_objective_spec_path = None
    if replay_source_run_dir is not None:
        reports_dir = run_base_dir / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        persisted_objective_spec_path = reports_dir / "resolved_objective_spec.json"
        persisted_objective_spec_path.write_text(
            json.dumps(resolved_objective_payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        replay_target_stage = str(effective_args.replay_target_stage or infer_replay_target_stage(effective_args, variant=variant) or "")
        if not replay_target_stage:
            raise RuntimeError(
                "replay benchmarking requires either --replay-target-stage or a sweep that maps to an inferable impacted stage"
            )
        replay_end_stage = resolve_replay_end_stage(effective_args, replay_target_stage=replay_target_stage, variant=variant)
        command, expected_run_dir, replay_resume_plan, replay_end_stage = build_replay_run_command(
            effective_args,
            run_base_dir=run_base_dir,
            replay_source_run_dir=replay_source_run_dir,
            replay_target_stage=replay_target_stage,
            replay_end_stage=replay_end_stage,
        )
    else:
        command = build_run_command(effective_args, run_base_dir=run_base_dir)
        expected_run_dir = None
        replay_end_stage = None
    child_env = dict(os.environ)
    if effective_args.spark_profile:
        child_env["S3NTINEL_SPARK_PROFILE"] = str(effective_args.spark_profile)
    for key, value in effective_args.extra_env:
        child_env[str(key)] = str(value)
    for key, value in variant.env_overrides.items():
        child_env[str(key)] = str(value)

    logger.info(
        "benchmark_variant_start variant=%s repeat=%s mode=%s run_base_dir=%s",
        variant.name,
        repeat_index,
        effective_args.mode,
        run_base_dir,
    )
    completed = process_runner(command, cwd=script_cwd, env=child_env, check=False)
    run_dir = expected_run_dir if expected_run_dir is not None else find_child_run_dir(run_base_dir)
    return build_benchmark_result(
        variant_name=variant.name,
        variant_description=variant.description,
        variant_env_overrides=dict(variant.env_overrides),
        variant_arg_overrides=dict(variant.arg_overrides or {}),
        variant_objective_preset_name=(None if variant.objective_preset is None else str(variant.objective_preset.name)),
        repeat_index=repeat_index,
        completed_return_code=int(completed.returncode),
        replay_source_run_dir=(str(replay_source_run_dir) if replay_source_run_dir is not None else None),
        replay_target_stage=(None if replay_resume_plan is None else str(replay_resume_plan.target_stage_script)),
        replay_start_stage=(None if replay_resume_plan is None else str(replay_resume_plan.selected_start_stage_script)),
        replay_end_stage=(None if replay_end_stage is None else str(replay_end_stage)),
        planned_replay_start_stage=(
            None if not planned_variant_replay.get("recommended_start_stage") else str(planned_variant_replay.get("recommended_start_stage"))
        ),
        planned_replay_stage_count=(
            None if planned_variant_replay.get("recommended_stage_count") is None else int(planned_variant_replay.get("recommended_stage_count"))
        ),
        evaluation_tier=resolved_evaluation_tier,
        objective_name=resolved_objective_name,
        objective_spec_path=(None if persisted_objective_spec_path is None else str(persisted_objective_spec_path)),
        objective_overrides=tuple({"path": path, "value": value} for path, value in merged_objective_overrides(args=effective_args, variant=variant)),
        run_dir=run_dir,
        summary_name=summary_name,
        objective_spec=objective_spec,
    )
