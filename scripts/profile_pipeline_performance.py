"""Benchmark semantics-preserving pipeline tuning variants on canonical simulation runs.

TODO:
- add an explicit dataset-size scale sweep mode; the current script compares tuning variants
  on fixed workloads and does not measure how wall time, memory, disk, and stage timings
  scale with dataset size.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
from typing import Any

from libs.perf import get_logger
from libs.simulation.cli import add_backbone_args, add_event_args, add_profile_args, add_source_args, add_window_args
from libs.tuning import (
    all_validation_metrics_from_harness,
    build_benchmark_result,
    build_benchmark_dir,
    build_replay_run_command,
    build_run_command,
    BenchmarkResult,
    BenchmarkVariant,
    KNOWN_SEARCH_STAGES,
    KNOWN_SEARCH_STRATEGIES,
    DEFAULT_VALIDATION_PANEL_LIMIT,
    DEFAULT_VALIDATION_PANEL_MODE,
    EVALUATION_END_STAGE_BY_TIER,
    KNOWN_OBJECTIVE_PRESET_NAMES,
    KNOWN_OBJECTIVE_SPEC_NAMES,
    KNOWN_VALIDATION_PANEL_MODES,
    OBJECTIVE_PRESET_BY_NAME,
    VARIANT_BY_NAME,
    VARIANT_SET_BY_NAME,
    build_experiment_plan_markdown,
    build_experiment_plan_payload,
    build_markdown_summary,
    build_summary_payload,
    build_search_variants,
    build_validation_metric_panel,
    build_variant_plan_payload,
    clone_replay_source_run,
    count_by_result_field,
    count_metric_names,
    find_child_run_dir,
    infer_replay_target_stage,
    load_json,
    load_objective_spec,
    merged_objective_overrides,
    objective_spec_from_payload,
    replay_drift_status_counts,
    replay_drift_status,
    resolved_objective_payload,
    resolve_default_objective_name,
    resolve_effective_evaluation_tier,
    resolve_effective_objective_name,
    resolve_effective_objective_spec,
    resolve_evaluation_end_stage,
    resolve_objective_spec,
    resolve_replay_end_stage,
    results_by_variant,
    run_benchmark_variant,
    selected_validation_metrics_for_objective,
    stage_elapsed_ms,
    validate_benchmark_args,
    validation_metric_index,
    variant_aggregate_payload,
)


LOGGER_NAME = "s3ntinel.profile_pipeline_performance"
SUMMARY_NAME_BY_MODE = {
    "profile": "profile_pipeline_run_summary.json",
    "event": "event_pipeline_run_summary.json",
    "structural": "structural_pipeline_run_summary.json",
    "full": "pipeline_run_summary.json",
}
TIMED_STAGE_SCRIPTS = (
    "20_events_extract.py",
    "25_window_policy_profile.py",
    "30_windows_adaptive.py",
    "50_build_graph.py",
    "70_phase_fit.py",
)
def _parse_env_assignment(raw_value: str) -> tuple[str, str]:
    key, separator, value = str(raw_value).partition("=")
    if not separator or not key.strip():
        raise argparse.ArgumentTypeError("environment overrides must use KEY=VALUE syntax")
    return key.strip(), value


def _parse_objective_override(raw_value: str) -> tuple[str, Any]:
    path, separator, raw_payload = str(raw_value).partition("=")
    if not separator or not path.strip():
        raise argparse.ArgumentTypeError("objective overrides must use PATH=VALUE syntax")
    normalized_path = path.strip()
    try:
        parsed_value = json.loads(raw_payload)
    except json.JSONDecodeError:
        parsed_value = raw_payload
    return normalized_path, parsed_value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark semantics-preserving pipeline tuning variants against the canonical simulation runner"
    )
    add_source_args(parser)
    add_profile_args(parser)
    add_event_args(parser)
    add_window_args(parser)
    add_backbone_args(parser)
    parser.add_argument("--base-dir", default="data/performance_profiles", help="Base directory for benchmark bundles")
    parser.add_argument("--mode", default="full", choices=("profile", "event", "structural", "full"))
    parser.add_argument("--format", default="parquet", choices=("parquet", "delta"))
    parser.add_argument("--write-mode", default="overwrite", choices=("overwrite", "append", "merge"))
    parser.add_argument("--min-warm", default=1, type=int)
    parser.add_argument("--phase-count", default=4, type=int)
    parser.add_argument("--variant-set", default="quick", choices=tuple(sorted(VARIANT_SET_BY_NAME)))
    parser.add_argument(
        "--variant",
        dest="variants",
        action="append",
        default=[],
        choices=tuple(sorted(VARIANT_BY_NAME)),
        help="Run only the named variant; may be repeated",
    )
    parser.add_argument("--repeat", default=1, type=int, help="Repeat each variant this many times")
    parser.add_argument(
        "--search-stage",
        default=None,
        choices=KNOWN_SEARCH_STAGES,
        help="Generate a stage-local combinatorial search instead of using a named variant set",
    )
    parser.add_argument(
        "--search-strategy",
        default="grid",
        choices=KNOWN_SEARCH_STRATEGIES,
        help="Search strategy for --search-stage",
    )
    parser.add_argument(
        "--search-budget",
        default=None,
        type=int,
        help="Optional maximum number of non-baseline search configurations to run",
    )
    parser.add_argument(
        "--search-seed",
        default=0,
        type=int,
        help="Random seed used when --search-strategy=random",
    )
    parser.add_argument("--spark-profile", default=None, help="Optional S3NTINEL_SPARK_PROFILE override for child runs")
    parser.add_argument(
        "--replay-source-run-dir",
        default=None,
        help="Optional source simulation run dir to clone and replay from instead of launching fresh full runs",
    )
    parser.add_argument(
        "--replay-target-stage",
        default=None,
        help="Target stage script for replay benchmarking, e.g. 50_build_graph.py; requires --replay-source-run-dir",
    )
    parser.add_argument(
        "--evaluation-tier",
        default=None,
        choices=tuple(EVALUATION_END_STAGE_BY_TIER),
        help="Minimal downstream evaluation closure to run when replay benchmarking",
    )
    parser.add_argument(
        "--objective-name",
        default=None,
        choices=KNOWN_OBJECTIVE_SPEC_NAMES,
        help="Objective spec name to optimize; drives evaluation closure and must agree with --evaluation-tier when both are set",
    )
    parser.add_argument(
        "--objective-preset",
        default=None,
        choices=KNOWN_OBJECTIVE_PRESET_NAMES,
        help="Named objective preset to optimize; may provide a base objective plus preset-level overrides",
    )
    parser.add_argument(
        "--objective-spec-path",
        default=None,
        help="Path to a custom objective spec JSON or objective_evaluation_report.json; drives replay closure like --objective-name",
    )
    parser.add_argument(
        "--objective-override",
        dest="objective_overrides",
        action="append",
        default=[],
        type=_parse_objective_override,
        help="Override a resolved objective payload field using PATH=VALUE, e.g. primary_terms.0.weight=2.0",
    )
    parser.add_argument(
        "--validation-panel-mode",
        default=DEFAULT_VALIDATION_PANEL_MODE,
        choices=KNOWN_VALIDATION_PANEL_MODES,
        help="Which validation metrics to surface in the markdown summary panel",
    )
    parser.add_argument(
        "--validation-panel-limit",
        default=DEFAULT_VALIDATION_PANEL_LIMIT,
        type=int,
        help="Maximum number of validation metrics to include in the markdown summary panel",
    )
    parser.add_argument(
        "--env",
        dest="extra_env",
        action="append",
        default=[],
        type=_parse_env_assignment,
        help="Extra semantics-preserving environment override in KEY=VALUE form",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Deprecated compatibility flag; benchmark runs now continue after variant failures by default.",
    )
    parser.add_argument(
        "--fail-on-variant-error",
        action="store_true",
        help="Exit non-zero when any variant fails instead of recording the failure in the summary and continuing.",
    )
    return parser.parse_args()


_resolved_objective_payload = resolved_objective_payload
_merged_objective_overrides = merged_objective_overrides
resolved_effective_objective_spec = resolve_effective_objective_spec
_build_benchmark_dir = build_benchmark_dir
_build_run_command = build_run_command
_clone_replay_source_run = clone_replay_source_run
_build_replay_run_command = build_replay_run_command
_load_json = load_json
_find_child_run_dir = find_child_run_dir
_stage_elapsed_ms = stage_elapsed_ms
_replay_drift_status = replay_drift_status
_replay_drift_status_counts = replay_drift_status_counts
_count_by_result_field = count_by_result_field
_count_metric_names = count_metric_names
_validation_metric_index = validation_metric_index
_all_validation_metrics_from_harness = all_validation_metrics_from_harness
_selected_validation_metrics_for_objective = selected_validation_metrics_for_objective
_results_by_variant = results_by_variant
_variant_aggregate_payload = variant_aggregate_payload
_validation_metric_panel = build_validation_metric_panel
_variant_plan_payload = build_variant_plan_payload
_build_experiment_plan_markdown = build_experiment_plan_markdown


def _resolved_reporting_context(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "evaluation_tier": resolve_effective_evaluation_tier(args),
        "objective_name": resolve_effective_objective_name(args),
        "objective_spec_path": (str(args.objective_spec_path) if args.objective_spec_path is not None else None),
        "objective_overrides": [{"path": path, "value": value} for path, value in args.objective_overrides],
        "search_stage": args.search_stage,
        "search_strategy": args.search_strategy,
        "search_budget": args.search_budget,
        "search_seed": args.search_seed,
    }


def _build_experiment_plan_payload(
    *,
    benchmark_dir: Path,
    args: argparse.Namespace,
    variants: tuple[BenchmarkVariant, ...],
) -> dict[str, Any]:
    reporting_context = _resolved_reporting_context(args)
    return build_experiment_plan_payload(
        benchmark_dir=str(benchmark_dir),
        flight_name=str(args.flight_name),
        mode=str(args.mode),
        variant_set=("search" if args.search_stage is not None else str(args.variant_set)),
        repeat=int(args.repeat),
        spark_profile=args.spark_profile,
        search_stage=args.search_stage,
        search_strategy=args.search_strategy,
        search_budget=args.search_budget,
        search_seed=int(args.search_seed),
        replay_source_run_dir=args.replay_source_run_dir,
        requested_target_stage=args.replay_target_stage,
        requested_evaluation_tier=args.evaluation_tier,
        objective_name=args.objective_name,
        objective_preset=args.objective_preset,
        objective_spec_path=reporting_context["objective_spec_path"],
        objective_overrides=reporting_context["objective_overrides"],
        variants=[
            _variant_plan_payload(args, variant=variant)
            for variant in variants
        ],
    )


def _build_summary_payload(
    *,
    benchmark_dir: Path,
    args: argparse.Namespace,
    results: list[BenchmarkResult],
) -> dict[str, object]:
    reporting_context = _resolved_reporting_context(args)
    return build_summary_payload(
        benchmark_dir=str(benchmark_dir),
        flight_name=str(args.flight_name),
        mode=str(args.mode),
        variant_set=("search" if args.search_stage is not None else str(args.variant_set)),
        repeat=int(args.repeat),
        replay_source_run_dir=args.replay_source_run_dir,
        replay_target_stage=args.replay_target_stage,
        evaluation_tier=reporting_context["evaluation_tier"],
        objective_name=reporting_context["objective_name"],
        objective_preset=args.objective_preset,
        objective_spec_path=reporting_context["objective_spec_path"],
        objective_overrides=reporting_context["objective_overrides"],
        validation_panel_mode=str(args.validation_panel_mode),
        validation_panel_limit=int(args.validation_panel_limit),
        spark_profile=args.spark_profile,
        extra_env={key: value for key, value in args.extra_env},
        search_stage=args.search_stage,
        search_strategy=args.search_strategy,
        search_budget=args.search_budget,
        search_seed=int(args.search_seed),
        results=results,
    )


def _build_markdown_summary(*, args: argparse.Namespace, results: list[BenchmarkResult]) -> str:
    reporting_context = _resolved_reporting_context(args)
    return build_markdown_summary(
        flight_name=str(args.flight_name),
        mode=str(args.mode),
        variant_set=("search" if args.search_stage is not None else str(args.variant_set)),
        repeat=int(args.repeat),
        replay_source_run_dir=args.replay_source_run_dir,
        replay_target_stage=args.replay_target_stage,
        evaluation_tier=reporting_context["evaluation_tier"],
        objective_name=reporting_context["objective_name"],
        objective_preset=args.objective_preset,
        objective_spec_path=reporting_context["objective_spec_path"],
        objective_override_count=len(args.objective_overrides),
        validation_panel_mode=str(args.validation_panel_mode),
        validation_panel_limit=int(args.validation_panel_limit),
        search_stage=args.search_stage,
        search_strategy=args.search_strategy,
        search_budget=args.search_budget,
        results=results,
    )


def _run_variant(
    *,
    args: argparse.Namespace,
    benchmark_dir: Path,
    variant: BenchmarkVariant,
    repeat_index: int,
) -> BenchmarkResult:
    planned_variant_replay = _variant_plan_payload(args, variant=variant)
    replay_source_run_dir = (
        Path(str(args.replay_source_run_dir)).resolve()
        if args.replay_source_run_dir is not None
        else None
    )
    return run_benchmark_variant(
        args=args,
        benchmark_dir=benchmark_dir,
        variant=variant,
        repeat_index=repeat_index,
        logger_name=LOGGER_NAME,
        summary_name=SUMMARY_NAME_BY_MODE[str(args.mode)],
        planned_variant_replay=planned_variant_replay,
        resolved_objective_payload=(
            None
            if replay_source_run_dir is None
            else _resolved_objective_payload(args, variant=variant)
        ),
        resolve_effective_objective_spec=resolve_effective_objective_spec,
        resolve_effective_objective_name=resolve_effective_objective_name,
        resolve_effective_evaluation_tier=resolve_effective_evaluation_tier,
        infer_replay_target_stage=infer_replay_target_stage,
        resolve_replay_end_stage=resolve_replay_end_stage,
        merged_objective_overrides=_merged_objective_overrides,
        process_runner=subprocess.run,
        script_cwd=Path(__file__).resolve().parents[1],
    )


def main() -> None:
    args = parse_args()
    validate_benchmark_args(args)
    benchmark_dir = _build_benchmark_dir(base_dir=str(args.base_dir), flight_name=str(args.flight_name), mode=str(args.mode))
    benchmark_dir.mkdir(parents=True, exist_ok=True)
    logger = get_logger(LOGGER_NAME)
    variants = (
        build_search_variants(
            search_stage=str(args.search_stage),
            search_strategy=str(args.search_strategy),
            search_budget=args.search_budget,
            search_seed=int(args.search_seed),
        )
        if args.search_stage is not None
        else (
            tuple(VARIANT_BY_NAME[name] for name in args.variants)
            if args.variants
            else VARIANT_SET_BY_NAME[str(args.variant_set)]
        )
    )
    reports_dir = benchmark_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    experiment_plan_payload = _build_experiment_plan_payload(
        benchmark_dir=benchmark_dir,
        args=args,
        variants=variants,
    )
    (reports_dir / "performance_profile_plan.json").write_text(
        json.dumps(experiment_plan_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / "performance_profile_plan.md").write_text(
        _build_experiment_plan_markdown(experiment_plan_payload),
        encoding="utf-8",
    )
    results: list[BenchmarkResult] = []

    for variant in variants:
        for repeat_index in range(1, max(int(args.repeat), 1) + 1):
            result = _run_variant(
                args=args,
                benchmark_dir=benchmark_dir,
                variant=variant,
                repeat_index=repeat_index,
            )
            results.append(result)
            logger.info(
                "benchmark_variant_end variant=%s repeat=%s status=%s elapsed_ms=%s run_dir=%s",
                result.name,
                result.repeat_index,
                result.status,
                result.elapsed_ms,
                result.run_dir,
            )

    summary_payload = _build_summary_payload(benchmark_dir=benchmark_dir, args=args, results=results)
    (reports_dir / "performance_profile_summary.json").write_text(
        json.dumps(summary_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / "performance_profile_summary.md").write_text(
        _build_markdown_summary(args=args, results=results),
        encoding="utf-8",
    )

    any_success = any(result.status == "success" for result in results)
    any_failure = any(result.status != "success" for result in results)
    if args.fail_on_variant_error and any_failure:
        raise SystemExit(1)
    if not any_success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
