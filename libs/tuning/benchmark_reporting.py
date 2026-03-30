"""Benchmark reporting models and summary builders for tuning workflows."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import statistics
from typing import Any

from libs.tuning.validation_panels import build_validation_metric_panel


TIMED_STAGE_SCRIPTS = (
    "20_events_extract.py",
    "25_window_policy_profile.py",
    "30_windows_adaptive.py",
    "50_build_graph.py",
    "70_phase_fit.py",
)


@dataclass(frozen=True)
class BenchmarkResult:
    name: str
    description: str
    repeat_index: int
    status: str
    env_overrides: dict[str, str]
    run_dir: str | None
    manifest_path: str | None
    elapsed_ms: float | None
    stage_elapsed_ms: dict[str, float]
    return_code: int
    error: str | None = None
    arg_overrides: dict[str, Any] | None = None
    replay_source_run_dir: str | None = None
    replay_target_stage: str | None = None
    planned_replay_start_stage: str | None = None
    planned_replay_stage_count: int | None = None
    replay_start_stage: str | None = None
    replay_end_stage: str | None = None
    replay_drift_status: str | None = None
    evaluation_tier: str | None = None
    objective_name: str | None = None
    objective_preset: str | None = None
    objective_spec_path: str | None = None
    objective_overrides: tuple[dict[str, Any], ...] = ()
    objective_status: str | None = None
    objective_ready_for_search: bool | None = None
    objective_combined_score: float | None = None
    selected_validation_metrics: dict[str, float | int] | None = None
    all_validation_metrics: dict[str, float | int] | None = None


def replay_drift_status_counts(results: list[BenchmarkResult]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for result in results:
        status = result.replay_drift_status
        if status is None:
            continue
        counts[status] = counts.get(status, 0) + 1
    return counts


def count_by_result_field(
    results: list[BenchmarkResult],
    *,
    field_name: str,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for result in results:
        value = getattr(result, field_name)
        if value is None:
            continue
        normalized = str(value)
        counts[normalized] = counts.get(normalized, 0) + 1
    return counts


def count_metric_names(
    results: list[BenchmarkResult],
    *,
    field_name: str,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for result in results:
        for metric_name in list((getattr(result, field_name) or {}).keys()):
            counts[str(metric_name)] = counts.get(str(metric_name), 0) + 1
    return dict(sorted(counts.items()))


def results_by_variant(results: list[BenchmarkResult]) -> dict[str, list[BenchmarkResult]]:
    grouped: dict[str, list[BenchmarkResult]] = {}
    for result in results:
        grouped.setdefault(str(result.name), []).append(result)
    return grouped


def variant_aggregate_payload(results: list[BenchmarkResult]) -> list[dict[str, Any]]:
    aggregates: list[dict[str, Any]] = []
    for variant_name, variant_results in sorted(results_by_variant(results).items()):
        successful_results = [result for result in variant_results if result.status == "success" and result.elapsed_ms is not None]
        fastest_result = min(successful_results, key=lambda result: float(result.elapsed_ms)) if successful_results else None
        objective_name_counts = count_by_result_field(variant_results, field_name="objective_name")
        objective_preset_counts = count_by_result_field(variant_results, field_name="objective_preset")
        objective_ready_count = sum(1 for result in variant_results if result.objective_ready_for_search is True)
        objective_scored_results = [
            result
            for result in variant_results
            if result.objective_combined_score is not None
        ]
        best_objective_result = (
            max(
                objective_scored_results,
                key=lambda result: (
                    float(result.objective_combined_score),
                    -float(result.elapsed_ms if result.elapsed_ms is not None else float("inf")),
                ),
            )
            if objective_scored_results
            else None
        )
        replay_drift_counts = replay_drift_status_counts(variant_results)
        stage_timing_aggregates: dict[str, dict[str, float]] = {}
        for stage_script in TIMED_STAGE_SCRIPTS:
            values = [
                float(result.stage_elapsed_ms[stage_script])
                for result in variant_results
                if stage_script in result.stage_elapsed_ms
            ]
            if not values:
                continue
            stage_timing_aggregates[stage_script] = {
                "best_elapsed_ms": min(values),
                "median_elapsed_ms": float(statistics.median(values)),
            }
        validation_metric_aggregates: dict[str, dict[str, float]] = {}
        validation_metric_names = sorted(
            {
                metric_name
                for result in variant_results
                for metric_name in list((result.selected_validation_metrics or {}).keys())
            }
        )
        for metric_name in validation_metric_names:
            values = [
                float((result.selected_validation_metrics or {})[metric_name])
                for result in variant_results
                if metric_name in (result.selected_validation_metrics or {})
            ]
            if not values:
                continue
            validation_metric_aggregates[metric_name] = {
                "best_value": max(values),
                "median_value": float(statistics.median(values)),
            }
        all_validation_metric_aggregates: dict[str, dict[str, float]] = {}
        all_validation_metric_names = sorted(
            {
                metric_name
                for result in variant_results
                for metric_name in list((result.all_validation_metrics or {}).keys())
            }
        )
        for metric_name in all_validation_metric_names:
            values = [
                float((result.all_validation_metrics or {})[metric_name])
                for result in variant_results
                if metric_name in (result.all_validation_metrics or {})
            ]
            if not values:
                continue
            all_validation_metric_aggregates[metric_name] = {
                "best_value": max(values),
                "median_value": float(statistics.median(values)),
            }
        aggregates.append(
            {
                "name": variant_name,
                "description": str(variant_results[0].description) if variant_results else "",
                "repeat_count": len(variant_results),
                "success_count": sum(1 for result in variant_results if result.status == "success"),
                "failure_count": sum(1 for result in variant_results if result.status != "success"),
                "fastest_elapsed_ms": (
                    None if fastest_result is None or fastest_result.elapsed_ms is None else float(fastest_result.elapsed_ms)
                ),
                "objective_name_counts": objective_name_counts,
                "objective_preset_counts": objective_preset_counts,
                "objective_ready_count": objective_ready_count,
                "best_objective_combined_score": (
                    None
                    if best_objective_result is None or best_objective_result.objective_combined_score is None
                    else float(best_objective_result.objective_combined_score)
                ),
                "replay_drift_status_counts": replay_drift_counts,
                "stage_timing_aggregates": stage_timing_aggregates,
                "validation_metric_aggregates": validation_metric_aggregates,
                "all_validation_metric_aggregates": all_validation_metric_aggregates,
            }
        )
    return aggregates


def best_objective_result(results: list[BenchmarkResult]) -> BenchmarkResult | None:
    candidates = [
        result
        for result in results
        if result.status == "success"
        and result.objective_ready_for_search is True
        and result.objective_combined_score is not None
    ]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda result: (
            float(result.objective_combined_score),
            -float(result.elapsed_ms if result.elapsed_ms is not None else float("inf")),
        ),
    )


def build_promotion_recommendation(
    *,
    search_stage: str | None,
    results: list[BenchmarkResult],
) -> dict[str, Any] | None:
    if search_stage is None:
        return None
    selected = best_objective_result(results)
    if selected is None:
        return None
    return {
        "search_stage": search_stage,
        "promoted_variant_name": selected.name,
        "objective_name": selected.objective_name,
        "objective_status": selected.objective_status,
        "objective_ready_for_search": selected.objective_ready_for_search,
        "objective_combined_score": selected.objective_combined_score,
        "arg_overrides": dict(selected.arg_overrides or {}),
        "env_overrides": dict(selected.env_overrides or {}),
        "run_dir": selected.run_dir,
    }


def build_experiment_plan_payload(
    *,
    benchmark_dir: str,
    flight_name: str,
    mode: str,
    variant_set: str,
    repeat: int,
    spark_profile: str | None,
    replay_source_run_dir: str | None,
    requested_target_stage: str | None,
    requested_evaluation_tier: str | None,
    objective_name: str | None,
    objective_preset: str | None,
    objective_spec_path: str | None,
    objective_overrides: list[dict[str, Any]],
    variants: list[dict[str, Any]],
    search_stage: str | None = None,
    search_strategy: str | None = None,
    search_budget: int | None = None,
    search_seed: int | None = None,
) -> dict[str, Any]:
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "benchmark_dir": benchmark_dir,
        "flight_name": flight_name,
        "mode": mode,
        "variant_set": variant_set,
        "selected_variants": [str(variant.get("name") or "") for variant in variants],
        "repeat": int(repeat),
        "spark_profile": spark_profile,
        "search": {
            "stage": search_stage,
            "strategy": search_strategy,
            "budget": search_budget,
            "seed": search_seed,
        },
        "replay": {
            "source_run_dir": replay_source_run_dir,
            "requested_target_stage": requested_target_stage,
            "requested_evaluation_tier": requested_evaluation_tier,
        },
        "objective": {
            "objective_name": objective_name,
            "objective_preset": objective_preset,
            "objective_spec_path": objective_spec_path,
            "objective_overrides": objective_overrides,
        },
        "variants": variants,
    }


def build_experiment_plan_markdown(plan_payload: dict[str, Any]) -> str:
    lines = [
        "# Benchmark Experiment Plan",
        "",
        f"- flight: `{plan_payload.get('flight_name')}`",
        f"- mode: `{plan_payload.get('mode')}`",
        f"- variant set: `{plan_payload.get('variant_set')}`",
        f"- repeats: `{plan_payload.get('repeat')}`",
        f"- search stage: `{((plan_payload.get('search') or {}).get('stage')) or 'none'}`",
        f"- search strategy: `{((plan_payload.get('search') or {}).get('strategy')) or 'none'}`",
        f"- search budget: `{((plan_payload.get('search') or {}).get('budget')) or 'none'}`",
        f"- replay source run dir: `{((plan_payload.get('replay') or {}).get('source_run_dir')) or 'none'}`",
        f"- requested replay target: `{((plan_payload.get('replay') or {}).get('requested_target_stage')) or 'none'}`",
        f"- requested evaluation tier: `{((plan_payload.get('replay') or {}).get('requested_evaluation_tier')) or 'none'}`",
        f"- global objective name: `{((plan_payload.get('objective') or {}).get('objective_name')) or 'none'}`",
        f"- global objective preset: `{((plan_payload.get('objective') or {}).get('objective_preset')) or 'none'}`",
        f"- global objective spec path: `{((plan_payload.get('objective') or {}).get('objective_spec_path')) or 'none'}`",
        "",
        "## Variants",
        "",
        "| variant | objective | tier | replay start | replay target | replay end |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for variant_payload in list(plan_payload.get("variants") or []):
        if not isinstance(variant_payload, dict):
            continue
        lines.append(
            "| "
            + " | ".join(
                (
                    str(variant_payload.get("name") or "n/a"),
                    str(variant_payload.get("resolved_objective_name") or "n/a"),
                    str(variant_payload.get("resolved_evaluation_tier") or "n/a"),
                    str(variant_payload.get("recommended_start_stage") or "n/a"),
                    str(variant_payload.get("replay_target_stage") or "n/a"),
                    str(variant_payload.get("resolved_end_stage") or "n/a"),
                )
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def build_summary_payload(
    *,
    benchmark_dir: str,
    flight_name: str,
    mode: str,
    variant_set: str,
    repeat: int,
    replay_source_run_dir: str | None,
    replay_target_stage: str | None,
    evaluation_tier: str | None,
    objective_name: str | None,
    objective_preset: str | None,
    objective_spec_path: str | None,
    objective_overrides: list[dict[str, Any]],
    validation_panel_mode: str,
    validation_panel_limit: int,
    spark_profile: str | None,
    extra_env: dict[str, str],
    results: list[BenchmarkResult],
    search_stage: str | None = None,
    search_strategy: str | None = None,
    search_budget: int | None = None,
    search_seed: int | None = None,
) -> dict[str, object]:
    successful_results = [result for result in results if result.status == "success" and result.elapsed_ms is not None]
    failed_results = [result for result in results if result.status != "success"]
    fastest_result = min(successful_results, key=lambda result: float(result.elapsed_ms)) if successful_results else None
    replay_drift_counts = replay_drift_status_counts(results)
    objective_name_counts = count_by_result_field(results, field_name="objective_name")
    objective_preset_counts = count_by_result_field(results, field_name="objective_preset")
    validation_metric_name_counts = count_metric_names(results, field_name="all_validation_metrics")
    selected_validation_metric_panel = build_validation_metric_panel(
        results,
        mode=validation_panel_mode,
        limit=validation_panel_limit,
    )
    variant_aggregates = variant_aggregate_payload(results)
    promotion_recommendation = build_promotion_recommendation(
        search_stage=search_stage,
        results=results,
    )
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "benchmark_dir": benchmark_dir,
        "flight_name": flight_name,
        "mode": mode,
        "variant_set": variant_set,
        "repeat": int(repeat),
        "search_stage": search_stage,
        "search_strategy": search_strategy,
        "search_budget": search_budget,
        "search_seed": search_seed,
        "replay_source_run_dir": replay_source_run_dir,
        "replay_target_stage": replay_target_stage,
        "evaluation_tier": evaluation_tier,
        "objective_name": objective_name,
        "objective_preset": objective_preset,
        "objective_spec_path": objective_spec_path,
        "objective_overrides": objective_overrides,
        "validation_panel_mode": validation_panel_mode,
        "validation_panel_limit": int(validation_panel_limit),
        "status": ("success" if not failed_results else "partial_failure" if successful_results else "failed"),
        "successful_result_count": len(successful_results),
        "failed_result_count": len(failed_results),
        "replay_drift_status_counts": replay_drift_counts,
        "objective_name_counts": objective_name_counts,
        "objective_preset_counts": objective_preset_counts,
        "validation_metric_name_counts": validation_metric_name_counts,
        "selected_validation_metric_panel": selected_validation_metric_panel,
        "variant_aggregates": variant_aggregates,
        "promotion_recommendation": promotion_recommendation,
        "spark_profile": spark_profile,
        "extra_env": extra_env,
        "results": [asdict(result) for result in results],
        "fastest_result": (asdict(fastest_result) if fastest_result is not None else None),
    }


def build_markdown_summary(
    *,
    flight_name: str,
    mode: str,
    variant_set: str,
    repeat: int,
    replay_source_run_dir: str | None,
    replay_target_stage: str | None,
    evaluation_tier: str | None,
    objective_name: str | None,
    objective_preset: str | None,
    objective_spec_path: str | None,
    objective_override_count: int,
    validation_panel_mode: str,
    validation_panel_limit: int,
    results: list[BenchmarkResult],
    search_stage: str | None = None,
    search_strategy: str | None = None,
    search_budget: int | None = None,
) -> str:
    replay_drift_counts = replay_drift_status_counts(results)
    objective_name_counts = count_by_result_field(results, field_name="objective_name")
    objective_preset_counts = count_by_result_field(results, field_name="objective_preset")
    validation_metric_name_counts = count_metric_names(results, field_name="all_validation_metrics")
    selected_validation_metric_panel = build_validation_metric_panel(
        results,
        mode=validation_panel_mode,
        limit=validation_panel_limit,
    )
    variant_aggregates = variant_aggregate_payload(results)
    promotion_recommendation = build_promotion_recommendation(
        search_stage=search_stage,
        results=results,
    )
    lines = [
        "# Pipeline Performance Profile",
        "",
        f"- flight: `{flight_name}`",
        f"- mode: `{mode}`",
        f"- variant set: `{variant_set}`",
        f"- repeats: `{repeat}`",
        f"- search stage: `{search_stage or 'none'}`",
        f"- search strategy: `{search_strategy or 'none'}`",
        f"- search budget: `{search_budget if search_budget is not None else 'none'}`",
        f"- replay source run dir: `{replay_source_run_dir or 'none'}`",
        f"- replay target stage: `{replay_target_stage or 'none'}`",
        f"- evaluation tier: `{evaluation_tier or 'none'}`",
        f"- objective name: `{objective_name or 'none'}`",
        f"- objective preset: `{objective_preset or 'none'}`",
        f"- objective spec path: `{objective_spec_path or 'none'}`",
        f"- objective override count: `{objective_override_count}`",
        f"- validation panel mode: `{validation_panel_mode}`",
        f"- validation panel limit: `{validation_panel_limit}`",
        f"- replay drift counts: `{json.dumps(replay_drift_counts, sort_keys=True)}`",
        f"- objective name counts: `{json.dumps(objective_name_counts, sort_keys=True)}`",
        f"- objective preset counts: `{json.dumps(objective_preset_counts, sort_keys=True)}`",
        f"- validation metric name counts: `{json.dumps(validation_metric_name_counts, sort_keys=True)}`",
        "",
        "| variant | repeat | status | replay start | replay drift | total_s | events_s | windows_s | graph_s | phase_s | run_dir |",
        "| --- | ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for result in results:
        total_seconds = (float(result.elapsed_ms) / 1000.0) if result.elapsed_ms is not None else None
        stage_seconds = {
            stage_script: (result.stage_elapsed_ms.get(stage_script, 0.0) / 1000.0)
            for stage_script in TIMED_STAGE_SCRIPTS
        }
        replay_start_summary = "n/a"
        if result.planned_replay_start_stage or result.replay_start_stage:
            planned = str(result.planned_replay_start_stage or "n/a")
            actual = str(result.replay_start_stage or "n/a")
            replay_start_summary = f"{planned}->{actual}"
        lines.append(
            "| "
            + " | ".join(
                (
                    result.name,
                    str(result.repeat_index),
                    result.status,
                    replay_start_summary,
                    str(result.replay_drift_status or "n/a"),
                    (f"{total_seconds:.1f}" if total_seconds is not None else "n/a"),
                    f"{stage_seconds['20_events_extract.py']:.1f}" if stage_seconds["20_events_extract.py"] else "n/a",
                    f"{stage_seconds['30_windows_adaptive.py']:.1f}" if stage_seconds["30_windows_adaptive.py"] else "n/a",
                    f"{stage_seconds['50_build_graph.py']:.1f}" if stage_seconds["50_build_graph.py"] else "n/a",
                    f"{stage_seconds['70_phase_fit.py']:.1f}" if stage_seconds["70_phase_fit.py"] else "n/a",
                    (result.run_dir or "n/a"),
                )
            )
            + " |"
        )
    if promotion_recommendation:
        lines.extend(
            [
                "",
                "## Promotion Recommendation",
                "",
                "```json",
                json.dumps(promotion_recommendation, indent=2, sort_keys=True),
                "```",
            ]
        )
    if selected_validation_metric_panel:
        lines.extend(
            [
                "",
                "## Validation Panel",
                "",
                "| metric | count | best | median |",
                "| --- | ---: | ---: | ---: |",
            ]
        )
        for row in selected_validation_metric_panel:
            lines.append(
                "| "
                + " | ".join(
                    (
                        str(row.get("metric_name") or "n/a"),
                        str(row.get("count") or 0),
                        f"{float(row.get('best_value') or 0.0):.3f}",
                        f"{float(row.get('median_value') or 0.0):.3f}",
                    )
                )
                + " |"
            )
    if variant_aggregates:
        lines.extend(
            [
                "",
                "## Variant Aggregates",
                "",
                "| variant | repeats | success | failure | best_s | event_s best/med | window_s best/med | graph_s best/med | phase_s best/med | validation metrics | replay drift | objective names | objective presets |",
                "| --- | ---: | ---: | ---: | ---: | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for aggregate in variant_aggregates:
            fastest_elapsed_ms = aggregate.get("fastest_elapsed_ms")
            stage_timing_aggregates = dict(aggregate.get("stage_timing_aggregates") or {})
            validation_metric_aggregates = dict(aggregate.get("validation_metric_aggregates") or {})

            def _stage_pair(stage_script: str) -> str:
                stage_payload = dict(stage_timing_aggregates.get(stage_script) or {})
                best_elapsed_ms = stage_payload.get("best_elapsed_ms")
                median_elapsed_ms = stage_payload.get("median_elapsed_ms")
                if best_elapsed_ms is None or median_elapsed_ms is None:
                    return "n/a"
                return f"{float(best_elapsed_ms) / 1000.0:.1f}/{float(median_elapsed_ms) / 1000.0:.1f}"

            validation_metric_summary = "n/a"
            if validation_metric_aggregates:
                validation_metric_summary = json.dumps(validation_metric_aggregates, sort_keys=True)
            lines.append(
                "| "
                + " | ".join(
                    (
                        str(aggregate.get("name") or "n/a"),
                        str(aggregate.get("repeat_count") or 0),
                        str(aggregate.get("success_count") or 0),
                        str(aggregate.get("failure_count") or 0),
                        ("n/a" if fastest_elapsed_ms is None else f"{float(fastest_elapsed_ms) / 1000.0:.1f}"),
                        _stage_pair("20_events_extract.py"),
                        _stage_pair("30_windows_adaptive.py"),
                        _stage_pair("50_build_graph.py"),
                        _stage_pair("70_phase_fit.py"),
                        validation_metric_summary,
                        json.dumps(aggregate.get("replay_drift_status_counts") or {}, sort_keys=True),
                        json.dumps(aggregate.get("objective_name_counts") or {}, sort_keys=True),
                        json.dumps(aggregate.get("objective_preset_counts") or {}, sort_keys=True),
                    )
                )
                + " |"
            )
    return "\n".join(lines) + "\n"
