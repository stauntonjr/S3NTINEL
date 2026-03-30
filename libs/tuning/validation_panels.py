"""Validation metric panel policy for tuning/reporting workflows."""

from __future__ import annotations

import statistics
from typing import Any, Mapping


KNOWN_VALIDATION_PANEL_MODES = (
    "objective_selected",
    "shortlist",
    "top_changing",
)
DEFAULT_VALIDATION_PANEL_MODE = "objective_selected"
DEFAULT_VALIDATION_PANEL_LIMIT = 8
EVENT_STAGE_VALIDATION_PANEL_PRIORITY = (
    "overall:event_validation:slope_run_capture_metrics.slope_pos.run_recall",
    "overall:event_validation:slope_run_capture_metrics.slope_neg.run_recall",
    "overall:event_validation:event_family_metrics.slope_pos.f1",
    "overall:event_validation:event_family_metrics.slope_neg.f1",
    "overall:event_validation:precision",
    "overall:event_validation:detected_event_count",
    "overall:event_validation:median_unmatched_label_nearest_delta_seconds",
)
WINDOW_STAGE_VALIDATION_PANEL_PRIORITY = (
    "overall:window_policy_profile:edge_stability.mean_boundary_jaccard",
    "overall:window_policy_profile:selected_balance_penalty",
    "overall:window_policy_profile:downstream_cost_proxy.pair_cost_proxy",
    "overall:window_policy_profile:downstream_cost_proxy.same_window_pair_expansion_proxy",
    "overall:window_policy_profile:closure_mix.event_threshold_rate",
    "overall:hierarchy_validation:module_exact_match",
    "overall:hierarchy_validation:subsystem_exact_match",
)

VALIDATION_PANEL_SHORTLIST = (
    "overall:profile_validation:datatype_accuracy",
    "overall:profile_validation:behavior_accuracy",
    "overall:window_policy_profile:edge_stability.mean_boundary_jaccard",
    "overall:window_policy_profile:selected_balance_penalty",
    "overall:window_policy_profile:downstream_cost_proxy.pair_cost_proxy",
    "overall:event_validation:slope_run_capture_metrics.slope_pos.run_recall",
    "overall:event_validation:slope_run_capture_metrics.slope_neg.run_recall",
    "overall:event_validation:event_family_metrics.slope_pos.f1",
    "overall:event_validation:event_family_metrics.slope_neg.f1",
    "overall:event_validation:precision",
    "overall:event_validation:detected_event_count",
    "overall:event_validation:median_unmatched_label_nearest_delta_seconds",
    "overall:phase_validation:macro_f1",
    "overall:hierarchy_validation:module_exact_match",
    "overall:score_validation:detected_fault_window_rate",
    "overall:score_validation:emit_ready_fault_window_rate",
    "overall:attribution_validation:telemetry_parameter_match_rate",
)


def _metric_map(result: Any, field_name: str) -> Mapping[str, float | int]:
    if isinstance(result, dict):
        value = result.get(field_name)
    else:
        value = getattr(result, field_name, None)
    if not isinstance(value, Mapping):
        return {}
    return {
        str(metric_name): metric_value
        for metric_name, metric_value in value.items()
        if isinstance(metric_name, str) and isinstance(metric_value, (int, float)) and not isinstance(metric_value, bool)
    }


def _ordered_metric_names(results: list[Any], field_name: str) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for result in results:
        for metric_name in _metric_map(result, field_name).keys():
            normalized = str(metric_name)
            if normalized in seen:
                continue
            seen.add(normalized)
            ordered.append(normalized)
    return ordered


def _looks_like_event_stage_results(results: list[Any]) -> bool:
    for result in results:
        objective_name = getattr(result, "objective_name", None) if not isinstance(result, dict) else result.get("objective_name")
        evaluation_tier = getattr(result, "evaluation_tier", None) if not isinstance(result, dict) else result.get("evaluation_tier")
        if str(objective_name or "") == "sim_event_default_v1" or str(evaluation_tier or "") == "event":
            return True
        all_metrics = _metric_map(result, "all_validation_metrics")
        if any(
            metric_name.startswith("overall:event_validation:event_family_metrics.")
            for metric_name in all_metrics.keys()
        ):
            return True
    return False


def _looks_like_window_stage_results(results: list[Any]) -> bool:
    for result in results:
        objective_name = getattr(result, "objective_name", None) if not isinstance(result, dict) else result.get("objective_name")
        all_metrics = _metric_map(result, "all_validation_metrics")
        if str(objective_name or "") == "sim_windowing_default_v1":
            return True
        if any(metric_name.startswith("overall:window_policy_profile:") for metric_name in all_metrics.keys()):
            return True
    return False


def _select_prioritized_metric_names(
    results: list[Any],
    *,
    metric_names: tuple[str, ...],
) -> list[str]:
    return [
        metric_name
        for metric_name in metric_names
        if any(metric_name in _metric_map(result, "all_validation_metrics") for result in results)
    ]


def build_validation_metric_panel(
    results: list[Any],
    *,
    mode: str,
    limit: int,
) -> list[dict[str, Any]]:
    normalized_mode = str(mode)
    resolved_limit = max(int(limit), 1)
    if normalized_mode == "objective_selected":
        if _looks_like_window_stage_results(results):
            metric_names = _select_prioritized_metric_names(
                results,
                metric_names=WINDOW_STAGE_VALIDATION_PANEL_PRIORITY,
            )
            metric_value_lookup = lambda result, metric_name: _metric_map(result, "all_validation_metrics").get(metric_name)
        elif _looks_like_event_stage_results(results):
            metric_names = _select_prioritized_metric_names(
                results,
                metric_names=EVENT_STAGE_VALIDATION_PANEL_PRIORITY,
            )
            metric_value_lookup = lambda result, metric_name: _metric_map(result, "all_validation_metrics").get(metric_name)
        else:
            metric_names = _ordered_metric_names(results, "selected_validation_metrics")
            metric_value_lookup = lambda result, metric_name: _metric_map(result, "selected_validation_metrics").get(metric_name)
    elif normalized_mode == "shortlist":
        metric_names = [metric_name for metric_name in VALIDATION_PANEL_SHORTLIST]
        metric_value_lookup = lambda result, metric_name: _metric_map(result, "all_validation_metrics").get(metric_name)
    elif normalized_mode == "top_changing":
        candidate_metric_names = sorted(
            {
                metric_name
                for result in results
                for metric_name in _metric_map(result, "all_validation_metrics").keys()
            }
        )
        scored_names: list[tuple[float, str]] = []
        for metric_name in candidate_metric_names:
            values = [
                float(_metric_map(result, "all_validation_metrics")[metric_name])
                for result in results
                if metric_name in _metric_map(result, "all_validation_metrics")
            ]
            if not values:
                continue
            scored_names.append((max(values) - min(values), metric_name))
        scored_names.sort(key=lambda item: (-item[0], item[1]))
        metric_names = [metric_name for _, metric_name in scored_names[:resolved_limit]]
        metric_value_lookup = lambda result, metric_name: _metric_map(result, "all_validation_metrics").get(metric_name)
    else:
        raise RuntimeError(f"unsupported validation panel mode {mode!r}")
    panel: list[dict[str, Any]] = []
    for metric_name in metric_names:
        values = [
            float(metric_value_lookup(result, metric_name))
            for result in results
            if metric_value_lookup(result, metric_name) is not None
        ]
        if not values:
            continue
        panel.append(
            {
                "metric_name": metric_name,
                "count": len(values),
                "best_value": max(values),
                "median_value": float(statistics.median(values)),
            }
        )
    return panel
