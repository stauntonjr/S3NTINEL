"""Benchmark objective-resolution and replay-planning policy."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from libs.simulation.replay_report import build_simulation_replay_report, recommend_resume_plan
from libs.tuning.benchmark_variants import BenchmarkVariant
from libs.tuning.objectives import (
    load_objective_spec,
    objective_spec_from_payload,
    resolve_default_objective_name,
    resolve_objective_spec,
)
from libs.tuning.presets import OBJECTIVE_PRESET_BY_NAME


EVALUATION_END_STAGE_BY_TIER = {
    "profile": "15_event_profiles_fit.py",
    "event": "20_events_extract.py",
    "structural": "60_fit_hierarchy.py",
    "phase": "70_phase_fit.py",
    "scoring": "85_window_scores_calibrate.py",
    "anomaly": "90_anomaly_attribution.py",
    "full": "95_emit_explorer_bundle.py",
}
DEFAULT_EVALUATION_TIER_BY_MODE = {
    "profile": "profile",
    "event": "event",
    "structural": "structural",
    "full": "full",
}
STAGE_ORDER = (
    "00_ingest_raw.py",
    "10_parameter_profiles_fit.py",
    "12_behavior_profiles_fit.py",
    "15_event_profiles_fit.py",
    "20_events_extract.py",
    "25_window_policy_profile.py",
    "30_windows_adaptive.py",
    "40_backbone_fit.py",
    "50_build_graph.py",
    "60_fit_hierarchy.py",
    "70_phase_fit.py",
    "80_window_scores_raw.py",
    "85_window_scores_calibrate.py",
    "90_anomaly_attribution.py",
    "95_emit_explorer_bundle.py",
)
ARG_DEFAULTS = {
    "profile_numeric_ratio_threshold": 0.8,
    "profile_categorical_cardinality_max": 200,
    "profile_behavior_significant_diff_threshold": 0.05,
    "profile_behavior_center_band_width": 1.0,
    "profile_behavior_soft_bound_width": 2.5,
    "profile_behavior_hard_bound_width": 2.0,
    "profile_behavior_mixed_unknown_low_score_threshold": 0.38,
    "profile_behavior_mixed_unknown_ambiguous_score_threshold": 0.55,
    "profile_behavior_mixed_unknown_ambiguous_margin_threshold": 0.03,
    "min_warm": 1,
    "delta_threshold": 0.0,
    "slope_source": "ema",
    "ema_alpha": 0.35,
    "slope_threshold_mode": "fixed",
    "slope_threshold_quantile": 0.75,
    "slope_threshold_scale": 0.35,
    "slope_threshold_min": 1e-6,
    "slope_abs_threshold": 2.0,
    "slope_min_persistence_samples": 2,
    "slope_reemit_ratio": 1.5,
    "event_warmup_points": 4,
    "event_low_scale_responsiveness": 1.0,
    "event_repeatability_aggressiveness": 1.0,
    "event_drift_conservatism": 1.0,
    "event_chatter_suppression": 1.0,
    "window_max_ms": 10000,
    "window_event_threshold": 20,
    "window_min_ms": 50,
    "window_inactivity_timeout_ms": 0,
    "window_strategy": "segmented",
    "phase_count": 3,
    "backbone_parameter_count": 8,
    "backbone_ridge_lambda": 1.0,
    "backbone_event_prior_alpha": 0.35,
}
REPLAY_UNSAFE_SOURCE_ARG_NAMES = ("n_steps", "dt_seconds", "sim_seed")
EVENT_EXTRACT_ARG_NAMES = ("delta_threshold",)
EVENT_PROFILE_ARG_NAMES = (
    "slope_source",
    "ema_alpha",
    "slope_threshold_mode",
    "slope_threshold_quantile",
    "slope_threshold_scale",
    "slope_threshold_min",
    "slope_abs_threshold",
    "slope_min_persistence_samples",
    "slope_reemit_ratio",
    "event_warmup_points",
    "event_low_scale_responsiveness",
    "event_repeatability_aggressiveness",
    "event_drift_conservatism",
    "event_chatter_suppression",
)
WINDOW_ARG_NAMES = (
    "window_max_ms",
    "window_event_threshold",
    "window_min_ms",
    "window_inactivity_timeout_ms",
    "window_strategy",
)
BACKBONE_ARG_NAMES = (
    "backbone_parameter_count",
    "backbone_ridge_lambda",
    "backbone_event_prior_alpha",
)
ARG_STAGE_BY_NAME = {
    "profile_numeric_ratio_threshold": "10_parameter_profiles_fit.py",
    "profile_categorical_cardinality_max": "10_parameter_profiles_fit.py",
    "profile_behavior_significant_diff_threshold": "12_behavior_profiles_fit.py",
    "profile_behavior_center_band_width": "12_behavior_profiles_fit.py",
    "profile_behavior_soft_bound_width": "12_behavior_profiles_fit.py",
    "profile_behavior_hard_bound_width": "12_behavior_profiles_fit.py",
    "profile_behavior_mixed_unknown_low_score_threshold": "12_behavior_profiles_fit.py",
    "profile_behavior_mixed_unknown_ambiguous_score_threshold": "12_behavior_profiles_fit.py",
    "profile_behavior_mixed_unknown_ambiguous_margin_threshold": "12_behavior_profiles_fit.py",
    **{name: "15_event_profiles_fit.py" for name in EVENT_PROFILE_ARG_NAMES},
    **{name: "20_events_extract.py" for name in EVENT_EXTRACT_ARG_NAMES},
    **{name: "30_windows_adaptive.py" for name in WINDOW_ARG_NAMES},
    **{name: "40_backbone_fit.py" for name in BACKBONE_ARG_NAMES},
    "phase_count": "70_phase_fit.py",
    "min_warm": "85_window_scores_calibrate.py",
}
ENV_STAGE_BY_PREFIX = {
    "S3NTINEL_EVENT_SEGMENT_": "20_events_extract.py",
    "S3NTINEL_WINDOW_SEGMENT_": "30_windows_adaptive.py",
    "S3NTINEL_PHASE_SEGMENT_": "70_phase_fit.py",
    "S3NTINEL_V2_GRAPH_": "60_fit_hierarchy.py",
    "S3NTINEL_EVENT_": "20_events_extract.py",
    "S3NTINEL_WINDOW_": "30_windows_adaptive.py",
    "S3NTINEL_BACKBONE_": "40_backbone_fit.py",
    "S3NTINEL_PHASE_": "70_phase_fit.py",
    "S3NTINEL_MIN_WARM": "85_window_scores_calibrate.py",
}


def stage_order_index(stage_script: str) -> int:
    try:
        return STAGE_ORDER.index(stage_script)
    except ValueError as exc:
        raise RuntimeError(f"unknown stage script {stage_script!r}") from exc


def apply_objective_override(payload: dict[str, Any], *, path: str, value: Any) -> None:
    segments = [segment for segment in str(path).split(".") if segment]
    if not segments:
        raise RuntimeError("objective override path must not be empty")
    current: Any = payload
    for segment in segments[:-1]:
        if isinstance(current, dict):
            if segment not in current:
                raise RuntimeError(f"objective override path {path!r} is missing segment {segment!r}")
            current = current[segment]
            continue
        if isinstance(current, list):
            index = int(segment)
            if index < 0 or index >= len(current):
                raise RuntimeError(f"objective override path {path!r} index {index} is out of range")
            current = current[index]
            continue
        raise RuntimeError(f"objective override path {path!r} cannot traverse {type(current).__name__}")
    leaf = segments[-1]
    if isinstance(current, list):
        index = int(leaf)
        if index < 0 or index >= len(current):
            raise RuntimeError(f"objective override path {path!r} index {index} is out of range")
        current[index] = value
        return
    if isinstance(current, dict):
        if leaf not in current:
            raise RuntimeError(f"objective override path {path!r} is missing leaf {leaf!r}")
        current[leaf] = value
        return
    raise RuntimeError(f"objective override path {path!r} cannot write through {type(current).__name__}")


def variant_objective_name(args: Any, *, variant: BenchmarkVariant | None = None) -> str:
    if variant is not None and variant.objective_preset is not None and variant.objective_preset.objective_name is not None:
        return str(variant.objective_preset.objective_name)
    if args.objective_preset is not None:
        preset = OBJECTIVE_PRESET_BY_NAME[str(args.objective_preset)]
        if preset.objective_name is not None:
            return str(preset.objective_name)
    return str(args.objective_name or resolve_default_objective_name(mode=str(args.mode)))


def variant_objective_spec_path(args: Any, *, variant: BenchmarkVariant | None = None) -> str | None:
    if variant is not None and variant.objective_preset is not None and variant.objective_preset.objective_spec_path is not None:
        return str(variant.objective_preset.objective_spec_path)
    if args.objective_preset is not None:
        preset = OBJECTIVE_PRESET_BY_NAME[str(args.objective_preset)]
        if preset.objective_spec_path is not None:
            return str(preset.objective_spec_path)
    if args.objective_spec_path is not None:
        return str(args.objective_spec_path)
    return None


def base_objective_payload(args: Any, *, variant: BenchmarkVariant | None = None) -> dict[str, Any]:
    objective_spec_path = variant_objective_spec_path(args, variant=variant)
    if objective_spec_path is not None:
        return load_objective_spec(objective_spec_path).to_payload()
    objective_name = variant_objective_name(args, variant=variant)
    return resolve_objective_spec(objective_name=objective_name).to_payload()


def merged_objective_overrides(*, args: Any, variant: BenchmarkVariant | None = None) -> tuple[tuple[str, Any], ...]:
    merged: list[tuple[str, Any]] = list(args.objective_overrides)
    if args.objective_preset is not None:
        merged.extend(list(OBJECTIVE_PRESET_BY_NAME[str(args.objective_preset)].objective_overrides))
    if variant is not None and variant.objective_preset is not None:
        merged.extend(list(variant.objective_preset.objective_overrides))
    if variant is not None:
        merged.extend(list(variant.objective_overrides))
    return tuple(merged)


def resolved_objective_payload(args: Any, *, variant: BenchmarkVariant | None = None) -> dict[str, Any]:
    payload = base_objective_payload(args, variant=variant)
    for path, value in merged_objective_overrides(args=args, variant=variant):
        apply_objective_override(payload, path=path, value=value)
    return payload


def resolve_effective_objective_spec(args: Any, *, variant: BenchmarkVariant | None = None) -> Any:
    if (
        args.objective_preset is not None
        or args.objective_overrides
        or (variant is not None and (variant.objective_preset is not None or variant.objective_overrides))
        or args.objective_spec_path is not None
        or args.objective_name is not None
    ):
        if (
            args.objective_preset is not None
            or args.objective_overrides
            or (variant is not None and (variant.objective_preset is not None or variant.objective_overrides))
        ):
            return objective_spec_from_payload(resolved_objective_payload(args, variant=variant))
        return objective_spec_from_payload(base_objective_payload(args, variant=variant))
    objective_name = str(resolve_default_objective_name(mode=str(args.mode)))
    return resolve_objective_spec(objective_name=objective_name)


def resolve_effective_objective_name(args: Any, *, variant: BenchmarkVariant | None = None) -> str:
    return str(resolve_effective_objective_spec(args, variant=variant).name)


def resolve_effective_evaluation_tier(args: Any, *, variant: BenchmarkVariant | None = None) -> str:
    if args.objective_name is not None or args.objective_preset is not None or args.objective_spec_path is not None or args.objective_overrides or (
        variant is not None and (variant.objective_preset is not None or variant.objective_overrides)
    ):
        objective_spec = resolve_effective_objective_spec(args, variant=variant)
        objective_tier = str(objective_spec.evaluation_tier)
        if args.evaluation_tier is not None and str(args.evaluation_tier) != objective_tier:
            raise RuntimeError(
                f"objective {objective_spec.name!r} requires evaluation tier {objective_tier!r}, "
                f"but received {args.evaluation_tier!r}"
            )
        return objective_tier
    return str(args.evaluation_tier or DEFAULT_EVALUATION_TIER_BY_MODE[str(args.mode)])


def resolve_evaluation_end_stage(args: Any, *, variant: BenchmarkVariant | None = None) -> str:
    if args.objective_name is not None or args.objective_preset is not None or args.objective_spec_path is not None or args.objective_overrides or (
        variant is not None and (variant.objective_preset is not None or variant.objective_overrides)
    ):
        return str(resolve_effective_objective_spec(args, variant=variant).required_end_stage_script)
    tier = resolve_effective_evaluation_tier(args, variant=variant)
    return EVALUATION_END_STAGE_BY_TIER[tier]


def infer_replay_target_stage(args: Any, *, variant: BenchmarkVariant) -> str | None:
    impacted_stages: list[str] = []
    if any(getattr(args, arg_name, None) is not None for arg_name in REPLAY_UNSAFE_SOURCE_ARG_NAMES):
        return None
    for arg_name, stage_script in ARG_STAGE_BY_NAME.items():
        if not hasattr(args, arg_name):
            continue
        value = (
            variant.arg_overrides[arg_name]
            if variant.arg_overrides is not None and arg_name in variant.arg_overrides
            else getattr(args, arg_name)
        )
        default_value = ARG_DEFAULTS.get(arg_name)
        if value != default_value:
            impacted_stages.append(stage_script)
    env_overrides = {str(key): str(value) for key, value in [*args.extra_env, *variant.env_overrides.items()]}
    for env_key in env_overrides:
        for prefix, stage_script in ENV_STAGE_BY_PREFIX.items():
            if env_key.startswith(prefix):
                impacted_stages.append(stage_script)
                break
    if not impacted_stages:
        return None
    return max(impacted_stages, key=stage_order_index)


def resolve_replay_end_stage(
    args: Any,
    *,
    replay_target_stage: str,
    variant: BenchmarkVariant | None = None,
) -> str:
    evaluation_end_stage = resolve_evaluation_end_stage(args, variant=variant)
    return max((replay_target_stage, evaluation_end_stage), key=stage_order_index)


def build_variant_replay_plan_payload(
    args: Any,
    *,
    variant: BenchmarkVariant,
    replay_target_stage: str | None,
    replay_end_stage: str | None,
) -> dict[str, Any]:
    if args.replay_source_run_dir is None or not replay_target_stage:
        return {
            "recommended_start_stage": None,
            "recommended_stage_count": None,
            "recommended_resume_command": None,
        }
    replay_source_run_dir = Path(str(args.replay_source_run_dir)).resolve()
    if not replay_source_run_dir.exists():
        return {
            "recommended_start_stage": None,
            "recommended_stage_count": None,
            "recommended_resume_command": None,
        }
    try:
        replay_report = build_simulation_replay_report(replay_source_run_dir)
        resume_plan = recommend_resume_plan(replay_report, target_stage_script=replay_target_stage)
    except (FileNotFoundError, RuntimeError):
        resume_plan = None
    if resume_plan is None:
        return {
            "recommended_start_stage": None,
            "recommended_stage_count": None,
            "recommended_resume_command": None,
        }
    recommended_resume_command = str(resume_plan.resume_command)
    if replay_end_stage and replay_end_stage != replay_target_stage:
        recommended_resume_command = f"{recommended_resume_command} --end-stage {replay_end_stage}"
    return {
        "recommended_start_stage": str(resume_plan.selected_start_stage_script),
        "recommended_stage_count": int(resume_plan.selected_stage_count),
        "recommended_resume_command": recommended_resume_command,
    }


def build_variant_plan_payload(args: Any, *, variant: BenchmarkVariant) -> dict[str, Any]:
    resolved_objective_name = resolve_effective_objective_name(args, variant=variant)
    resolved_evaluation_tier = resolve_effective_evaluation_tier(args, variant=variant)
    replay_target_stage = None
    replay_end_stage = None
    if args.replay_source_run_dir is not None:
        replay_target_stage = str(args.replay_target_stage or infer_replay_target_stage(args, variant=variant) or "")
        replay_end_stage = None if not replay_target_stage else resolve_replay_end_stage(
            args,
            replay_target_stage=replay_target_stage,
            variant=variant,
        )
    replay_plan = build_variant_replay_plan_payload(
        args,
        variant=variant,
        replay_target_stage=replay_target_stage,
        replay_end_stage=replay_end_stage,
    )
    return {
        "name": variant.name,
        "description": variant.description,
        "env_overrides": dict(variant.env_overrides),
        "arg_overrides": dict(variant.arg_overrides or {}),
        "objective_preset": (
            None
            if variant.objective_preset is None
            else {
                "name": variant.objective_preset.name,
                "description": variant.objective_preset.description,
                "objective_name": variant.objective_preset.objective_name,
                "objective_spec_path": variant.objective_preset.objective_spec_path,
                "objective_overrides": [
                    {"path": path, "value": value}
                    for path, value in variant.objective_preset.objective_overrides
                ],
            }
        ),
        "objective_overrides": [{"path": path, "value": value} for path, value in variant.objective_overrides],
        "resolved_objective_name": resolved_objective_name,
        "resolved_evaluation_tier": resolved_evaluation_tier,
        "resolved_end_stage": replay_end_stage,
        "replay_target_stage": replay_target_stage,
        "recommended_start_stage": replay_plan["recommended_start_stage"],
        "recommended_stage_count": replay_plan["recommended_stage_count"],
        "recommended_resume_command": replay_plan["recommended_resume_command"],
    }
