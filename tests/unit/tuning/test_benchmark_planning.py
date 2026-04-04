from __future__ import annotations

import argparse

from libs.tuning import (
    BenchmarkVariant,
    build_variant_plan_payload,
    infer_replay_target_stage,
    resolve_effective_evaluation_tier,
    resolve_effective_objective_name,
    resolve_replay_end_stage,
)


def _args() -> argparse.Namespace:
    return argparse.Namespace(
        mode="full",
        search_stage=None,
        objective_name=None,
        objective_preset=None,
        objective_spec_path=None,
        objective_overrides=[],
        evaluation_tier=None,
        replay_source_run_dir=None,
        replay_target_stage=None,
        extra_env=[],
        n_steps=None,
        dt_seconds=None,
        sim_seed=None,
        profile_numeric_ratio_threshold=0.8,
        profile_categorical_cardinality_max=200,
        profile_behavior_significant_diff_threshold=0.05,
        profile_behavior_center_band_width=1.0,
        profile_behavior_soft_bound_width=2.5,
        profile_behavior_hard_bound_width=2.0,
        profile_behavior_mixed_unknown_low_score_threshold=0.38,
        profile_behavior_mixed_unknown_ambiguous_score_threshold=0.55,
        profile_behavior_mixed_unknown_ambiguous_margin_threshold=0.03,
        min_warm=1,
        delta_threshold=0.0,
        slope_source="ema",
        ema_alpha=0.35,
        slope_threshold_mode="fixed",
        slope_threshold_quantile=0.75,
        slope_threshold_scale=0.35,
        slope_threshold_min=1e-6,
        slope_abs_threshold=2.0,
        slope_min_persistence_samples=2,
        slope_reemit_ratio=1.5,
        event_warmup_points=4,
        event_low_scale_responsiveness=1.0,
        event_repeatability_aggressiveness=1.0,
        event_drift_conservatism=1.0,
        event_chatter_suppression=1.0,
        window_max_ms=5000,
        window_event_threshold=10,
        window_min_ms=25,
        window_inactivity_timeout_ms=0,
        window_strategy="segmented",
        phase_count=4,
        backbone_parameter_count=8,
        backbone_ridge_lambda=1.0,
        backbone_event_prior_alpha=0.35,
    )


def test_benchmark_planning_resolves_objective_and_replay_closure():
    args = _args()
    args.objective_preset = "event_recall_heavy"
    variant = BenchmarkVariant(name="baseline", description="baseline", env_overrides={})

    assert resolve_effective_objective_name(args, variant=variant) == "sim_event_recall_heavy_v1"
    assert resolve_effective_evaluation_tier(args, variant=variant) == "event"
    assert resolve_replay_end_stage(args, replay_target_stage="15_event_profiles_fit.py", variant=variant) == "20_events_extract.py"


def test_benchmark_planning_infers_replay_target_and_variant_payload():
    args = _args()
    variant = BenchmarkVariant(
        name="window_tuned",
        description="window tuned",
        env_overrides={"S3NTINEL_WINDOW_SEGMENT_MAX_ROWS": "25000"},
    )

    target_stage = infer_replay_target_stage(args, variant=variant)
    payload = build_variant_plan_payload(args, variant=variant)

    assert target_stage == "30_windows_adaptive.py"
    assert payload["name"] == "window_tuned"
    assert payload["replay_target_stage"] is None
    assert payload["resolved_objective_name"] == "sim_full_default_v1"
    assert payload["resolved_evaluation_tier"] == "full"


def test_benchmark_planning_uses_variant_arg_overrides_for_stage_inference():
    args = _args()
    variant = BenchmarkVariant(
        name="backbone_tuned",
        description="backbone tuned",
        env_overrides={},
        arg_overrides={"backbone_ridge_lambda": 2.0},
    )

    target_stage = infer_replay_target_stage(args, variant=variant)
    payload = build_variant_plan_payload(args, variant=variant)

    assert target_stage == "40_backbone_fit.py"
    assert payload["arg_overrides"] == {"backbone_ridge_lambda": 2.0}


def test_benchmark_planning_defaults_windowing_search_to_windowing_objective():
    args = _args()
    args.mode = "structural"
    args.search_stage = "windowing"

    assert resolve_effective_objective_name(args) == "sim_windowing_default_v1"
    assert resolve_effective_evaluation_tier(args) == "structural"
