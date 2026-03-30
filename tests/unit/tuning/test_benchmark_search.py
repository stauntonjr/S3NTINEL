from __future__ import annotations

import argparse

from libs.tuning import build_search_variants, resolve_search_spec, validate_benchmark_args


def test_profile_search_spec_declares_profile_mode():
    spec = resolve_search_spec("profile")

    assert spec.mode == "profile"
    assert tuple(dimension.name for dimension in spec.dimensions) == (
        "profile_behavior_significant_diff_threshold",
        "profile_behavior_center_band_width",
        "profile_behavior_soft_bound_width",
        "profile_behavior_hard_bound_width",
        "profile_behavior_mixed_unknown_low_score_threshold",
        "profile_behavior_mixed_unknown_ambiguous_margin_threshold",
    )


def test_event_search_spec_declares_event_mode_and_expected_knobs():
    spec = resolve_search_spec("event")

    assert spec.mode == "event"
    assert tuple(dimension.name for dimension in spec.dimensions) == (
        "slope_threshold_scale",
        "slope_abs_threshold",
        "slope_min_persistence_samples",
        "slope_reemit_ratio",
        "event_warmup_points",
        "event_low_scale_responsiveness",
        "event_repeatability_aggressiveness",
        "event_drift_conservatism",
        "event_chatter_suppression",
    )
    assert spec.dimensions[0].values == (0.3, 0.35, 0.4)
    assert spec.dimensions[1].values == (1.5, 2.0, 2.5)
    assert spec.dimensions[2].values == (2, 3)
    assert spec.dimensions[3].values == (1.5, 1.75)
    assert spec.dimensions[4].values == (3, 4, 5)
    assert spec.dimensions[5].values == (0.9, 1.0, 1.1)
    assert spec.dimensions[6].values == (0.9, 1.0, 1.1)
    assert spec.dimensions[7].values == (0.9, 1.0, 1.1)
    assert spec.dimensions[8].values == (1.0, 1.15, 1.3)


def test_windowing_search_spec_declares_structural_mode_and_expected_knobs():
    spec = resolve_search_spec("windowing")

    assert spec.mode == "structural"
    assert tuple(dimension.name for dimension in spec.dimensions) == (
        "window_max_ms",
        "window_event_threshold",
        "window_min_ms",
        "window_inactivity_timeout_ms",
    )


def test_structure_search_spec_declares_structural_mode_and_mixed_knobs():
    spec = resolve_search_spec("structure")

    assert spec.mode == "structural"
    assert tuple(dimension.name for dimension in spec.dimensions) == (
        "backbone_parameter_count",
        "backbone_ridge_lambda",
        "backbone_event_prior_alpha",
        "S3NTINEL_V2_MIN_ABS_PARTIAL_CORR",
        "S3NTINEL_V2_GRAPH_MIN_FUSED_EDGE_WEIGHT",
        "S3NTINEL_V2_HIERARCHY_TOP_K_PER_SENSOR",
    )
    assert tuple(dimension.kind for dimension in spec.dimensions[-3:]) == ("env", "env", "env")


def test_phase_search_spec_declares_full_mode_and_expected_knobs():
    spec = resolve_search_spec("phase")

    assert spec.mode == "full"
    assert tuple(dimension.name for dimension in spec.dimensions) == (
        "phase_count",
        "S3NTINEL_PHASE_DETECT_SENSOR_COUNT",
        "S3NTINEL_PHASE_DETECT_EVENT_TYPE_COUNT",
        "S3NTINEL_PHASE_DETECT_CATEGORICAL_STATE_COUNT",
        "S3NTINEL_PHASE_STABLE_DRIFT_QUANTILE",
        "S3NTINEL_PHASE_SMOOTHING_RADIUS",
        "S3NTINEL_PHASE_TRANSITION_PENALTY",
        "S3NTINEL_PHASE_MIN_DWELL_WINDOWS",
    )
    assert spec.dimensions[0].kind == "arg"
    assert all(dimension.kind == "env" for dimension in spec.dimensions[1:])


def test_anomaly_search_spec_declares_full_mode_and_expected_knobs():
    spec = resolve_search_spec("anomaly")

    assert spec.mode == "full"
    assert tuple(dimension.name for dimension in spec.dimensions) == (
        "min_warm",
        "S3NTINEL_MAX_BRIDGE_REFERENCE_ROWS",
        "S3NTINEL_SUBSYSTEM_TOP_SENSORS_K",
    )
    assert spec.dimensions[0].kind == "arg"
    assert tuple(dimension.kind for dimension in spec.dimensions[1:]) == ("env", "env")


def test_build_search_variants_grid_includes_baseline_and_cartesian_product():
    variants = build_search_variants(
        search_stage="profile",
        search_strategy="grid",
        search_budget=2,
        search_seed=0,
    )

    assert variants[0].name == "baseline"
    assert variants[1].arg_overrides == {
        "profile_behavior_significant_diff_threshold": 0.03,
        "profile_behavior_center_band_width": 0.8,
        "profile_behavior_soft_bound_width": 2.0,
        "profile_behavior_hard_bound_width": 1.6,
        "profile_behavior_mixed_unknown_low_score_threshold": 0.3,
        "profile_behavior_mixed_unknown_ambiguous_margin_threshold": 0.02,
    }
    assert len(variants) == 3


def test_build_search_variants_supports_env_backed_structure_dimensions():
    variants = build_search_variants(
        search_stage="structure",
        search_strategy="grid",
        search_budget=1,
        search_seed=0,
    )

    assert variants[1].arg_overrides == {
        "backbone_parameter_count": 6,
        "backbone_ridge_lambda": 0.5,
        "backbone_event_prior_alpha": 0.2,
    }
    assert variants[1].env_overrides == {
        "S3NTINEL_V2_MIN_ABS_PARTIAL_CORR": "0.03",
        "S3NTINEL_V2_GRAPH_MIN_FUSED_EDGE_WEIGHT": "0.03",
        "S3NTINEL_V2_HIERARCHY_TOP_K_PER_SENSOR": "2",
    }


def test_build_search_variants_supports_mixed_phase_dimensions():
    variants = build_search_variants(
        search_stage="phase",
        search_strategy="grid",
        search_budget=1,
        search_seed=0,
    )

    assert variants[1].arg_overrides == {"phase_count": 3}
    assert variants[1].env_overrides == {
        "S3NTINEL_PHASE_DETECT_SENSOR_COUNT": "6",
        "S3NTINEL_PHASE_DETECT_EVENT_TYPE_COUNT": "4",
        "S3NTINEL_PHASE_DETECT_CATEGORICAL_STATE_COUNT": "4",
        "S3NTINEL_PHASE_STABLE_DRIFT_QUANTILE": "0.25",
        "S3NTINEL_PHASE_SMOOTHING_RADIUS": "1",
        "S3NTINEL_PHASE_TRANSITION_PENALTY": "1.0",
        "S3NTINEL_PHASE_MIN_DWELL_WINDOWS": "4",
    }


def test_build_search_variants_supports_anomaly_dimensions():
    variants = build_search_variants(
        search_stage="anomaly",
        search_strategy="grid",
        search_budget=1,
        search_seed=0,
    )

    assert variants[1].arg_overrides == {"min_warm": 1}
    assert variants[1].env_overrides == {
        "S3NTINEL_MAX_BRIDGE_REFERENCE_ROWS": "5000",
        "S3NTINEL_SUBSYSTEM_TOP_SENSORS_K": "3",
    }


def test_validate_benchmark_args_rejects_mismatched_search_mode():
    args = argparse.Namespace(
        search_stage="profile",
        search_strategy="grid",
        search_budget=None,
        search_seed=0,
        variants=[],
        variant_set="quick",
        mode="full",
        replay_source_run_dir=None,
        replay_target_stage=None,
        evaluation_tier=None,
        objective_name=None,
        objective_preset=None,
        objective_spec_path=None,
        objective_overrides=[],
    )

    try:
        validate_benchmark_args(args)
    except SystemExit as exc:
        assert str(exc) == "--search-stage 'profile' requires --mode 'profile'"
    else:
        raise AssertionError("expected search-stage mode validation to fail")


def test_validate_benchmark_args_accepts_event_search_in_event_mode():
    args = argparse.Namespace(
        search_stage="event",
        search_strategy="random",
        search_budget=8,
        search_seed=7,
        variants=[],
        variant_set="quick",
        mode="event",
        replay_source_run_dir=None,
        replay_target_stage=None,
        evaluation_tier=None,
        objective_name=None,
        objective_preset=None,
        objective_spec_path=None,
        objective_overrides=[],
    )

    validate_benchmark_args(args)


def test_validate_benchmark_args_accepts_windowing_search_in_structural_mode():
    args = argparse.Namespace(
        search_stage="windowing",
        search_strategy="grid",
        search_budget=12,
        search_seed=0,
        variants=[],
        variant_set="quick",
        mode="structural",
        replay_source_run_dir=None,
        replay_target_stage=None,
        evaluation_tier=None,
        objective_name=None,
        objective_preset=None,
        objective_spec_path=None,
        objective_overrides=[],
    )

    validate_benchmark_args(args)


def test_validate_benchmark_args_accepts_structure_search_in_structural_mode():
    args = argparse.Namespace(
        search_stage="structure",
        search_strategy="random",
        search_budget=12,
        search_seed=3,
        variants=[],
        variant_set="quick",
        mode="structural",
        replay_source_run_dir=None,
        replay_target_stage=None,
        evaluation_tier=None,
        objective_name=None,
        objective_preset=None,
        objective_spec_path=None,
        objective_overrides=[],
    )

    validate_benchmark_args(args)


def test_validate_benchmark_args_accepts_phase_search_in_full_mode():
    args = argparse.Namespace(
        search_stage="phase",
        search_strategy="random",
        search_budget=12,
        search_seed=5,
        variants=[],
        variant_set="quick",
        mode="full",
        replay_source_run_dir=None,
        replay_target_stage=None,
        evaluation_tier=None,
        objective_name=None,
        objective_preset=None,
        objective_spec_path=None,
        objective_overrides=[],
    )

    validate_benchmark_args(args)


def test_validate_benchmark_args_accepts_anomaly_search_in_full_mode():
    args = argparse.Namespace(
        search_stage="anomaly",
        search_strategy="grid",
        search_budget=12,
        search_seed=0,
        variants=[],
        variant_set="quick",
        mode="full",
        replay_source_run_dir=None,
        replay_target_stage=None,
        evaluation_tier=None,
        objective_name=None,
        objective_preset=None,
        objective_spec_path=None,
        objective_overrides=[],
    )

    validate_benchmark_args(args)
