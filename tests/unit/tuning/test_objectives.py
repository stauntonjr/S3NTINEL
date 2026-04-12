from __future__ import annotations

import json
from pathlib import Path

from libs.tuning import (
    DEFAULT_VALIDATION_PANEL_LIMIT,
    DEFAULT_VALIDATION_PANEL_MODE,
    KNOWN_VALIDATION_PANEL_MODES,
    SAFE_SMALL_SEGMENT_ENV,
    VARIANT_BY_NAME,
    VARIANT_SET_BY_NAME,
    build_validation_metric_panel,
    build_default_objective_spec,
    evaluate_objective_spec,
    OBJECTIVE_PRESET_BY_NAME,
    VALIDATION_PANEL_SHORTLIST,
    load_objective_spec,
    objective_spec_from_payload,
    resolve_objective_required_end_stage,
    resolve_objective_spec,
)


def _full_harness_report() -> dict[str, object]:
    return {
        "status": "success",
        "workload_signature": {
            "source": {
                "flight_name": "power_chain",
                "tail_id": "T1",
                "flight_id": "F1",
            },
            "simulation": {
                "n_steps": 12,
                "dt_seconds": 1.0,
            },
            "pipeline": {
                "mode": "full",
            },
            "stochasticity": {
                "profile_name": "deterministic",
                "profile_version": "v1",
                "seed": 0,
            },
        },
        "validation_metrics": {
            "metric_records": [
                {"category": "validation", "scope_name": "overall", "subscope_name": "profile_validation", "metric_path": "behavior_accuracy", "value": 0.9},
                {"category": "validation", "scope_name": "overall", "subscope_name": "event_validation", "metric_path": "f1", "value": 0.8},
                {"category": "validation", "scope_name": "overall", "subscope_name": "hierarchy_validation", "metric_path": "module_exact_match", "value": 0.85},
                {"category": "validation", "scope_name": "overall", "subscope_name": "phase_validation", "metric_path": "macro_f1", "value": 0.75},
                {
                    "category": "validation",
                    "scope_name": "overall",
                    "subscope_name": "benchmark_scope_validation",
                    "metric_path": "score_validation_by_benchmark_scope.detection.detected_fault_window_rate",
                    "value": 0.7,
                },
                {
                    "category": "validation",
                    "scope_name": "overall",
                    "subscope_name": "benchmark_scope_validation",
                    "metric_path": "score_validation_by_benchmark_scope.detection.emit_ready_fault_window_rate",
                    "value": 0.6,
                },
                {
                    "category": "validation",
                    "scope_name": "overall",
                    "subscope_name": "benchmark_scope_validation",
                    "metric_path": "attribution_validation_by_benchmark_scope.subsystem.dominant_subsystem_match_rate",
                    "value": 0.65,
                },
                {
                    "category": "validation",
                    "scope_name": "overall",
                    "subscope_name": "benchmark_scope_validation",
                    "metric_path": "attribution_validation_by_benchmark_scope.parameter.telemetry_parameter_match_rate",
                    "value": 0.8,
                },
                {
                    "category": "validation",
                    "scope_name": "overall",
                    "subscope_name": "benchmark_scope_validation",
                    "metric_path": "attribution_validation_by_benchmark_scope.parameter.event_parameter_match_rate",
                    "value": 0.55,
                },
            ]
        },
        "compute_performance": {
            "metric_records": [
                {"category": "compute", "scope_name": "overall", "subscope_name": "overall", "metric_path": "pipeline_summary.total_elapsed_ms", "value": 900.0},
                {"category": "compute", "scope_name": "overall", "subscope_name": "overall", "metric_path": "artifact_disk_bytes_total", "value": 4096.0},
            ]
        },
    }


def test_full_default_objective_evaluation_is_ready_for_search():
    harness_report = _full_harness_report()

    spec = build_default_objective_spec(harness_report=harness_report)
    evaluation = evaluate_objective_spec(
        harness_report=harness_report,
        objective_spec=spec,
    )

    assert spec.name == "sim_full_default_v1"
    assert evaluation.overall_status == "ok"
    assert evaluation.comparable is True
    assert evaluation.required_primary_term_coverage_pass is True
    assert evaluation.ready_for_search is True
    assert evaluation.objective_score is not None
    assert evaluation.tie_break_score is not None
    assert evaluation.combined_score is not None


def test_event_default_objective_uses_stage_family_metrics_and_is_ready_for_search():
    harness_report = _full_harness_report()
    harness_report["workload_signature"]["pipeline"]["mode"] = "event"
    harness_report["validation_metrics"]["metric_records"] = [
        {"category": "validation", "scope_name": "overall", "subscope_name": "event_validation", "metric_path": "precision", "value": 0.7},
        {"category": "validation", "scope_name": "overall", "subscope_name": "event_validation", "metric_path": "slope_run_capture_metrics.slope_pos.run_recall", "value": 0.7},
        {"category": "validation", "scope_name": "overall", "subscope_name": "event_validation", "metric_path": "slope_run_capture_metrics.slope_neg.run_recall", "value": 0.65},
        {"category": "validation", "scope_name": "overall", "subscope_name": "event_validation", "metric_path": "event_family_metrics.slope_pos.f1", "value": 0.12},
        {"category": "validation", "scope_name": "overall", "subscope_name": "event_validation", "metric_path": "event_family_metrics.slope_neg.f1", "value": 0.14},
        {"category": "validation", "scope_name": "overall", "subscope_name": "event_validation", "metric_path": "event_family_metrics.transition.f1", "value": 1.0},
    ]
    harness_report["compute_performance"]["metric_records"] = [
        {"category": "compute", "scope_name": "15_event_profiles_fit.py", "subscope_name": "engineering_performance", "metric_path": "elapsed_ms", "value": 400.0},
        {"category": "compute", "scope_name": "20_events_extract.py", "subscope_name": "engineering_performance", "metric_path": "elapsed_ms", "value": 800.0},
        {"category": "compute", "scope_name": "overall", "subscope_name": "overall", "metric_path": "pipeline_summary.total_elapsed_ms", "value": 1500.0},
    ]

    spec = build_default_objective_spec(harness_report=harness_report)
    evaluation = evaluate_objective_spec(
        harness_report=harness_report,
        objective_spec=spec,
    )

    assert spec.name == "sim_event_default_v1"
    assert evaluation.overall_status == "ok"
    assert evaluation.constraint_pass is True
    assert evaluation.ready_for_search is True
    assert evaluation.objective_score is not None


def test_missing_required_full_metric_marks_evaluation_incomplete():
    harness_report = _full_harness_report()
    harness_report["validation_metrics"]["metric_records"] = [
        record
        for record in harness_report["validation_metrics"]["metric_records"]
        if record["metric_path"] != "score_validation_by_benchmark_scope.detection.detected_fault_window_rate"
    ]

    spec = build_default_objective_spec(harness_report=harness_report)
    evaluation = evaluate_objective_spec(
        harness_report=harness_report,
        objective_spec=spec,
    )

    assert evaluation.overall_status == "incomplete"
    assert evaluation.required_primary_term_coverage_pass is False
    assert evaluation.ready_for_search is False
    assert any("detected fault window rate" in note for note in evaluation.notes)


def test_event_default_objective_requires_slope_family_metrics():
    harness_report = _full_harness_report()
    harness_report["workload_signature"]["pipeline"]["mode"] = "event"
    harness_report["validation_metrics"]["metric_records"] = [
        {"category": "validation", "scope_name": "overall", "subscope_name": "event_validation", "metric_path": "precision", "value": 0.18},
        {"category": "validation", "scope_name": "overall", "subscope_name": "event_validation", "metric_path": "slope_run_capture_metrics.slope_pos.run_recall", "value": 0.7},
        {"category": "validation", "scope_name": "overall", "subscope_name": "event_validation", "metric_path": "event_family_metrics.slope_pos.f1", "value": 0.12},
        {"category": "validation", "scope_name": "overall", "subscope_name": "event_validation", "metric_path": "event_family_metrics.slope_neg.f1", "value": 0.14},
    ]

    spec = build_default_objective_spec(harness_report=harness_report)
    evaluation = evaluate_objective_spec(
        harness_report=harness_report,
        objective_spec=spec,
    )

    assert evaluation.overall_status == "incomplete"
    assert evaluation.required_primary_term_coverage_pass is False
    assert evaluation.ready_for_search is False
    assert any("slope_neg run recall" in note for note in evaluation.notes)


def test_default_compare_by_excludes_synthetic_ids_for_comparability():
    harness_report = _full_harness_report()
    spec = build_default_objective_spec(harness_report=harness_report)

    assert "workload_signature.source.tail_id" not in spec.compare_by
    assert "workload_signature.source.flight_id" not in spec.compare_by


def test_default_objective_specs_declare_their_own_execution_scope():
    structural_spec = resolve_objective_spec(objective_name="sim_structural_default_v1")
    event_spec = resolve_objective_spec(objective_name="sim_event_default_v1")
    windowing_spec = resolve_objective_spec(objective_name="sim_windowing_default_v1")

    assert structural_spec.evaluation_tier == "structural"
    assert structural_spec.required_end_stage_script == "60_fit_hierarchy.py"
    assert event_spec.evaluation_tier == "event"
    assert event_spec.required_end_stage_script == "20_events_extract.py"
    assert windowing_spec.evaluation_tier == "structural"
    assert windowing_spec.required_end_stage_script == "60_fit_hierarchy.py"


def test_objective_required_end_stage_is_resolved_from_spec():
    assert resolve_objective_required_end_stage(objective_name="sim_full_default_v1") == "95_emit_explorer_bundle.py"


def test_objective_spec_round_trips_from_payload():
    spec = resolve_objective_spec(objective_name="sim_event_default_v1")
    loaded = objective_spec_from_payload(spec.to_payload())

    assert loaded.name == spec.name
    assert loaded.evaluation_tier == spec.evaluation_tier
    assert loaded.required_end_stage_script == spec.required_end_stage_script
    assert len(loaded.primary_terms) == len(spec.primary_terms)


def test_load_objective_spec_accepts_objective_report_payload(tmp_path: Path):
    spec = resolve_objective_spec(objective_name="sim_structural_default_v1")
    report_path = tmp_path / "objective_evaluation_report.json"
    report_path.write_text(
        json.dumps({"evaluation": {"objective_spec": spec.to_payload()}}),
        encoding="utf-8",
    )

    loaded = load_objective_spec(report_path)

    assert loaded.name == spec.name
    assert loaded.required_end_stage_script == "60_fit_hierarchy.py"


def test_named_objective_presets_are_available_from_tuning_package():
    event_preset = OBJECTIVE_PRESET_BY_NAME["event_recall_heavy"]
    structural_preset = OBJECTIVE_PRESET_BY_NAME["structural_latency_biased"]

    assert event_preset.objective_name == "sim_event_default_v1"
    assert any(path == "primary_terms.0.weight" for path, _ in event_preset.objective_overrides)
    assert any(path == "primary_terms.1.weight" for path, _ in event_preset.objective_overrides)
    assert any(path == "primary_terms.4.weight" for path, _ in event_preset.objective_overrides)
    assert structural_preset.objective_name == "sim_structural_default_v1"


def test_windowing_objective_uses_window_policy_metrics_and_is_ready_for_search():
    harness_report = _full_harness_report()
    harness_report["workload_signature"]["pipeline"]["mode"] = "structural"
    harness_report["validation_metrics"]["metric_records"] = [
        {
            "category": "validation",
            "scope_name": "overall",
            "subscope_name": "window_policy_profile",
            "metric_path": "edge_stability.mean_boundary_jaccard",
            "value": 0.72,
        },
        {
            "category": "validation",
            "scope_name": "overall",
            "subscope_name": "window_policy_profile",
            "metric_path": "selected_balance_penalty",
            "value": 0.18,
        },
        {
            "category": "validation",
            "scope_name": "overall",
            "subscope_name": "window_policy_profile",
            "metric_path": "downstream_cost_proxy.pair_cost_proxy",
            "value": 28.0,
        },
        {
            "category": "validation",
            "scope_name": "overall",
            "subscope_name": "window_policy_profile",
            "metric_path": "downstream_cost_proxy.same_window_pair_expansion_proxy",
            "value": 16.0,
        },
        {
            "category": "validation",
            "scope_name": "overall",
            "subscope_name": "hierarchy_validation",
            "metric_path": "module_exact_match",
            "value": 0.8,
        },
        {
            "category": "validation",
            "scope_name": "overall",
            "subscope_name": "hierarchy_validation",
            "metric_path": "subsystem_exact_match",
            "value": 0.9,
        },
    ]
    harness_report["compute_performance"]["metric_records"] = [
        {
            "category": "compute",
            "scope_name": "25_window_policy_profile.py",
            "subscope_name": "engineering_performance",
            "metric_path": "elapsed_ms",
            "value": 220.0,
        },
        {
            "category": "compute",
            "scope_name": "30_windows_adaptive.py",
            "subscope_name": "engineering_performance",
            "metric_path": "elapsed_ms",
            "value": 280.0,
        },
        {
            "category": "compute",
            "scope_name": "overall",
            "subscope_name": "overall",
            "metric_path": "pipeline_summary.total_elapsed_ms",
            "value": 1200.0,
        },
    ]

    spec = resolve_objective_spec(objective_name="sim_windowing_default_v1")
    evaluation = evaluate_objective_spec(
        harness_report=harness_report,
        objective_spec=spec,
    )

    assert spec.name == "sim_windowing_default_v1"
    assert evaluation.overall_status == "ok"
    assert evaluation.constraint_pass is True
    assert evaluation.ready_for_search is True
    assert evaluation.objective_score is not None


def test_validation_metric_panel_objective_selected_prioritizes_window_metrics():
    results = [
        {
            "objective_name": "sim_windowing_default_v1",
            "all_validation_metrics": {
                "overall:window_policy_profile:edge_stability.mean_boundary_jaccard": 0.7,
                "overall:window_policy_profile:selected_balance_penalty": 0.18,
                "overall:window_policy_profile:downstream_cost_proxy.pair_cost_proxy": 30.0,
                "overall:window_policy_profile:downstream_cost_proxy.same_window_pair_expansion_proxy": 18.0,
                "overall:window_policy_profile:closure_mix.event_threshold_rate": 0.74,
                "overall:hierarchy_validation:module_exact_match": 0.8,
            },
        },
        {
            "objective_name": "sim_windowing_default_v1",
            "all_validation_metrics": {
                "overall:window_policy_profile:edge_stability.mean_boundary_jaccard": 0.8,
                "overall:window_policy_profile:selected_balance_penalty": 0.12,
                "overall:window_policy_profile:downstream_cost_proxy.pair_cost_proxy": 24.0,
                "overall:window_policy_profile:downstream_cost_proxy.same_window_pair_expansion_proxy": 15.0,
                "overall:window_policy_profile:closure_mix.event_threshold_rate": 0.77,
                "overall:hierarchy_validation:module_exact_match": 0.82,
            },
        },
    ]

    panel = build_validation_metric_panel(results, mode="objective_selected", limit=8)

    assert [entry["metric_name"] for entry in panel[:3]] == [
        "overall:window_policy_profile:edge_stability.mean_boundary_jaccard",
        "overall:window_policy_profile:selected_balance_penalty",
        "overall:window_policy_profile:downstream_cost_proxy.pair_cost_proxy",
    ]


def test_validation_panel_shortlist_is_exported_from_tuning_package():
    assert "overall:event_validation:slope_run_capture_metrics.slope_pos.run_recall" in VALIDATION_PANEL_SHORTLIST
    assert "overall:event_validation:slope_run_capture_metrics.slope_neg.run_recall" in VALIDATION_PANEL_SHORTLIST
    assert "overall:event_validation:event_family_metrics.slope_pos.f1" in VALIDATION_PANEL_SHORTLIST
    assert "overall:event_validation:event_family_metrics.slope_neg.f1" in VALIDATION_PANEL_SHORTLIST
    assert "overall:profile_validation:datatype_accuracy" in VALIDATION_PANEL_SHORTLIST
    assert (
        "overall:benchmark_scope_validation:score_validation_by_benchmark_scope.detection.detected_fault_window_rate"
        in VALIDATION_PANEL_SHORTLIST
    )


def test_validation_panel_defaults_are_exported_from_tuning_package():
    assert DEFAULT_VALIDATION_PANEL_MODE == "objective_selected"
    assert DEFAULT_VALIDATION_PANEL_LIMIT == 8
    assert KNOWN_VALIDATION_PANEL_MODES == ("objective_selected", "shortlist", "top_changing")


def test_benchmark_variant_registry_is_exported_from_tuning_package():
    assert SAFE_SMALL_SEGMENT_ENV["S3NTINEL_EVENT_SEGMENT_MAX_ROWS"] == "25000"
    assert tuple(sorted(VARIANT_SET_BY_NAME)) == ("detailed", "full_parameter_sweep", "quick")
    assert VARIANT_BY_NAME["baseline"].description == "Canonical settings with no extra tuning overrides."
    assert VARIANT_BY_NAME["event_small_segments_recall_heavy"].objective_preset is not None
    assert VARIANT_BY_NAME["event_small_segments_recall_heavy"].objective_preset.name == "event_recall_heavy"
    assert VARIANT_BY_NAME["backbone_parameter_count_12"].arg_overrides == {"backbone_parameter_count": 12}


def test_build_validation_metric_panel_supports_shortlist_and_top_changing():
    results = [
        {
            "selected_validation_metrics": {
                "slope_pos f1": 0.1,
                "slope_neg f1": 0.2,
            },
            "all_validation_metrics": {
                "overall:profile_validation:behavior_accuracy": 0.5,
                "overall:event_validation:slope_run_capture_metrics.slope_pos.run_recall": 0.7,
                "overall:event_validation:slope_run_capture_metrics.slope_neg.run_recall": 0.65,
                "overall:event_validation:event_family_metrics.slope_pos.f1": 0.1,
                "overall:event_validation:event_family_metrics.slope_neg.f1": 0.2,
                "m1": 0.1,
                "m2": 0.2,
            },
        },
        {
            "selected_validation_metrics": {
                "slope_pos f1": 0.05,
            },
            "all_validation_metrics": {
                "m1": 0.9,
                "m2": 0.3,
            },
        },
    ]

    shortlist_panel = build_validation_metric_panel(results, mode="shortlist", limit=8)
    top_changing_panel = build_validation_metric_panel(results, mode="top_changing", limit=1)

    assert shortlist_panel == [
        {"metric_name": "overall:profile_validation:behavior_accuracy", "count": 1, "best_value": 0.5, "median_value": 0.5},
        {"metric_name": "overall:event_validation:slope_run_capture_metrics.slope_pos.run_recall", "count": 1, "best_value": 0.7, "median_value": 0.7},
        {"metric_name": "overall:event_validation:slope_run_capture_metrics.slope_neg.run_recall", "count": 1, "best_value": 0.65, "median_value": 0.65},
        {"metric_name": "overall:event_validation:event_family_metrics.slope_pos.f1", "count": 1, "best_value": 0.1, "median_value": 0.1},
        {"metric_name": "overall:event_validation:event_family_metrics.slope_neg.f1", "count": 1, "best_value": 0.2, "median_value": 0.2},
    ]
    assert top_changing_panel == [
        {"metric_name": "m1", "count": 2, "best_value": 0.9, "median_value": 0.5},
    ]
