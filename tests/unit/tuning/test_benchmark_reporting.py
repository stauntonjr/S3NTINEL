from __future__ import annotations

from libs.tuning import BenchmarkResult, build_markdown_summary, build_summary_payload


def test_build_summary_payload_and_markdown_from_shared_reporting_surface():
    result = BenchmarkResult(
        name="baseline",
        description="baseline",
        repeat_index=1,
        status="success",
        env_overrides={},
        run_dir="/tmp/run",
        manifest_path="/tmp/run/reports/run_manifest.json",
        elapsed_ms=1500.0,
        stage_elapsed_ms={"20_events_extract.py": 100.0},
        return_code=0,
        replay_drift_status="matched",
        evaluation_tier="event",
        objective_name="sim_event_default_v1",
        objective_preset="event_recall_heavy",
        objective_status="ok",
        objective_ready_for_search=True,
        objective_combined_score=0.83,
        selected_validation_metrics={
            "slope_pos run recall": 0.7,
            "slope_neg run recall": 0.65,
            "slope_pos f1": 0.12,
            "slope_neg f1": 0.14,
            "event validation precision": 0.18,
        },
        all_validation_metrics={
            "overall:event_validation:slope_run_capture_metrics.slope_pos.run_recall": 0.7,
            "overall:event_validation:slope_run_capture_metrics.slope_neg.run_recall": 0.65,
            "overall:event_validation:event_family_metrics.slope_pos.f1": 0.12,
            "overall:event_validation:event_family_metrics.slope_neg.f1": 0.14,
            "overall:event_validation:precision": 0.18,
            "overall:event_validation:detected_event_count": 103,
            "overall:event_validation:median_unmatched_label_nearest_delta_seconds": 2.75,
            "overall:event_validation:event_family_metrics.transition.f1": 1.0,
        },
    )

    payload = build_summary_payload(
        benchmark_dir="/tmp/bench",
        flight_name="power_chain",
        mode="event",
        variant_set="quick",
        repeat=1,
        replay_source_run_dir=None,
        replay_target_stage=None,
        evaluation_tier=None,
        objective_name=None,
        objective_preset=None,
        objective_spec_path=None,
        objective_overrides=[],
        validation_panel_mode="objective_selected",
        validation_panel_limit=8,
        spark_profile=None,
        extra_env={},
        results=[result],
        search_stage="event",
    )
    markdown = build_markdown_summary(
        flight_name="power_chain",
        mode="event",
        variant_set="quick",
        repeat=1,
        replay_source_run_dir=None,
        replay_target_stage=None,
        evaluation_tier=None,
        objective_name=None,
        objective_preset=None,
        objective_spec_path=None,
        objective_override_count=0,
        validation_panel_mode="objective_selected",
        validation_panel_limit=8,
        results=[result],
        search_stage="event",
    )

    assert payload["status"] == "success"
    assert payload["objective_name_counts"] == {"sim_event_default_v1": 1}
    assert payload["objective_preset_counts"] == {"event_recall_heavy": 1}
    assert payload["promotion_recommendation"] == {
        "search_stage": "event",
        "promoted_variant_name": "baseline",
        "objective_name": "sim_event_default_v1",
        "objective_status": "ok",
        "objective_ready_for_search": True,
        "objective_combined_score": 0.83,
        "arg_overrides": {},
        "env_overrides": {},
        "run_dir": "/tmp/run",
    }
    assert payload["selected_validation_metric_panel"] == [
        {
            "metric_name": "overall:event_validation:slope_run_capture_metrics.slope_pos.run_recall",
            "count": 1,
            "best_value": 0.7,
            "median_value": 0.7,
        },
        {
            "metric_name": "overall:event_validation:slope_run_capture_metrics.slope_neg.run_recall",
            "count": 1,
            "best_value": 0.65,
            "median_value": 0.65,
        },
        {
            "metric_name": "overall:event_validation:event_family_metrics.slope_pos.f1",
            "count": 1,
            "best_value": 0.12,
            "median_value": 0.12,
        },
        {
            "metric_name": "overall:event_validation:event_family_metrics.slope_neg.f1",
            "count": 1,
            "best_value": 0.14,
            "median_value": 0.14,
        },
        {"metric_name": "overall:event_validation:precision", "count": 1, "best_value": 0.18, "median_value": 0.18},
        {"metric_name": "overall:event_validation:detected_event_count", "count": 1, "best_value": 103.0, "median_value": 103.0},
        {
            "metric_name": "overall:event_validation:median_unmatched_label_nearest_delta_seconds",
            "count": 1,
            "best_value": 2.75,
            "median_value": 2.75,
        },
    ]
    assert "Pipeline Performance Profile" in markdown
    assert 'objective name counts: `{"sim_event_default_v1": 1}`' in markdown
    assert "## Promotion Recommendation" in markdown
    assert "| overall:event_validation:slope_run_capture_metrics.slope_pos.run_recall | 1 | 0.700 | 0.700 |" in markdown
    assert "| overall:event_validation:event_family_metrics.slope_pos.f1 | 1 | 0.120 | 0.120 |" in markdown
