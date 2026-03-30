from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts import profile_pipeline_performance as perf


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _args(tmp_path: Path) -> argparse.Namespace:
    return argparse.Namespace(
        flight_name="power_chain",
        tail_id="T1",
        flight_id="F1",
        sim_seed=None,
        base_dir=str(tmp_path / "perf"),
        mode="full",
        format="parquet",
        write_mode="overwrite",
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
        slope_source="raw",
        ema_alpha=0.2,
        slope_threshold_mode="adaptive_run",
        slope_threshold_quantile=0.75,
        slope_threshold_scale=0.5,
        slope_threshold_min=1e-6,
        slope_abs_threshold=1.0,
        slope_min_persistence_samples=2,
        slope_reemit_ratio=1.5,
        event_warmup_points=1,
        window_max_ms=10000,
        window_event_threshold=2,
        window_min_ms=50,
        window_inactivity_timeout_ms=0,
        window_strategy="segmented",
        phase_count=3,
        backbone_parameter_count=4,
        backbone_ridge_lambda=1.0,
        backbone_event_prior_alpha=0.35,
        n_steps=None,
        dt_seconds=None,
        spark_profile=None,
        extra_env=[],
        replay_source_run_dir=None,
        replay_target_stage=None,
        evaluation_tier=None,
        objective_name=None,
        objective_preset=None,
        objective_spec_path=None,
        objective_overrides=[],
        validation_panel_mode="objective_selected",
        validation_panel_limit=8,
        variant_set="quick",
        variants=[],
        search_stage=None,
        search_strategy="grid",
        search_budget=None,
        search_seed=0,
        repeat=1,
        fail_on_variant_error=False,
    )


def test_build_replay_run_command_clones_source_and_appends_stage_range(tmp_path):
    replay_source = tmp_path / "source_run"
    reports_dir = replay_source / "reports"
    stages_dir = reports_dir / "stages"
    delta_dir = replay_source / "delta"
    delta_dir.mkdir(parents=True, exist_ok=True)
    (delta_dir / "windows").write_text("ok", encoding="utf-8")
    _write_json(
        reports_dir / "run_manifest.json",
        {
            "source": {"flight_name": "power_chain", "tail_id": "T1", "flight_id": "F1"},
            "pipeline": {"mode": "full", "table_format": "parquet", "write_mode": "overwrite"},
        },
    )
    _write_json(
        reports_dir / "pipeline_run_summary.json",
        {
            "stages": [
                {"stage_script": "20_events_extract.py"},
                {"stage_script": "30_windows_adaptive.py"},
                {"stage_script": "40_backbone_fit.py"},
            ]
        },
    )
    _write_json(
        stages_dir / "30_windows_adaptive_manifest.json",
        {
            "replayable_from": ["windows"],
            "input_artifacts": {"windows": {"path": str(delta_dir / "windows")}},
        },
    )
    args = _args(tmp_path)

    command, cloned_run_dir, resume_plan, replay_end_stage = perf._build_replay_run_command(
        args,
        run_base_dir=tmp_path / "bench" / "runs" / "baseline" / "repeat_1",
        replay_source_run_dir=replay_source,
        replay_target_stage="40_backbone_fit.py",
    )

    assert cloned_run_dir.exists()
    assert (cloned_run_dir / "reports" / "run_manifest.json").exists()
    assert resume_plan.selected_start_stage_script == "30_windows_adaptive.py"
    assert replay_end_stage == "40_backbone_fit.py"
    assert "--replay-run-dir" in command
    assert str(cloned_run_dir) in command
    assert command[-4:] == [
        "--start-stage",
        "30_windows_adaptive.py",
        "--end-stage",
        "40_backbone_fit.py",
    ]


def test_run_variant_carries_planned_and_actual_replay_start_stage(tmp_path, monkeypatch):
    replay_source = tmp_path / "source_run"
    reports_dir = replay_source / "reports"
    stages_dir = reports_dir / "stages"
    delta_dir = replay_source / "delta"
    delta_dir.mkdir(parents=True, exist_ok=True)
    (delta_dir / "windows").write_text("ok", encoding="utf-8")
    _write_json(
        reports_dir / "run_manifest.json",
        {
            "source": {"flight_name": "power_chain", "tail_id": "T1", "flight_id": "F1"},
            "pipeline": {"mode": "full", "table_format": "parquet", "write_mode": "overwrite"},
            "timing": {"elapsed_ms": 123.0},
            "status": "success",
        },
    )
    _write_json(
        reports_dir / "pipeline_run_summary.json",
        {
            "stages": [
                {"stage_script": "20_events_extract.py", "elapsed_ms": 10.0},
                {"stage_script": "30_windows_adaptive.py", "elapsed_ms": 20.0},
                {"stage_script": "40_backbone_fit.py", "elapsed_ms": 30.0},
            ]
        },
    )
    _write_json(
        stages_dir / "30_windows_adaptive_manifest.json",
        {
            "replayable_from": ["windows"],
            "input_artifacts": {"windows": {"path": str(delta_dir / "windows")}},
        },
    )
    args = _args(tmp_path)
    args.replay_source_run_dir = str(replay_source)
    args.replay_target_stage = "40_backbone_fit.py"
    variant = perf.BenchmarkVariant(
        name="baseline",
        description="baseline",
        env_overrides={},
    )

    class _Completed:
        returncode = 0

    monkeypatch.setattr(perf.subprocess, "run", lambda *_, **__: _Completed())

    result = perf._run_variant(
        args=args,
        benchmark_dir=tmp_path / "bench",
        variant=variant,
        repeat_index=1,
    )

    assert result.planned_replay_start_stage == "30_windows_adaptive.py"
    assert result.planned_replay_stage_count == 2
    assert result.replay_start_stage == "30_windows_adaptive.py"
    assert result.replay_drift_status == "matched"


def test_build_summary_payload_carries_replay_settings(tmp_path):
    args = _args(tmp_path)
    args.replay_source_run_dir = "/tmp/source"
    args.replay_target_stage = "50_build_graph.py"
    args.evaluation_tier = "structural"
    args.objective_name = "sim_structural_default_v1"

    payload = perf._build_summary_payload(
        benchmark_dir=tmp_path / "bench",
        args=args,
        results=[],
    )

    assert payload["replay_source_run_dir"] == "/tmp/source"
    assert payload["replay_target_stage"] == "50_build_graph.py"
    assert payload["evaluation_tier"] == "structural"
    assert payload["objective_name"] == "sim_structural_default_v1"
    assert payload["objective_preset"] is None
    assert payload["objective_spec_path"] is None
    assert payload["objective_overrides"] == []
    assert payload["validation_panel_mode"] == "objective_selected"
    assert payload["validation_panel_limit"] == 8
    assert payload["replay_drift_status_counts"] == {}
    assert payload["objective_name_counts"] == {}
    assert payload["objective_preset_counts"] == {}
    assert payload["validation_metric_name_counts"] == {}


def test_build_markdown_summary_shows_planned_vs_actual_replay_start(tmp_path):
    args = _args(tmp_path)
    args.replay_source_run_dir = "/tmp/source"
    result = perf.BenchmarkResult(
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
        replay_source_run_dir="/tmp/source",
        replay_target_stage="40_backbone_fit.py",
        planned_replay_start_stage="30_windows_adaptive.py",
        planned_replay_stage_count=2,
        replay_start_stage="20_events_extract.py",
        replay_end_stage="40_backbone_fit.py",
        replay_drift_status="drifted",
        evaluation_tier="structural",
        objective_name="sim_structural_default_v1",
        objective_preset="structural_latency_biased",
        objective_spec_path=None,
        objective_overrides=(),
        selected_validation_metrics={
            "phase macro f1": 0.42,
            "profile behavior accuracy": 0.55,
        },
    )

    markdown = perf._build_markdown_summary(args=args, results=[result])

    assert "replay start" in markdown
    assert "replay drift" in markdown
    assert 'replay drift counts: `{"drifted": 1}`' in markdown
    assert 'objective name counts: `{"sim_structural_default_v1": 1}`' in markdown
    assert 'objective preset counts: `{"structural_latency_biased": 1}`' in markdown
    assert 'validation metric name counts: `{}`' in markdown
    assert "validation panel mode: `objective_selected`" in markdown
    assert "validation panel limit: `8`" in markdown
    assert "30_windows_adaptive.py->20_events_extract.py" in markdown
    assert "drifted" in markdown
    assert "Validation Panel" in markdown
    assert "| phase macro f1 | 1 | 0.420 | 0.420 |" in markdown
    assert "Variant Aggregates" in markdown
    assert '"phase macro f1": {"best_value": 0.42, "median_value": 0.42}' in markdown
    assert '"profile behavior accuracy": {"best_value": 0.55, "median_value": 0.55}' in markdown


def test_replay_drift_status_classification():
    assert perf._replay_drift_status(planned_start_stage=None, actual_start_stage=None) is None
    assert perf._replay_drift_status(planned_start_stage="30_windows_adaptive.py", actual_start_stage="30_windows_adaptive.py") == "matched"
    assert perf._replay_drift_status(planned_start_stage="30_windows_adaptive.py", actual_start_stage="20_events_extract.py") == "drifted"
    assert perf._replay_drift_status(planned_start_stage=None, actual_start_stage="20_events_extract.py") == "unplanned"
    assert perf._replay_drift_status(planned_start_stage="30_windows_adaptive.py", actual_start_stage=None) == "missing_actual"


def test_replay_drift_status_counts_roll_up_summary_categories():
    results = [
        perf.BenchmarkResult(
            name="a",
            description="a",
            repeat_index=1,
            status="success",
            env_overrides={},
            run_dir=None,
            manifest_path=None,
            elapsed_ms=None,
            stage_elapsed_ms={},
            return_code=0,
            replay_drift_status="matched",
        ),
        perf.BenchmarkResult(
            name="b",
            description="b",
            repeat_index=1,
            status="success",
            env_overrides={},
            run_dir=None,
            manifest_path=None,
            elapsed_ms=None,
            stage_elapsed_ms={},
            return_code=0,
            replay_drift_status="drifted",
        ),
        perf.BenchmarkResult(
            name="c",
            description="c",
            repeat_index=1,
            status="success",
            env_overrides={},
            run_dir=None,
            manifest_path=None,
            elapsed_ms=None,
            stage_elapsed_ms={},
            return_code=0,
            replay_drift_status="drifted",
        ),
    ]

    assert perf._replay_drift_status_counts(results) == {"matched": 1, "drifted": 2}


def test_count_by_result_field_rolls_up_objective_fields():
    results = [
        perf.BenchmarkResult(
            name="a",
            description="a",
            repeat_index=1,
            status="success",
            env_overrides={},
            run_dir=None,
            manifest_path=None,
            elapsed_ms=None,
            stage_elapsed_ms={},
            return_code=0,
            objective_name="sim_event_default_v1",
            objective_preset="event_recall_heavy",
        ),
        perf.BenchmarkResult(
            name="b",
            description="b",
            repeat_index=1,
            status="success",
            env_overrides={},
            run_dir=None,
            manifest_path=None,
            elapsed_ms=None,
            stage_elapsed_ms={},
            return_code=0,
            objective_name="sim_event_default_v1",
            objective_preset="event_recall_heavy",
        ),
        perf.BenchmarkResult(
            name="c",
            description="c",
            repeat_index=1,
            status="success",
            env_overrides={},
            run_dir=None,
            manifest_path=None,
            elapsed_ms=None,
            stage_elapsed_ms={},
            return_code=0,
            objective_name="sim_structural_default_v1",
            objective_preset=None,
        ),
    ]

    assert perf._count_by_result_field(results, field_name="objective_name") == {
        "sim_event_default_v1": 2,
        "sim_structural_default_v1": 1,
    }
    assert perf._count_by_result_field(results, field_name="objective_preset") == {
        "event_recall_heavy": 2,
    }


def test_count_metric_names_rolls_up_validation_metric_presence():
    results = [
        perf.BenchmarkResult(
            name="a",
            description="a",
            repeat_index=1,
            status="success",
            env_overrides={},
            run_dir=None,
            manifest_path=None,
            elapsed_ms=None,
            stage_elapsed_ms={},
            return_code=0,
            all_validation_metrics={
                "overall:event_validation:event_family_metrics.slope_pos.f1": 0.12,
                "overall:profile_validation:datatype_accuracy": 1.0,
            },
        ),
        perf.BenchmarkResult(
            name="b",
            description="b",
            repeat_index=1,
            status="success",
            env_overrides={},
            run_dir=None,
            manifest_path=None,
            elapsed_ms=None,
            stage_elapsed_ms={},
            return_code=0,
            all_validation_metrics={
                "overall:event_validation:event_family_metrics.slope_pos.f1": 0.15,
            },
        ),
    ]

    assert perf._count_metric_names(results, field_name="all_validation_metrics") == {
        "overall:event_validation:event_family_metrics.slope_pos.f1": 2,
        "overall:profile_validation:datatype_accuracy": 1,
    }


def test_all_validation_metrics_from_harness_flattens_full_metric_surface():
    harness_payload = {
        "validation_metrics": {
            "metric_records": [
                {
                    "category": "validation",
                    "scope_name": "overall",
                    "subscope_name": "profile_validation",
                    "metric_path": "datatype_accuracy",
                    "value": 1.0,
                },
                {
                    "category": "validation",
                    "scope_name": "overall",
                    "subscope_name": "event_validation",
                    "metric_path": "event_family_metrics.slope_pos.f1",
                    "value": 0.12,
                },
            ]
        }
    }

    assert perf._all_validation_metrics_from_harness(harness_payload) == {
        "overall:event_validation:event_family_metrics.slope_pos.f1": 0.12,
        "overall:profile_validation:datatype_accuracy": 1.0,
    }


def test_selected_validation_metrics_for_objective_extracts_objective_terms():
    objective_spec = perf.resolve_objective_spec(objective_name="sim_event_default_v1")
    harness_payload = {
        "validation_metrics": {
            "metric_records": [
                {
                    "category": "validation",
                    "scope_name": "overall",
                    "subscope_name": "profile_validation",
                    "metric_path": "unused_metric",
                    "value": 1.0,
                },
                {
                    "category": "validation",
                    "scope_name": "overall",
                    "subscope_name": "event_validation",
                    "metric_path": "precision",
                    "value": 0.18,
                },
                {
                    "category": "validation",
                    "scope_name": "overall",
                    "subscope_name": "event_validation",
                    "metric_path": "slope_run_capture_metrics.slope_pos.run_recall",
                    "value": 0.7,
                },
                {
                    "category": "validation",
                    "scope_name": "overall",
                    "subscope_name": "event_validation",
                    "metric_path": "slope_run_capture_metrics.slope_neg.run_recall",
                    "value": 0.65,
                },
                {
                    "category": "validation",
                    "scope_name": "overall",
                    "subscope_name": "event_validation",
                    "metric_path": "event_family_metrics.slope_pos.f1",
                    "value": 0.12,
                },
                {
                    "category": "validation",
                    "scope_name": "overall",
                    "subscope_name": "event_validation",
                    "metric_path": "event_family_metrics.slope_neg.f1",
                    "value": 0.14,
                },
                {
                    "category": "validation",
                    "scope_name": "overall",
                    "subscope_name": "event_validation",
                    "metric_path": "event_family_metrics.transition.f1",
                    "value": 1.0,
                },
            ]
        }
    }

    selected = perf._selected_validation_metrics_for_objective(
        objective_spec=objective_spec,
        harness_payload=harness_payload,
    )

    assert selected == {
        "slope_pos run recall": 0.7,
        "slope_neg run recall": 0.65,
        "slope_pos f1": 0.12,
        "slope_neg f1": 0.14,
        "event validation precision": 0.18,
        "transition f1": 1.0,
    }


def test_validation_metric_panel_objective_selected_rolls_up_selected_metrics():
    results = [
        perf.BenchmarkResult(
            name="a",
            description="a",
            repeat_index=1,
            status="success",
            env_overrides={},
            run_dir=None,
            manifest_path=None,
            elapsed_ms=None,
            stage_elapsed_ms={},
            return_code=0,
            selected_validation_metrics={
                "slope_pos f1": 0.12,
                "slope_neg f1": 0.14,
            },
        ),
        perf.BenchmarkResult(
            name="b",
            description="b",
            repeat_index=1,
            status="success",
            env_overrides={},
            run_dir=None,
            manifest_path=None,
            elapsed_ms=None,
            stage_elapsed_ms={},
            return_code=0,
            selected_validation_metrics={
                "slope_pos f1": 0.05,
            },
        ),
    ]

    assert perf._validation_metric_panel(results, mode="objective_selected", limit=8) == [
        {"metric_name": "slope_pos f1", "count": 2, "best_value": 0.12, "median_value": 0.08499999999999999},
        {"metric_name": "slope_neg f1", "count": 1, "best_value": 0.14, "median_value": 0.14},
    ]


def test_validation_metric_panel_shortlist_uses_full_metric_surface():
    results = [
        perf.BenchmarkResult(
            name="a",
            description="a",
            repeat_index=1,
            status="success",
            env_overrides={},
            run_dir=None,
            manifest_path=None,
            elapsed_ms=None,
            stage_elapsed_ms={},
            return_code=0,
            all_validation_metrics={
                "overall:profile_validation:behavior_accuracy": 0.5,
                "overall:event_validation:event_family_metrics.slope_pos.f1": 0.12,
                "overall:event_validation:event_family_metrics.slope_neg.f1": 0.14,
                "overall:phase_validation:macro_f1": 0.4,
            },
        ),
    ]

    panel = perf._validation_metric_panel(results, mode="shortlist", limit=8)

    assert panel == [
        {"metric_name": "overall:profile_validation:behavior_accuracy", "count": 1, "best_value": 0.5, "median_value": 0.5},
        {"metric_name": "overall:event_validation:event_family_metrics.slope_pos.f1", "count": 1, "best_value": 0.12, "median_value": 0.12},
        {"metric_name": "overall:event_validation:event_family_metrics.slope_neg.f1", "count": 1, "best_value": 0.14, "median_value": 0.14},
        {"metric_name": "overall:phase_validation:macro_f1", "count": 1, "best_value": 0.4, "median_value": 0.4},
    ]


def test_validation_metric_panel_top_changing_picks_highest_spread_metrics():
    results = [
        perf.BenchmarkResult(
            name="a",
            description="a",
            repeat_index=1,
            status="success",
            env_overrides={},
            run_dir=None,
            manifest_path=None,
            elapsed_ms=None,
            stage_elapsed_ms={},
            return_code=0,
            all_validation_metrics={
                "m1": 0.1,
                "m2": 0.2,
                "m3": 0.5,
            },
        ),
        perf.BenchmarkResult(
            name="b",
            description="b",
            repeat_index=1,
            status="success",
            env_overrides={},
            run_dir=None,
            manifest_path=None,
            elapsed_ms=None,
            stage_elapsed_ms={},
            return_code=0,
            all_validation_metrics={
                "m1": 0.9,
                "m2": 0.3,
                "m3": 0.6,
            },
        ),
    ]

    panel = perf._validation_metric_panel(results, mode="top_changing", limit=2)

    assert panel == [
        {"metric_name": "m1", "count": 2, "best_value": 0.9, "median_value": 0.5},
        {"metric_name": "m2", "count": 2, "best_value": 0.3, "median_value": 0.25},
    ]


def test_variant_aggregate_payload_rolls_up_repeat_metrics():
    results = [
        perf.BenchmarkResult(
            name="baseline",
            description="baseline",
            repeat_index=1,
            status="success",
            env_overrides={},
            run_dir=None,
            manifest_path=None,
            elapsed_ms=1200.0,
            stage_elapsed_ms={
                "20_events_extract.py": 100.0,
                "30_windows_adaptive.py": 200.0,
                "50_build_graph.py": 300.0,
                "70_phase_fit.py": 400.0,
            },
            return_code=0,
            replay_drift_status="matched",
            objective_name="sim_event_default_v1",
            objective_preset="event_recall_heavy",
            selected_validation_metrics={
                "slope_pos f1": 0.12,
                "slope_neg f1": 0.14,
            },
            all_validation_metrics={
                "overall:event_validation:event_family_metrics.slope_pos.f1": 0.12,
                "overall:event_validation:event_family_metrics.slope_neg.f1": 0.14,
            },
        ),
        perf.BenchmarkResult(
            name="baseline",
            description="baseline",
            repeat_index=2,
            status="failed",
            env_overrides={},
            run_dir=None,
            manifest_path=None,
            elapsed_ms=None,
            stage_elapsed_ms={
                "20_events_extract.py": 300.0,
                "30_windows_adaptive.py": 500.0,
                "50_build_graph.py": 700.0,
                "70_phase_fit.py": 900.0,
            },
            return_code=1,
            replay_drift_status="drifted",
            objective_name="sim_event_default_v1",
            objective_preset="event_recall_heavy",
            selected_validation_metrics={
                "slope_pos f1": 0.06,
                "slope_neg f1": 0.08,
            },
            all_validation_metrics={
                "overall:event_validation:event_family_metrics.slope_pos.f1": 0.06,
                "overall:event_validation:event_family_metrics.slope_neg.f1": 0.08,
                "overall:phase_validation:macro_f1": 0.4,
            },
        ),
    ]

    aggregates = perf._variant_aggregate_payload(results)

    assert aggregates == [
        {
            "name": "baseline",
            "description": "baseline",
            "repeat_count": 2,
            "success_count": 1,
            "failure_count": 1,
            "fastest_elapsed_ms": 1200.0,
            "objective_name_counts": {"sim_event_default_v1": 2},
            "objective_preset_counts": {"event_recall_heavy": 2},
            "objective_ready_count": 0,
            "best_objective_combined_score": None,
            "replay_drift_status_counts": {"matched": 1, "drifted": 1},
            "stage_timing_aggregates": {
                "20_events_extract.py": {"best_elapsed_ms": 100.0, "median_elapsed_ms": 200.0},
                "30_windows_adaptive.py": {"best_elapsed_ms": 200.0, "median_elapsed_ms": 350.0},
                "50_build_graph.py": {"best_elapsed_ms": 300.0, "median_elapsed_ms": 500.0},
                "70_phase_fit.py": {"best_elapsed_ms": 400.0, "median_elapsed_ms": 650.0},
            },
            "validation_metric_aggregates": {
                "slope_neg f1": {"best_value": 0.14, "median_value": 0.11000000000000001},
                "slope_pos f1": {"best_value": 0.12, "median_value": 0.09},
            },
            "all_validation_metric_aggregates": {
                "overall:event_validation:event_family_metrics.slope_neg.f1": {"best_value": 0.14, "median_value": 0.11000000000000001},
                "overall:event_validation:event_family_metrics.slope_pos.f1": {"best_value": 0.12, "median_value": 0.09},
                "overall:phase_validation:macro_f1": {"best_value": 0.4, "median_value": 0.4},
            },
        }
    ]


def test_build_run_command_forwards_extended_event_and_backbone_args(tmp_path):
    args = _args(tmp_path)
    args.sim_seed = 3301
    command = perf._build_run_command(args, run_base_dir=tmp_path / "bench")

    assert "--sim-seed" in command
    assert "--slope-threshold-mode" in command
    assert "--slope-threshold-quantile" in command
    assert "--slope-threshold-scale" in command
    assert "--slope-threshold-min" in command
    assert "--slope-abs-threshold" in command
    assert "--slope-min-persistence-samples" in command
    assert "--slope-reemit-ratio" in command
    assert "--event-warmup-points" in command
    assert "--backbone-event-prior-alpha" in command


def test_infer_replay_target_stage_uses_furthest_impacted_stage(tmp_path):
    args = _args(tmp_path)
    args.phase_count = 4
    variant = perf.BenchmarkVariant(
        name="mixed",
        description="mixed overrides",
        env_overrides={
            "S3NTINEL_EVENT_SEGMENT_MAX_ROWS": "25000",
            "S3NTINEL_WINDOW_SEGMENT_MAX_ROWS": "25000",
        },
        objective_overrides=(),
    )

    target_stage = perf.infer_replay_target_stage(args, variant=variant)

    assert target_stage == "70_phase_fit.py"


def test_infer_replay_target_stage_returns_none_for_replay_unsafe_source_changes(tmp_path):
    args = _args(tmp_path)
    args.n_steps = 100
    variant = perf.BenchmarkVariant(
        name="baseline",
        description="baseline",
        env_overrides={},
        objective_overrides=(),
    )

    assert perf.infer_replay_target_stage(args, variant=variant) is None


def test_resolve_replay_end_stage_expands_to_evaluation_tier_closure(tmp_path):
    args = _args(tmp_path)
    args.evaluation_tier = "anomaly"

    assert perf.resolve_replay_end_stage(args, replay_target_stage="50_build_graph.py") == "90_anomaly_attribution.py"


def test_resolve_replay_end_stage_keeps_later_target_stage_when_tier_is_earlier(tmp_path):
    args = _args(tmp_path)
    args.mode = "full"
    args.evaluation_tier = "event"

    assert perf.resolve_replay_end_stage(args, replay_target_stage="70_phase_fit.py") == "70_phase_fit.py"


def test_resolve_effective_evaluation_tier_from_objective_name(tmp_path):
    args = _args(tmp_path)
    args.objective_name = "sim_structural_default_v1"

    assert perf.resolve_effective_evaluation_tier(args) == "structural"


def test_resolve_effective_objective_name_defaults_from_mode(tmp_path):
    args = _args(tmp_path)
    args.mode = "event"

    assert perf.resolve_effective_objective_name(args) == "sim_event_default_v1"


def test_resolve_effective_objective_name_defaults_from_windowing_search_stage(tmp_path):
    args = _args(tmp_path)
    args.mode = "structural"
    args.search_stage = "windowing"

    assert perf.resolve_effective_objective_name(args) == "sim_windowing_default_v1"


def test_resolve_effective_objective_name_from_cli_preset(tmp_path):
    args = _args(tmp_path)
    args.objective_preset = "event_recall_heavy"

    assert perf.resolve_effective_objective_name(args) == "sim_event_recall_heavy_v1"
    assert perf.resolve_effective_evaluation_tier(args) == "event"
    assert perf.resolve_evaluation_end_stage(args) == "20_events_extract.py"


def test_resolve_effective_evaluation_tier_rejects_mismatched_objective_and_tier(tmp_path):
    args = _args(tmp_path)
    args.objective_name = "sim_event_default_v1"
    args.evaluation_tier = "structural"

    try:
        perf.resolve_effective_evaluation_tier(args)
    except RuntimeError as exc:
        assert "sim_event_default_v1" in str(exc)
    else:
        raise AssertionError("expected mismatched objective and tier to raise")


def test_resolve_evaluation_end_stage_comes_from_objective_spec_when_present(tmp_path):
    args = _args(tmp_path)
    args.objective_name = "sim_structural_default_v1"

    assert perf.resolve_evaluation_end_stage(args) == "60_fit_hierarchy.py"


def test_resolve_effective_objective_spec_from_raw_spec_json(tmp_path):
    args = _args(tmp_path)
    objective_spec_path = tmp_path / "custom_objective.json"
    _write_json(
        objective_spec_path,
        {
            "name": "custom_eventish_v1",
            "evaluation_tier": "event",
            "required_end_stage_script": "20_events_extract.py",
            "compare_by": ["workload_signature.pipeline.mode"],
            "primary_terms": [],
            "constraints": [],
            "tie_break_terms": [],
        },
    )
    args.objective_spec_path = str(objective_spec_path)

    objective_spec = perf.resolve_effective_objective_spec(args)

    assert objective_spec.name == "custom_eventish_v1"
    assert perf.resolve_effective_evaluation_tier(args) == "event"
    assert perf.resolve_evaluation_end_stage(args) == "20_events_extract.py"


def test_resolve_effective_objective_spec_from_objective_report_json(tmp_path):
    args = _args(tmp_path)
    objective_spec_path = tmp_path / "objective_evaluation_report.json"
    _write_json(
        objective_spec_path,
        {
            "evaluation": {
                "objective_spec": {
                    "name": "custom_structural_v1",
                    "evaluation_tier": "structural",
                    "required_end_stage_script": "60_fit_hierarchy.py",
                    "compare_by": ["workload_signature.pipeline.mode"],
                    "primary_terms": [],
                    "constraints": [],
                    "tie_break_terms": [],
                }
            }
        },
    )
    args.objective_spec_path = str(objective_spec_path)

    assert perf.resolve_effective_objective_name(args) == "custom_structural_v1"
    assert perf.resolve_effective_evaluation_tier(args) == "structural"


def test_resolve_effective_evaluation_tier_rejects_mismatched_custom_objective_and_tier(tmp_path):
    args = _args(tmp_path)
    objective_spec_path = tmp_path / "custom_objective.json"
    _write_json(
        objective_spec_path,
        {
            "name": "custom_eventish_v1",
            "evaluation_tier": "event",
            "required_end_stage_script": "20_events_extract.py",
            "compare_by": ["workload_signature.pipeline.mode"],
            "primary_terms": [],
            "constraints": [],
            "tie_break_terms": [],
        },
    )
    args.objective_spec_path = str(objective_spec_path)
    args.evaluation_tier = "structural"

    try:
        perf.resolve_effective_evaluation_tier(args)
    except RuntimeError as exc:
        assert "custom_eventish_v1" in str(exc)
    else:
        raise AssertionError("expected mismatched custom objective and tier to raise")


def test_parse_objective_override_parses_json_scalars_and_strings():
    assert perf._parse_objective_override("primary_terms.0.weight=2.5") == ("primary_terms.0.weight", 2.5)
    assert perf._parse_objective_override("name=\"custom_spec\"") == ("name", "custom_spec")
    assert perf._parse_objective_override("description=plain-text") == ("description", "plain-text")


def test_resolved_objective_payload_applies_overrides(tmp_path):
    args = _args(tmp_path)
    args.objective_name = "sim_event_default_v1"
    args.objective_overrides = [
        ("name", "custom_event_tuned_v1"),
        ("evaluation_tier", "event"),
        ("required_end_stage_script", "20_events_extract.py"),
        ("primary_terms.0.weight", 3.0),
    ]

    payload = perf._resolved_objective_payload(args)

    assert payload["name"] == "custom_event_tuned_v1"
    assert payload["primary_terms"][0]["weight"] == 3.0
    assert perf.resolve_effective_objective_name(args) == "custom_event_tuned_v1"


def test_variant_objective_overrides_extend_cli_overrides(tmp_path):
    args = _args(tmp_path)
    args.objective_name = "sim_event_default_v1"
    args.objective_overrides = [("primary_terms.0.weight", 2.0)]
    variant = perf.BenchmarkVariant(
        name="recall_heavy",
        description="variant objective overrides",
        env_overrides={},
        objective_overrides=(
            ("name", "variant_event_focus_v1"),
            ("primary_terms.1.weight", 4.0),
        ),
    )

    payload = perf._resolved_objective_payload(args, variant=variant)

    assert payload["name"] == "variant_event_focus_v1"
    assert payload["primary_terms"][0]["weight"] == 2.0
    assert payload["primary_terms"][1]["weight"] == 4.0
    assert perf.resolve_effective_objective_name(args, variant=variant) == "variant_event_focus_v1"


def test_variant_objective_overrides_drive_variant_evaluation_scope(tmp_path):
    args = _args(tmp_path)
    variant = perf.BenchmarkVariant(
        name="profile_scope",
        description="variant objective scope",
        env_overrides={},
        objective_overrides=(
            ("name", "variant_profile_scope_v1"),
            ("evaluation_tier", "profile"),
            ("required_end_stage_script", "15_event_profiles_fit.py"),
        ),
    )

    assert perf.resolve_effective_evaluation_tier(args, variant=variant) == "profile"
    assert perf.resolve_evaluation_end_stage(args, variant=variant) == "15_event_profiles_fit.py"


def test_variant_objective_preset_sets_base_objective_without_raw_overrides(tmp_path):
    args = _args(tmp_path)
    variant = perf.BenchmarkVariant(
        name="preset_only",
        description="preset only",
        env_overrides={},
        objective_preset=perf.OBJECTIVE_PRESET_BY_NAME["structural_latency_biased"],
    )

    assert perf.resolve_effective_objective_name(args, variant=variant) == "sim_structural_latency_biased_v1"
    assert perf.resolve_effective_evaluation_tier(args, variant=variant) == "structural"
    assert perf.resolve_evaluation_end_stage(args, variant=variant) == "60_fit_hierarchy.py"


def test_build_summary_payload_carries_objective_overrides(tmp_path):
    args = _args(tmp_path)
    args.replay_source_run_dir = "/tmp/source"
    args.objective_name = "sim_event_default_v1"
    args.objective_overrides = [("primary_terms.0.weight", 3.0)]

    payload = perf._build_summary_payload(
        benchmark_dir=tmp_path / "bench",
        args=args,
        results=[],
    )

    assert payload["objective_overrides"] == [{"path": "primary_terms.0.weight", "value": 3.0}]


def test_build_summary_payload_carries_objective_preset(tmp_path):
    args = _args(tmp_path)
    args.replay_source_run_dir = "/tmp/source"
    args.objective_preset = "structural_latency_biased"

    payload = perf._build_summary_payload(
        benchmark_dir=tmp_path / "bench",
        args=args,
        results=[],
    )

    assert payload["objective_preset"] == "structural_latency_biased"
    assert payload["objective_name"] == "sim_structural_latency_biased_v1"


def test_build_experiment_plan_payload_carries_variant_resolution(tmp_path):
    args = _args(tmp_path)
    args.replay_source_run_dir = "/tmp/source"
    args.objective_preset = "event_recall_heavy"
    variant = perf.BenchmarkVariant(
        name="preset_variant",
        description="variant with preset",
        env_overrides={},
        objective_preset=perf.OBJECTIVE_PRESET_BY_NAME["structural_latency_biased"],
    )

    payload = perf._build_experiment_plan_payload(
        benchmark_dir=tmp_path / "bench",
        args=args,
        variants=(variant,),
    )

    assert payload["objective"]["objective_preset"] == "event_recall_heavy"
    assert payload["variants"][0]["name"] == "preset_variant"
    assert payload["variants"][0]["resolved_objective_name"] == "sim_structural_latency_biased_v1"
    assert payload["variants"][0]["resolved_evaluation_tier"] == "structural"


def test_build_experiment_plan_payload_carries_replay_start_recommendation(tmp_path):
    replay_source = tmp_path / "source_run"
    reports_dir = replay_source / "reports"
    stages_dir = reports_dir / "stages"
    delta_dir = replay_source / "delta"
    delta_dir.mkdir(parents=True, exist_ok=True)
    (delta_dir / "windows").write_text("ok", encoding="utf-8")
    _write_json(
        reports_dir / "run_manifest.json",
        {
            "source": {"flight_name": "power_chain", "tail_id": "T1", "flight_id": "F1"},
            "pipeline": {"mode": "full", "table_format": "parquet", "write_mode": "overwrite"},
        },
    )
    _write_json(
        reports_dir / "pipeline_run_summary.json",
        {
            "stages": [
                {"stage_script": "20_events_extract.py"},
                {"stage_script": "30_windows_adaptive.py"},
                {"stage_script": "40_backbone_fit.py"},
            ]
        },
    )
    _write_json(
        stages_dir / "30_windows_adaptive_manifest.json",
        {
            "replayable_from": ["windows"],
            "input_artifacts": {"windows": {"path": str(delta_dir / "windows")}},
        },
    )
    args = _args(tmp_path)
    args.replay_source_run_dir = str(replay_source)
    args.replay_target_stage = "40_backbone_fit.py"
    variant = perf.BenchmarkVariant(
        name="baseline",
        description="baseline",
        env_overrides={},
    )

    payload = perf._build_experiment_plan_payload(
        benchmark_dir=tmp_path / "bench",
        args=args,
        variants=(variant,),
    )

    assert payload["variants"][0]["recommended_start_stage"] == "30_windows_adaptive.py"
    assert payload["variants"][0]["recommended_stage_count"] == 2
    assert "--start-stage 30_windows_adaptive.py" in payload["variants"][0]["recommended_resume_command"]


def test_build_experiment_plan_markdown_includes_variant_rows(tmp_path):
    args = _args(tmp_path)
    args.replay_source_run_dir = "/tmp/source"
    variant = perf.BenchmarkVariant(
        name="baseline",
        description="baseline",
        env_overrides={},
    )
    payload = perf._build_experiment_plan_payload(
        benchmark_dir=tmp_path / "bench",
        args=args,
        variants=(variant,),
    )

    markdown = perf._build_experiment_plan_markdown(payload)

    assert "Benchmark Experiment Plan" in markdown
    assert "| baseline |" in markdown
