from __future__ import annotations

import json
from pathlib import Path

from libs.tuning import (
    all_validation_metrics_from_harness,
    build_benchmark_result,
    replay_drift_status,
    selected_validation_metrics_for_objective,
    resolve_objective_spec,
)


def test_benchmark_runtime_extracts_validation_metrics():
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
    objective_spec = resolve_objective_spec(objective_name="sim_event_default_v1")

    assert all_validation_metrics_from_harness(harness_payload) == {
        "overall:event_validation:event_family_metrics.slope_neg.f1": 0.14,
        "overall:event_validation:event_family_metrics.slope_pos.f1": 0.12,
        "overall:event_validation:event_family_metrics.transition.f1": 1.0,
        "overall:event_validation:precision": 0.18,
        "overall:event_validation:slope_run_capture_metrics.slope_neg.run_recall": 0.65,
        "overall:event_validation:slope_run_capture_metrics.slope_pos.run_recall": 0.7,
        "overall:profile_validation:datatype_accuracy": 1.0,
    }
    assert selected_validation_metrics_for_objective(
        objective_spec=objective_spec,
        harness_payload=harness_payload,
    ) == {
        "slope_pos run recall": 0.7,
        "slope_neg run recall": 0.65,
        "slope_pos f1": 0.12,
        "slope_neg f1": 0.14,
        "event validation precision": 0.18,
        "transition f1": 1.0,
    }


def test_benchmark_runtime_builds_result_from_persisted_run(tmp_path: Path):
    run_dir = tmp_path / "run"
    reports_dir = run_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "run_manifest.json").write_text(
        json.dumps({"status": "success", "timing": {"elapsed_ms": 123.0}}),
        encoding="utf-8",
    )
    (reports_dir / "event_pipeline_run_summary.json").write_text(
        json.dumps({"stages": [{"stage_script": "20_events_extract.py", "elapsed_ms": 10.0}]}),
        encoding="utf-8",
    )
    (reports_dir / "validation_harness_report.json").write_text(
        json.dumps(
            {
                "validation_metrics": {
                    "metric_records": [
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
                        }
                    ]
                }
            }
        ),
        encoding="utf-8",
    )
    (reports_dir / "objective_evaluation_report.json").write_text(
        json.dumps(
            {
                "evaluation": {
                    "overall_status": "ok",
                    "ready_for_search": True,
                    "combined_score": 0.82,
                }
            }
        ),
        encoding="utf-8",
    )
    objective_spec = resolve_objective_spec(objective_name="sim_event_default_v1")

    result = build_benchmark_result(
        variant_name="baseline",
        variant_description="baseline",
        variant_env_overrides={},
        variant_arg_overrides={"window_event_threshold": 7},
        variant_objective_preset_name="event_recall_heavy",
        repeat_index=1,
        completed_return_code=0,
        replay_source_run_dir=None,
        replay_target_stage=None,
        replay_start_stage=None,
        replay_end_stage=None,
        planned_replay_start_stage=None,
        planned_replay_stage_count=None,
        evaluation_tier="event",
        objective_name="sim_event_default_v1",
        objective_spec_path=None,
        objective_overrides=(),
        run_dir=run_dir,
        summary_name="event_pipeline_run_summary.json",
        objective_spec=objective_spec,
    )

    assert result.elapsed_ms == 123.0
    assert result.stage_elapsed_ms == {"20_events_extract.py": 10.0}
    assert result.arg_overrides == {"window_event_threshold": 7}
    assert result.selected_validation_metrics == {
        "slope_pos run recall": 0.7,
        "slope_neg run recall": 0.65,
        "slope_pos f1": 0.12,
        "slope_neg f1": 0.14,
        "event validation precision": 0.18,
        "transition f1": 1.0,
    }
    assert result.objective_preset == "event_recall_heavy"
    assert result.objective_status == "ok"
    assert result.objective_ready_for_search is True
    assert result.objective_combined_score == 0.82
    assert replay_drift_status(planned_start_stage="30_windows_adaptive.py", actual_start_stage="20_events_extract.py") == "drifted"
