from __future__ import annotations

import argparse
import json
from pathlib import Path

from libs.tuning import BenchmarkVariant, resolve_objective_spec, run_benchmark_variant


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
        phase_count=4,
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


def test_run_benchmark_variant_builds_result_from_fresh_run(tmp_path: Path):
    args = _args(tmp_path)
    variant = BenchmarkVariant(
        name="baseline",
        description="baseline",
        env_overrides={},
        arg_overrides={"window_event_threshold": 7},
    )
    benchmark_dir = tmp_path / "bench"
    run_base_dir = benchmark_dir / "runs" / variant.name / "repeat_1"
    run_dir = run_base_dir / "20260101T000000Z_power_chain"
    reports_dir = run_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "run_manifest.json").write_text(
        json.dumps({"status": "success", "timing": {"elapsed_ms": 123.0}}),
        encoding="utf-8",
    )
    (reports_dir / "pipeline_run_summary.json").write_text(
        json.dumps({"stages": [{"stage_script": "20_events_extract.py", "elapsed_ms": 10.0}]}),
        encoding="utf-8",
    )
    (reports_dir / "validation_harness_report.json").write_text(
        json.dumps({"validation_metrics": {"metric_records": []}}),
        encoding="utf-8",
    )
    (reports_dir / "objective_evaluation_report.json").write_text(
        json.dumps(
            {
                "evaluation": {
                    "overall_status": "ok",
                    "ready_for_search": True,
                    "combined_score": 0.5,
                }
            }
        ),
        encoding="utf-8",
    )

    class _Completed:
        returncode = 0

    captured_command: list[str] = []

    def _process_runner(command, **_kwargs):
        captured_command[:] = list(command)
        return _Completed()

    result = run_benchmark_variant(
        args=args,
        benchmark_dir=benchmark_dir,
        variant=variant,
        repeat_index=1,
        logger_name="test.benchmark",
        summary_name="pipeline_run_summary.json",
        planned_variant_replay={},
        resolved_objective_payload=None,
        resolve_effective_objective_spec=lambda *_, **__: resolve_objective_spec(objective_name="sim_full_default_v1"),
        resolve_effective_objective_name=lambda *_, **__: "sim_full_default_v1",
        resolve_effective_evaluation_tier=lambda *_, **__: "full",
        infer_replay_target_stage=lambda *_, **__: None,
        resolve_replay_end_stage=lambda *_, **__: "unused",
        merged_objective_overrides=lambda *_, **__: (),
        process_runner=_process_runner,
        script_cwd=tmp_path,
    )

    assert result.status == "success"
    assert result.elapsed_ms == 123.0
    assert result.stage_elapsed_ms == {"20_events_extract.py": 10.0}
    assert result.arg_overrides == {"window_event_threshold": 7}
    assert result.objective_name == "sim_full_default_v1"
    assert result.evaluation_tier == "full"
    assert result.objective_status == "ok"
    assert result.objective_ready_for_search is True
    assert result.objective_combined_score == 0.5
    assert "--window-event-threshold" in captured_command
    assert captured_command[captured_command.index("--window-event-threshold") + 1] == "7"
