from __future__ import annotations

import argparse
import json
from pathlib import Path

from libs.tuning import build_replay_run_command, build_run_command


def _args(tmp_path: Path) -> argparse.Namespace:
    return argparse.Namespace(
        flight_name="power_chain",
        tail_id="T1",
        flight_id="F1",
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
        slope_threshold_mode="adaptive_run",
        slope_threshold_quantile=0.75,
        slope_threshold_scale=0.5,
        slope_threshold_min=1e-6,
        slope_source="raw",
        ema_alpha=0.2,
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
        sim_seed=None,
        n_steps=None,
        dt_seconds=None,
    )


def test_build_run_command_carries_extended_args(tmp_path: Path):
    args = _args(tmp_path)
    args.sim_seed = 3301

    command = build_run_command(args, run_base_dir=tmp_path / "bench")

    assert command[:3] == [command[0], "-m", "scripts.run_sim_pipeline"]
    assert "--sim-seed" in command
    assert "--backbone-event-prior-alpha" in command
    assert "--window-event-threshold" in command


def test_build_replay_run_command_clones_and_appends_stage_range(tmp_path: Path):
    replay_source = tmp_path / "source_run"
    reports_dir = replay_source / "reports"
    stages_dir = reports_dir / "stages"
    delta_dir = replay_source / "delta"
    stages_dir.mkdir(parents=True, exist_ok=True)
    delta_dir.mkdir(parents=True, exist_ok=True)
    (delta_dir / "windows").write_text("ok", encoding="utf-8")
    (reports_dir / "run_manifest.json").write_text(
        json.dumps(
            {
                "source": {"flight_name": "power_chain", "tail_id": "T1", "flight_id": "F1"},
                "pipeline": {"mode": "full", "table_format": "parquet", "write_mode": "overwrite"},
            }
        ),
        encoding="utf-8",
    )
    (reports_dir / "pipeline_run_summary.json").write_text(
        json.dumps(
            {
                "stages": [
                    {"stage_script": "20_events_extract.py"},
                    {"stage_script": "30_windows_adaptive.py"},
                    {"stage_script": "40_backbone_fit.py"},
                ]
            }
        ),
        encoding="utf-8",
    )
    (stages_dir / "30_windows_adaptive_manifest.json").write_text(
        json.dumps(
            {
                "replayable_from": ["windows"],
                "input_artifacts": {"windows": {"path": str(delta_dir / "windows")}},
            }
        ),
        encoding="utf-8",
    )
    args = _args(tmp_path)

    command, cloned_run_dir, resume_plan, replay_end_stage = build_replay_run_command(
        args,
        run_base_dir=tmp_path / "bench" / "runs" / "baseline" / "repeat_1",
        replay_source_run_dir=replay_source,
        replay_target_stage="40_backbone_fit.py",
    )

    assert cloned_run_dir.exists()
    assert resume_plan.selected_start_stage_script == "30_windows_adaptive.py"
    assert replay_end_stage == "40_backbone_fit.py"
    assert command[-4:] == [
        "--start-stage",
        "30_windows_adaptive.py",
        "--end-stage",
        "40_backbone_fit.py",
    ]
