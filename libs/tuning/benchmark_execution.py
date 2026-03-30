"""Benchmark child-run execution helpers."""

from __future__ import annotations

import shutil
import sys
from pathlib import Path
from typing import Any

from libs.simulation.replay_report import build_simulation_replay_report, recommend_resume_plan


def build_run_command(args: Any, *, run_base_dir: Path) -> list[str]:
    command = [
        sys.executable,
        "-m",
        "scripts.run_sim_pipeline",
        "--flight-name",
        str(args.flight_name),
        "--tail-id",
        str(args.tail_id),
        "--flight-id",
        str(args.flight_id),
        "--base-dir",
        str(run_base_dir),
        "--mode",
        str(args.mode),
        "--format",
        str(args.format),
        "--write-mode",
        str(args.write_mode),
        "--profile-numeric-ratio-threshold",
        str(args.profile_numeric_ratio_threshold),
        "--profile-categorical-cardinality-max",
        str(args.profile_categorical_cardinality_max),
        "--profile-behavior-significant-diff-threshold",
        str(args.profile_behavior_significant_diff_threshold),
        "--profile-behavior-center-band-width",
        str(args.profile_behavior_center_band_width),
        "--profile-behavior-soft-bound-width",
        str(args.profile_behavior_soft_bound_width),
        "--profile-behavior-hard-bound-width",
        str(args.profile_behavior_hard_bound_width),
        "--profile-behavior-mixed-unknown-low-score-threshold",
        str(args.profile_behavior_mixed_unknown_low_score_threshold),
        "--profile-behavior-mixed-unknown-ambiguous-score-threshold",
        str(args.profile_behavior_mixed_unknown_ambiguous_score_threshold),
        "--profile-behavior-mixed-unknown-ambiguous-margin-threshold",
        str(args.profile_behavior_mixed_unknown_ambiguous_margin_threshold),
        "--min-warm",
        str(args.min_warm),
        "--delta-threshold",
        str(args.delta_threshold),
        "--slope-threshold-mode",
        str(args.slope_threshold_mode),
        "--slope-threshold-quantile",
        str(args.slope_threshold_quantile),
        "--slope-threshold-scale",
        str(args.slope_threshold_scale),
        "--slope-threshold-min",
        str(args.slope_threshold_min),
        "--slope-source",
        str(args.slope_source),
        "--ema-alpha",
        str(args.ema_alpha),
        "--slope-abs-threshold",
        str(args.slope_abs_threshold),
        "--slope-min-persistence-samples",
        str(args.slope_min_persistence_samples),
        "--slope-reemit-ratio",
        str(args.slope_reemit_ratio),
        "--event-warmup-points",
        str(args.event_warmup_points),
        "--event-low-scale-responsiveness",
        str(getattr(args, "event_low_scale_responsiveness", 1.0)),
        "--event-repeatability-aggressiveness",
        str(getattr(args, "event_repeatability_aggressiveness", 1.0)),
        "--event-drift-conservatism",
        str(getattr(args, "event_drift_conservatism", 1.0)),
        "--event-chatter-suppression",
        str(getattr(args, "event_chatter_suppression", 1.0)),
        "--window-max-ms",
        str(args.window_max_ms),
        "--window-event-threshold",
        str(args.window_event_threshold),
        "--window-min-ms",
        str(args.window_min_ms),
        "--window-inactivity-timeout-ms",
        str(args.window_inactivity_timeout_ms),
        "--window-strategy",
        str(args.window_strategy),
        "--phase-count",
        str(args.phase_count),
        "--backbone-parameter-count",
        str(args.backbone_parameter_count),
        "--backbone-ridge-lambda",
        str(args.backbone_ridge_lambda),
        "--backbone-event-prior-alpha",
        str(args.backbone_event_prior_alpha),
    ]
    if args.sim_seed is not None:
        command.extend(("--sim-seed", str(args.sim_seed)))
    if args.n_steps is not None:
        command.extend(("--n-steps", str(args.n_steps)))
    if args.dt_seconds is not None:
        command.extend(("--dt-seconds", str(args.dt_seconds)))
    return command


def clone_replay_source_run(*, replay_source_run_dir: Path, run_base_dir: Path) -> Path:
    cloned_run_dir = run_base_dir / replay_source_run_dir.name
    if cloned_run_dir.exists():
        shutil.rmtree(cloned_run_dir)
    shutil.copytree(replay_source_run_dir, cloned_run_dir)
    return cloned_run_dir


def build_replay_run_command(
    args: Any,
    *,
    run_base_dir: Path,
    replay_source_run_dir: Path,
    replay_target_stage: str,
    replay_end_stage: str | None = None,
) -> tuple[list[str], Path, object, str]:
    replay_report = build_simulation_replay_report(replay_source_run_dir)
    resume_plan = recommend_resume_plan(replay_report, target_stage_script=replay_target_stage)
    if resume_plan is None:
        raise RuntimeError(
            f"no valid replay boundary found in {replay_source_run_dir} for target stage {replay_target_stage!r}"
        )
    cloned_run_dir = clone_replay_source_run(
        replay_source_run_dir=replay_source_run_dir,
        run_base_dir=run_base_dir,
    )
    resolved_end_stage = str(replay_end_stage or replay_target_stage)
    command = build_run_command(args, run_base_dir=run_base_dir)
    command.extend(
        [
            "--replay-run-dir",
            str(cloned_run_dir),
            "--start-stage",
            str(resume_plan.selected_start_stage_script),
            "--end-stage",
            resolved_end_stage,
        ]
    )
    return command, cloned_run_dir, resume_plan, resolved_end_stage
