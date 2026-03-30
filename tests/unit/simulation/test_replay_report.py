from __future__ import annotations

import json
from pathlib import Path

from libs.simulation.replay_report import (
    build_simulation_replay_report,
    discover_latest_simulation_run_dir,
    recommend_resume_plan,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_build_simulation_replay_report_surfaces_ready_stage_and_command(tmp_path):
    run_dir = tmp_path / "20260321T000000Z_power_chain"
    reports_dir = run_dir / "reports"
    stages_dir = reports_dir / "stages"
    delta_dir = run_dir / "delta"
    delta_dir.mkdir(parents=True, exist_ok=True)
    (delta_dir / "events").write_text("ok", encoding="utf-8")

    _write_json(
        reports_dir / "run_manifest.json",
        {
            "source": {
                "flight_name": "power_chain",
                "tail_id": "T1",
                "flight_id": "F1",
            },
            "pipeline": {
                "mode": "full",
                "table_format": "parquet",
                "write_mode": "overwrite",
            },
        },
    )
    _write_json(
        reports_dir / "pipeline_run_summary.json",
        {
            "stages": [
                {"stage_script": "00_ingest_raw.py"},
                {"stage_script": "20_events_extract.py"},
            ]
        },
    )
    _write_json(
        stages_dir / "20_events_extract_manifest.json",
        {
            "replayable_from": ["events"],
            "input_artifacts": {
                "events": {"path": str(delta_dir / "events")},
            },
        },
    )

    report = build_simulation_replay_report(run_dir)

    assert report.flight_name == "power_chain"
    assert report.mode == "full"
    assert report.summary_artifact_path == "reports/pipeline_run_summary.json"
    assert len(report.stage_replays) == 1
    stage_report = report.stage_replays[0]
    assert stage_report.stage_script == "20_events_extract.py"
    assert stage_report.ready is True
    assert "--start-stage 20_events_extract.py" in str(stage_report.suggested_resume_command)
    assert report.ordered_stage_scripts == ("00_ingest_raw.py", "20_events_extract.py")


def test_build_simulation_replay_report_marks_missing_input_not_ready(tmp_path):
    run_dir = tmp_path / "20260321T000000Z_power_chain"
    reports_dir = run_dir / "reports"
    stages_dir = reports_dir / "stages"

    _write_json(
        reports_dir / "run_manifest.json",
        {
            "source": {"flight_name": "power_chain", "tail_id": "T1", "flight_id": "F1"},
            "pipeline": {"mode": "event", "table_format": "parquet", "write_mode": "overwrite"},
        },
    )
    _write_json(
        reports_dir / "event_pipeline_run_summary.json",
        {
            "stages": [
                {"stage_script": "15_event_profiles_fit.py"},
            ]
        },
    )
    _write_json(
        stages_dir / "15_event_profiles_fit_manifest.json",
        {
            "replayable_from": ["parameter_behavior_profile"],
            "input_artifacts": {
                "parameter_behavior_profile": {"path": str(run_dir / "delta" / "parameter_behavior_profile")},
            },
        },
    )

    report = build_simulation_replay_report(run_dir)

    assert len(report.stage_replays) == 1
    stage_report = report.stage_replays[0]
    assert stage_report.ready is False
    assert stage_report.suggested_resume_command is None


def test_discover_latest_simulation_run_dir_picks_latest_manifest(tmp_path):
    older = tmp_path / "20260320T000000Z_a"
    newer = tmp_path / "20260321T000000Z_b"
    _write_json(older / "reports" / "run_manifest.json", {"status": "success"})
    _write_json(newer / "reports" / "run_manifest.json", {"status": "success"})

    latest = discover_latest_simulation_run_dir(tmp_path)

    assert latest == newer


def test_recommend_resume_plan_picks_latest_ready_boundary_before_target(tmp_path):
    run_dir = tmp_path / "20260321T000000Z_power_chain"
    reports_dir = run_dir / "reports"
    stages_dir = reports_dir / "stages"
    delta_dir = run_dir / "delta"
    delta_dir.mkdir(parents=True, exist_ok=True)
    (delta_dir / "events").write_text("ok", encoding="utf-8")
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
                {"stage_script": "25_window_policy_profile.py"},
                {"stage_script": "30_windows_adaptive.py"},
                {"stage_script": "40_backbone_fit.py"},
            ]
        },
    )
    _write_json(
        stages_dir / "20_events_extract_manifest.json",
        {
            "replayable_from": ["events"],
            "input_artifacts": {"events": {"path": str(delta_dir / "events")}},
        },
    )
    _write_json(
        stages_dir / "30_windows_adaptive_manifest.json",
        {
            "replayable_from": ["windows"],
            "input_artifacts": {"windows": {"path": str(delta_dir / "windows")}},
        },
    )

    report = build_simulation_replay_report(run_dir)
    resume_plan = recommend_resume_plan(report, target_stage_script="40_backbone_fit.py")

    assert resume_plan is not None
    assert resume_plan.selected_start_stage_script == "30_windows_adaptive.py"
    assert resume_plan.selected_end_stage_script == "40_backbone_fit.py"
    assert resume_plan.selected_stage_count == 2
    assert "--start-stage 30_windows_adaptive.py" in resume_plan.resume_command
    assert "--end-stage 40_backbone_fit.py" in resume_plan.resume_command
