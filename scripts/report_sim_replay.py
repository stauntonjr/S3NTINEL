"""Inspect a persisted simulation run and report replayable stage boundaries."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from libs.simulation.replay_report import (
    build_simulation_replay_report,
    discover_latest_simulation_run_dir,
    recommend_resume_plan,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Report replayable simulation stage boundaries for a persisted run")
    parser.add_argument("--run-dir", default=None, help="Explicit simulation run directory to inspect")
    parser.add_argument("--base-dir", default="data/simulation_runs", help="Base directory used with --latest")
    parser.add_argument("--latest", action="store_true", help="Inspect the latest run under --base-dir")
    parser.add_argument("--target-stage", default=None, help="Optional target stage script to compute the cheapest valid resume plan")
    parser.add_argument("--json", action="store_true", help="Emit the replay report as JSON")
    return parser.parse_args()


def _resolve_run_dir(args: argparse.Namespace) -> Path:
    if args.run_dir:
        return Path(str(args.run_dir)).resolve()
    return discover_latest_simulation_run_dir(args.base_dir if args.latest else "data/simulation_runs")


def _print_text_report(report, *, resume_plan=None) -> None:
    print(f"run_dir: {report.run_dir}")
    print(f"flight_name: {report.flight_name}")
    print(f"mode: {report.mode}")
    print(f"summary_artifact_path: {report.summary_artifact_path}")
    if resume_plan is not None:
        print("recommended_resume_plan:")
        print(f"- target_stage: {resume_plan.target_stage_script}")
        print(f"  start_stage: {resume_plan.selected_start_stage_script}")
        print(f"  end_stage: {resume_plan.selected_end_stage_script}")
        print(f"  selected_stage_count: {resume_plan.selected_stage_count}")
        print(f"  resume_command: {resume_plan.resume_command}")
    if not report.stage_replays:
        print("replayable_stages: none")
        return
    print("replayable_stages:")
    for item in report.stage_replays:
        print(f"- stage: {item.stage_script}")
        print(f"  ready: {item.ready}")
        print(f"  replayable_from: {', '.join(item.replayable_from)}")
        for input_item in item.inputs:
            print(f"  input: {input_item.artifact_name} exists={input_item.exists} path={input_item.path}")
        if item.suggested_resume_command is not None:
            print(f"  resume_command: {item.suggested_resume_command}")


def main() -> None:
    args = parse_args()
    report = build_simulation_replay_report(_resolve_run_dir(args))
    resume_plan = (
        recommend_resume_plan(report, target_stage_script=str(args.target_stage))
        if args.target_stage is not None
        else None
    )
    if args.json:
        payload = report.to_payload()
        if resume_plan is not None:
            payload["recommended_resume_plan"] = resume_plan.to_payload()
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    _print_text_report(report, resume_plan=resume_plan)


if __name__ == "__main__":
    main()
