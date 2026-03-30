"""Replay inspection helpers for persisted simulation run bundles."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ReplayableInputStatus:
    artifact_name: str
    path: str
    exists: bool

    def to_payload(self) -> dict[str, Any]:
        return {
            "artifact_name": self.artifact_name,
            "path": self.path,
            "exists": self.exists,
        }


@dataclass(frozen=True)
class StageReplayReport:
    stage_script: str
    manifest_path: str
    replayable_from: tuple[str, ...]
    inputs: tuple[ReplayableInputStatus, ...]
    ready: bool
    suggested_resume_command: str | None

    def to_payload(self) -> dict[str, Any]:
        return {
            "stage_script": self.stage_script,
            "manifest_path": self.manifest_path,
            "replayable_from": list(self.replayable_from),
            "inputs": [item.to_payload() for item in self.inputs],
            "ready": self.ready,
            "suggested_resume_command": self.suggested_resume_command,
        }


@dataclass(frozen=True)
class SimulationReplayReport:
    run_dir: str
    flight_name: str | None
    tail_id: str | None
    flight_id: str | None
    mode: str | None
    summary_artifact_path: str | None
    ordered_stage_scripts: tuple[str, ...]
    stage_replays: tuple[StageReplayReport, ...]

    def to_payload(self) -> dict[str, Any]:
        return {
            "run_dir": self.run_dir,
            "flight_name": self.flight_name,
            "tail_id": self.tail_id,
            "flight_id": self.flight_id,
            "mode": self.mode,
            "summary_artifact_path": self.summary_artifact_path,
            "ordered_stage_scripts": list(self.ordered_stage_scripts),
            "stage_replays": [item.to_payload() for item in self.stage_replays],
        }


@dataclass(frozen=True)
class ReplayResumePlan:
    target_stage_script: str
    selected_start_stage_script: str
    selected_end_stage_script: str
    selected_stage_count: int
    resume_command: str

    def to_payload(self) -> dict[str, Any]:
        return {
            "target_stage_script": self.target_stage_script,
            "selected_start_stage_script": self.selected_start_stage_script,
            "selected_end_stage_script": self.selected_end_stage_script,
            "selected_stage_count": self.selected_stage_count,
            "resume_command": self.resume_command,
        }


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def discover_latest_simulation_run_dir(base_dir: str | Path = "data/simulation_runs") -> Path:
    base_path = Path(base_dir)
    candidates = [
        path.parent.parent
        for path in base_path.rglob("reports/run_manifest.json")
        if path.is_file()
    ]
    if not candidates:
        raise FileNotFoundError(f"no simulation run bundles with reports/run_manifest.json found under {base_path}")
    return max(candidates, key=lambda item: (item.name, item.stat().st_mtime))


def _resolve_summary_artifact_path(run_dir: Path) -> str | None:
    report_dir = run_dir / "reports"
    for name in (
        "profile_pipeline_run_summary.json",
        "event_pipeline_run_summary.json",
        "structural_pipeline_run_summary.json",
        "pipeline_run_summary.json",
    ):
        if (report_dir / name).exists():
            return f"reports/{name}"
    return None


def _stage_scripts_from_summary(run_dir: Path, summary_artifact_path: str | None) -> list[str]:
    if summary_artifact_path is None:
        return []
    payload = _load_json(run_dir / summary_artifact_path)
    return [
        str(item.get("stage_script"))
        for item in (payload.get("stages") or [])
        if str(item.get("stage_script") or "").endswith(".py")
    ]


def _build_resume_command(
    *,
    run_dir: Path,
    manifest: dict[str, Any],
    start_stage_script: str,
    end_stage_script: str | None = None,
) -> str | None:
    source = dict(manifest.get("source") or {})
    pipeline = dict(manifest.get("pipeline") or {})
    flight_name = str(source.get("flight_name") or "").strip()
    if not flight_name:
        return None
    parts = [
        "python -m scripts.run_sim_pipeline",
        f"--flight-name {flight_name}",
        f"--tail-id {source.get('tail_id')}",
        f"--flight-id {source.get('flight_id')}",
        f"--mode {pipeline.get('mode')}",
        f"--format {pipeline.get('table_format')}",
        f"--write-mode {pipeline.get('write_mode')}",
        f"--replay-run-dir {run_dir}",
        f"--start-stage {start_stage_script}",
    ]
    if end_stage_script is not None:
        parts.append(f"--end-stage {end_stage_script}")
    return " ".join(parts)


def recommend_resume_plan(
    report: SimulationReplayReport,
    *,
    target_stage_script: str,
) -> ReplayResumePlan | None:
    ordered_stage_scripts = list(report.ordered_stage_scripts)
    if target_stage_script not in ordered_stage_scripts:
        raise RuntimeError(f"unknown target stage {target_stage_script!r} for run {report.run_dir}")
    target_index = ordered_stage_scripts.index(target_stage_script)
    replay_by_stage = {item.stage_script: item for item in report.stage_replays if item.ready}
    best_start_stage: str | None = None
    for stage_script in ordered_stage_scripts[: target_index + 1]:
        if stage_script in replay_by_stage:
            best_start_stage = stage_script
    if best_start_stage is None:
        return None
    replay_stage = replay_by_stage[best_start_stage]
    selected_stage_count = (target_index - ordered_stage_scripts.index(best_start_stage)) + 1
    if replay_stage.suggested_resume_command is None:
        return None
    resume_command = replay_stage.suggested_resume_command
    if target_stage_script != best_start_stage:
        resume_command = f"{resume_command} --end-stage {target_stage_script}"
    return ReplayResumePlan(
        target_stage_script=target_stage_script,
        selected_start_stage_script=best_start_stage,
        selected_end_stage_script=target_stage_script,
        selected_stage_count=selected_stage_count,
        resume_command=resume_command,
    )


def build_simulation_replay_report(run_dir: str | Path) -> SimulationReplayReport:
    resolved_run_dir = Path(run_dir).resolve()
    manifest_path = resolved_run_dir / "reports" / "run_manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"missing simulation run manifest: {manifest_path}")
    manifest = _load_json(manifest_path)
    summary_artifact_path = _resolve_summary_artifact_path(resolved_run_dir)
    stage_scripts = _stage_scripts_from_summary(resolved_run_dir, summary_artifact_path)
    stage_reports: list[StageReplayReport] = []
    for stage_script in stage_scripts:
        stage_manifest_path = resolved_run_dir / "reports" / "stages" / f"{stage_script.removesuffix('.py')}_manifest.json"
        if not stage_manifest_path.exists():
            continue
        stage_manifest = _load_json(stage_manifest_path)
        replayable_from = tuple(str(item) for item in (stage_manifest.get("replayable_from") or ()))
        if not replayable_from:
            continue
        input_artifacts = dict(stage_manifest.get("input_artifacts") or {})
        inputs = tuple(
            ReplayableInputStatus(
                artifact_name=artifact_name,
                path=str(dict(input_artifacts.get(artifact_name) or {}).get("path") or ""),
                exists=Path(str(dict(input_artifacts.get(artifact_name) or {}).get("path") or "")).exists(),
            )
            for artifact_name in replayable_from
        )
        ready = all(item.exists for item in inputs)
        stage_reports.append(
            StageReplayReport(
                stage_script=stage_script,
                manifest_path=str(stage_manifest_path),
                replayable_from=replayable_from,
                inputs=inputs,
                ready=ready,
                suggested_resume_command=(
                    _build_resume_command(
                        run_dir=resolved_run_dir,
                        manifest=manifest,
                        start_stage_script=stage_script,
                    )
                    if ready
                    else None
                ),
            )
        )
    source = dict(manifest.get("source") or {})
    pipeline = dict(manifest.get("pipeline") or {})
    return SimulationReplayReport(
        run_dir=str(resolved_run_dir),
        flight_name=(None if source.get("flight_name") is None else str(source.get("flight_name"))),
        tail_id=(None if source.get("tail_id") is None else str(source.get("tail_id"))),
        flight_id=(None if source.get("flight_id") is None else str(source.get("flight_id"))),
        mode=(None if pipeline.get("mode") is None else str(pipeline.get("mode"))),
        summary_artifact_path=summary_artifact_path,
        ordered_stage_scripts=tuple(stage_scripts),
        stage_replays=tuple(stage_reports),
    )
