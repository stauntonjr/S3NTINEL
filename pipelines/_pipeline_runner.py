"""Shared stage-group runner for MLflow-tracked pipeline orchestration."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
import runpy
import time
from pathlib import Path

from libs.perf import (
    active_run_id,
    build_stage_manifest,
    capture_memory_snapshot,
    get_logger,
    log_dict_artifact_if_active,
    log_memory_usage,
    log_metric_if_active,
    log_stage_manifest_if_active,
    pipeline_run_context,
)
from pipelines.plans import StageRunPlan


@dataclass(frozen=True, kw_only=True)
class StageGroupSpec(StageRunPlan):
    logger_name: str


@dataclass(frozen=True)
class StageExecutionResult:
    stage_script: str
    status: str
    elapsed_ms: float
    error: str | None = None

    def to_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "stage_script": self.stage_script,
            "status": self.status,
            "elapsed_ms": float(self.elapsed_ms),
        }
        if self.error is not None:
            payload["error"] = self.error
        return payload


@dataclass(frozen=True)
class StageGroupRunSummary:
    project: str
    run_name: str
    pipeline_mode: str
    parent_run_id: str | None
    status: str
    total_elapsed_ms: float
    stage_count: int
    selected_stage_count: int
    completed_stage_count: int
    failed_stage_count: int
    stages: tuple[StageExecutionResult, ...]
    memory_snapshot_end: dict[str, object]
    replay_run_dir: str | None = None
    start_stage_script: str | None = None
    end_stage_script: str | None = None

    def to_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "project": self.project,
            "run_name": self.run_name,
            "pipeline_mode": self.pipeline_mode,
            "parent_run_id": self.parent_run_id,
            "status": self.status,
            "total_elapsed_ms": float(self.total_elapsed_ms),
            "stage_count": int(self.stage_count),
            "selected_stage_count": int(self.selected_stage_count),
            "completed_stage_count": int(self.completed_stage_count),
            "failed_stage_count": int(self.failed_stage_count),
            "stages": [stage.to_payload() for stage in self.stages],
            "memory_snapshot_end": dict(self.memory_snapshot_end),
        }
        if self.replay_run_dir is not None:
            payload["replay_run_dir"] = self.replay_run_dir
        if self.start_stage_script is not None:
            payload["start_stage_script"] = self.start_stage_script
        if self.end_stage_script is not None:
            payload["end_stage_script"] = self.end_stage_script
        return payload


def _select_stage_scripts(
    stage_plan: StageRunPlan,
    *,
    start_stage_script: str | None = None,
    end_stage_script: str | None = None,
) -> tuple[str, ...]:
    return tuple(
        stage_plan.selected_stage_scripts(
            start_stage=start_stage_script,
            end_stage=end_stage_script,
        )
    )


def _manifest_path_for_stage(run_dir: Path, stage_script: str) -> Path:
    return run_dir / "reports" / "stages" / f"{stage_script.removesuffix('.py')}_manifest.json"


def _validate_replay_inputs(*, replay_run_dir: str, first_stage_script: str) -> None:
    manifest_path = _manifest_path_for_stage(Path(replay_run_dir), first_stage_script)
    if not manifest_path.exists():
        raise RuntimeError(
            f"cannot replay stage {first_stage_script!r}; missing stage manifest at {manifest_path}"
        )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    replayable_from = [str(item) for item in (manifest.get("replayable_from") or [])]
    input_artifacts = dict(manifest.get("input_artifacts") or {})
    if not replayable_from:
        raise RuntimeError(
            f"cannot replay stage {first_stage_script!r}; manifest does not declare replayable_from inputs"
        )
    missing_inputs: list[str] = []
    for artifact_name in replayable_from:
        payload = dict(input_artifacts.get(artifact_name) or {})
        path = str(payload.get("path") or "").strip()
        if not path or not Path(path).exists():
            missing_inputs.append(f"{artifact_name}:{path or '<missing-path>'}")
    if missing_inputs:
        raise RuntimeError(
            f"cannot replay stage {first_stage_script!r}; replayable inputs are missing: {', '.join(missing_inputs)}"
        )


def _coerce_stage_group_spec(
    *,
    spec: StageGroupSpec | None,
    run_name: str | None,
    pipeline_mode: str | None,
    stage_scripts: list[str] | tuple[str, ...] | None,
    summary_artifact_path: str | None,
    manifest_artifact_path: str | None,
    logger_name: str | None,
) -> StageGroupSpec:
    if spec is not None:
        return spec
    if not all(
        value is not None
        for value in (run_name, pipeline_mode, stage_scripts, summary_artifact_path, logger_name)
    ):
        raise TypeError("run_stage_group requires either spec=StageGroupSpec(...) or the legacy keyword arguments")
    return StageGroupSpec(
        run_name=str(run_name),
        pipeline_mode=str(pipeline_mode),
        stage_scripts=tuple(str(stage_script) for stage_script in (stage_scripts or [])),
        summary_artifact_path=str(summary_artifact_path),
        manifest_artifact_path=(None if manifest_artifact_path is None else str(manifest_artifact_path)),
        logger_name=str(logger_name),
    )


def _local_artifact_base_dir() -> Path:
    base_dir = str(os.getenv("S3NTINEL_LOCAL_ARTIFACT_BASE_DIR") or ".").strip()
    return Path(base_dir)


def _stage_manifest_path(stage_script: str) -> Path:
    return _local_artifact_base_dir() / "reports" / "stages" / f"{stage_script.removesuffix('.py')}_manifest.json"


def _summary_path(artifact_path: str) -> Path:
    return _local_artifact_base_dir() / Path(artifact_path)


def _path_artifact_manifest(path: Path, *, artifact_version: str | None = None, extra: dict[str, object] | None = None) -> dict[str, object]:
    payload: dict[str, object] = {
        "path": str(path),
        "exists": path.exists(),
        "kind": "file",
    }
    if artifact_version is not None:
        payload["artifact_version"] = artifact_version
    if extra:
        payload.update(extra)
    return payload


def _load_logged_stage_manifest(stage_script: str) -> dict[str, object] | None:
    path = _stage_manifest_path(stage_script)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _build_stage_group_manifest(
    *,
    stage_group: StageGroupSpec,
    selected_stage_scripts: tuple[str, ...],
    summary: StageGroupRunSummary,
) -> dict[str, object]:
    first_child_manifest = (
        _load_logged_stage_manifest(selected_stage_scripts[0]) if selected_stage_scripts else None
    ) or {}
    replayable_from = [str(item) for item in (first_child_manifest.get("replayable_from") or [])]
    input_artifacts = dict(first_child_manifest.get("input_artifacts") or {})
    output_artifacts: dict[str, dict[str, object]] = {
        "stage_group_summary": _path_artifact_manifest(
            _summary_path(stage_group.summary_artifact_path),
            artifact_version="STAGE_GROUP_RUN_SUMMARY_V1",
            extra={"pipeline_mode": stage_group.pipeline_mode},
        ),
    }
    for stage_script in selected_stage_scripts:
        output_artifacts[f"{stage_script.removesuffix('.py')}_manifest"] = _path_artifact_manifest(
            _stage_manifest_path(stage_script),
            artifact_version="STAGE_MANIFEST_V2",
            extra={"stage_script": stage_script},
        )
    return build_stage_manifest(
        stage_name=stage_group.run_name,
        stage_version="v2",
        config={
            "pipeline_mode": stage_group.pipeline_mode,
            "selected_stage_scripts": list(selected_stage_scripts),
            "start_stage_script": summary.start_stage_script,
            "end_stage_script": summary.end_stage_script,
        },
        input_artifacts=input_artifacts,
        output_artifacts=output_artifacts,
        replayable_from=replayable_from,
    )


@log_memory_usage(label="pipeline_stage_group")
def run_stage_group(
    *,
    spec: StageGroupSpec | None = None,
    run_name: str | None = None,
    pipeline_mode: str | None = None,
    stage_scripts: list[str] | tuple[str, ...] | None = None,
    summary_artifact_path: str | None = None,
    manifest_artifact_path: str | None = None,
    logger_name: str | None = None,
    start_stage_script: str | None = None,
    end_stage_script: str | None = None,
    replay_run_dir: str | None = None,
) -> StageGroupRunSummary:
    stage_group = _coerce_stage_group_spec(
        spec=spec,
        run_name=run_name,
        pipeline_mode=pipeline_mode,
        stage_scripts=stage_scripts,
        summary_artifact_path=summary_artifact_path,
        manifest_artifact_path=manifest_artifact_path,
        logger_name=logger_name,
    )
    logger = get_logger(stage_group.logger_name)
    run_start = time.perf_counter()
    selected_stage_scripts = _select_stage_scripts(
        stage_group,
        start_stage_script=start_stage_script,
        end_stage_script=end_stage_script,
    )
    if replay_run_dir is not None and start_stage_script is not None and selected_stage_scripts:
        _validate_replay_inputs(
            replay_run_dir=str(replay_run_dir),
            first_stage_script=selected_stage_scripts[0],
        )

    with pipeline_run_context(
        run_name=stage_group.run_name,
        logger=logger,
        tags=stage_group.resolved_tags(),
    ):
        parent_run_id = active_run_id()
        logger.info("pipeline_parent_run=%s run_id=%s", stage_group.run_name, parent_run_id)

        pipeline_dir = Path(__file__).resolve().parent
        stage_results: list[StageExecutionResult] = []
        failure: Exception | None = None

        for stage_script in selected_stage_scripts:
            stage_path = pipeline_dir / stage_script
            stage_start = time.perf_counter()
            logger.info("stage_start script=%s", stage_script)
            try:
                runpy.run_path(str(stage_path), run_name="__main__")
                elapsed_ms = (time.perf_counter() - stage_start) * 1000.0
                stage_results.append(
                    StageExecutionResult(
                        stage_script=stage_script,
                        status="success",
                        elapsed_ms=elapsed_ms,
                    )
                )
                logger.info("stage_end script=%s elapsed_ms=%.3f", stage_script, elapsed_ms)
            except Exception as exc:
                elapsed_ms = (time.perf_counter() - stage_start) * 1000.0
                stage_results.append(
                    StageExecutionResult(
                        stage_script=stage_script,
                        status="failed",
                        elapsed_ms=elapsed_ms,
                        error=str(exc),
                    )
                )
                logger.exception("stage_failed script=%s elapsed_ms=%.3f", stage_script, elapsed_ms)
                failure = exc
                break

        total_elapsed_ms = (time.perf_counter() - run_start) * 1000.0
        completed_count = sum(1 for item in stage_results if item.status == "success")
        failed_count = sum(1 for item in stage_results if item.status == "failed")

        summary = StageGroupRunSummary(
            project="S3NTINEL",
            run_name=stage_group.run_name,
            pipeline_mode=stage_group.pipeline_mode,
            parent_run_id=parent_run_id,
            status=("failed" if failure else "success"),
            total_elapsed_ms=total_elapsed_ms,
            stage_count=len(stage_group.stage_scripts),
            selected_stage_count=len(selected_stage_scripts),
            completed_stage_count=completed_count,
            failed_stage_count=failed_count,
            stages=tuple(stage_results),
            replay_run_dir=replay_run_dir,
            start_stage_script=start_stage_script,
            end_stage_script=end_stage_script,
            memory_snapshot_end=capture_memory_snapshot(
            label=stage_group.run_name,
            event="pipeline_summary",
            started_at=run_start,
            status=("failed" if failure else "success"),
            include_spark=True,
            ),
        )

        log_metric_if_active("pipeline_total_elapsed_ms", total_elapsed_ms)
        log_metric_if_active("pipeline_completed_stage_count", float(completed_count))
        log_metric_if_active("pipeline_failed_stage_count", float(failed_count))
        log_dict_artifact_if_active(summary.to_payload(), stage_group.summary_artifact_path)
        if stage_group.manifest_artifact_path is not None:
            stage_group_manifest = _build_stage_group_manifest(
                stage_group=stage_group,
                selected_stage_scripts=selected_stage_scripts,
                summary=summary,
            )
            log_stage_manifest_if_active(stage_group_manifest, stage_group.manifest_artifact_path)

        logger.info(
            "pipeline_summary mode=%s status=%s completed=%s failed=%s total_elapsed_ms=%.3f",
            stage_group.pipeline_mode,
            summary.status,
            completed_count,
            failed_count,
            total_elapsed_ms,
        )

        if failure is not None:
            raise failure
        return summary
