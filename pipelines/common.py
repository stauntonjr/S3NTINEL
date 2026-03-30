# File: pipelines/common.py
"""Common helpers for pipeline jobs."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from libs.config import (
    PipelineArtifactPaths,
    PipelineContextSettings,
    PipelineExecutionSettings,
    load_pipeline_artifact_paths,
    load_pipeline_context_settings,
    load_pipeline_execution_settings,
)


@dataclass(frozen=True)
class PipelineContext:
    config: dict[str, Any]
    execution: PipelineExecutionSettings
    artifacts: PipelineArtifactPaths
    settings: PipelineContextSettings
    tail_id: str | None = None
    flight_id: str | None = None
    date_utc: str | None = None


@dataclass(frozen=True)
class StageReportPaths:
    summary_artifact_path: str
    manifest_artifact_path: str

    @classmethod
    def for_stage(cls, stage_name: str) -> "StageReportPaths":
        return cls(
            summary_artifact_path=f"reports/stages/{stage_name}_summary.json",
            manifest_artifact_path=f"reports/stages/{stage_name}_manifest.json",
        )


@dataclass(frozen=True)
class StageRuntime:
    stage_name: str
    context: PipelineContext
    execution: PipelineExecutionSettings
    artifacts: PipelineArtifactPaths
    settings: PipelineContextSettings
    report_paths: StageReportPaths


@lru_cache(maxsize=1)
def load_defaults() -> dict[str, Any]:
    config_path = Path(__file__).resolve().parent.parent / "conf" / "defaults.yaml"
    with config_path.open("r", encoding="utf-8") as file_obj:
        return yaml.safe_load(file_obj)


def build_context(
    tail_id: str | None = None,
    flight_id: str | None = None,
    date_utc: str | None = None,
) -> PipelineContext:
    return PipelineContext(
        config=deepcopy(load_defaults()),
        execution=load_pipeline_execution_settings(),
        artifacts=load_pipeline_artifact_paths(),
        settings=load_pipeline_context_settings(load_defaults()),
        tail_id=tail_id,
        flight_id=flight_id,
        date_utc=date_utc,
    )


def build_stage_runtime(
    stage_name: str,
    *,
    tail_id: str | None = None,
    flight_id: str | None = None,
    date_utc: str | None = None,
) -> StageRuntime:
    context = build_context(tail_id=tail_id, flight_id=flight_id, date_utc=date_utc)
    return StageRuntime(
        stage_name=stage_name,
        context=context,
        execution=context.execution,
        artifacts=context.artifacts,
        settings=context.settings,
        report_paths=StageReportPaths.for_stage(stage_name),
    )


def context_execution(context: Any) -> PipelineExecutionSettings:
    return getattr(context, "execution", load_pipeline_execution_settings())


def context_artifacts(context: Any) -> PipelineArtifactPaths:
    return getattr(context, "artifacts", load_pipeline_artifact_paths())


def context_settings(context: Any) -> PipelineContextSettings:
    config = getattr(context, "config", load_defaults())
    return getattr(context, "settings", load_pipeline_context_settings(config))


def require_artifact_path(path: str, *, env_name: str, artifact_name: str) -> Path:
    resolved = Path(str(path).strip()) if str(path).strip() else None
    if resolved is None:
        raise RuntimeError(
            f"{artifact_name} is required for this stage; expected {env_name} to point to the canonical persisted artifact."
        )
    if not resolved.exists():
        raise RuntimeError(
            f"{artifact_name} is required for this stage; {env_name}={resolved} does not exist."
        )
    return resolved
