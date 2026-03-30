"""Shared stage-sequence plan objects for grouped runners and simulation modes."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class StageRunPlan:
    run_name: str
    pipeline_mode: str
    stage_scripts: tuple[str, ...]
    summary_artifact_path: str
    manifest_artifact_path: str | None = None
    tags: dict[str, str] = field(default_factory=dict)

    def resolved_tags(self) -> dict[str, str]:
        return {
            "project": "S3NTINEL",
            "pipeline_mode": self.pipeline_mode,
            **dict(self.tags),
        }

    def selected_stage_scripts(
        self,
        *,
        start_stage: str | None = None,
        end_stage: str | None = None,
    ) -> list[str]:
        scripts = list(self.stage_scripts)
        if not scripts:
            return []
        if start_stage is not None and start_stage not in scripts:
            raise RuntimeError(f"unknown start stage {start_stage!r} for mode {self.pipeline_mode}")
        if end_stage is not None and end_stage not in scripts:
            raise RuntimeError(f"unknown end stage {end_stage!r} for mode {self.pipeline_mode}")
        start_idx = scripts.index(start_stage) if start_stage is not None else 0
        end_idx = scripts.index(end_stage) if end_stage is not None else (len(scripts) - 1)
        if start_idx > end_idx:
            raise RuntimeError(
                f"invalid stage range for mode {self.pipeline_mode}: start={start_stage!r} end={end_stage!r}"
            )
        return scripts[start_idx : end_idx + 1]
