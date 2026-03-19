# File: pipelines/common.py
"""Common helpers for pipeline jobs."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class PipelineContext:
    config: dict[str, Any]
    tail_id: str | None = None
    flight_id: str | None = None
    date_utc: str | None = None


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
        tail_id=tail_id,
        flight_id=flight_id,
        date_utc=date_utc,
    )


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
