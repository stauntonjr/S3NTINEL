"""Stage artifact manifest helpers for replayable V2 pipeline stages."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from libs.perf.mlflow import active_run_id, log_dict_artifact_if_active


def _stable_json_dumps(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def schema_snapshot_for_dataframe(dataframe: Any) -> dict[str, Any]:
    """Return a stable schema snapshot for Spark or pandas-like dataframes."""
    schema = getattr(dataframe, "schema", None)
    if schema is not None and hasattr(schema, "jsonValue"):
        return {"kind": "spark", "schema": schema.jsonValue()}

    columns = [str(column) for column in list(getattr(dataframe, "columns", []))]
    dtypes = getattr(dataframe, "dtypes", None)
    if isinstance(dtypes, dict):
        dtype_items = {str(key): str(value) for key, value in dtypes.items()}
    elif dtypes is not None:
        dtype_items = {str(key): str(value) for key, value in list(dtypes)}
    else:
        dtype_items = {}
    return {
        "kind": "pandas",
        "columns": columns,
        "dtypes": dtype_items,
    }


def schema_hash_for_dataframe(dataframe: Any) -> str:
    snapshot = schema_snapshot_for_dataframe(dataframe)
    return hashlib.sha256(_stable_json_dumps(snapshot).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ArtifactManifest:
    path: str
    schema_hash: str
    schema: dict[str, Any]
    row_count: int | None = None
    artifact_version: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "path": self.path,
            "schema_hash": self.schema_hash,
            "schema": self.schema,
        }
        if self.row_count is not None:
            payload["row_count"] = int(self.row_count)
        if self.artifact_version is not None:
            payload["artifact_version"] = str(self.artifact_version)
        if self.extra:
            payload.update(self.extra)
        return payload

    @classmethod
    def from_dataframe(
        cls,
        *,
        path: str,
        dataframe: Any,
        row_count: int | None = None,
        artifact_version: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> "ArtifactManifest":
        return cls(
            path=str(path),
            schema_hash=schema_hash_for_dataframe(dataframe),
            schema=schema_snapshot_for_dataframe(dataframe),
            row_count=(int(row_count) if row_count is not None else None),
            artifact_version=(str(artifact_version) if artifact_version is not None else None),
            extra=dict(extra or {}),
        )


@dataclass(frozen=True)
class StageManifest:
    stage_name: str
    config: dict[str, Any]
    input_artifacts: dict[str, dict[str, Any]]
    output_artifacts: dict[str, dict[str, Any]]
    stage_version: str = "v2"
    run_id: str | None = None
    created_at_utc: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    replayable_from: list[str] = field(default_factory=list)
    cache_artifacts: dict[str, dict[str, Any]] = field(default_factory=dict)
    timing: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "stage_name": self.stage_name,
            "stage_version": self.stage_version,
            "run_id": self.run_id,
            "created_at_utc": self.created_at_utc,
            "config": self.config,
            "input_artifacts": self.input_artifacts,
            "output_artifacts": self.output_artifacts,
        }
        if self.replayable_from:
            payload["replayable_from"] = list(self.replayable_from)
        if self.cache_artifacts:
            payload["cache_artifacts"] = self.cache_artifacts
        if self.timing:
            payload["timing"] = self.timing
        return payload


def build_artifact_manifest(
    *,
    path: str,
    dataframe: Any,
    row_count: int | None = None,
    artifact_version: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return ArtifactManifest.from_dataframe(
        path=path,
        dataframe=dataframe,
        row_count=row_count,
        artifact_version=artifact_version,
        extra=extra,
    ).to_payload()


def build_stage_manifest(
    *,
    stage_name: str,
    config: dict[str, Any],
    input_artifacts: dict[str, dict[str, Any]],
    output_artifacts: dict[str, dict[str, Any]],
    stage_version: str = "v2",
    replayable_from: list[str] | None = None,
    cache_artifacts: dict[str, dict[str, Any]] | None = None,
    timing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return StageManifest(
        stage_name=stage_name,
        stage_version=stage_version,
        run_id=active_run_id(),
        config=config,
        input_artifacts=input_artifacts,
        output_artifacts=output_artifacts,
        replayable_from=list(replayable_from or []),
        cache_artifacts=dict(cache_artifacts or {}),
        timing=dict(timing or {}),
    ).to_payload()


def log_stage_manifest_if_active(stage_manifest: dict[str, Any], artifact_file: str) -> None:
    log_dict_artifact_if_active(stage_manifest, artifact_file)
