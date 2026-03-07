"""Stage artifact manifest helpers for replayable V2 pipeline stages."""

from __future__ import annotations

import hashlib
import json
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


def build_artifact_manifest(
    *,
    path: str,
    dataframe: Any,
    row_count: int | None = None,
    artifact_version: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    artifact_manifest: dict[str, Any] = {
        "path": path,
        "schema_hash": schema_hash_for_dataframe(dataframe),
        "schema": schema_snapshot_for_dataframe(dataframe),
    }
    if row_count is not None:
        artifact_manifest["row_count"] = int(row_count)
    if artifact_version is not None:
        artifact_manifest["artifact_version"] = str(artifact_version)
    if extra:
        artifact_manifest.update(extra)
    return artifact_manifest


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
    manifest: dict[str, Any] = {
        "stage_name": stage_name,
        "stage_version": stage_version,
        "run_id": active_run_id(),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "config": config,
        "input_artifacts": input_artifacts,
        "output_artifacts": output_artifacts,
    }
    if replayable_from:
        manifest["replayable_from"] = list(replayable_from)
    if cache_artifacts:
        manifest["cache_artifacts"] = cache_artifacts
    if timing:
        manifest["timing"] = timing
    return manifest


def log_stage_manifest_if_active(stage_manifest: dict[str, Any], artifact_file: str) -> None:
    log_dict_artifact_if_active(stage_manifest, artifact_file)
