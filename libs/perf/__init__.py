# File: libs/perf/__init__.py
"""Performance annotations and helpers."""

from libs.perf.annotations import hot_path, is_hot_path, log_wall_time
from libs.perf.logger import get_logger
from libs.perf.mlflow import (
    active_run_id,
    log_artifact_if_active,
    log_dataframe_dataset_if_active,
    log_dict_artifact_if_active,
    log_metric_if_active,
    log_params_if_active,
    pipeline_run_context,
    register_model_if_available,
    track_mlflow_run,
)
from libs.perf.memory import (
    capture_memory_snapshot,
    log_memory_artifact_if_active,
    log_memory_snapshot,
    log_memory_usage,
    memory_observability_enabled,
    memory_observability_mode,
    spark_memory_collection_enabled,
)
from libs.perf.stage_manifest import (
    build_artifact_manifest,
    build_stage_manifest,
    log_stage_manifest_if_active,
    schema_hash_for_dataframe,
    schema_snapshot_for_dataframe,
)

__all__ = [
    "hot_path",
    "is_hot_path",
    "log_wall_time",
    "get_logger",
    "track_mlflow_run",
    "pipeline_run_context",
    "active_run_id",
    "log_dataframe_dataset_if_active",
    "log_metric_if_active",
    "log_params_if_active",
    "log_dict_artifact_if_active",
    "log_artifact_if_active",
    "register_model_if_available",
    "log_memory_usage",
    "log_memory_snapshot",
    "log_memory_artifact_if_active",
    "capture_memory_snapshot",
    "memory_observability_enabled",
    "memory_observability_mode",
    "spark_memory_collection_enabled",
    "schema_snapshot_for_dataframe",
    "schema_hash_for_dataframe",
    "build_artifact_manifest",
    "build_stage_manifest",
    "log_stage_manifest_if_active",
]
