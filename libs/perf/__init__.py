# File: libs/perf/__init__.py
"""Performance annotations and helpers."""

from libs.perf.annotations import hot_path, is_hot_path, log_wall_time
from libs.perf.logger import get_logger
from libs.perf.mlflow import (
	active_run_id,
	log_artifact_if_active,
	log_dict_artifact_if_active,
	log_metric_if_active,
	log_params_if_active,
	pipeline_run_context,
	register_model_if_available,
	track_mlflow_run,
)

__all__ = [
	"hot_path",
	"is_hot_path",
	"log_wall_time",
	"get_logger",
	"track_mlflow_run",
	"pipeline_run_context",
	"active_run_id",
	"log_metric_if_active",
	"log_params_if_active",
	"log_dict_artifact_if_active",
	"log_artifact_if_active",
	"register_model_if_available",
]

