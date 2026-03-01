# File: libs/perf/mlflow.py
"""MLflow tracking helpers with safe lazy imports for Databricks runtime."""

from __future__ import annotations

import contextlib
import functools
import logging
from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any

from libs.perf.logger import get_logger


def _get_mlflow() -> Any | None:
    try:
        import mlflow  # type: ignore
    except Exception:
        return None
    return mlflow


def log_metric_if_active(name: str, value: float, step: int | None = None) -> None:
    """Log a metric only when MLflow is available and a run is active."""
    mlflow = _get_mlflow()
    if mlflow is None or mlflow.active_run() is None:
        return
    if step is None:
        mlflow.log_metric(name, value)
        return
    mlflow.log_metric(name, value, step=step)


def log_params_if_active(params: dict[str, Any]) -> None:
    """Log params when an MLflow run is active."""
    mlflow = _get_mlflow()
    if mlflow is None or mlflow.active_run() is None:
        return
    sanitized = {key: str(value) for key, value in params.items()}
    mlflow.log_params(sanitized)


def log_dict_artifact_if_active(payload: dict[str, Any], artifact_file: str) -> None:
    """Log dict payload as a JSON artifact when MLflow run is active."""
    mlflow = _get_mlflow()
    if mlflow is None or mlflow.active_run() is None:
        return
    mlflow.log_dict(payload, artifact_file)


def log_artifact_if_active(local_path: str, artifact_path: str | None = None) -> None:
    """Log local artifact path when MLflow run is active."""
    mlflow = _get_mlflow()
    if mlflow is None or mlflow.active_run() is None:
        return
    mlflow.log_artifact(local_path, artifact_path=artifact_path)


def register_model_if_available(model_uri: str, name: str) -> Any | None:
    """Register a model in MLflow Model Registry when available."""
    mlflow = _get_mlflow()
    if mlflow is None:
        return None
    return mlflow.register_model(model_uri=model_uri, name=name)


def active_run_id() -> str | None:
    """Return the active MLflow run_id when available."""
    mlflow = _get_mlflow()
    if mlflow is None or mlflow.active_run() is None:
        return None
    return str(mlflow.active_run().info.run_id)


def pipeline_run_context(
    run_name: str,
    logger: logging.Logger | None = None,
    tags: dict[str, Any] | None = None,
) -> AbstractContextManager[Any]:
    """Create a parent MLflow run context if MLflow is available, otherwise no-op."""
    active_logger = logger or get_logger("s3ntinel.pipeline")
    mlflow = _get_mlflow()
    if mlflow is None:
        active_logger.info("mlflow_unavailable parent_run=%s", run_name)
        return contextlib.nullcontext()

    run_ctx = mlflow.start_run(run_name=run_name, nested=False)
    if tags:
        sanitized = {key: str(value) for key, value in tags.items()}
        mlflow.set_tags(sanitized)
    return run_ctx


def track_mlflow_run(
    stage_name: str,
    logger: logging.Logger | None = None,
    nested: bool = True,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator to create an MLflow run around a stage when available."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        active_logger = logger or get_logger(func.__module__)

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            mlflow = _get_mlflow()
            if mlflow is None:
                active_logger.info("mlflow_unavailable stage=%s", stage_name)
                return func(*args, **kwargs)

            run_name = f"s3ntinel.{stage_name}"
            with mlflow.start_run(run_name=run_name, nested=nested):
                log_params_if_active(
                    {
                        "project": "S3NTINEL",
                        "stage": stage_name,
                    }
                )
                return func(*args, **kwargs)

        return wrapper

    return decorator
