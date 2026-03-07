# File: libs/perf/mlflow.py
"""MLflow tracking helpers with safe lazy imports for Databricks runtime."""

from __future__ import annotations

import contextlib
import functools
import logging
import os
import uuid
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


def _resolve_experiment_name() -> str:
    return str(
        os.getenv(
            "S3NTINEL_MLFLOW_EXPERIMENT",
            os.getenv("MLFLOW_EXPERIMENT_NAME", "S3NTINEL"),
        )
    ).strip()


def _set_experiment_if_available(mlflow: Any, logger: logging.Logger, stage_or_run: str) -> None:
    experiment_name = _resolve_experiment_name()
    if not experiment_name:
        return
    try:
        mlflow.set_experiment(experiment_name)
    except Exception as exc:
        logger.warning(
            "mlflow_set_experiment_failed target=%s stage_or_run=%s reason=%s",
            experiment_name,
            stage_or_run,
            exc.__class__.__name__,
        )


def _trace_session_metadata_key() -> str:
    try:
        from mlflow.tracing.constant import TraceMetadataKey  # type: ignore

        return str(TraceMetadataKey.TRACE_SESSION)
    except Exception:
        return "mlflow.trace.session"


def _resolve_session_id(default_value: str | None = None) -> str:
    from_env = str(os.getenv("S3NTINEL_MLFLOW_SESSION_ID", "")).strip()
    if from_env:
        return from_env
    if default_value:
        return str(default_value)
    return f"s3ntinel-session-{uuid.uuid4().hex[:12]}"


def _log_dataset_records_if_active(
    *,
    mlflow: Any,
    logger: logging.Logger,
    name: str,
    records: list[dict[str, Any]],
    context: str,
) -> None:
    if mlflow.active_run() is None:
        return
    try:
        import pandas as pd  # type: ignore

        df = pd.DataFrame(records)
        dataset = mlflow.data.from_pandas(df, name=name)
        mlflow.log_input(dataset, context=context)
    except Exception as exc:
        logger.info(
            "mlflow_log_input_skipped name=%s context=%s reason=%s",
            name,
            context,
            exc.__class__.__name__,
        )


def log_dataframe_dataset_if_active(
    *,
    name: str,
    dataframe: Any,
    context: str,
    logger: logging.Logger | None = None,
) -> None:
    """Log a DataFrame as an MLflow input dataset when a run is active.

    Uses `mlflow.data.from_spark` when available and falls back to a small pandas
    sample for non-Spark DataFrames.
    """
    mlflow = _get_mlflow()
    if mlflow is None or mlflow.active_run() is None:
        return

    active_logger = logger or get_logger("s3ntinel.mlflow")
    columns: list[str] = []
    row_count: int | None = None
    schema_repr = ""
    partition_count: int | None = None

    try:
        columns = [str(col) for col in list(getattr(dataframe, "columns", []))]
    except Exception:
        columns = []

    try:
        if hasattr(dataframe, "count"):
            row_count = int(dataframe.count())
    except Exception:
        row_count = None

    try:
        schema = getattr(dataframe, "schema", None)
        if schema is not None:
            schema_repr = str(schema.simpleString() if hasattr(schema, "simpleString") else schema)
    except Exception:
        schema_repr = ""

    try:
        rdd = getattr(dataframe, "rdd", None)
        if rdd is not None and hasattr(rdd, "getNumPartitions"):
            partition_count = int(rdd.getNumPartitions())
    except Exception:
        partition_count = None

    _log_dataset_records_if_active(
        mlflow=mlflow,
        logger=active_logger,
        name=name,
        records=[
            {
                "dataset_name": name,
                "context": context,
                "row_count": row_count if row_count is not None else -1,
                "column_count": len(columns),
                "columns": "|".join(columns[:200]),
                "schema": schema_repr,
                "partition_count": partition_count if partition_count is not None else -1,
            }
        ],
        context=context,
    )


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

    _set_experiment_if_available(mlflow, active_logger, run_name)

    @contextlib.contextmanager
    def _ctx() -> Any:
        with mlflow.start_run(run_name=run_name, nested=False) as run:
            if tags:
                sanitized = {key: str(value) for key, value in tags.items()}
                mlflow.set_tags(sanitized)

            session_id = _resolve_session_id(str(run.info.run_id))
            prior_session = os.getenv("S3NTINEL_MLFLOW_SESSION_ID")
            os.environ["S3NTINEL_MLFLOW_SESSION_ID"] = session_id

            _log_dataset_records_if_active(
                mlflow=mlflow,
                logger=active_logger,
                name="pipeline_run_metadata",
                records=[
                    {
                        "run_name": run_name,
                        "run_id": str(run.info.run_id),
                        "session_id": session_id,
                        "project": "S3NTINEL",
                    }
                ],
                context="pipeline_run",
            )

            try:
                yield run
            finally:
                if prior_session is None:
                    os.environ.pop("S3NTINEL_MLFLOW_SESSION_ID", None)
                else:
                    os.environ["S3NTINEL_MLFLOW_SESSION_ID"] = prior_session

    return _ctx()


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
            _set_experiment_if_available(mlflow, active_logger, run_name)
            with mlflow.start_run(run_name=run_name, nested=nested):
                run_id = active_run_id()
                session_id = _resolve_session_id(run_id)

                log_params_if_active(
                    {
                        "project": "S3NTINEL",
                        "stage": stage_name,
                        "session_id": session_id,
                    }
                )

                _log_dataset_records_if_active(
                    mlflow=mlflow,
                    logger=active_logger,
                    name=f"{stage_name}_run_metadata",
                    records=[
                        {
                            "stage": stage_name,
                            "run_name": run_name,
                            "run_id": str(run_id or ""),
                            "session_id": session_id,
                        }
                    ],
                    context="stage_run",
                )

                trace_ctx: AbstractContextManager[Any] = contextlib.nullcontext()
                try:
                    trace_ctx = mlflow.start_span(
                        name=f"{run_name}.trace",
                        span_type="CHAIN",
                        attributes={
                            "project": "S3NTINEL",
                            "stage": stage_name,
                            "run_id": str(run_id or ""),
                        },
                    )
                except Exception as exc:
                    active_logger.info(
                        "mlflow_trace_disabled stage=%s reason=%s",
                        stage_name,
                        exc.__class__.__name__,
                    )

                with trace_ctx:
                    if hasattr(mlflow, "update_current_trace"):
                        try:
                            mlflow.update_current_trace(
                                metadata={_trace_session_metadata_key(): session_id}
                            )
                        except Exception as exc:
                            active_logger.info(
                                "mlflow_trace_metadata_skipped stage=%s reason=%s",
                                stage_name,
                                exc.__class__.__name__,
                            )
                    return func(*args, **kwargs)

        return wrapper

    return decorator
