"""Memory observability helpers for pipeline stages and grouped runners."""

from __future__ import annotations

import functools
import logging
import os
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from libs.perf.logger import get_logger
from libs.perf.mlflow import log_dict_artifact_if_active, log_metric_if_active


def _env_flag(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, str(default))).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def memory_observability_enabled() -> bool:
    return _env_flag("S3NTINEL_OBSERVABILITY_MEMORY_ENABLED", False)


def memory_observability_mode() -> str:
    raw = str(os.getenv("S3NTINEL_OBSERVABILITY_MEMORY_MODE", "light")).strip().lower()
    return raw if raw in {"light", "detailed"} else "light"


def spark_memory_collection_enabled() -> bool:
    return _env_flag("S3NTINEL_OBSERVABILITY_MEMORY_SPARK_ENABLED", True)


def _artifact_emission_enabled() -> bool:
    return _env_flag("S3NTINEL_OBSERVABILITY_MEMORY_ARTIFACTS_ENABLED", True)


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except Exception:
        return None


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def _read_resource_peak_rss_bytes() -> int | None:
    try:
        import resource

        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except Exception:
        return None
    if peak is None:
        return None
    peak = int(peak)
    if peak <= 0:
        return None
    if os.name == "posix" and "darwin" not in os.sys.platform:
        return peak * 1024
    return peak


def _capture_process_memory() -> dict[str, Any]:
    payload: dict[str, Any] = {
        "rss_bytes": None,
        "vms_bytes": None,
        "shared_bytes": None,
        "text_bytes": None,
        "data_bytes": None,
        "uss_bytes": None,
        "pss_bytes": None,
        "peak_rss_bytes": _read_resource_peak_rss_bytes(),
        "provider": "resource",
    }
    try:
        import psutil  # type: ignore

        proc = psutil.Process()
        mem = proc.memory_info()
        payload.update(
            {
                "rss_bytes": _safe_int(getattr(mem, "rss", None)),
                "vms_bytes": _safe_int(getattr(mem, "vms", None)),
                "shared_bytes": _safe_int(getattr(mem, "shared", None)),
                "text_bytes": _safe_int(getattr(mem, "text", None)),
                "data_bytes": _safe_int(getattr(mem, "data", None)),
                "provider": "psutil",
            }
        )
        try:
            full_mem = proc.memory_full_info()
            payload["uss_bytes"] = _safe_int(getattr(full_mem, "uss", None))
            payload["pss_bytes"] = _safe_int(getattr(full_mem, "pss", None))
        except Exception:
            pass
    except Exception:
        pass
    return payload


def _scala_map_to_python(scala_map: Any) -> list[tuple[Any, Any]]:
    entries: list[tuple[Any, Any]] = []
    iterator = scala_map.iterator()
    while iterator.hasNext():
        entry = iterator.next()
        entries.append((entry._1(), entry._2()))
    return entries


def _tuple2_values(tuple2: Any) -> tuple[Any, Any]:
    try:
        return tuple2._1(), tuple2._2()
    except Exception:
        try:
            return tuple2[0], tuple2[1]
        except Exception:
            return None, None


def _executor_id_from_block_manager_id(block_manager_id: Any) -> str:
    for attr in ("executorId", "executorIdString"):
        accessor = getattr(block_manager_id, attr, None)
        if accessor is None:
            continue
        try:
            value = accessor()
        except TypeError:
            value = accessor
        if value not in (None, ""):
            return str(value)
    return str(block_manager_id)


def _executor_host_port(block_manager_id: Any) -> str:
    host = None
    port = None
    for attr in ("host",):
        accessor = getattr(block_manager_id, attr, None)
        if accessor is None:
            continue
        try:
            host = accessor()
        except TypeError:
            host = accessor
    for attr in ("port",):
        accessor = getattr(block_manager_id, attr, None)
        if accessor is None:
            continue
        try:
            port = accessor()
        except TypeError:
            port = accessor
    if host is None:
        return str(block_manager_id)
    if port is None:
        return str(host)
    return f"{host}:{port}"


def _collect_spark_memory_summary(spark: Any) -> tuple[dict[str, Any] | None, list[str]]:
    warnings: list[str] = []
    if spark is None:
        return None, warnings
    if memory_observability_mode() != "detailed":
        warnings.append("spark_memory_detail_disabled")
        return None, warnings
    if not spark_memory_collection_enabled():
        warnings.append("spark_memory_collection_disabled")
        return None, warnings

    try:
        sc = spark.sparkContext
        conf_get = getattr(sc, "getConf", None)
        conf = conf_get() if callable(conf_get) else None
        memory_status = sc._jsc.sc().getExecutorMemoryStatus()
        entries = _scala_map_to_python(memory_status)
    except Exception as exc:
        warnings.append(f"spark_memory_collection_failed:{exc.__class__.__name__}")
        return None, warnings

    executors: list[dict[str, Any]] = []
    total_max = 0
    total_remaining = 0
    total_used = 0
    for block_manager_id, status in entries:
        max_mem, remaining_mem = _tuple2_values(status)
        max_bytes = _safe_int(max_mem) or 0
        remaining_bytes = _safe_int(remaining_mem) or 0
        used_bytes = max(max_bytes - remaining_bytes, 0)
        total_max += max_bytes
        total_remaining += remaining_bytes
        total_used += used_bytes
        executors.append(
            {
                "executor_id": _executor_id_from_block_manager_id(block_manager_id),
                "host_port": _executor_host_port(block_manager_id),
                "max_memory_bytes": max_bytes,
                "remaining_memory_bytes": remaining_bytes,
                "used_memory_bytes": used_bytes,
            }
        )

    config: dict[str, Any] = {}
    if conf is not None:
        for key in (
            "spark.master",
            "spark.app.name",
            "spark.executor.memory",
            "spark.executor.instances",
            "spark.executor.memoryOverhead",
            "spark.driver.memory",
        ):
            try:
                value = conf.get(key, None)
            except TypeError:
                value = conf.get(key)
            except Exception:
                value = None
            if value not in (None, ""):
                config[key] = str(value)

    summary = {
        "provider": "spark_executor_memory_status",
        "executor_count": len(executors),
        "max_memory_bytes": total_max,
        "remaining_memory_bytes": total_remaining,
        "used_memory_bytes": total_used,
        "executors": executors,
        "spark_conf": config,
        "note": "Spark executor memory status is best-effort and may reflect storage-memory availability rather than full JVM heap usage.",
    }
    return summary, warnings


def _resolve_active_spark_session() -> Any | None:
    try:
        from pyspark.sql import SparkSession

        active = SparkSession.getActiveSession()
        if active is not None:
            return active
        return SparkSession.getDefaultSession()
    except Exception:
        return None


def capture_memory_snapshot(
    *,
    label: str,
    event: str,
    started_at: float | None = None,
    status: str | None = None,
    spark: Any | None = None,
    include_spark: bool | None = None,
) -> dict[str, Any]:
    """Capture a best-effort process and Spark memory snapshot."""
    timestamp = datetime.now(UTC).isoformat()
    process = _capture_process_memory()
    elapsed_ms = (time.perf_counter() - started_at) * 1000.0 if started_at is not None else None

    snapshot: dict[str, Any] = {
        "label": label,
        "event": event,
        "status": status or event,
        "timestamp_utc": timestamp,
        "mode": memory_observability_mode(),
        "driver_process": process,
        "elapsed_ms": elapsed_ms,
        "warnings": [],
    }

    should_include_spark = include_spark
    if should_include_spark is None:
        should_include_spark = memory_observability_mode() == "detailed"
    if should_include_spark:
        spark_session = spark if spark is not None else _resolve_active_spark_session()
        spark_summary, warnings = _collect_spark_memory_summary(spark_session)
        snapshot["spark"] = spark_summary
        snapshot["warnings"].extend(warnings)

    return snapshot


def _log_snapshot_metrics(snapshot: dict[str, Any]) -> None:
    label = str(snapshot.get("label", "memory")).replace(".", "_")
    event = str(snapshot.get("event", "snapshot")).replace(".", "_")
    prefix = f"{label}_{event}"
    driver = snapshot.get("driver_process", {})
    metrics = {
        f"{prefix}_rss_bytes": driver.get("rss_bytes"),
        f"{prefix}_vms_bytes": driver.get("vms_bytes"),
        f"{prefix}_peak_rss_bytes": driver.get("peak_rss_bytes"),
        f"{prefix}_elapsed_ms": snapshot.get("elapsed_ms"),
    }
    spark = snapshot.get("spark")
    if isinstance(spark, dict):
        metrics[f"{prefix}_spark_executor_count"] = spark.get("executor_count")
        metrics[f"{prefix}_spark_used_memory_bytes"] = spark.get("used_memory_bytes")
        metrics[f"{prefix}_spark_max_memory_bytes"] = spark.get("max_memory_bytes")
    for metric_name, metric_value in metrics.items():
        value = _safe_float(metric_value)
        if value is not None:
            log_metric_if_active(metric_name, value)


def log_memory_artifact_if_active(snapshot: dict[str, Any], artifact_file: str) -> None:
    if not _artifact_emission_enabled():
        return
    log_dict_artifact_if_active(snapshot, artifact_file)


def log_memory_snapshot(
    *,
    logger: logging.Logger,
    label: str,
    event: str,
    started_at: float | None = None,
    status: str | None = None,
    spark: Any | None = None,
    include_spark: bool | None = None,
    artifact_file: str | None = None,
    level: int = logging.INFO,
) -> dict[str, Any]:
    snapshot = capture_memory_snapshot(
        label=label,
        event=event,
        started_at=started_at,
        status=status,
        spark=spark,
        include_spark=include_spark,
    )
    driver = snapshot.get("driver_process", {})
    logger.log(
        level,
        "memory_snapshot label=%s event=%s status=%s rss_bytes=%s vms_bytes=%s peak_rss_bytes=%s elapsed_ms=%s warnings=%s",
        snapshot.get("label"),
        snapshot.get("event"),
        snapshot.get("status"),
        driver.get("rss_bytes"),
        driver.get("vms_bytes"),
        driver.get("peak_rss_bytes"),
        snapshot.get("elapsed_ms"),
        "|".join(snapshot.get("warnings", [])),
    )
    _log_snapshot_metrics(snapshot)
    if artifact_file:
        log_memory_artifact_if_active(snapshot, artifact_file)
    return snapshot


def log_memory_usage(
    *,
    logger: logging.Logger | None = None,
    label: str | None = None,
    include_spark: bool | None = None,
    artifact_dir: str = "reports/memory",
    level: int = logging.INFO,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator that logs process and optional Spark memory snapshots around a function."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        active_logger = logger or get_logger(func.__module__)
        metric_label = label or f"{func.__module__}.{func.__name__}"

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if not memory_observability_enabled():
                return func(*args, **kwargs)

            started_at = time.perf_counter()
            base_artifact = f"{artifact_dir}/{metric_label.replace('.', '_')}"
            log_memory_snapshot(
                logger=active_logger,
                label=metric_label,
                event="start",
                started_at=started_at,
                status="running",
                include_spark=include_spark,
                artifact_file=f"{base_artifact}_start.json",
                level=level,
            )
            try:
                result = func(*args, **kwargs)
            except Exception:
                log_memory_snapshot(
                    logger=active_logger,
                    label=metric_label,
                    event="failure",
                    started_at=started_at,
                    status="failed",
                    include_spark=include_spark,
                    artifact_file=f"{base_artifact}_failure.json",
                    level=level,
                )
                log_memory_snapshot(
                    logger=active_logger,
                    label=metric_label,
                    event="end",
                    started_at=started_at,
                    status="failed",
                    include_spark=include_spark,
                    artifact_file=f"{base_artifact}_end.json",
                    level=level,
                )
                raise
            log_memory_snapshot(
                logger=active_logger,
                label=metric_label,
                event="success",
                started_at=started_at,
                status="success",
                include_spark=include_spark,
                artifact_file=f"{base_artifact}_success.json",
                level=level,
            )
            log_memory_snapshot(
                logger=active_logger,
                label=metric_label,
                event="end",
                started_at=started_at,
                status="success",
                include_spark=include_spark,
                artifact_file=f"{base_artifact}_end.json",
                level=level,
            )
            return result

        return wrapper

    return decorator
