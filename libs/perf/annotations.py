# File: libs/perf/annotations.py
"""Machine-discoverable function annotations for performance-sensitive code paths."""

from __future__ import annotations

import functools
import logging
import time
from collections.abc import Callable
from typing import Any

from libs.perf.logger import get_logger
from libs.perf.mlflow import log_metric_if_active


def hot_path(func: Callable[..., Any]) -> Callable[..., Any]:
    """Mark a function as performance-critical (hot path)."""
    setattr(func, "__hot_path__", True)
    return func


def is_hot_path(func: Callable[..., Any]) -> bool:
    """Return whether a function has been marked with ``@hot_path``."""
    return bool(getattr(func, "__hot_path__", False))


def log_wall_time(
    logger: logging.Logger | None = None,
    level: int = logging.INFO,
    label: str | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator that logs wall-clock execution time for a function."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        active_logger = logger or get_logger(func.__module__)
        metric_label = label or f"{func.__module__}.{func.__name__}"

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            start = time.perf_counter()
            try:
                return func(*args, **kwargs)
            finally:
                elapsed_ms = (time.perf_counter() - start) * 1000.0
                active_logger.log(level, "wall_time_ms=%.3f label=%s", elapsed_ms, metric_label)
                metric_name = metric_label.replace(".", "_") + "_wall_time_ms"
                log_metric_if_active(metric_name, elapsed_ms)

        return wrapper

    return decorator
