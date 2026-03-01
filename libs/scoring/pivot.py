# File: libs/scoring/pivot.py
"""Pivot-block scoring utilities."""

from __future__ import annotations

from libs.perf.annotations import hot_path


@hot_path
def pivot_distance(values: list[float]) -> float:
    # HOT PATH: per-window continuous distance metric; use optimized vector backends for production workloads.
    return float(sum(value * value for value in values))
