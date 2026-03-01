# File: libs/scoring/fuse.py
"""Block/subsystem/global fusion utilities."""

from __future__ import annotations

from libs.perf.annotations import hot_path


@hot_path
def weighted_sum(values: list[float], weights: list[float]) -> float:
    # HOT PATH: global fusion runs every window; ensure numerically stable and vectorizable reductions.
    return float(sum(value * weight for value, weight in zip(values, weights, strict=False)))
