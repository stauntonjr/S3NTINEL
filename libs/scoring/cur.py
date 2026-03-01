# File: libs/scoring/cur.py
"""CUR reconstruction-error scoring utilities."""

from __future__ import annotations

from libs.perf.annotations import hot_path


@hot_path
def cur_distance(values: list[float]) -> float:
    # HOT PATH: reconstruction distance is central to anomaly scoring and must stay allocation-light.
    return float(sum(value * value for value in values))
