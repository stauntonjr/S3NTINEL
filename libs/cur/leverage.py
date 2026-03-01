# File: libs/cur/leverage.py
"""Leverage score estimation from sketches."""

from __future__ import annotations

from libs.perf.annotations import hot_path


@hot_path
def estimate_leverage_scores() -> list[float]:
    # HOT PATH: leverage estimation runs over wide sensor spaces; avoid row-wise Python loops at scale.
    return []
