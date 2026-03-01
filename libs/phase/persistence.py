# File: libs/phase/persistence.py
"""Persistence evidence accumulation logic."""

from __future__ import annotations

from libs.perf.annotations import hot_path


@hot_path
def update_persistence(previous: float, drift: float, breadth: float, delta_t: float) -> float:
    # HOT PATH: persistence update executes every scoring step; keep arithmetic simple and stable.
    return previous + drift * breadth * delta_t
