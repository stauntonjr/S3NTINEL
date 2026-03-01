# File: libs/scoring/events.py
"""Event-block scoring utilities."""

from __future__ import annotations

from libs.perf.annotations import hot_path


@hot_path
def event_distance(flags: list[int]) -> float:
    # HOT PATH: event distance executes at window cadence over sparse blocks; keep sparse-friendly operations.
    return float(sum(flags))
