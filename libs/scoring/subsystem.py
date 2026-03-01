# File: libs/scoring/subsystem.py
"""Subsystem projection helpers."""

from __future__ import annotations

from libs.perf.annotations import hot_path


@hot_path
def subsystem_score(block_scores: dict[str, float], factor: float = 1.0) -> float:
    # HOT PATH: subsystem projection must scale across many subsystem partitions and sensors.
    return float(sum(block_scores.values()) * factor)
