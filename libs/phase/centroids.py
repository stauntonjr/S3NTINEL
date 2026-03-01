# File: libs/phase/centroids.py
"""Phase centroid utilities for per-tail state."""

from __future__ import annotations

from libs.perf.annotations import hot_path


@hot_path
def nearest_centroid_index(distances: list[float]) -> int | None:
    # HOT PATH: nearest-centroid lookup is frequent; replace with vectorized nearest-neighbor search as scale grows.
    if not distances:
        return None
    return min(range(len(distances)), key=lambda index: distances[index])
