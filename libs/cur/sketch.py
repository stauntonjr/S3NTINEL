# File: libs/cur/sketch.py
"""Column and row sketch utilities for out-of-core CUR fitting."""

from __future__ import annotations

from libs.perf.annotations import hot_path


@hot_path
def compute_column_sketch() -> dict[str, float]:
    # HOT PATH: sketch computation is large-scale and must remain vectorized/JVM-side in production.
    return {"status": 0.0}
