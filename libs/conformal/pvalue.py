# File: libs/conformal/pvalue.py
"""Conformal p-value computations."""

from __future__ import annotations

from libs.perf.annotations import hot_path


@hot_path
def right_tail_pvalue(buffer: list[float], score: float) -> float | None:
    # HOT PATH: p-value computation is window-frequency critical; replace with vectorized rank ops at scale.
    if not buffer:
        return None
    exceedances = sum(1 for value in buffer if value >= score)
    return exceedances / len(buffer)
