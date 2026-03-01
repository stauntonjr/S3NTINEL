# File: libs/conformal/buffers.py
"""Phase-conditioned calibration buffers."""

from __future__ import annotations

from libs.perf.annotations import hot_path


@hot_path
def is_warm(buffer_size: int, min_warm: int) -> bool:
    # HOT PATH: warm-check is on critical scoring path; keep branch-free/simple.
    return buffer_size >= min_warm
