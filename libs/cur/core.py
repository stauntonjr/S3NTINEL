# File: libs/cur/core.py
"""CUR core matrix utilities."""

from __future__ import annotations

from libs.perf.annotations import hot_path


@hot_path
def build_u_core() -> list[list[float]]:
    # HOT PATH: core matrix construction should be delegated to optimized linear algebra backends.
    return [[1.0]]
