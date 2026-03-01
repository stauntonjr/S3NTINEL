# File: libs/windows/zoh.py
"""Zero-order-hold feature assembly."""

from __future__ import annotations


def zoh_snapshot(last_seen: dict[str, str]) -> dict[str, str]:
    # HOT PATH: snapshot assembly is window-frequency critical; avoid deep copies in production paths.
    return dict(last_seen)
