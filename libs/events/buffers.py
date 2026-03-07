# File: libs/events/buffers.py
"""Per-sensor ZOH and event buffers."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from libs.perf.annotations import hot_path


@hot_path
def update_sensor_buffer(
    sensor: str,
    timestamp_utc: datetime,
    value: str,
    last_seen: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    # HOT PATH: called per sample/event; keep update O(1) and mutation-local.
    entry = {
        "sensor": sensor,
        "timestamp_utc": timestamp_utc,
        "value": value,
    }
    if last_seen is not None:
        last_seen[sensor] = entry
    return entry


def event_value_for_buffer(event: dict[str, Any]) -> str:
    payload = event.get("payload")
    if isinstance(payload, dict):
        if payload.get("to") is not None:
            return str(payload.get("to"))
        if payload.get("value") is not None:
            return str(payload.get("value"))
        if payload.get("state") is not None:
            return str(payload.get("state"))
    return str(event.get("event_type_detected", "unknown"))


def buffer_snapshot(last_seen: dict[str, dict[str, Any]]) -> dict[str, str]:
    snapshot: dict[str, str] = {}
    for sensor, item in last_seen.items():
        value = item.get("value") if isinstance(item, dict) else None
        snapshot[sensor] = str(value) if value is not None else ""
    return snapshot
