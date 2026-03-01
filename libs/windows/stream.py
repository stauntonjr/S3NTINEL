# File: libs/windows/stream.py
"""Streaming adaptive window assembly over detected events."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Iterator

from libs.events.buffers import buffer_snapshot, event_value_for_buffer, update_sensor_buffer
from libs.perf.annotations import hot_path
from libs.windows.adaptive import close_reason_for_thresholds, should_close_window
from libs.windows.zoh import zoh_snapshot


@dataclass(frozen=True)
class StreamWindowConfig:
    max_ms: int = 200
    min_ms: int = 50
    event_threshold: int = 20
    inactivity_timeout_ms: int = 0
    include_window_events: bool = False


def _emit_window(
    *,
    tail_id: str,
    flight_id: str,
    win_id: int,
    current: dict[str, Any],
    min_ms: int,
    close_reason: str,
    include_window_events: bool,
) -> dict[str, Any]:
    duration_ms = int((current["t_end"] - current["t_start"]).total_seconds() * 1000.0)
    duration_ms_effective = max(duration_ms, int(min_ms))

    out: dict[str, Any] = {
        "tail_id": tail_id,
        "flight_id": flight_id,
        "win_id": int(win_id),
        "t_start": current["t_start"],
        "t_end": current["t_end"],
        "duration_ms": duration_ms_effective,
        "event_count": int(current["event_count"]),
        "zoh_version": 1,
        "date_utc": current["t_start"].date(),
        "sensor_count": len(current["last_seen"]),
        "event_type_counts": dict(current["event_type_counts"]),
        "zoh_snapshot": zoh_snapshot(buffer_snapshot(current["last_seen"])),
        "close_reason": close_reason,
    }
    if include_window_events:
        out["window_events"] = list(current["window_events"])
    return out


@hot_path
def build_adaptive_windows_stream(
    events: Iterable[dict[str, Any]],
    config: StreamWindowConfig | None = None,
) -> Iterator[dict[str, Any]]:
    active = config if config else StreamWindowConfig()
    state_by_flight: dict[tuple[str, str], dict[str, Any]] = {}

    for event in events:
        ts = event.get("ts")
        if not isinstance(ts, datetime):
            continue

        tail_id = str(event.get("tail_id", ""))
        flight_id = str(event.get("flight_id", ""))
        sensor = str(event.get("sensor", ""))
        if not tail_id or not flight_id:
            continue

        key = (tail_id, flight_id)
        state = state_by_flight.get(key)
        if state is None:
            state = {
                "next_win_id": 1,
                "current": None,
            }
            state_by_flight[key] = state

        current = state.get("current")
        if current is None:
            current = {
                "t_start": ts,
                "t_end": ts,
                "event_count": 0,
                "last_seen": {},
                "event_type_counts": {},
                "window_events": [] if active.include_window_events else None,
            }
            state["current"] = current
        else:
            inactivity_timeout_ms = int(active.inactivity_timeout_ms)
            if inactivity_timeout_ms > 0:
                inactivity_gap_ms = int((ts - current["t_end"]).total_seconds() * 1000.0)
                if inactivity_gap_ms >= inactivity_timeout_ms and int(current.get("event_count", 0)) > 0:
                    win_id = int(state["next_win_id"])
                    state["next_win_id"] = win_id + 1
                    yield _emit_window(
                        tail_id=tail_id,
                        flight_id=flight_id,
                        win_id=win_id,
                        current=current,
                        min_ms=int(active.min_ms),
                        close_reason="inactivity_timeout",
                        include_window_events=active.include_window_events,
                    )
                    current = {
                        "t_start": ts,
                        "t_end": ts,
                        "event_count": 0,
                        "last_seen": {},
                        "event_type_counts": {},
                        "window_events": [] if active.include_window_events else None,
                    }
                    state["current"] = current

        current["t_end"] = ts
        current["event_count"] = int(current["event_count"]) + 1
        update_sensor_buffer(
            sensor=sensor,
            timestamp_utc=ts,
            value=event_value_for_buffer(event),
            last_seen=current["last_seen"],
        )

        event_type = str(event.get("event_type", "unknown"))
        event_type_counts: dict[str, int] = current["event_type_counts"]
        event_type_counts[event_type] = int(event_type_counts.get(event_type, 0)) + 1
        if active.include_window_events:
            current["window_events"].append(event)

        duration_ms = int((current["t_end"] - current["t_start"]).total_seconds() * 1000.0)
        should_close = should_close_window(
            duration_ms=duration_ms,
            event_count=int(current["event_count"]),
            max_ms=int(active.max_ms),
            event_threshold=int(active.event_threshold),
        )

        if not should_close:
            continue

        win_id = int(state["next_win_id"])
        state["next_win_id"] = win_id + 1
        close_reason = close_reason_for_thresholds(
            duration_ms=duration_ms,
            event_count=int(current["event_count"]),
            max_ms=int(active.max_ms),
            event_threshold=int(active.event_threshold),
        )
        yield _emit_window(
            tail_id=tail_id,
            flight_id=flight_id,
            win_id=win_id,
            current=current,
            min_ms=int(active.min_ms),
            close_reason=close_reason,
            include_window_events=active.include_window_events,
        )

        state["current"] = None

    for (tail_id, flight_id), state in state_by_flight.items():
        current = state.get("current")
        if current is None or int(current.get("event_count", 0)) <= 0:
            continue

        win_id = int(state["next_win_id"])
        yield _emit_window(
            tail_id=tail_id,
            flight_id=flight_id,
            win_id=win_id,
            current=current,
            min_ms=int(active.min_ms),
            close_reason="end_of_stream",
            include_window_events=active.include_window_events,
        )


def build_window_cooccurrence_events(
    windows: Iterable[dict[str, Any]],
    min_distinct_sensors: int = 2,
) -> Iterator[dict[str, Any]]:
    for window in windows:
        sensor_set = set(str(item) for item in window.get("zoh_snapshot", {}).keys() if str(item))
        if len(sensor_set) < int(min_distinct_sensors):
            continue

        yield {
            "tail_id": str(window.get("tail_id", "")),
            "flight_id": str(window.get("flight_id", "")),
            "sensor": "cooccurrence",
            "ts": window.get("t_end"),
            "event_type": "cooccur",
            "payload": {
                "win_id": int(window.get("win_id", 0)),
                "sensor_count": len(sensor_set),
                "event_count": int(window.get("event_count", 0)),
                "sensors": sorted(sensor_set),
            },
        }
