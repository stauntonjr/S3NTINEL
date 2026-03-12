# File: libs/windows/stream.py
"""Streaming adaptive window assembly over detected events."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable, Iterator

from libs.io.contracts import AdaptiveWindowRow, DetectedEventRow
from libs.perf.annotations import hot_path
from libs.windows.window import DEFAULT_MIN_SAMPLING_RATE_HZ, Window, WindowPolicy


@dataclass(frozen=True)
class StreamWindowConfig:
    policy: WindowPolicy = field(
        default_factory=lambda: WindowPolicy(
            max_ms=WindowPolicy.max_ms_from_min_sampling_rate(DEFAULT_MIN_SAMPLING_RATE_HZ),
            min_ms=50,
            event_threshold=20,
            inactivity_timeout_ms=0,
        )
    )
    include_window_events: bool = False

    def __init__(
        self,
        *,
        max_ms: int | None = None,
        min_ms: int = 50,
        event_threshold: int = 20,
        inactivity_timeout_ms: int = 0,
        include_window_events: bool = False,
        policy: WindowPolicy | None = None,
    ) -> None:
        resolved_policy = policy or WindowPolicy(
            max_ms=max_ms if max_ms is not None else WindowPolicy.max_ms_from_min_sampling_rate(DEFAULT_MIN_SAMPLING_RATE_HZ),
            min_ms=min_ms,
            event_threshold=event_threshold,
            inactivity_timeout_ms=inactivity_timeout_ms,
        )
        object.__setattr__(self, "policy", resolved_policy)
        object.__setattr__(self, "include_window_events", bool(include_window_events))

    @property
    def max_ms(self) -> int:
        return int(self.policy.max_ms)

    @property
    def min_ms(self) -> int:
        return int(self.policy.min_ms)

    @property
    def event_threshold(self) -> int:
        return int(self.policy.event_threshold)

    @property
    def inactivity_timeout_ms(self) -> int:
        return int(self.policy.inactivity_timeout_ms)


@dataclass
class WindowStream:
    config: StreamWindowConfig

    @property
    def policy(self) -> WindowPolicy:
        return self.config.policy

    def start_window(self, ts: datetime) -> Window:
        return Window.open(ts, include_window_events=self.config.include_window_events)

    def window_cap_timestamp(self, current: Window) -> datetime:
        return current.cap_timestamp(self.policy.max_ms)

    def emit_window(self, *, tail_id: str, flight_id: str, win_id: int, current: Window, close_reason: str) -> AdaptiveWindowRow:
        return current.to_row(
            tail_id=tail_id,
            flight_id=flight_id,
            win_id=win_id,
            policy=self.policy,
            close_reason=close_reason,
        )

    @hot_path
    def iter_windows(self, events: Iterable[DetectedEventRow]) -> Iterator[AdaptiveWindowRow]:
        state_by_flight: dict[tuple[str, str], dict[str, int | Window | None]] = {}

        for event in events:
            ts = event.get("timestamp_utc", event.get("ts"))
            if not isinstance(ts, datetime):
                continue

            tail_id = str(event.get("tail_id", ""))
            flight_id = str(event.get("flight_id", ""))
            if not tail_id or not flight_id:
                continue

            key = (tail_id, flight_id)
            state = state_by_flight.get(key)
            if state is None:
                state = {"next_win_id": 1, "current": None}
                state_by_flight[key] = state

            current = state.get("current")
            if current is None:
                current = self.start_window(ts)
                state["current"] = current
            else:
                inactivity_timeout_ms = int(self.policy.inactivity_timeout_ms)
                if inactivity_timeout_ms > 0:
                    inactivity_gap_ms = int((ts - current.t_end).total_seconds() * 1000.0)
                    if inactivity_gap_ms >= inactivity_timeout_ms and int(current.event_count) > 0:
                        win_id = int(state["next_win_id"])
                        state["next_win_id"] = win_id + 1
                        yield self.emit_window(
                            tail_id=tail_id,
                            flight_id=flight_id,
                            win_id=win_id,
                            current=current,
                            close_reason="inactivity_timeout",
                        )
                        current = self.start_window(ts)
                        state["current"] = current

                if int(current.event_count) > 0:
                    window_cap = self.window_cap_timestamp(current)
                    if ts >= window_cap:
                        capped_current = current.clone_capped(window_cap)
                        win_id = int(state["next_win_id"])
                        state["next_win_id"] = win_id + 1
                        yield self.emit_window(
                            tail_id=tail_id,
                            flight_id=flight_id,
                            win_id=win_id,
                            current=capped_current,
                            close_reason="max_ms",
                        )
                        current = self.start_window(ts)
                        state["current"] = current

            event_type_detected = str(event.get("event_type_detected", "")).strip()
            if not event_type_detected:
                continue

            current.ingest_event(event)
            duration_ms = int(current.duration_ms)
            should_close = self.policy.should_close(duration_ms=duration_ms, event_count=int(current.event_count))

            if not should_close:
                continue

            win_id = int(state["next_win_id"])
            state["next_win_id"] = win_id + 1
            close_reason = self.policy.close_reason(duration_ms=duration_ms, event_count=int(current.event_count))
            yield self.emit_window(
                tail_id=tail_id,
                flight_id=flight_id,
                win_id=win_id,
                current=current,
                close_reason=close_reason,
            )

            state["current"] = None

        for (tail_id, flight_id), state in state_by_flight.items():
            current = state.get("current")
            if current is None or int(current.event_count) <= 0:
                continue

            win_id = int(state["next_win_id"])
            yield self.emit_window(
                tail_id=tail_id,
                flight_id=flight_id,
                win_id=win_id,
                current=current,
                close_reason="end_of_stream",
            )


@hot_path
def build_adaptive_windows_stream(
    events: Iterable[DetectedEventRow],
    config: StreamWindowConfig | None = None,
) -> Iterator[AdaptiveWindowRow]:
    active = config if config else StreamWindowConfig()
    yield from WindowStream(config=active).iter_windows(events)
