"""Window domain objects and closure policy."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

from libs.io.contracts import AdaptiveWindowRow, DetectedEventRow
from libs.windows.buffer import WindowSensorBuffer


DEFAULT_MIN_SAMPLING_RATE_HZ = 1.0


@dataclass(frozen=True)
class WindowPolicy:
    max_ms: int
    event_threshold: int
    min_ms: int = 50
    inactivity_timeout_ms: int = 0

    @staticmethod
    def max_ms_from_min_sampling_rate(min_sampling_rate_hz: float) -> int:
        rate_hz = max(float(min_sampling_rate_hz), 1e-6)
        return max(int(round((10.0 / rate_hz) * 1000.0)), 1)

    @classmethod
    def default(cls) -> "WindowPolicy":
        return cls(
            max_ms=cls.max_ms_from_min_sampling_rate(DEFAULT_MIN_SAMPLING_RATE_HZ),
            event_threshold=20,
            min_ms=50,
            inactivity_timeout_ms=0,
        )

    def should_close(self, *, duration_ms: int, event_count: int) -> bool:
        return duration_ms >= int(self.max_ms) or event_count >= int(self.event_threshold)

    def close_reason(self, *, duration_ms: int, event_count: int) -> str:
        by_duration = duration_ms >= int(self.max_ms)
        by_count = event_count >= int(self.event_threshold)
        if by_duration and by_count:
            return "event_threshold+max_ms"
        if by_count:
            return "event_threshold"
        return "max_ms"

    def effective_duration_ms(self, duration_ms: int) -> int:
        return max(int(duration_ms), int(self.min_ms))

    def duration_ms_expr(self, *, t_start: "Column", t_end: "Column") -> "Column":
        from pyspark.sql import functions as F

        return (F.unix_millis(t_end) - F.unix_millis(t_start)).cast("int")

    def cap_timestamp_expr(self, *, t_start: "Column") -> "Column":
        from pyspark.sql import functions as F

        return F.timestamp_millis(F.unix_millis(t_start) + F.lit(int(self.max_ms)))

    def should_close_expr(self, *, duration_ms: "Column", event_count: "Column") -> "Column":
        return (duration_ms >= int(self.max_ms)) | (event_count >= int(self.event_threshold))

    def close_reason_expr(self, *, duration_ms: "Column", event_count: "Column") -> "Column":
        from pyspark.sql import functions as F

        by_duration = duration_ms >= int(self.max_ms)
        by_count = event_count >= int(self.event_threshold)
        return (
            F.when(by_duration & by_count, F.lit("event_threshold+max_ms"))
            .when(by_count, F.lit("event_threshold"))
            .otherwise(F.lit("max_ms"))
        )


@dataclass
class Window:
    t_start: datetime
    t_end: datetime
    event_count: int = 0
    sensor_buffer: WindowSensorBuffer = field(default_factory=WindowSensorBuffer)
    event_type_counts: dict[str, int] = field(default_factory=dict)
    window_events: list[DetectedEventRow] | None = None

    @classmethod
    def open(cls, ts: datetime, *, include_window_events: bool = False) -> "Window":
        return cls(
            t_start=ts,
            t_end=ts,
            event_count=0,
            sensor_buffer=WindowSensorBuffer(),
            event_type_counts={},
            window_events=[] if include_window_events else None,
        )

    @property
    def duration_ms(self) -> int:
        return int((self.t_end - self.t_start).total_seconds() * 1000.0)

    @property
    def date_utc(self) -> date:
        return self.t_start.date()

    @property
    def sensor_count(self) -> int:
        return len(self.sensor_buffer.last_seen)

    def cap_timestamp(self, max_ms: int) -> datetime:
        return self.t_start + timedelta(milliseconds=int(max_ms))

    def ingest_event(self, event: DetectedEventRow) -> None:
        ts = event.get("timestamp_utc", event.get("ts"))
        if not isinstance(ts, datetime):
            return
        event_type_detected = str(event.get("event_type_detected", "")).strip()
        if not event_type_detected:
            return
        self.t_end = ts
        self.event_count = int(self.event_count) + 1
        self.sensor_buffer.ingest_event(event)
        self.event_type_counts[event_type_detected] = int(self.event_type_counts.get(event_type_detected, 0)) + 1
        if self.window_events is not None:
            self.window_events.append(event)

    def clone_capped(self, t_end: datetime) -> "Window":
        return Window(
            t_start=self.t_start,
            t_end=t_end,
            event_count=int(self.event_count),
            sensor_buffer=self.sensor_buffer.copy(),
            event_type_counts=dict(self.event_type_counts),
            window_events=list(self.window_events) if self.window_events is not None else None,
        )

    def to_row(self, *, tail_id: str, flight_id: str, win_id: int, policy: WindowPolicy, close_reason: str) -> AdaptiveWindowRow:
        row: AdaptiveWindowRow = {
            "tail_id": tail_id,
            "flight_id": flight_id,
            "win_id": int(win_id),
            "t_start": self.t_start,
            "t_end": self.t_end,
            "duration_ms": policy.effective_duration_ms(self.duration_ms),
            "event_count": int(self.event_count),
            "zoh_version": 1,
            "date_utc": self.date_utc,
            "sensor_count": self.sensor_count,
            "event_type_counts": dict(self.event_type_counts),
            "zoh_snapshot": self.sensor_buffer.snapshot(),
            "close_reason": close_reason,
        }
        if self.window_events is not None:
            row["window_events"] = list(self.window_events)
        return row


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.column import Column
