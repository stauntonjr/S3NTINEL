"""Window domain objects and closure policy."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

from libs.io.contracts import AdaptiveWindowRow, DetectedEventRow
from libs.windows.buffer import WindowSensorBuffer


DEFAULT_MIN_SAMPLING_RATE_HZ = 1.0


@dataclass(frozen=True)
class WindowClosureBudgetPolicy:
    quiet_horizon_ms: int
    event_threshold: int

    @classmethod
    def from_window_policy(cls, policy: "WindowPolicy") -> "WindowClosureBudgetPolicy":
        # `max_ms` remains the external policy field, but the planner now uses it
        # as the quiet-time horizon for continuous closure budget.
        return cls(
            quiet_horizon_ms=max(int(policy.max_ms), 1),
            event_threshold=max(int(policy.event_threshold), 1),
        )

    def quiet_credit(self, *, duration_ms: int) -> float:
        return float(max(int(duration_ms), 0)) * float(self.event_threshold) / float(self.quiet_horizon_ms)

    def closure_budget(self, *, duration_ms: int, real_event_count: int) -> float:
        return float(max(int(real_event_count), 0)) + self.quiet_credit(duration_ms=duration_ms)

    def should_close(self, *, duration_ms: int, real_event_count: int) -> bool:
        return self.closure_budget(duration_ms=duration_ms, real_event_count=real_event_count) >= float(
            self.event_threshold
        )

    def close_reason(self, *, real_event_count: int) -> str:
        return "event_threshold" if int(real_event_count) >= int(self.event_threshold) else "budget_threshold"

    def budget_close_elapsed_ms(self, *, real_event_count: int) -> int:
        remaining = max(int(self.event_threshold) - max(int(real_event_count), 0), 0)
        if remaining <= 0:
            return 0
        return int(math.ceil(float(remaining * self.quiet_horizon_ms) / float(self.event_threshold)))

    def quiet_credit_expr(self, *, duration_ms: "Column") -> "Column":
        from pyspark.sql import functions as F

        return duration_ms.cast("double") * (
            F.lit(float(self.event_threshold)) / F.lit(float(max(int(self.quiet_horizon_ms), 1)))
        )

    def closure_budget_expr(
        self,
        *,
        duration_ms: "Column",
        real_event_count: "Column",
    ) -> "Column":
        return real_event_count.cast("double") + self.quiet_credit_expr(duration_ms=duration_ms)

    def should_close_expr(
        self,
        *,
        duration_ms: "Column",
        real_event_count: "Column",
    ) -> "Column":
        from pyspark.sql import functions as F

        return self.closure_budget_expr(
            duration_ms=duration_ms,
            real_event_count=real_event_count,
        ) >= F.lit(float(self.event_threshold))

    def close_reason_expr(self, *, real_event_count: "Column") -> "Column":
        from pyspark.sql import functions as F

        return F.when(real_event_count >= F.lit(int(self.event_threshold)), F.lit("event_threshold")).otherwise(
            F.lit("budget_threshold")
        )

    def elapsed_ms_for_budget_units_expr(self, *, budget_units: "Column") -> "Column":
        from pyspark.sql import functions as F

        return F.ceil(
            budget_units.cast("double")
            * F.lit(float(max(int(self.quiet_horizon_ms), 1)))
            / F.lit(float(max(int(self.event_threshold), 1)))
        ).cast("long")

    def budget_close_elapsed_ms_expr(self, *, real_event_count: "Column") -> "Column":
        from pyspark.sql import functions as F

        remaining = F.greatest(F.lit(float(self.event_threshold)) - real_event_count.cast("double"), F.lit(0.0))
        return self.elapsed_ms_for_budget_units_expr(budget_units=remaining)


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
        return WindowClosureBudgetPolicy.from_window_policy(self).should_close(
            duration_ms=duration_ms,
            real_event_count=event_count,
        )

    def close_reason(self, *, duration_ms: int, event_count: int) -> str:
        if not self.should_close(duration_ms=duration_ms, event_count=event_count):
            return "open"
        return WindowClosureBudgetPolicy.from_window_policy(self).close_reason(real_event_count=event_count)

    def effective_duration_ms(self, duration_ms: int) -> int:
        return max(int(duration_ms), int(self.min_ms))

    def duration_ms_expr(self, *, t_start: "Column", t_end: "Column") -> "Column":
        from pyspark.sql import functions as F

        return (F.unix_millis(t_end) - F.unix_millis(t_start)).cast("int")


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
        budget_policy = WindowClosureBudgetPolicy.from_window_policy(policy)
        quiet_credit_end = budget_policy.quiet_credit(duration_ms=self.duration_ms)
        closure_budget_end = budget_policy.closure_budget(duration_ms=self.duration_ms, real_event_count=self.event_count)
        row: AdaptiveWindowRow = {
            "tail_id": tail_id,
            "flight_id": flight_id,
            "win_id": int(win_id),
            "t_start": self.t_start,
            "t_end": self.t_end,
            "duration_ms": policy.effective_duration_ms(self.duration_ms),
            "event_count": int(self.event_count),
            "real_event_count": int(self.event_count),
            "quiet_credit_end": float(quiet_credit_end),
            "closure_budget_end": float(closure_budget_end),
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
