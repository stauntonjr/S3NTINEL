from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from typing import TYPE_CHECKING

from libs.perf.annotations import hot_path


@dataclass
class WindowSensorBuffer:
    last_seen: dict[str, dict[str, Any]] = field(default_factory=dict)

    @staticmethod
    def event_value(event: dict[str, Any]) -> str:
        payload = event.get("payload")
        if isinstance(payload, dict):
            if payload.get("to") is not None:
                return str(payload.get("to"))
            if payload.get("value") is not None:
                return str(payload.get("value"))
            if payload.get("state") is not None:
                return str(payload.get("state"))
        return str(event.get("event_type_detected", "unknown"))

    @staticmethod
    def spark_event_value_expr() -> "Column":
        from pyspark.sql import functions as F

        payload_col = F.col("payload")
        return (
            F.when(F.element_at(payload_col, F.lit("to")).isNotNull(), F.element_at(payload_col, F.lit("to")).cast("string"))
            .when(F.element_at(payload_col, F.lit("value")).isNotNull(), F.element_at(payload_col, F.lit("value")).cast("string"))
            .when(F.element_at(payload_col, F.lit("state")).isNotNull(), F.element_at(payload_col, F.lit("state")).cast("string"))
            .otherwise(F.col("event_type_detected").cast("string"))
        )

    @hot_path
    def update(self, *, sensor: str, timestamp_utc: datetime, value: str) -> dict[str, Any]:
        entry = {
            "sensor": sensor,
            "timestamp_utc": timestamp_utc,
            "value": value,
        }
        self.last_seen[sensor] = entry
        return entry

    @hot_path
    def ingest_event(self, event: dict[str, Any]) -> dict[str, Any] | None:
        ts = event.get("timestamp_utc", event.get("ts"))
        if not isinstance(ts, datetime):
            return None
        sensor = str(event.get("parameter_name", event.get("sensor", "")))
        if not sensor:
            return None
        return self.update(sensor=sensor, timestamp_utc=ts, value=self.event_value(event))

    def snapshot(self) -> dict[str, str]:
        snapshot: dict[str, str] = {}
        for sensor, item in self.last_seen.items():
            value = item.get("value") if isinstance(item, dict) else None
            snapshot[sensor] = str(value) if value is not None else ""
        return snapshot

    def copy(self) -> "WindowSensorBuffer":
        return WindowSensorBuffer(last_seen={key: dict(value) for key, value in self.last_seen.items()})


if TYPE_CHECKING:
    from pyspark.sql.column import Column
