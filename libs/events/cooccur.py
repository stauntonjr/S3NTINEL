# File: libs/events/cooccur.py
"""Subsystem event co-occurrence construction."""

from __future__ import annotations

from collections import Counter, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterable, Iterator

from libs.perf.annotations import hot_path


@hot_path
def compute_cooccurrence(event_ids: list[str]) -> dict[str, float]:
    # HOT PATH: co-occurrence expansion can be combinatorial; use bounded/windowed joins in production.
    filtered_ids = [str(item).strip() for item in event_ids if str(item).strip()]
    if len(filtered_ids) < 2:
        return {}

    counts = Counter(filtered_ids)
    unique_ids = sorted(counts.keys())
    if len(unique_ids) < 2:
        return {}

    total_pairs = (len(filtered_ids) * (len(filtered_ids) - 1)) // 2
    if total_pairs <= 0:
        return {}

    out: dict[str, float] = {}
    for left_index, left_id in enumerate(unique_ids):
        for right_id in unique_ids[left_index + 1 :]:
            pair_count = counts[left_id] * counts[right_id]
            if pair_count <= 0:
                continue
            out[f"{left_id}|{right_id}"] = float(pair_count) / float(total_pairs)
    return out


@dataclass(frozen=True)
class CooccurrenceDetectorConfig:
    window_seconds: float = 0.5
    min_distinct_sensors: int = 2
    emit_refractory_seconds: float = 0.5


def detect_cooccurrence_events_stream(
    events: Iterable[dict[str, Any]],
    config: CooccurrenceDetectorConfig | None = None,
) -> Iterator[dict[str, Any]]:
    """Emit stream co-occurrence events when multiple sensors fire within a short window."""
    active = config if config else CooccurrenceDetectorConfig()
    window_seconds = max(float(active.window_seconds), 0.0)
    window_delta = timedelta(seconds=window_seconds)
    refractory_delta = timedelta(seconds=max(float(active.emit_refractory_seconds), 0.0))

    state_by_group: dict[tuple[str, str], dict[str, Any]] = {}

    for event in events:
        event_type = str(event.get("event_type", ""))
        if event_type == "cooccur":
            continue

        ts = event.get("ts")
        if not isinstance(ts, datetime):
            continue

        tail_id = str(event.get("tail_id", ""))
        flight_id = str(event.get("flight_id", ""))
        sensor = str(event.get("sensor", ""))
        group_key = (tail_id, flight_id)

        group_state = state_by_group.get(group_key)
        if group_state is None:
            group_state = {
                "buffer": deque(),
                "last_emit_ts": None,
            }
            state_by_group[group_key] = group_state

        buffer: deque[dict[str, Any]] = group_state["buffer"]
        lower_bound = ts - window_delta
        while buffer and buffer[0]["ts"] < lower_bound:
            buffer.popleft()

        buffer.append(
            {
                "ts": ts,
                "sensor": sensor,
                "event_type": event_type,
            }
        )

        sensor_set = {str(item["sensor"]) for item in buffer if str(item["sensor"])}
        if len(sensor_set) < int(active.min_distinct_sensors):
            continue

        last_emit_ts = group_state["last_emit_ts"]
        if isinstance(last_emit_ts, datetime) and (ts - last_emit_ts) < refractory_delta:
            continue

        group_state["last_emit_ts"] = ts
        yield {
            "tail_id": tail_id,
            "flight_id": flight_id,
            "sensor": "cooccurrence",
            "ts": ts,
            "event_type": "cooccur",
            "payload": {
                "sensor_count": len(sensor_set),
                "event_count": len(buffer),
                "window_seconds": window_seconds,
                "sensors": sorted(sensor_set),
            },
        }


@hot_path
def build_cooccurrence_events(events_df: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    cooccur = (
        events_df.groupBy("tail_id", "flight_id", "ts", "date_utc")
        .agg(F.countDistinct("sensor").alias("sensor_count"))
        .where(F.col("sensor_count") > 1)
        .select(
            "tail_id",
            "flight_id",
            F.lit(None).cast("long").alias("win_id"),
            "ts",
            F.lit("cooccurrence").alias("sensor"),
            F.lit("fleet").alias("subsystem"),
            F.lit("cooccur").alias("event_type"),
            F.create_map(F.lit("sensor_count"), F.col("sensor_count").cast("string")).alias("payload"),
            "date_utc",
        )
    )
    return cooccur

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
