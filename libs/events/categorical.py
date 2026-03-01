# File: libs/events/categorical.py
"""Categorical transition and missing/dropped event detectors."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Iterator

from libs.perf.annotations import hot_path


@dataclass(frozen=True)
class CategoricalSample:
    tail_id: str
    flight_id: str
    sensor: str
    ts: datetime
    state: str | None


@dataclass(frozen=True)
class CategoricalDetectorConfig:
    min_dwell_seconds: float = 0.0
    max_dwell_seconds: float = 0.0
    emit_state_exit: bool = True
    emit_dwell_bucket: bool = True
    illegal_transitions: frozenset[tuple[str, str]] = frozenset()


@hot_path
def detect_transitions(states: list[str]) -> list[str]:
    # HOT PATH: transition detection runs on every categorical update; avoid expensive branching/object churn.
    if not states:
        return []
    transitions: list[str] = []
    prev_state = states[0]
    for state in states[1:]:
        if state != prev_state:
            transitions.append(f"{prev_state}->{state}")
        prev_state = state
    return transitions


def detect_categorical_events_stream(
    samples: Iterable[CategoricalSample],
    config: CategoricalDetectorConfig | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield categorical events from streaming state samples without DataFrame materialization."""
    active = config if config else CategoricalDetectorConfig()
    state_by_sensor: dict[tuple[str, str, str], dict[str, Any]] = {}

    for sample in samples:
        key = (sample.tail_id, sample.flight_id, sample.sensor)
        state = state_by_sensor.get(key)
        if state is None:
            state = {
                "last_state": None,
                "last_state_ts": None,
                "last_dwell_guard_ts": None,
                "missing": False,
            }
            state_by_sensor[key] = state

        last_state = state["last_state"]
        last_state_ts = state["last_state_ts"]
        last_dwell_guard_ts = state["last_dwell_guard_ts"]
        was_missing = bool(state["missing"])
        current_state = sample.state

        if current_state is None:
            if not was_missing:
                if active.emit_state_exit and last_state is not None:
                    yield {
                        "tail_id": sample.tail_id,
                        "flight_id": sample.flight_id,
                        "sensor": sample.sensor,
                        "ts": sample.ts,
                        "event_type": "state_exit",
                        "payload": {
                            "from": str(last_state),
                            "to": "missing",
                        },
                    }
                yield {
                    "tail_id": sample.tail_id,
                    "flight_id": sample.flight_id,
                    "sensor": sample.sensor,
                    "ts": sample.ts,
                    "event_type": "dropped",
                    "payload": {"from": str(last_state) if last_state is not None else "none", "to": "missing"},
                }
            state["missing"] = True
            state["last_state"] = None
            state["last_state_ts"] = sample.ts
            state["last_dwell_guard_ts"] = sample.ts
            continue

        if last_state is None:
            yield {
                "tail_id": sample.tail_id,
                "flight_id": sample.flight_id,
                "sensor": sample.sensor,
                "ts": sample.ts,
                "event_type": "state_enter",
                "payload": {"from": "none", "to": current_state},
            }
            state["missing"] = False
            state["last_state"] = current_state
            state["last_state_ts"] = sample.ts
            state["last_dwell_guard_ts"] = None
            continue

        if current_state != last_state:
            dwell_seconds = 0.0
            if last_state_ts is not None:
                dwell_seconds = max((sample.ts - last_state_ts).total_seconds(), 0.0)

            if dwell_seconds < 1.0:
                dwell_bucket = "lt_1s"
            elif dwell_seconds < 5.0:
                dwell_bucket = "1s_to_5s"
            elif dwell_seconds < 30.0:
                dwell_bucket = "5s_to_30s"
            else:
                dwell_bucket = "gte_30s"

            if active.emit_state_exit:
                yield {
                    "tail_id": sample.tail_id,
                    "flight_id": sample.flight_id,
                    "sensor": sample.sensor,
                    "ts": sample.ts,
                    "event_type": "state_exit",
                    "payload": {
                        "from": last_state,
                        "to": current_state,
                        "dwell_seconds": dwell_seconds,
                    },
                }

            if active.emit_dwell_bucket:
                yield {
                    "tail_id": sample.tail_id,
                    "flight_id": sample.flight_id,
                    "sensor": sample.sensor,
                    "ts": sample.ts,
                    "event_type": "dwell_bucket",
                    "payload": {
                        "state": last_state,
                        "dwell_seconds": dwell_seconds,
                        "bucket": dwell_bucket,
                    },
                }

            yield {
                "tail_id": sample.tail_id,
                "flight_id": sample.flight_id,
                "sensor": sample.sensor,
                "ts": sample.ts,
                "event_type": "transition",
                "payload": {
                    "from": last_state,
                    "to": current_state,
                    "dwell_seconds": dwell_seconds,
                },
            }

            if active.min_dwell_seconds > 0 and dwell_seconds < float(active.min_dwell_seconds):
                yield {
                    "tail_id": sample.tail_id,
                    "flight_id": sample.flight_id,
                    "sensor": sample.sensor,
                    "ts": sample.ts,
                    "event_type": "dwell_violation",
                    "payload": {
                        "from": last_state,
                        "to": current_state,
                        "dwell_seconds": dwell_seconds,
                        "min_dwell_seconds": float(active.min_dwell_seconds),
                    },
                }

            if (last_state, current_state) in active.illegal_transitions:
                yield {
                    "tail_id": sample.tail_id,
                    "flight_id": sample.flight_id,
                    "sensor": sample.sensor,
                    "ts": sample.ts,
                    "event_type": "illegal_transition",
                    "payload": {
                        "from": last_state,
                        "to": current_state,
                    },
                }

            state["last_state"] = current_state
            state["last_state_ts"] = sample.ts
            state["last_dwell_guard_ts"] = None
            state["missing"] = False
            continue

        if active.max_dwell_seconds > 0 and last_state_ts is not None:
            dwell_seconds = max((sample.ts - last_state_ts).total_seconds(), 0.0)
            seconds_since_last_guard = float("inf")
            if last_dwell_guard_ts is not None:
                seconds_since_last_guard = max((sample.ts - last_dwell_guard_ts).total_seconds(), 0.0)

            if (
                dwell_seconds >= float(active.max_dwell_seconds)
                and seconds_since_last_guard >= float(active.max_dwell_seconds)
            ):
                yield {
                    "tail_id": sample.tail_id,
                    "flight_id": sample.flight_id,
                    "sensor": sample.sensor,
                    "ts": sample.ts,
                    "event_type": "dwell_guard",
                    "payload": {
                        "state": current_state,
                        "dwell_seconds": dwell_seconds,
                        "max_dwell_seconds": float(active.max_dwell_seconds),
                    },
                }
                state["last_dwell_guard_ts"] = sample.ts

        state["missing"] = False


@hot_path
def build_categorical_events(raw_df: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    order_window = Window.partitionBy("tail_id", "flight_id", "sensor").orderBy("timestamp_utc")

    categorical = (
        raw_df.where(F.col("state").isNotNull() | F.col("parameter_value").isNull())
        .withColumn("current_state", F.coalesce(F.col("state"), F.lit("missing")))
        .withColumn("prev_state", F.lag("current_state").over(order_window))
        .withColumn(
            "event_type",
            F.when(F.col("parameter_value").isNull(), F.lit("dropped"))
            .when(F.col("prev_state").isNull(), F.lit("state_enter"))
            .when(F.col("current_state") != F.col("prev_state"), F.lit("transition"))
            .otherwise(F.lit(None).cast("string")),
        )
    )

    return (
        categorical.where(F.col("event_type").isNotNull())
        .select(
            "tail_id",
            "flight_id",
            F.lit(None).cast("long").alias("win_id"),
            F.col("timestamp_utc").alias("ts"),
            "sensor",
            F.lit("unknown").alias("subsystem"),
            "event_type",
            F.create_map(
                F.lit("from"),
                F.coalesce(F.col("prev_state"), F.lit("none")).cast("string"),
                F.lit("to"),
                F.col("current_state").cast("string"),
            ).alias("payload"),
            "date_utc",
        )
    )

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
