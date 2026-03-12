# File: libs/events/cooccur.py
"""Event pair-counting helpers for lag and co-presence relations."""

from __future__ import annotations

from collections import Counter, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterable, Iterator

from libs.perf.annotations import hot_path


@dataclass(frozen=True)
class CooccurrencePairCountConfig:
    buffer_ms: int
    include_self_pairs: bool = False


def stream_cooccurrence_pair_counts(
    events: Iterable[dict[str, Any]],
    *,
    config: CooccurrencePairCountConfig,
) -> Iterator[dict[str, Any]]:
    """Emit incremental pair-count updates within a lag buffer.

    For each incoming event at time t, all events currently present in
    the preceding [t-buffer_ms, t] window contribute to directed pairs:
    ((parameter_name_first, event_type_first) -> (parameter_name_second, event_type_second)).
    """
    buffer_ms = max(int(config.buffer_ms), 1)
    include_self_pairs = bool(config.include_self_pairs)
    buffer_delta = timedelta(milliseconds=buffer_ms)

    state_by_group: dict[tuple[str, str], dict[str, Any]] = {}

    for event in events:
        event_type_second = str(event.get("event_type_detected", "")).strip()
        parameter_name_second = str(event.get("parameter_name", event.get("sensor", ""))).strip()
        ts = event.get("timestamp_utc", event.get("ts"))
        if not event_type_second or not parameter_name_second or not isinstance(ts, datetime):
            continue

        tail_id = str(event.get("tail_id", ""))
        flight_id = str(event.get("flight_id", ""))
        group_key = (tail_id, flight_id)

        group_state = state_by_group.get(group_key)
        if group_state is None:
            group_state = {
                "buffer": deque(),
                "active_node_counts": Counter(),
            }
            state_by_group[group_key] = group_state

        buffer: deque[tuple[datetime, str, str]] = group_state["buffer"]
        active_node_counts: Counter[tuple[str, str]] = group_state["active_node_counts"]

        lower_bound = ts - buffer_delta
        while buffer and buffer[0][0] < lower_bound:
            _, parameter_name_first, event_type_first = buffer.popleft()
            node_first = (parameter_name_first, event_type_first)
            active_node_counts[node_first] -= 1
            if active_node_counts[node_first] <= 0:
                active_node_counts.pop(node_first, None)

        node_second = (parameter_name_second, event_type_second)
        for node_first, count in list(active_node_counts.items()):
            pair_count_increment = int(count)
            if pair_count_increment <= 0:
                continue
            if (not include_self_pairs) and node_first == node_second:
                continue
            yield {
                "tail_id": tail_id,
                "flight_id": flight_id,
                "timestamp_utc": ts,
                "buffer_ms": int(buffer_ms),
                "parameter_name_first": str(node_first[0]),
                "event_type_first": str(node_first[1]),
                "parameter_name_second": parameter_name_second,
                "event_type_second": str(event_type_second),
                "increment": pair_count_increment,
            }

        buffer.append((ts, parameter_name_second, event_type_second))
        active_node_counts[node_second] += 1


def stream_immediate_precedence_pair_counts(
    events: Iterable[dict[str, Any]],
) -> Iterator[dict[str, Any]]:
    """Emit directed counts for adjacent events in each (tail_id, flight_id) stream."""
    last_node_by_group: dict[tuple[str, str], tuple[str, str] | None] = {}
    last_ts_by_group: dict[tuple[str, str], datetime | None] = {}

    for event in events:
        parameter_name_second = str(event.get("parameter_name", event.get("sensor", ""))).strip()
        event_type_second = str(event.get("event_type_detected", "")).strip()
        ts = event.get("timestamp_utc", event.get("ts"))
        if not parameter_name_second or not event_type_second or not isinstance(ts, datetime):
            continue

        tail_id = str(event.get("tail_id", ""))
        flight_id = str(event.get("flight_id", ""))
        group_key = (tail_id, flight_id)
        node_second = (parameter_name_second, event_type_second)

        node_first = last_node_by_group.get(group_key)
        ts_first = last_ts_by_group.get(group_key)
        if node_first is not None:
            yield {
                "tail_id": tail_id,
                "flight_id": flight_id,
                "timestamp_utc": ts,
                "ts_prev": ts_first,
                "parameter_name_first": str(node_first[0]),
                "event_type_first": str(node_first[1]),
                "parameter_name_second": parameter_name_second,
                "event_type_second": event_type_second,
                "increment": 1,
            }

        last_node_by_group[group_key] = node_second
        last_ts_by_group[group_key] = ts


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
