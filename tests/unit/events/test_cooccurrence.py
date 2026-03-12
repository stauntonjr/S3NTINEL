from datetime import datetime, timedelta, timezone

from libs.events.cooccur import (
    CooccurrencePairCountConfig,
    stream_cooccurrence_pair_counts,
    stream_immediate_precedence_pair_counts,
)


def _ts(offset_seconds: float) -> datetime:
    base = datetime(2026, 3, 1, tzinfo=timezone.utc)
    return base + timedelta(seconds=float(offset_seconds))


def test_stream_cooccurrence_pair_counts_emits_directional_pairs():
    events = [
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(0.0), "event_type_detected": "switch"},
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S2", "timestamp_utc": _ts(0.2), "event_type_detected": "peak"},
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S3", "timestamp_utc": _ts(0.3), "event_type_detected": "slope_pos"},
    ]

    updates = list(
        stream_cooccurrence_pair_counts(
            events,
            config=CooccurrencePairCountConfig(buffer_ms=500, include_self_pairs=False),
        )
    )
    pairs = {
        (
            item["parameter_name_first"],
            item["event_type_first"],
            item["parameter_name_second"],
            item["event_type_second"],
        ): int(item["increment"])
        for item in updates
    }

    assert pairs[("S1", "switch", "S2", "peak")] == 1
    assert pairs[("S1", "switch", "S3", "slope_pos")] == 1
    assert pairs[("S2", "peak", "S3", "slope_pos")] == 1


def test_stream_cooccurrence_pair_counts_respects_buffer_expiry():
    events = [
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(0.0), "event_type_detected": "switch"},
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S2", "timestamp_utc": _ts(2.0), "event_type_detected": "peak"},
    ]
    updates = list(
        stream_cooccurrence_pair_counts(
            events,
            config=CooccurrencePairCountConfig(buffer_ms=500, include_self_pairs=False),
        )
    )
    assert updates == []


def test_stream_cooccurrence_pair_counts_skips_same_node_pairs_by_default():
    events = [
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(0.0), "event_type_detected": "switch"},
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(0.1), "event_type_detected": "switch"},
    ]
    updates = list(
        stream_cooccurrence_pair_counts(
            events,
            config=CooccurrencePairCountConfig(buffer_ms=1000, include_self_pairs=False),
        )
    )
    assert updates == []


def test_stream_immediate_precedence_pair_counts_emits_adjacent_pairs():
    events = [
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(0.0), "event_type_detected": "switch"},
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S2", "timestamp_utc": _ts(0.1), "event_type_detected": "peak"},
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S3", "timestamp_utc": _ts(0.2), "event_type_detected": "slope_pos"},
    ]
    updates = list(stream_immediate_precedence_pair_counts(events))
    assert len(updates) == 2
    first = updates[0]
    second = updates[1]

    assert first["parameter_name_first"] == "S1"
    assert first["event_type_first"] == "switch"
    assert first["parameter_name_second"] == "S2"
    assert first["event_type_second"] == "peak"
    assert int(first["increment"]) == 1

    assert second["parameter_name_first"] == "S2"
    assert second["event_type_first"] == "peak"
    assert second["parameter_name_second"] == "S3"
    assert second["event_type_second"] == "slope_pos"
    assert int(second["increment"]) == 1
