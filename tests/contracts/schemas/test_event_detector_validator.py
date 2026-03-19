from datetime import datetime, timedelta, timezone

from libs.events import iter_event_validation_snapshots, simulator_label_events


def _ts(offset: int) -> datetime:
    return datetime(2026, 3, 1, tzinfo=timezone.utc) + timedelta(seconds=offset)


def test_simulator_label_events_extracts_event_type_label():
    rows = [
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(0), "event_type_label": None},
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(1), "event_type_label": "threshold"},
    ]
    out = list(simulator_label_events(rows))
    assert len(out) == 1
    assert out[0]["event_type_label"] == "threshold"


def test_iter_event_validation_snapshots_accumulates_confusion_counts():
    simulator_rows = [
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(0), "event_type_label": None},
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(1), "event_type_label": "threshold"},
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(2), "event_type_label": None},
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(3), "event_type_label": "switch"},
    ]
    detected_events = [
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(1), "event_type_detected": "threshold"},  # TP
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(2), "event_type_detected": "slope_pos"},  # FP
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(8), "event_type_detected": "switch"},  # unmatched FP
    ]

    snapshots = list(
        iter_event_validation_snapshots(
            simulator_rows=simulator_rows,
            detected_events=detected_events,
            tolerance_seconds=0.2,
        )
    )
    assert snapshots
    last = snapshots[-1]
    assert last["tp"] == 1
    assert last["fn"] == 1
    assert last["fp"] == 2
    assert last["tn"] == 1
