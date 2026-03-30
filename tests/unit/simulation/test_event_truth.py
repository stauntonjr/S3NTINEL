from datetime import datetime, timezone, timedelta

from libs.simulation.event_truth import annotate_event_type_labels


def _ts(offset: int) -> datetime:
    return datetime(2026, 3, 20, tzinfo=timezone.utc) + timedelta(seconds=offset)


def test_annotate_event_type_labels_marks_persistent_numeric_run() -> None:
    rows = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "parameter_name": "N1",
            "timestamp_utc": _ts(0),
            "step_index": 0,
            "parameter_datatype_label": "numeric",
            "parameter_value_clean": 0.0,
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "parameter_name": "N1",
            "timestamp_utc": _ts(1),
            "step_index": 1,
            "parameter_datatype_label": "numeric",
            "parameter_value_clean": 1.0,
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "parameter_name": "N1",
            "timestamp_utc": _ts(2),
            "step_index": 2,
            "parameter_datatype_label": "numeric",
            "parameter_value_clean": 2.0,
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "parameter_name": "N1",
            "timestamp_utc": _ts(3),
            "step_index": 3,
            "parameter_datatype_label": "numeric",
            "parameter_value_clean": 3.0,
        },
    ]

    labeled = annotate_event_type_labels(rows)

    assert [row.get("event_type_label", "") for row in labeled] == ["", "", "slope_pos", ""]


def test_annotate_event_type_labels_does_not_reemit_within_single_monotone_run() -> None:
    rows = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "parameter_name": "N1",
            "timestamp_utc": _ts(index),
            "step_index": index,
            "parameter_datatype_label": "numeric",
            "parameter_value_clean": float(value),
        }
        for index, value in enumerate((0.0, 2.0, 4.0, 7.0, 11.0, 16.0))
    ]

    labeled = annotate_event_type_labels(rows)

    assert [row.get("event_type_label", "") for row in labeled] == ["", "", "slope_pos", "", "", ""]


def test_annotate_event_type_labels_reemits_after_run_reset() -> None:
    rows = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "parameter_name": "N1",
            "timestamp_utc": _ts(index),
            "step_index": index,
            "parameter_datatype_label": "numeric",
            "parameter_value_clean": float(value),
        }
        for index, value in enumerate((0.0, 1.0, 2.0, 3.0, 3.1, 4.1, 5.1))
    ]

    labeled = annotate_event_type_labels(rows)

    assert [row.get("event_type_label", "") for row in labeled] == ["", "", "slope_pos", "", "", "", "slope_pos"]


def test_annotate_event_type_labels_marks_discrete_transition() -> None:
    rows = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "parameter_name": "S1",
            "timestamp_utc": _ts(0),
            "step_index": 0,
            "parameter_datatype_label": "binary",
            "parameter_value_clean": "OFF",
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "parameter_name": "S1",
            "timestamp_utc": _ts(1),
            "step_index": 1,
            "parameter_datatype_label": "binary",
            "parameter_value_clean": "ON",
        },
    ]

    labeled = annotate_event_type_labels(rows)

    assert [row.get("event_type_label", "") for row in labeled] == ["", "transition"]
