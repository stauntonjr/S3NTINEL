from datetime import datetime, timedelta, timezone

from libs.events import build_event_validation_summary, iter_event_validation_snapshots, simulator_label_events


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


def test_simulator_label_events_can_extract_custom_label_field():
    rows = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "parameter_name": "S1",
            "timestamp_utc": _ts(0),
            "event_misbehavior_label": None,
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "parameter_name": "S1",
            "timestamp_utc": _ts(1),
            "event_misbehavior_label": "threshold",
        },
    ]
    out = list(simulator_label_events(rows, label_field="event_misbehavior_label"))
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


def test_build_event_validation_summary_reports_match_and_near_miss_timing():
    simulator_rows = [
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(1), "event_type_label": "threshold"},
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(4), "event_type_label": "threshold"},
    ]
    detected_events = [
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(1) + timedelta(milliseconds=100), "event_type_detected": "threshold"},
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(5), "event_type_detected": "threshold"},
    ]

    summary = build_event_validation_summary(
        simulator_rows=simulator_rows,
        detected_events=detected_events,
        tolerance_seconds=0.5,
    )

    assert summary["tp"] == 1
    assert summary["fp"] == 1
    assert summary["fn"] == 1
    assert summary["matched_event_count"] == 1
    assert summary["median_matched_delta_seconds"] == 0.1
    assert summary["p90_matched_delta_seconds"] == 0.1
    assert summary["max_matched_delta_seconds"] == 0.1
    assert summary["unmatched_label_with_nearest_detection_count"] == 1
    assert summary["median_unmatched_label_nearest_delta_seconds"] == 1.0
    assert summary["near_miss_label_within_1s_count"] == 1
    assert summary["near_miss_label_within_2s_count"] == 1
    assert summary["near_miss_label_within_5s_count"] == 1
    assert summary["unmatched_detection_with_nearest_label_count"] == 1
    assert summary["median_unmatched_detection_nearest_delta_seconds"] == 1.0


def test_build_event_validation_summary_reports_per_family_metrics():
    simulator_rows = [
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "N1", "timestamp_utc": _ts(1), "event_type_label": "slope_pos"},
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "N1", "timestamp_utc": _ts(4), "event_type_label": "slope_neg"},
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "C1", "timestamp_utc": _ts(6), "event_type_label": "transition"},
    ]
    detected_events = [
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "N1", "timestamp_utc": _ts(1), "event_type_detected": "slope_pos"},
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "N1", "timestamp_utc": _ts(5), "event_type_detected": "slope_neg"},
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "C1", "timestamp_utc": _ts(6), "event_type_detected": "transition"},
    ]

    summary = build_event_validation_summary(
        simulator_rows=simulator_rows,
        detected_events=detected_events,
        tolerance_seconds=0.5,
    )

    assert summary["event_family_metrics"]["slope_pos"] == {
        "label_event_count": 1,
        "detected_event_count": 1,
        "tp": 1,
        "fp": 0,
        "fn": 0,
        "precision": 1.0,
        "recall": 1.0,
        "f1": 1.0,
        "detected_per_label_ratio": 1.0,
        "matched_event_count": 1,
        "median_matched_delta_seconds": 0.0,
        "p90_matched_delta_seconds": 0.0,
        "max_matched_delta_seconds": 0.0,
        "unmatched_label_with_nearest_detection_count": 0,
        "median_unmatched_label_nearest_delta_seconds": None,
        "near_miss_label_within_1s_count": 0,
        "near_miss_label_within_2s_count": 0,
        "near_miss_label_within_5s_count": 0,
        "unmatched_detection_with_nearest_label_count": 0,
        "median_unmatched_detection_nearest_delta_seconds": None,
        "tolerance_seconds": 0.5,
    }
    assert summary["event_family_metrics"]["slope_neg"]["tp"] == 0
    assert summary["event_family_metrics"]["slope_neg"]["fp"] == 1
    assert summary["event_family_metrics"]["slope_neg"]["fn"] == 1
    assert summary["event_family_metrics"]["slope_neg"]["precision"] == 0.0
    assert summary["event_family_metrics"]["slope_neg"]["recall"] == 0.0
    assert summary["event_family_metrics"]["slope_neg"]["f1"] is None
    assert summary["event_family_metrics"]["slope_neg"]["median_unmatched_label_nearest_delta_seconds"] == 1.0
    assert summary["event_family_metrics"]["slope_neg"]["median_unmatched_detection_nearest_delta_seconds"] == 1.0
    assert summary["event_family_metrics"]["transition"]["f1"] == 1.0


def test_build_event_validation_summary_reports_slope_label_contract_metrics():
    simulator_rows = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "parameter_name": "actuator_position_pct",
            "timestamp_utc": _ts(0),
            "parameter_value_clean": 0.0,
            "event_type_label": None,
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "parameter_name": "actuator_position_pct",
            "timestamp_utc": _ts(1),
            "parameter_value_clean": 0.0,
            "event_type_label": None,
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "parameter_name": "actuator_position_pct",
            "timestamp_utc": _ts(2),
            "parameter_value_clean": 2.0,
            "event_type_label": "slope_pos",
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "parameter_name": "actuator_position_pct",
            "timestamp_utc": _ts(4),
            "parameter_value_clean": 5.0,
            "event_type_label": "slope_pos",
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "parameter_name": "actuator_position_pct",
            "timestamp_utc": _ts(6),
            "parameter_value_clean": 9.0,
            "event_type_label": "slope_pos",
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "parameter_name": "actuator_position_pct",
            "timestamp_utc": _ts(8),
            "parameter_value_clean": 6.0,
            "event_type_label": None,
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "parameter_name": "actuator_position_pct",
            "timestamp_utc": _ts(9),
            "parameter_value_clean": 4.0,
            "event_type_label": "slope_neg",
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "parameter_name": "actuator_position_pct",
            "timestamp_utc": _ts(11),
            "parameter_value_clean": 0.0,
            "event_type_label": "slope_neg",
        },
    ]

    summary = build_event_validation_summary(
        simulator_rows=simulator_rows,
        detected_events=[],
        tolerance_seconds=0.5,
    )

    slope_pos_contract = summary["slope_label_contract_metrics"]["families"]["slope_pos"]
    assert slope_pos_contract == {
        "label_event_count": 3,
        "labeled_run_count": 1,
        "runs_with_repeated_labels_count": 1,
        "repeated_same_run_label_count": 2,
        "repeated_same_run_label_fraction": 2.0 / 3.0,
        "median_labels_per_labeled_run": 3.0,
        "median_labels_per_repeated_run": 3.0,
        "median_repeated_label_spacing_seconds": 2.0,
        "p90_repeated_label_spacing_seconds": 2.0,
    }
    slope_neg_contract = summary["slope_label_contract_metrics"]["families"]["slope_neg"]
    assert slope_neg_contract["label_event_count"] == 2
    assert slope_neg_contract["runs_with_repeated_labels_count"] == 1
    assert slope_neg_contract["repeated_same_run_label_count"] == 1
    assert slope_neg_contract["median_repeated_label_spacing_seconds"] == 2.0
    assert summary["slope_label_contract_metrics"]["parameters_with_repeated_labels"] == [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "parameter_name": "actuator_position_pct",
            "event_family": "slope_pos",
            "label_event_count": 3,
            "repeated_same_run_label_count": 2,
            "median_repeated_label_spacing_seconds": 2.0,
            "max_labels_in_single_run": 3,
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "parameter_name": "actuator_position_pct",
            "event_family": "slope_neg",
            "label_event_count": 2,
            "repeated_same_run_label_count": 1,
            "median_repeated_label_spacing_seconds": 2.0,
            "max_labels_in_single_run": 2,
        },
    ]


def test_build_event_validation_summary_reports_slope_run_capture_metrics():
    simulator_rows = [
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(0), "parameter_value_clean": 0.0, "event_type_label": None},
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(1), "parameter_value_clean": 2.0, "event_type_label": "slope_pos"},
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(3), "parameter_value_clean": 5.0, "event_type_label": "slope_pos"},
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(5), "parameter_value_clean": 9.0, "event_type_label": "slope_pos"},
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(7), "parameter_value_clean": 9.0, "event_type_label": None},
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(8), "parameter_value_clean": 5.0, "event_type_label": "slope_neg"},
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(10), "parameter_value_clean": 1.0, "event_type_label": "slope_neg"},
    ]
    detected_events = [
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(4), "event_type_detected": "slope_pos"},
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(20), "event_type_detected": "slope_pos"},
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(9), "event_type_detected": "slope_neg"},
    ]

    summary = build_event_validation_summary(
        simulator_rows=simulator_rows,
        detected_events=detected_events,
        tolerance_seconds=0.5,
    )

    assert summary["slope_run_capture_metrics"]["slope_pos"] == {
        "truth_run_count": 1,
        "runs_with_detection_count": 1,
        "run_recall": 1.0,
        "detected_event_count": 2,
        "detections_inside_truth_runs_count": 1,
        "detections_outside_truth_runs_count": 1,
        "detection_in_truth_run_fraction": 0.5,
        "median_truth_run_duration_seconds": 4.0,
        "median_first_detection_offset_seconds": 3.0,
        "p90_first_detection_offset_seconds": 3.0,
        "tolerance_seconds": 0.5,
    }
    assert summary["slope_run_capture_metrics"]["slope_neg"]["truth_run_count"] == 1
    assert summary["slope_run_capture_metrics"]["slope_neg"]["runs_with_detection_count"] == 1
    assert summary["slope_run_capture_metrics"]["slope_neg"]["run_recall"] == 1.0
