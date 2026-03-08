from datetime import datetime, timedelta, timezone

import pytest

from libs.profiling.validator import profiler_datatype_rows, simulator_datatype_label_rows, stream_profiler_validation


def _ts(offset: int) -> datetime:
    return datetime(2026, 3, 1, tzinfo=timezone.utc) + timedelta(seconds=offset)


def test_stream_profiler_validation_confusion_counts_and_monotonicity():
    simulator_rows = [
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(0), "parameter_datatype_label": "numeric"},  # TP
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(1), "parameter_datatype_label": "numeric"},  # mismatch FP+FN
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(2), "parameter_datatype_label": "binary"},  # missing profiler FN
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(3), "parameter_datatype_label": ""},  # TN
    ]
    profiler_rows = [
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(0), "parameter_datatype_profiled": "numeric"},
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(1), "parameter_datatype_profiled": "categorical"},
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(8), "parameter_datatype_profiled": "numeric"},  # orphan FP
    ]

    snapshots = list(
        stream_profiler_validation(
            simulator_rows=simulator_rows,
            profiler_rows=profiler_rows,
            emit_orphan_fp=True,
        )
    )
    assert snapshots

    last = snapshots[-1]
    assert last["tp"] == 1
    assert last["fp"] == 2
    assert last["fn"] == 2
    assert last["tn"] == 1

    tp_values = [int(item["tp"]) for item in snapshots]
    fp_values = [int(item["fp"]) for item in snapshots]
    fn_values = [int(item["fn"]) for item in snapshots]
    tn_values = [int(item["tn"]) for item in snapshots]
    assert tp_values == sorted(tp_values)
    assert fp_values == sorted(fp_values)
    assert fn_values == sorted(fn_values)
    assert tn_values == sorted(tn_values)


def test_stream_profiler_validation_without_orphan_fp():
    simulator_rows = [
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(0), "parameter_datatype_label": "numeric"},
    ]
    profiler_rows = [
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(0), "parameter_datatype_profiled": "numeric"},
        {"tail_id": "T9", "flight_id": "F9", "parameter_name": "S9", "timestamp_utc": _ts(9), "parameter_datatype_profiled": "binary"},
    ]
    snapshots = list(
        stream_profiler_validation(
            simulator_rows=simulator_rows,
            profiler_rows=profiler_rows,
            emit_orphan_fp=False,
        )
    )
    assert snapshots[-1]["tp"] == 1
    assert snapshots[-1]["fp"] == 0
    assert snapshots[-1]["fn"] == 0
    assert snapshots[-1]["tn"] == 0


def test_simulator_validator_rejects_legacy_parameter_datatype():
    with pytest.raises(ValueError, match="parameter_datatype"):
        list(
            simulator_datatype_label_rows(
                [{"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(0), "parameter_datatype": "numeric"}]
            )
        )


def test_profiler_validator_rejects_legacy_detected_type():
    with pytest.raises(ValueError, match="detected_type"):
        list(
            profiler_datatype_rows(
                [{"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(0), "detected_type": "numeric"}]
            )
        )
