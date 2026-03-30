from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from libs.profiling.validator import (
    build_profile_validation_summary,
    iter_profile_validation_snapshots,
    profiler_datatype_rows,
    simulator_datatype_label_rows,
)


def _ts(offset: int) -> datetime:
    return datetime(2026, 3, 1, tzinfo=timezone.utc) + timedelta(seconds=offset)


def test_iter_profile_validation_snapshots_confusion_counts_and_monotonicity():
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
        iter_profile_validation_snapshots(
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


def test_iter_profile_validation_snapshots_without_orphan_fp():
    simulator_rows = [
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(0), "parameter_datatype_label": "numeric"},
    ]
    profiler_rows = [
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "S1", "timestamp_utc": _ts(0), "parameter_datatype_profiled": "numeric"},
        {"tail_id": "T9", "flight_id": "F9", "parameter_name": "S9", "timestamp_utc": _ts(9), "parameter_datatype_profiled": "binary"},
    ]
    snapshots = list(
        iter_profile_validation_snapshots(
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


def test_build_profile_validation_summary_emits_confusions_and_mismatches():
    raw_df = pd.DataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": _ts(0),
                "parameter_name": "R1",
                "parameter_datatype_label": "numeric",
                "behavior_family_label": "regulated",
                "system_id": "SYS_A",
                "subsystem_id": "SUB_A",
                "module_id": "MOD_A",
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": _ts(1),
                "parameter_name": "I1",
                "parameter_datatype_label": "numeric",
                "behavior_family_label": "inertial",
                "system_id": "SYS_A",
                "subsystem_id": "SUB_B",
                "module_id": "MOD_B",
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": _ts(2),
                "parameter_name": "D1",
                "parameter_datatype_label": "categorical",
                "behavior_family_label": "discrete_state",
                "system_id": "SYS_B",
                "subsystem_id": "SUB_C",
                "module_id": "MOD_C",
            },
        ]
    )
    datatype_profile_df = pd.DataFrame(
        [
            {
                "parameter_name": "R1",
                "parameter_datatype_profiled": "numeric",
                "sampling_rate_profiled_hz": 1.0,
            },
            {
                "parameter_name": "I1",
                "parameter_datatype_profiled": "numeric",
                "sampling_rate_profiled_hz": 2.0,
            },
            {
                "parameter_name": "D1",
                "parameter_datatype_profiled": "numeric",
                "sampling_rate_profiled_hz": 1.0,
            },
        ]
    )
    behavior_profile_df = pd.DataFrame(
        [
            {
                "parameter_name": "R1",
                "behavior_family_profiled": "inertial",
                "behavior_profile_confidence": 0.72,
            },
            {
                "parameter_name": "I1",
                "behavior_family_profiled": "inertial",
                "behavior_profile_confidence": 0.91,
            },
            {
                "parameter_name": "D1",
                "behavior_family_profiled": "discrete_state",
                "behavior_profile_confidence": 0.88,
            },
        ]
    )

    summary = build_profile_validation_summary(
        raw_telemetry_df=raw_df,
        parameter_datatype_profile_df=datatype_profile_df,
        parameter_behavior_profile_df=behavior_profile_df,
    )

    assert summary["datatype_accuracy"] == pytest.approx(2 / 3)
    assert summary["behavior_accuracy"] == pytest.approx(2 / 3)
    confusion_rows = summary["behavior_details"]["confusion_matrix"]
    assert {
        (row["behavior_family_label"], row["behavior_family_profiled"], row["count"])
        for row in confusion_rows
    } == {
        ("regulated", "inertial", 1),
        ("inertial", "inertial", 1),
        ("discrete_state", "discrete_state", 1),
    }
    assert {"errors_by_label", "prediction_counts", "mismatch_examples", "confidence_by_predicted_family"}.issubset(
        summary["behavior_details"].keys()
    )
    mismatch = summary["behavior_details"]["mismatch_examples"][0]
    assert mismatch["parameter_name"] == "R1"
    assert mismatch["behavior_family_label"] == "regulated"
    assert mismatch["behavior_family_profiled"] == "inertial"
    assert mismatch["behavior_profile_confidence"] == pytest.approx(0.72)
    dtype_mismatch = summary["datatype_details"]["mismatch_examples"][0]
    assert dtype_mismatch["parameter_name"] == "D1"
    assert dtype_mismatch["parameter_datatype_label"] == "categorical"
    assert dtype_mismatch["parameter_datatype_profiled"] == "numeric"
