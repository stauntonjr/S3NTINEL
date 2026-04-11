from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from libs.anomaly import validate_attribution_against_fault_truth, validate_attribution_against_misbehavior_truth
from libs.graph import build_coupling_validation_summary, build_graph_validation_summary
from libs.phase import build_phase_validation_assignments, validate_detected_phases_from_tables
from libs.simulation.report_tables import RunArtifactBundle
from libs.simulation.reporting import _build_phase_validation_summary
from libs.scoring import (
    RECONSTRUCTION_ERROR_CHANNEL,
    REGIME_DEVIATION_CHANNEL,
    score_component_scores_with_updates,
    summarize_fault_window_detection,
    summarize_misbehavior_window_detection,
    validate_scores_against_fault_windows,
    validate_scores_against_misbehavior_windows,
)


def _ts(second: int) -> datetime:
    return datetime(2025, 1, 1, 0, 0, second, tzinfo=timezone.utc)


def test_validate_detected_phases_from_tables_uses_library_phase_evaluator():
    phase_windows_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "t_start": _ts(0),
                "t_end": _ts(2),
                "phase_id_detected": 0,
                "phase_state_detected": "stable",
                "phase_confidence_detected": 0.9,
                "distance_to_centroid_detected": 0.1,
            }
        ]
    )
    phase_labels_df = pd.DataFrame.from_records(
        [
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(0), "phase_label": "gate_turnaround"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(1), "phase_label": "gate_turnaround"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(2), "phase_label": "gate_turnaround"},
        ]
    )

    summary = validate_detected_phases_from_tables(
        phase_windows_df=phase_windows_df,
        phase_labels_df=phase_labels_df,
    )

    assert summary["status"] == "ok"
    assert summary["assignment_count"] == 1
    assert summary["overall_accuracy"] == 1.0
    assert summary["macro_f1"] == 1.0
    assert summary["weighted_f1"] == 1.0


def test_validate_detected_phases_from_tables_prefers_windows_timestamps_when_provided():
    phase_windows_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "t_start": _ts(0),
                "t_end": _ts(0),
                "phase_id_detected": 0,
                "phase_state_detected": "stable",
                "phase_confidence_detected": 0.9,
                "distance_to_centroid_detected": 0.1,
            }
        ]
    )
    windows_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "t_start": _ts(10),
                "t_end": _ts(12),
            }
        ]
    )
    phase_labels_df = pd.DataFrame.from_records(
        [
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(10), "phase_label": "gate_turnaround"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(11), "phase_label": "gate_turnaround"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(12), "phase_label": "gate_turnaround"},
        ]
    )

    summary = validate_detected_phases_from_tables(
        phase_windows_df=phase_windows_df,
        phase_labels_df=phase_labels_df,
        windows_df=windows_df,
    )

    assert summary["status"] == "ok"
    assert summary["assignment_count"] == 1
    assert summary["overall_accuracy"] == 1.0
    assert summary["macro_f1"] == 1.0


def test_build_phase_validation_assignments_derives_truth_transition_context():
    phase_windows_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "t_start": _ts(0),
                "t_end": _ts(2),
                "phase_id_detected": 0,
                "phase_state_detected": "stable",
                "phase_confidence_detected": 0.9,
                "distance_to_centroid_detected": 0.1,
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 2,
                "t_start": _ts(3),
                "t_end": _ts(5),
                "phase_id_detected": 1,
                "phase_state_detected": "transition_region",
                "phase_confidence_detected": 0.3,
                "distance_to_centroid_detected": 0.5,
            },
        ]
    )
    phase_labels_df = pd.DataFrame.from_records(
        [
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(0), "phase_label": "gate_turnaround"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(1), "phase_label": "gate_turnaround"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(2), "phase_label": "gate_turnaround"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(3), "phase_label": "gate_turnaround"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(4), "phase_label": "takeoff_climb"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(5), "phase_label": "takeoff_climb"},
        ]
    )

    assignments = build_phase_validation_assignments(
        phase_windows_df=phase_windows_df,
        phase_labels_df=phase_labels_df,
    )

    assert assignments[0]["truth_phase_label_primary"] == "gate_turnaround"
    assert assignments[0]["truth_phase_state"] == "stable"
    assert assignments[0]["truth_transition_from_label"] is None
    assert assignments[0]["truth_transition_to_label"] is None
    assert assignments[1]["truth_phase_label_primary"] == "takeoff_climb"
    assert assignments[1]["truth_phase_state"] == "transition_region"
    assert assignments[1]["truth_transition_from_label"] == "gate_turnaround"
    assert assignments[1]["truth_transition_to_label"] == "takeoff_climb"


def test_validate_detected_phases_from_tables_reports_supplemental_transition_metrics():
    phase_windows_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "t_start": _ts(0),
                "t_end": _ts(1),
                "phase_id_detected": 0,
                "phase_state_detected": "stable",
                "phase_confidence_detected": 0.9,
                "distance_to_centroid_detected": 0.1,
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 2,
                "t_start": _ts(2),
                "t_end": _ts(3),
                "phase_id_detected": 0,
                "phase_state_detected": "transition_region",
                "transition_from_phase_id_detected": 0,
                "transition_to_phase_id_detected": 1,
                "phase_confidence_detected": 0.2,
                "distance_to_centroid_detected": 0.4,
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 3,
                "t_start": _ts(4),
                "t_end": _ts(5),
                "phase_id_detected": 1,
                "phase_state_detected": "stable",
                "phase_confidence_detected": 0.9,
                "distance_to_centroid_detected": 0.1,
            },
        ]
    )
    phase_labels_df = pd.DataFrame.from_records(
        [
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(0), "phase_label": "gate_turnaround"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(1), "phase_label": "gate_turnaround"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(2), "phase_label": "gate_turnaround"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(3), "phase_label": "takeoff_climb"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(4), "phase_label": "takeoff_climb"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(5), "phase_label": "takeoff_climb"},
        ]
    )

    summary = validate_detected_phases_from_tables(
        phase_windows_df=phase_windows_df,
        phase_labels_df=phase_labels_df,
    )

    assert summary["macro_f1"] == 1.0
    assert summary["transition_state_validation"]["status"] == "ok"
    assert summary["transition_state_validation"]["transition_region_precision"] == 1.0
    assert summary["transition_state_validation"]["transition_region_recall"] == 1.0
    assert summary["transition_state_validation"]["transition_region_f1"] == 1.0
    assert summary["transition_state_validation"]["truth_transition_counts_by_label_pair"] == [
        {
            "transition_from_label": "gate_turnaround",
            "transition_to_label": "takeoff_climb",
            "count": 1,
        }
    ]
    assert summary["transition_state_validation"]["detected_transition_counts_by_label_pair"] == [
        {
            "transition_from_label": "gate_turnaround",
            "transition_to_label": "takeoff_climb",
            "count": 1,
        }
    ]
    assert summary["transition_state_validation"]["transition_event_alignment"] == {
        "status": "ok",
        "truth_transition_event_count": 1,
        "detected_transition_event_count": 1,
        "truth_transition_event_counts_by_label_pair": [
            {
                "transition_from_label": "gate_turnaround",
                "transition_to_label": "takeoff_climb",
                "count": 1,
            }
        ],
        "detected_transition_event_counts_by_label_pair": [
            {
                "transition_from_label": "gate_turnaround",
                "transition_to_label": "takeoff_climb",
                "count": 1,
            }
        ],
        "matched_truth_transition_event_count": 1,
        "mean_abs_win_id_delta": 0.0,
        "mean_abs_progress_delta": 0.0,
        "nearest_detected_event_by_truth_transition": [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "transition_from_label": "gate_turnaround",
                "transition_to_label": "takeoff_climb",
                "truth_win_id_center": 2.0,
                "detected_win_id_center": 2.0,
                "abs_win_id_delta": 0.0,
                "abs_progress_delta": 0.0,
            }
        ],
    }


def test_validate_detected_phases_from_tables_reports_transition_event_alignment_when_shifted():
    phase_windows_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "t_start": _ts(0),
                "t_end": _ts(1),
                "phase_id_detected": 0,
                "phase_state_detected": "stable",
                "phase_confidence_detected": 0.9,
                "distance_to_centroid_detected": 0.1,
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 2,
                "t_start": _ts(2),
                "t_end": _ts(3),
                "phase_id_detected": 0,
                "phase_state_detected": "stable",
                "phase_confidence_detected": 0.9,
                "distance_to_centroid_detected": 0.1,
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 3,
                "t_start": _ts(4),
                "t_end": _ts(5),
                "phase_id_detected": 0,
                "phase_state_detected": "transition_region",
                "transition_from_phase_id_detected": 0,
                "transition_to_phase_id_detected": 1,
                "phase_confidence_detected": 0.2,
                "distance_to_centroid_detected": 0.4,
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 4,
                "t_start": _ts(6),
                "t_end": _ts(7),
                "phase_id_detected": 1,
                "phase_state_detected": "stable",
                "phase_confidence_detected": 0.9,
                "distance_to_centroid_detected": 0.1,
            },
        ]
    )
    phase_labels_df = pd.DataFrame.from_records(
        [
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(0), "phase_label": "gate_turnaround"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(1), "phase_label": "gate_turnaround"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(2), "phase_label": "gate_turnaround"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(3), "phase_label": "takeoff_climb"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(4), "phase_label": "takeoff_climb"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(5), "phase_label": "takeoff_climb"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(6), "phase_label": "takeoff_climb"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(7), "phase_label": "takeoff_climb"},
        ]
    )

    summary = validate_detected_phases_from_tables(
        phase_windows_df=phase_windows_df,
        phase_labels_df=phase_labels_df,
    )

    assert summary["transition_state_validation"]["transition_event_alignment"] == {
        "status": "ok",
        "truth_transition_event_count": 1,
        "detected_transition_event_count": 1,
        "truth_transition_event_counts_by_label_pair": [
            {
                "transition_from_label": "gate_turnaround",
                "transition_to_label": "takeoff_climb",
                "count": 1,
            }
        ],
        "detected_transition_event_counts_by_label_pair": [
            {
                "transition_from_label": "gate_turnaround",
                "transition_to_label": "takeoff_climb",
                "count": 1,
            }
        ],
        "matched_truth_transition_event_count": 1,
        "mean_abs_win_id_delta": 1.0,
        "mean_abs_progress_delta": 1.0 / 3.0,
        "nearest_detected_event_by_truth_transition": [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "transition_from_label": "gate_turnaround",
                "transition_to_label": "takeoff_climb",
                "truth_win_id_center": 2.0,
                "detected_win_id_center": 3.0,
                "abs_win_id_delta": 1.0,
                "abs_progress_delta": 1.0 / 3.0,
            }
        ],
    }


def test_build_phase_validation_summary_preserves_detected_transition_pairs_through_reporting_view(spark):
    phase_windows_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "t_start": _ts(0),
                "t_end": _ts(1),
                "phase_id_detected": 0,
                "phase_state_detected": "stable",
                "phase_confidence_detected": 0.9,
                "distance_to_centroid_detected": 0.1,
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 2,
                "t_start": _ts(2),
                "t_end": _ts(3),
                "phase_id_detected": 0,
                "phase_state_detected": "transition_region",
                "transition_from_phase_id_detected": 0,
                "transition_to_phase_id_detected": 1,
                "phase_confidence_detected": 0.2,
                "distance_to_centroid_detected": 0.4,
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 3,
                "t_start": _ts(4),
                "t_end": _ts(5),
                "phase_id_detected": 1,
                "phase_state_detected": "stable",
                "phase_confidence_detected": 0.9,
                "distance_to_centroid_detected": 0.1,
            },
        ]
    )
    phase_labels_df = spark.createDataFrame(
        [
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(0), "phase_label": "gate_turnaround"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(1), "phase_label": "gate_turnaround"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(2), "phase_label": "gate_turnaround"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(3), "phase_label": "takeoff_climb"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(4), "phase_label": "takeoff_climb"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": _ts(5), "phase_label": "takeoff_climb"},
        ]
    )
    windows_df = spark.createDataFrame(
        [
            {"tail_id": "T1", "flight_id": "F1", "win_id": 1, "t_start": _ts(0), "t_end": _ts(1), "date_utc": _ts(0).date()},
            {"tail_id": "T1", "flight_id": "F1", "win_id": 2, "t_start": _ts(2), "t_end": _ts(3), "date_utc": _ts(0).date()},
            {"tail_id": "T1", "flight_id": "F1", "win_id": 3, "t_start": _ts(4), "t_end": _ts(5), "date_utc": _ts(0).date()},
        ]
    )

    summary = _build_phase_validation_summary(
        RunArtifactBundle(
            tables={
                "phase_windows": phase_windows_df,
                "phase_labels": phase_labels_df,
                "windows": windows_df,
            }
        )
    )

    assert summary["transition_state_validation"]["detected_transition_counts_by_label_pair"] == [
        {
            "transition_from_label": "gate_turnaround",
            "transition_to_label": "takeoff_climb",
            "count": 1,
        }
    ]
    assert summary["transition_state_validation"]["transition_event_alignment"]["matched_truth_transition_event_count"] == 1


def test_build_graph_validation_summary_reports_hierarchy_and_expected_lag_edges():
    hierarchy_sensor_map_df = pd.DataFrame.from_records(
        [
            {"parameter_name": "a", "system_id": "SYS_A", "subsystem_id": "SUB_A", "module_id": "MOD_A"},
            {"parameter_name": "b", "system_id": "SYS_A", "subsystem_id": "SUB_A", "module_id": "MOD_B"},
        ]
    )
    hierarchy_label_df = hierarchy_sensor_map_df.copy()
    lag_graph_df = pd.DataFrame.from_records(
        [{"parameter_name_u": "a", "parameter_name_v": "b", "lag_weight": 0.7}]
    )

    summary = build_graph_validation_summary(
        hierarchy_sensor_map_df=hierarchy_sensor_map_df,
        hierarchy_label_df=hierarchy_label_df,
        lag_graph_df=lag_graph_df,
        expected_lag_edges=({"parameter_name_u": "a", "parameter_name_v": "b"},),
    )

    assert summary["hierarchy"]["status"] == "ok"
    assert summary["hierarchy"]["module_exact_match"] == 1.0
    assert summary["graph_signatures"]["lag_expected_edge_hit_rate"] == 1.0
    assert summary["hierarchy"]["subsystem_partition"]["same_cluster_pair_precision"] == 1.0
    assert summary["hierarchy"]["subsystem_partition"]["same_cluster_pair_recall"] == 1.0
    assert summary["hierarchy"]["system_partition"]["normalized_mutual_information"] == 1.0
    assert summary["hierarchy"]["system_partition"]["adjusted_mutual_information"] == 1.0
    assert summary["hierarchy"]["system_partition"]["adjusted_rand_index"] == 1.0
    assert summary["hierarchy"]["subsystem_partition"]["normalized_mutual_information"] == 1.0
    assert summary["hierarchy"]["subsystem_partition"]["adjusted_mutual_information"] == 1.0
    assert summary["hierarchy"]["subsystem_partition"]["adjusted_rand_index"] == 1.0
    assert summary["hierarchy"]["module_partition"]["normalized_mutual_information"] == 1.0
    assert summary["hierarchy"]["module_partition"]["adjusted_mutual_information"] == 1.0
    assert summary["hierarchy"]["module_partition"]["adjusted_rand_index"] == 1.0


def test_build_coupling_validation_summary_reports_expected_signatures():
    coupling_truth_df = pd.DataFrame.from_records(
        [
            {
                "coupling_id": "C1",
                "misbehavior_window_id": "MBW_C1",
                "misbehavior_family_label": "timing_lag",
                "misbehavior_detail_label": "timing_lag",
                "start_step": 10,
                "end_step_exclusive": 20,
            }
        ]
    )
    lag_graph_df = pd.DataFrame.from_records(
        [
            {
                "parameter_name_u": "outflow_cmd_pct",
                "parameter_name_v": "actuator_position_pct",
                "lag_weight": 0.8,
                "mean_lag_seconds": 1.5,
            }
        ]
    )

    summary = build_coupling_validation_summary(
        coupling_truth_df=coupling_truth_df,
        lag_graph_df=lag_graph_df,
        expected_coupling_signatures=(
            {
                "coupling_id": "C1",
                "parameter_name_u": "outflow_cmd_pct",
                "parameter_name_v": "actuator_position_pct",
                "signature_type": "lag_shift",
            },
        ),
    )

    assert summary["status"] == "ok"
    assert summary["coupling_window_count"] == 1
    assert summary["signature_count"] == 1
    assert summary["signature_hit_rate"] == 1.0


def test_score_and_fault_window_validators_summarize_fault_overlap():
    raw_telemetry_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": _ts(1),
                "parameter_name": "bleed_supply_psi",
                "system_id": "SYS_AIRFRAME",
                "subsystem_id": "SUB_AIR_BLEED",
                "module_id": "MOD_BLEED_SUPPLY",
                "misbehavior_active": True,
                "misbehavior_family_label": "saturation",
                "misbehavior_detail_label": "saturation",
                "misbehavior_window_id": "MBW1",
                "fault_active": True,
                "fault_family_label": "regulated",
                "fault_type": "saturation",
                "fault_window_id": "FW1",
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": _ts(2),
                "parameter_name": "bleed_supply_psi",
                "system_id": "SYS_AIRFRAME",
                "subsystem_id": "SUB_AIR_BLEED",
                "module_id": "MOD_BLEED_SUPPLY",
                "misbehavior_active": True,
                "misbehavior_family_label": "saturation",
                "misbehavior_detail_label": "saturation",
                "misbehavior_window_id": "MBW1",
                "fault_active": True,
                "fault_family_label": "regulated",
                "fault_type": "saturation",
                "fault_window_id": "FW1",
            },
        ]
    )
    windows_df = pd.DataFrame.from_records(
        [
            {"tail_id": "T1", "flight_id": "F1", "win_id": 1, "t_start": _ts(1), "t_end": _ts(2), "date_utc": _ts(0).date()},
        ]
    )
    calibrated_scores_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "date_utc": _ts(0).date(),
                "global_score": 12.0,
                "severity": "high",
                "emit_ready": True,
            }
        ]
    )

    score_summary = validate_scores_against_fault_windows(
        raw_telemetry_df=raw_telemetry_df,
        windows_df=windows_df,
        calibrated_scores_df=calibrated_scores_df,
    )
    misbehavior_score_summary = validate_scores_against_misbehavior_windows(
        raw_telemetry_df=raw_telemetry_df,
        windows_df=windows_df,
        calibrated_scores_df=calibrated_scores_df,
    )
    fault_summary = summarize_fault_window_detection(
        raw_telemetry_df=raw_telemetry_df,
        windows_df=windows_df,
        calibrated_scores_df=calibrated_scores_df,
    )
    misbehavior_summary = summarize_misbehavior_window_detection(
        raw_telemetry_df=raw_telemetry_df,
        windows_df=windows_df,
        calibrated_scores_df=calibrated_scores_df,
    )

    assert score_summary["status"] == "ok"
    assert score_summary["detected_fault_window_count"] == 1
    assert score_summary["emit_ready_fault_window_count"] == 1
    assert score_summary["detected_fault_window_rate"] == 1.0
    assert score_summary["emit_ready_fault_window_rate"] == 1.0
    assert misbehavior_score_summary["detected_misbehavior_window_count"] == 1
    assert misbehavior_score_summary["emit_ready_misbehavior_window_count"] == 1
    assert misbehavior_score_summary["detected_misbehavior_window_rate"] == 1.0
    assert misbehavior_score_summary["emit_ready_misbehavior_window_rate"] == 1.0
    assert fault_summary["fault_window_count"] == 1
    assert fault_summary["fault_windows"][0]["fault_window_id"] == "FW1"
    assert misbehavior_summary["misbehavior_window_count"] == 1
    assert misbehavior_summary["misbehavior_windows"][0]["misbehavior_window_id"] == "MBW1"


def test_anomaly_validator_compares_attribution_to_fault_truth():
    raw_telemetry_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": _ts(1),
                "parameter_name": "bleed_supply_psi",
                "system_id": "SYS_AIRFRAME",
                "subsystem_id": "SUB_AIR_BLEED",
                "module_id": "MOD_BLEED_SUPPLY",
                "misbehavior_active": True,
                "misbehavior_family_label": "saturation",
                "misbehavior_detail_label": "saturation",
                "misbehavior_window_id": "MBW1",
                "fault_active": True,
                "fault_family_label": "regulated",
                "fault_type": "saturation",
                "fault_window_id": "FW1",
            },
        ]
    )
    windows_df = pd.DataFrame.from_records(
        [
            {"tail_id": "T1", "flight_id": "F1", "win_id": 1, "t_start": _ts(1), "t_end": _ts(1)},
        ]
    )
    anomaly_window_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "dominant_subsystem_id": "SUB_AIR_BLEED",
                "dominant_module_id": "MOD_BLEED_SUPPLY",
                "top_subsystem_candidates": [
                    {"id": "SUB_AIR_BLEED", "support": 1.0, "best_rank": 1},
                ],
                "top_module_candidates": [
                    {"id": "MOD_BLEED_SUPPLY", "subsystem_id": "SUB_AIR_BLEED", "support": 1.0, "best_rank": 1},
                ],
                "dominant_score_component": RECONSTRUCTION_ERROR_CHANNEL,
            },
        ]
    )
    anomaly_telemetry_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "parameter_name": "bleed_supply_psi",
                "parameter_localization_selected": True,
            },
        ]
    )
    anomaly_event_df = pd.DataFrame.from_records(
        [
            {"tail_id": "T1", "flight_id": "F1", "win_id": 1, "parameter_name": "bleed_supply_psi"},
        ]
    )
    hierarchy_map_df = pd.DataFrame.from_records(
        [
            {
                "parameter_name": "bleed_supply_psi",
                "system_id": "SYS_AIRFRAME",
                "subsystem_id": "SUB_AIR_BLEED",
                "module_id": "MOD_BLEED_SUPPLY",
            }
        ]
    )

    summary = validate_attribution_against_fault_truth(
        raw_telemetry_df=raw_telemetry_df,
        windows_df=windows_df,
        anomaly_window_attribution_df=anomaly_window_df,
        anomaly_telemetry_attribution_df=anomaly_telemetry_df,
        anomaly_event_attribution_df=anomaly_event_df,
        hierarchy_sensor_map_df=hierarchy_map_df,
        hierarchy_label_df=hierarchy_map_df,
    )
    misbehavior_summary = validate_attribution_against_misbehavior_truth(
        raw_telemetry_df=raw_telemetry_df,
        windows_df=windows_df,
        anomaly_window_attribution_df=anomaly_window_df,
        anomaly_telemetry_attribution_df=anomaly_telemetry_df,
        anomaly_event_attribution_df=anomaly_event_df,
        hierarchy_sensor_map_df=hierarchy_map_df,
        hierarchy_label_df=hierarchy_map_df,
    )

    assert summary["status"] == "ok"
    assert summary["dominant_subsystem_match_rate"] == 1.0
    assert summary["dominant_subsystem_mappable_rate"] == 1.0
    assert summary["top_subsystem_candidate_present_rate"] == 1.0
    assert summary["dominant_module_match_rate"] == 1.0
    assert summary["dominant_module_mappable_rate"] == 1.0
    assert summary["top_module_candidate_present_rate"] == 1.0
    assert summary["telemetry_parameter_match_count"] == 1
    assert summary["event_parameter_match_count"] == 1
    assert summary["telemetry_parameter_match_rate"] == 1.0
    assert summary["event_parameter_match_rate"] == 1.0
    assert summary["parameter_localization_validation"]["exact_parameter_match_count_by_source"] == {
        "telemetry": 1,
        "telemetry_selected": 1,
        "event": 1,
        "any": 1,
        "both": 1,
    }
    assert summary["parameter_localization_validation"]["parameter_localization_cases"] == [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "fault_window_id": "FW1",
            "misbehavior_window_id": "MBW1",
            "subsystem_id": "SUB_AIR_BLEED",
            "module_id": "MOD_BLEED_SUPPLY",
            "parameter_name": "bleed_supply_psi",
            "dominant_score_component": RECONSTRUCTION_ERROR_CHANNEL,
            "overlapping_window_count": 1,
            "matched_attribution_window_count": 1,
            "telemetry_parameter_match": True,
            "telemetry_selected_parameter_match": True,
            "event_parameter_match": True,
            "any_parameter_match": True,
            "both_sources_parameter_match": True,
            "telemetry_truth_subsystem_present": True,
            "telemetry_selected_truth_subsystem_present": True,
            "event_truth_subsystem_present": True,
            "telemetry_truth_module_present": True,
            "telemetry_selected_truth_module_present": True,
            "event_truth_module_present": True,
            "telemetry_attributed_parameter_names": ["bleed_supply_psi"],
            "telemetry_selected_attributed_parameter_names": ["bleed_supply_psi"],
            "event_attributed_parameter_names": ["bleed_supply_psi"],
        }
    ]
    assert summary["channel_localization_validation"] == {
        "status": "ok",
        "truth_window_count": 1,
        "truth_window_count_by_score_component": {RECONSTRUCTION_ERROR_CHANNEL: 1},
        "dominant_subsystem_match_rate_by_score_component": {RECONSTRUCTION_ERROR_CHANNEL: 1.0},
        "dominant_module_match_rate_by_score_component": {RECONSTRUCTION_ERROR_CHANNEL: 1.0},
        "top_subsystem_candidate_present_rate_by_score_component": {RECONSTRUCTION_ERROR_CHANNEL: 1.0},
        "top_module_candidate_present_rate_by_score_component": {RECONSTRUCTION_ERROR_CHANNEL: 1.0},
        "telemetry_parameter_match_rate_by_score_component": {RECONSTRUCTION_ERROR_CHANNEL: 1.0},
        "telemetry_selected_parameter_match_rate_by_score_component": {RECONSTRUCTION_ERROR_CHANNEL: 1.0},
        "event_parameter_match_rate_by_score_component": {RECONSTRUCTION_ERROR_CHANNEL: 1.0},
        "channel_localization_cases": [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "fault_window_id": "FW1",
                "misbehavior_window_id": "MBW1",
                "subsystem_id": "SUB_AIR_BLEED",
                "module_id": "MOD_BLEED_SUPPLY",
                "parameter_name": "bleed_supply_psi",
                "dominant_score_component": RECONSTRUCTION_ERROR_CHANNEL,
                "dominant_subsystem_match": True,
                "dominant_subsystem_mappable": True,
                "dominant_module_match": True,
                "dominant_module_mappable": True,
                "top_subsystem_candidate_present": True,
                "top_module_candidate_present": True,
                "top_subsystem_candidate_ids_detected": ["SUB_AIR_BLEED"],
                "top_module_candidate_ids_detected": ["MOD_BLEED_SUPPLY"],
                "telemetry_parameter_match": True,
                "telemetry_selected_parameter_match": True,
                "event_parameter_match": True,
                "telemetry_selected_attributed_parameter_names": ["bleed_supply_psi"],
            }
        ],
    }
    assert summary["module_localization_validation"]["dominant_module_match_count"] == 1
    assert summary["module_localization_validation"]["top_module_candidate_present_count"] == 1
    assert summary["module_localization_validation"]["truth_module_present_rate_by_source"] == {
        "telemetry": 1.0,
        "event": 1.0,
    }
    assert misbehavior_summary["misbehavior_window_count"] == 1
    assert misbehavior_summary["dominant_subsystem_match_rate"] == 1.0
    assert misbehavior_summary["dominant_module_match_rate"] == 1.0
    assert misbehavior_summary["top_subsystem_candidate_present_rate"] == 1.0
    assert misbehavior_summary["top_module_candidate_present_rate"] == 1.0


def test_anomaly_validator_reports_ranked_candidate_presence_when_dominant_winner_is_wrong():
    raw_telemetry_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": _ts(1),
                "parameter_name": "bleed_supply_psi_aft",
                "system_id": "SYS_AIRFRAME",
                "subsystem_id": "SUB_AIR_BLEED_AFT",
                "module_id": "MOD_BLEED_SUPPLY_AFT",
                "misbehavior_active": True,
                "misbehavior_family_label": "saturation",
                "misbehavior_detail_label": "saturation",
                "misbehavior_window_id": "MBW1",
                "fault_active": True,
                "fault_family_label": "regulated",
                "fault_type": "saturation",
                "fault_window_id": "FW1",
            },
        ]
    )
    windows_df = pd.DataFrame.from_records(
        [
            {"tail_id": "T1", "flight_id": "F1", "win_id": 1, "t_start": _ts(1), "t_end": _ts(1)},
        ]
    )
    anomaly_window_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "dominant_subsystem_id": "SUB_AIR_BLEED_FORWARD",
                "dominant_module_id": "MOD_BLEED_SUPPLY",
                "top_subsystem_candidates": [
                    {"id": "SUB_AIR_BLEED_FORWARD", "support": 0.6, "best_rank": 1},
                    {"id": "SUB_AIR_BLEED_AFT", "support": 0.55, "best_rank": 2},
                ],
                "top_module_candidates": [
                    {"id": "MOD_BLEED_SUPPLY", "subsystem_id": "SUB_AIR_BLEED_FORWARD", "support": 0.6, "best_rank": 1},
                    {"id": "MOD_BLEED_SUPPLY_AFT", "subsystem_id": "SUB_AIR_BLEED_AFT", "support": 0.55, "best_rank": 2},
                ],
                "dominant_score_component": RECONSTRUCTION_ERROR_CHANNEL,
            },
        ]
    )
    anomaly_telemetry_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "parameter_name": "bleed_supply_psi",
                "parameter_localization_selected": True,
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "parameter_name": "bleed_supply_psi_aft",
                "parameter_localization_selected": True,
            },
        ]
    )
    anomaly_event_df = pd.DataFrame.from_records([])
    hierarchy_map_df = pd.DataFrame.from_records(
        [
            {
                "parameter_name": "bleed_supply_psi",
                "system_id": "SYS_AIRFRAME",
                "subsystem_id": "SUB_AIR_BLEED_FORWARD",
                "module_id": "MOD_BLEED_SUPPLY",
            },
            {
                "parameter_name": "bleed_supply_psi_aft",
                "system_id": "SYS_AIRFRAME",
                "subsystem_id": "SUB_AIR_BLEED_AFT",
                "module_id": "MOD_BLEED_SUPPLY_AFT",
            },
        ]
    )

    summary = validate_attribution_against_misbehavior_truth(
        raw_telemetry_df=raw_telemetry_df,
        windows_df=windows_df,
        anomaly_window_attribution_df=anomaly_window_df,
        anomaly_telemetry_attribution_df=anomaly_telemetry_df,
        anomaly_event_attribution_df=anomaly_event_df,
        hierarchy_sensor_map_df=hierarchy_map_df,
        hierarchy_label_df=hierarchy_map_df,
    )

    assert summary["dominant_subsystem_match_rate"] == 0.0
    assert summary["top_subsystem_candidate_present_rate"] == 1.0
    assert summary["dominant_module_match_rate"] == 0.0
    assert summary["top_module_candidate_present_rate"] == 1.0
    assert summary["channel_localization_validation"]["top_subsystem_candidate_present_rate_by_score_component"] == {
        RECONSTRUCTION_ERROR_CHANNEL: 1.0
    }
    assert summary["channel_localization_validation"]["top_module_candidate_present_rate_by_score_component"] == {
        RECONSTRUCTION_ERROR_CHANNEL: 1.0
    }


def test_score_validator_reports_raw_calibrated_and_emission_diagnostics():
    raw_telemetry_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": _ts(10),
                "parameter_name": "bleed_supply_psi",
                "system_id": "SYS_AIRFRAME",
                "subsystem_id": "SUB_AIR_BLEED",
                "module_id": "MOD_BLEED_SUPPLY",
                "misbehavior_active": True,
                "misbehavior_family_label": "saturation",
                "misbehavior_detail_label": "saturation",
                "misbehavior_window_id": "MBW1",
                "fault_active": True,
                "fault_family_label": "regulated",
                "fault_type": "saturation",
                "fault_window_id": "FW1",
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": _ts(11),
                "parameter_name": "bleed_supply_psi",
                "system_id": "SYS_AIRFRAME",
                "subsystem_id": "SUB_AIR_BLEED",
                "module_id": "MOD_BLEED_SUPPLY",
                "misbehavior_active": True,
                "misbehavior_family_label": "saturation",
                "misbehavior_detail_label": "saturation",
                "misbehavior_window_id": "MBW1",
                "fault_active": True,
                "fault_family_label": "regulated",
                "fault_type": "saturation",
                "fault_window_id": "FW1",
            },
        ]
    )
    windows_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "t_start": _ts(10),
                "t_end": _ts(11),
                "event_count": 2,
                "real_event_count": 2,
                "close_reason": "event_threshold",
                "date_utc": _ts(10).date(),
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 2,
                "t_start": _ts(8),
                "t_end": _ts(11),
                "event_count": 1,
                "real_event_count": 1,
                "close_reason": "budget_threshold",
                "date_utc": _ts(8).date(),
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 3,
                "t_start": _ts(20),
                "t_end": _ts(21),
                "event_count": 0,
                "real_event_count": 0,
                "close_reason": "budget_threshold",
                "date_utc": _ts(20).date(),
            },
        ]
    )
    raw_scores_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "date_utc": _ts(10).date(),
                "phase_id_detected": 0,
                "phase_state_detected": "transition_region",
                "phase_confidence_detected": 0.4,
                "distance_to_centroid_detected": 1.0,
                "global_score": 12.0,
                "severity": "high",
                "dominant_subsystem_id": "SUB_AIR_BLEED",
                "dominant_score_component": RECONSTRUCTION_ERROR_CHANNEL,
                "score_component_scores": score_component_scores_with_updates(
                    {
                        RECONSTRUCTION_ERROR_CHANNEL: 12.0,
                    }
                ),
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 2,
                "date_utc": _ts(8).date(),
                "phase_id_detected": 0,
                "phase_state_detected": "transition_region",
                "phase_confidence_detected": 0.3,
                "distance_to_centroid_detected": 2.0,
                "global_score": 6.0,
                "severity": "medium",
                "dominant_subsystem_id": "SUB_AIR_BLEED",
                "dominant_score_component": REGIME_DEVIATION_CHANNEL,
                "score_component_scores": score_component_scores_with_updates(
                    {
                        REGIME_DEVIATION_CHANNEL: 6.0,
                    }
                ),
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 3,
                "date_utc": _ts(20).date(),
                "phase_id_detected": 1,
                "phase_state_detected": "stable",
                "phase_confidence_detected": 0.9,
                "distance_to_centroid_detected": 0.1,
                "global_score": 0.1,
                "severity": "normal",
                "dominant_subsystem_id": "SUB_AIR_BLEED",
                "dominant_score_component": REGIME_DEVIATION_CHANNEL,
                "score_component_scores": score_component_scores_with_updates(
                    {
                        REGIME_DEVIATION_CHANNEL: 0.1,
                    }
                ),
            },
        ]
    )
    calibrated_scores_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "date_utc": _ts(10).date(),
                "global_score": 12.0,
                "p_value": 0.01,
                "severity": "high",
                "emit_ready": True,
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 2,
                "date_utc": _ts(8).date(),
                "global_score": 6.0,
                "p_value": 0.02,
                "severity": "medium",
                "emit_ready": False,
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 3,
                "date_utc": _ts(20).date(),
                "global_score": 0.1,
                "p_value": 0.9,
                "severity": "normal",
                "emit_ready": False,
            },
        ]
    )

    summary = validate_scores_against_misbehavior_windows(
        raw_telemetry_df=raw_telemetry_df,
        windows_df=windows_df,
        raw_scores_df=raw_scores_df,
        calibrated_scores_df=calibrated_scores_df,
    )

    assert summary["status"] == "ok"
    assert summary["raw_score_validation"]["window_count_by_truth_overlap_bucket"] == {
        "strict_overlap": 1,
        "soft_overlap": 1,
        "no_overlap": 1,
    }
    assert summary["raw_score_validation"]["truth_window_recall_by_top_k_raw_score"]["any_overlap"]["top_1"] == 1.0
    assert summary["raw_score_validation"]["truth_window_recall_by_top_k_raw_score"]["strict_overlap"]["top_1"] == 1.0
    assert "channel_validation" in summary["raw_score_validation"]
    assert summary["calibrated_score_validation"]["truth_window_recall_by_top_k_calibrated_rarity"]["any_overlap"][
        "top_1"
    ] == 1.0
    assert summary["emission_validation"]["blocked_candidate_window_count_by_p_value_threshold"]["p_le_0p05"] == 1
    assert summary["emission_validation"]["emit_ready_candidate_window_count_by_p_value_threshold"]["p_le_0p05"] == 1
    assert summary["score_window_diagnostics"][0]["global_score_raw"] >= summary["score_window_diagnostics"][1]["global_score_raw"]
    assert any(row["truth_overlap_bucket"] == "soft_overlap" for row in summary["score_window_diagnostics"])


def test_anomaly_validator_handles_truth_windows_with_no_overlapping_detected_windows():
    raw_telemetry_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": _ts(10),
                "parameter_name": "bleed_supply_psi",
                "system_id": "SYS_AIRFRAME",
                "subsystem_id": "SUB_AIR_BLEED",
                "module_id": "MOD_BLEED_SUPPLY",
                "misbehavior_active": True,
                "misbehavior_family_label": "saturation",
                "misbehavior_detail_label": "saturation",
                "misbehavior_window_id": "MBW1",
                "fault_active": True,
                "fault_family_label": "regulated",
                "fault_type": "saturation",
                "fault_window_id": "FW1",
            },
        ]
    )
    windows_df = pd.DataFrame.from_records(
        [
            {"tail_id": "T1", "flight_id": "F1", "win_id": 1, "t_start": _ts(1), "t_end": _ts(2)},
        ]
    )

    summary = validate_attribution_against_misbehavior_truth(
        raw_telemetry_df=raw_telemetry_df,
        windows_df=windows_df,
        anomaly_window_attribution_df=pd.DataFrame(),
        anomaly_telemetry_attribution_df=pd.DataFrame(),
        anomaly_event_attribution_df=pd.DataFrame(),
    )

    assert summary["status"] == "ok"
    assert summary["misbehavior_window_count"] == 1
    assert summary["dominant_subsystem_match_count"] == 0
    assert summary["dominant_subsystem_match_rate"] == 0.0
    assert summary["telemetry_parameter_match_count"] == 0
    assert summary["event_parameter_match_count"] == 0
    assert summary["misbehavior_windows"][0]["overlapping_window_count"] == 0
    assert summary["misbehavior_windows"][0]["primary_win_id"] is None


def test_anomaly_validator_credits_short_contained_window_alignment():
    raw_telemetry_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": _ts(second),
                "parameter_name": "bleed_supply_psi",
                "system_id": "SYS_AIRFRAME",
                "subsystem_id": "SUB_AIR_BLEED",
                "module_id": "MOD_BLEED_SUPPLY",
                "misbehavior_active": True,
                "misbehavior_family_label": "saturation",
                "misbehavior_detail_label": "saturation",
                "misbehavior_window_id": "MBW1",
                "fault_active": True,
                "fault_family_label": "regulated",
                "fault_type": "saturation",
                "fault_window_id": "FW1",
            }
            for second in range(10, 21)
        ]
    )
    windows_df = pd.DataFrame.from_records(
        [
            {"tail_id": "T1", "flight_id": "F1", "win_id": 5, "t_start": _ts(18), "t_end": _ts(19)},
        ]
    )
    anomaly_window_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 5,
                "dominant_subsystem_id": "SUB_AIR_BLEED",
                "dominant_module_id": "MOD_BLEED_SUPPLY",
            },
        ]
    )
    anomaly_telemetry_df = pd.DataFrame.from_records(
        [
            {"tail_id": "T1", "flight_id": "F1", "win_id": 5, "parameter_name": "bleed_supply_psi"},
        ]
    )
    anomaly_event_df = anomaly_telemetry_df.copy()

    summary = validate_attribution_against_misbehavior_truth(
        raw_telemetry_df=raw_telemetry_df,
        windows_df=windows_df,
        anomaly_window_attribution_df=anomaly_window_df,
        anomaly_telemetry_attribution_df=anomaly_telemetry_df,
        anomaly_event_attribution_df=anomaly_event_df,
    )

    assert summary["dominant_subsystem_match_rate"] == 1.0
    assert summary["telemetry_parameter_match_rate"] == 1.0
    assert summary["event_parameter_match_rate"] == 1.0
    assert summary["misbehavior_windows"][0]["overlapping_window_count"] == 1
    assert summary["misbehavior_windows"][0]["matched_attribution_window_count"] == 1
    assert summary["misbehavior_windows"][0]["primary_win_id"] == 5
    assert summary["misbehavior_windows"][0]["telemetry_attributed_parameter_names"] == ["bleed_supply_psi"]
    assert summary["misbehavior_windows"][0]["event_attributed_parameter_names"] == ["bleed_supply_psi"]
    assert summary["misbehavior_windows"][0]["strict_window_coverage_threshold"] == 0.5
    assert summary["parameter_localization_validation"]["exact_parameter_match_rate_by_source"]["any"] == 1.0


def test_score_validator_does_not_credit_early_broad_overlap():
    raw_telemetry_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": _ts(10),
                "parameter_name": "bleed_supply_psi",
                "system_id": "SYS_AIRFRAME",
                "subsystem_id": "SUB_AIR_BLEED",
                "module_id": "MOD_BLEED_SUPPLY",
                "misbehavior_active": True,
                "misbehavior_family_label": "saturation",
                "misbehavior_detail_label": "saturation",
                "misbehavior_window_id": "MBW1",
                "fault_active": True,
                "fault_family_label": "regulated",
                "fault_type": "saturation",
                "fault_window_id": "FW1",
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": _ts(11),
                "parameter_name": "bleed_supply_psi",
                "system_id": "SYS_AIRFRAME",
                "subsystem_id": "SUB_AIR_BLEED",
                "module_id": "MOD_BLEED_SUPPLY",
                "misbehavior_active": True,
                "misbehavior_family_label": "saturation",
                "misbehavior_detail_label": "saturation",
                "misbehavior_window_id": "MBW1",
                "fault_active": True,
                "fault_family_label": "regulated",
                "fault_type": "saturation",
                "fault_window_id": "FW1",
            },
        ]
    )
    windows_df = pd.DataFrame.from_records(
        [
            {"tail_id": "T1", "flight_id": "F1", "win_id": 1, "t_start": _ts(8), "t_end": _ts(11), "date_utc": _ts(8).date()},
        ]
    )
    calibrated_scores_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "date_utc": _ts(8).date(),
                "global_score": 12.0,
                "severity": "high",
                "emit_ready": True,
            }
        ]
    )

    summary = validate_scores_against_fault_windows(
        raw_telemetry_df=raw_telemetry_df,
        windows_df=windows_df,
        calibrated_scores_df=calibrated_scores_df,
    )

    assert summary["detected_fault_window_count"] == 0
    assert summary["emit_ready_fault_window_count"] == 0


def test_attribution_validator_uses_earliest_qualifying_window_only():
    raw_telemetry_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": _ts(10),
                "parameter_name": "bleed_supply_psi",
                "system_id": "SYS_AIRFRAME",
                "subsystem_id": "SUB_AIR_BLEED",
                "module_id": "MOD_BLEED_SUPPLY",
                "misbehavior_active": True,
                "misbehavior_family_label": "saturation",
                "misbehavior_detail_label": "saturation",
                "misbehavior_window_id": "MBW1",
                "fault_active": True,
                "fault_family_label": "regulated",
                "fault_type": "saturation",
                "fault_window_id": "FW1",
            },
        ]
    )
    windows_df = pd.DataFrame.from_records(
        [
            {"tail_id": "T1", "flight_id": "F1", "win_id": 1, "t_start": _ts(8), "t_end": _ts(10)},
            {"tail_id": "T1", "flight_id": "F1", "win_id": 2, "t_start": _ts(10), "t_end": _ts(10)},
        ]
    )
    anomaly_window_df = pd.DataFrame.from_records(
        [
            {"tail_id": "T1", "flight_id": "F1", "win_id": 1, "dominant_subsystem_id": "SUB_WRONG"},
            {"tail_id": "T1", "flight_id": "F1", "win_id": 2, "dominant_subsystem_id": "SUB_AIR_BLEED"},
        ]
    )
    anomaly_telemetry_df = pd.DataFrame.from_records(
        [
            {"tail_id": "T1", "flight_id": "F1", "win_id": 1, "parameter_name": "wrong_parameter"},
            {"tail_id": "T1", "flight_id": "F1", "win_id": 2, "parameter_name": "bleed_supply_psi"},
        ]
    )
    anomaly_event_df = anomaly_telemetry_df.copy()

    summary = validate_attribution_against_fault_truth(
        raw_telemetry_df=raw_telemetry_df,
        windows_df=windows_df,
        anomaly_window_attribution_df=anomaly_window_df,
        anomaly_telemetry_attribution_df=anomaly_telemetry_df,
        anomaly_event_attribution_df=anomaly_event_df,
    )

    assert summary["telemetry_parameter_match_rate"] == 1.0
    assert summary["event_parameter_match_rate"] == 1.0
    assert summary["parameter_localization_validation"]["exact_parameter_match_count_by_source"]["both"] == 1
    assert summary["parameter_localization_validation"]["exact_parameter_match_count_by_source"]["telemetry_selected"] == 0
