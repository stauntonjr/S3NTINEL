from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from libs.anomaly import validate_attribution_against_fault_truth
from libs.graph import build_graph_validation_summary
from libs.phase import validate_detected_phases_from_tables
from libs.scoring import summarize_fault_window_detection, validate_scores_against_fault_windows


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
                "phase_state_detected": "gate_turnaround",
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
                "phase_state_detected": "gate_turnaround",
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
                "fault_active": True,
                "fault_family_label": "regulated",
                "fault_type": "saturation",
                "fault_window_id": "FW1",
            },
        ]
    )
    windows_df = pd.DataFrame.from_records(
        [
            {"tail_id": "T1", "flight_id": "F1", "win_id": 1, "t_start": _ts(0), "t_end": _ts(3), "date_utc": _ts(0).date()},
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
    fault_summary = summarize_fault_window_detection(
        raw_telemetry_df=raw_telemetry_df,
        windows_df=windows_df,
        calibrated_scores_df=calibrated_scores_df,
    )

    assert score_summary["status"] == "ok"
    assert score_summary["detected_fault_window_count"] == 1
    assert score_summary["emit_ready_fault_window_count"] == 1
    assert fault_summary["fault_window_count"] == 1
    assert fault_summary["fault_windows"][0]["fault_window_id"] == "FW1"


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
                "fault_active": True,
                "fault_family_label": "regulated",
                "fault_type": "saturation",
                "fault_window_id": "FW1",
            },
        ]
    )
    windows_df = pd.DataFrame.from_records(
        [
            {"tail_id": "T1", "flight_id": "F1", "win_id": 1, "t_start": _ts(0), "t_end": _ts(3)},
        ]
    )
    anomaly_window_df = pd.DataFrame.from_records(
        [
            {"tail_id": "T1", "flight_id": "F1", "win_id": 1, "dominant_subsystem_id": "SUB_AIR_BLEED"},
        ]
    )
    anomaly_telemetry_df = pd.DataFrame.from_records(
        [
            {"tail_id": "T1", "flight_id": "F1", "win_id": 1, "parameter_name": "bleed_supply_psi"},
        ]
    )
    anomaly_event_df = pd.DataFrame.from_records(
        [
            {"tail_id": "T1", "flight_id": "F1", "win_id": 1, "parameter_name": "bleed_supply_psi"},
        ]
    )

    summary = validate_attribution_against_fault_truth(
        raw_telemetry_df=raw_telemetry_df,
        windows_df=windows_df,
        anomaly_window_attribution_df=anomaly_window_df,
        anomaly_telemetry_attribution_df=anomaly_telemetry_df,
        anomaly_event_attribution_df=anomaly_event_df,
    )

    assert summary["status"] == "ok"
    assert summary["dominant_subsystem_match_rate"] == 1.0
    assert summary["dominant_subsystem_mappable_rate"] == 1.0
    assert summary["telemetry_parameter_match_rate"] == 1.0
    assert summary["event_parameter_match_rate"] == 1.0
