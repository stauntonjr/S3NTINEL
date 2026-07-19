from datetime import date, datetime

import pytest

from libs.anomaly import AnomalyAttributionPlan, AnomalyParameterCandidateEvidenceTable
from libs.scoring import SCORE_COMPONENT_NAMES
from libs.testing.data import (
    create_sample_calibrated_df,
    create_sample_events_df,
    create_sample_raw_table_df,
    create_sample_windows_df,
)


def test_anomaly_object_includes_panel_context_component_contrib_and_sensor_scores(spark):
    calibrated_df = create_sample_calibrated_df(spark)
    phase_windows_df = spark.createDataFrame(
        [
            {"tail_id": "T001", "flight_id": "F001", "win_id": 1, "phase_id_detected": 0, "phase_state_detected": "stable", "phase_confidence_detected": 0.91, "distance_to_centroid_detected": 0.12, "drift_magnitude": 1.8, "breadth": 0.35, "date_utc": date(2026, 2, 28)},
            {"tail_id": "T001", "flight_id": "F001", "win_id": 2, "phase_id_detected": 3, "phase_state_detected": "transition_region", "phase_confidence_detected": 0.44, "distance_to_centroid_detected": 0.88, "drift_magnitude": 4.5, "breadth": 0.72, "date_utc": date(2026, 2, 28)},
        ]
    )
    windows_df = create_sample_windows_df(spark)
    events_df = create_sample_events_df(spark)

    hierarchy_sensor_map_df = spark.createDataFrame(
        [
            {"parameter_name": "ENG_TEMP_1", "system_id": "SYS_0001", "subsystem_id": "SUBSYS_0001", "module_id": "MOD_0001"},
            {"parameter_name": "PUMP_STATE", "system_id": "SYS_0001", "subsystem_id": "SUBSYS_0002", "module_id": "MOD_0002"},
        ]
    )

    raw_df = spark.createDataFrame(
        [
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "timestamp_utc": datetime(2026, 2, 28, 0, 0, 0, 250000),
                "parameter_name": "LCD_MSG",
                "parameter_value": "HYD_PRESS_LOW",
                "date_utc": date(2026, 2, 28),
            },
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "timestamp_utc": datetime(2026, 2, 28, 0, 0, 0, 850000),
                "parameter_name": "PANEL_TEXT",
                "parameter_value": "ELEC_WARN_12",
                "date_utc": date(2026, 2, 28),
            },
        ]
    )

    anomaly_window_attribution_df = AnomalyAttributionPlan(top_k_per_subsystem=3).build_window_attribution(
        calibrated_df=calibrated_df,
        phase_windows_df=phase_windows_df,
        windows_df=windows_df,
        events_df=events_df,
        hierarchy_sensor_map_df=hierarchy_sensor_map_df,
        raw_df=raw_df,
    ).to_dataframe()

    first = anomaly_window_attribution_df.where("win_id = 1").collect()[0]

    assert first["panel_context"] is not None
    assert "HYD_PRESS_LOW" in list(first["panel_context"]["text"])
    assert "HYD_PRESS_LOW" in list(first["panel_context"]["message_codes"])
    assert "LCD_MSG" in list(first["panel_context"]["source"])

    assert first["subsystems"]
    first_subsystem = first["subsystems"][0]
    for key in SCORE_COMPONENT_NAMES:
        assert key in first_subsystem["score_component_contrib"]

    assert first["attribution_context"] is not None
    assert first["attribution_context"]["sensor_scores"]


def test_anomaly_window_attribution_sets_v2_version_fields(spark):
    calibrated_df = create_sample_calibrated_df(spark)
    phase_windows_df = spark.createDataFrame(
        [
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "win_id": 1,
                "phase_id_detected": 0,
                "phase_state_detected": "stable",
                "phase_confidence_detected": 0.91,
                "distance_to_centroid_detected": 0.12,
                "drift_magnitude": 1.8,
                "breadth": 0.35,
                "date_utc": date(2026, 2, 28),
            }
        ]
    )
    windows_df = create_sample_windows_df(spark)
    events_df = create_sample_events_df(spark)
    raw_df = create_sample_raw_table_df(spark)
    hierarchy_sensor_map_df = spark.createDataFrame(
        [
            {"parameter_name": "ENG_TEMP_1", "system_id": "SYS_0001", "subsystem_id": "SUBSYS_0001", "module_id": "MOD_0001"},
            {"parameter_name": "PUMP_STATE", "system_id": "SYS_0001", "subsystem_id": "SUBSYS_0002", "module_id": "MOD_0002"},
        ]
    )

    anomaly_window_attribution_df = AnomalyAttributionPlan(top_k_per_subsystem=3).build_window_attribution(
        calibrated_df=calibrated_df,
        phase_windows_df=phase_windows_df,
        windows_df=windows_df,
        events_df=events_df,
        hierarchy_sensor_map_df=hierarchy_sensor_map_df,
        raw_df=raw_df,
    ).to_dataframe()

    row = anomaly_window_attribution_df.where("win_id = 1").collect()[0]
    assert row["artifact_versions"]["backbone"] == 1
    assert row["artifact_versions"]["graph"] == 1
    assert row["artifact_versions"]["phase"] == 1
    assert row["panel_context"] is not None


def test_parameter_candidate_evidence_preserves_score_inputs_and_localization_cuts(spark):
    calibrated_df = create_sample_calibrated_df(spark)
    localization_df = spark.createDataFrame(
        [
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "win_id": 1,
                "date_utc": date(2026, 2, 28),
                "parameter_name": "ENG_TEMP_1",
                "parameter_localization_support": 0.75,
                "parameter_support_rank_in_window": 2,
            }
        ]
    )
    hierarchy_df = spark.createDataFrame(
        [
            {
                "parameter_name": "ENG_TEMP_1",
                "system_id": "SYS_0001",
                "subsystem_id": "SUBSYS_0001",
                "module_id": "MOD_0001",
            }
        ]
    )

    evidence_df = AnomalyParameterCandidateEvidenceTable.from_calibrated_scores_and_localization(
        calibrated_df=calibrated_df,
        parameter_localization_df=localization_df,
        hierarchy_sensor_map_df=hierarchy_df,
    ).to_dataframe()
    row = evidence_df.where("win_id = 1").collect()[0]

    assert row["parameter_name"] == "ENG_TEMP_1"
    assert set(row["candidate_sources"]) == {"residual", "event"}
    assert set(row["candidate_channels"]) == {"reconstruction_error", "event_discordance"}
    assert row["residual_share"] == pytest.approx(0.8)
    assert row["parameter_localization_support"] == pytest.approx(0.75)
    assert row["telemetry_retained"] is True
    assert row["structural_cut_retained"] is True
