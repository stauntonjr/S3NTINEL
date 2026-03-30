from datetime import date, datetime

from libs.anomaly import (
    AnomalyAttributionContextFrame,
    AnomalyAttributionPlan,
    AnomalyPanelContextFrame,
    AnomalySubsystemContextFrame,
)
from libs.testing.data import (
    create_sample_calibrated_df,
    create_sample_events_df,
    create_sample_raw_table_df,
    create_sample_windows_df,
)


def _hierarchy_sensor_map_df(spark):
    return spark.createDataFrame(
        [
            {"parameter_name": "ENG_TEMP_1", "system_id": "SYS_0001", "subsystem_id": "SUBSYS_0001", "module_id": "MOD_0001"},
            {"parameter_name": "PUMP_STATE", "system_id": "SYS_0001", "subsystem_id": "SUBSYS_0002", "module_id": "MOD_0002"},
        ]
    )


def test_anomaly_subsystem_context_builds_top_sensors_and_scores(spark):
    context_df = AnomalySubsystemContextFrame.from_events_and_windows(
        events_df=create_sample_events_df(spark),
        windows_df=create_sample_windows_df(spark),
        hierarchy_sensor_map_df=_hierarchy_sensor_map_df(spark),
        top_k_per_subsystem=3,
    ).to_dataframe()

    row = context_df.where("win_id = 1").collect()[0]
    assert row["top_sensors_by_subsystem"]
    assert row["sensor_scores"]


def test_anomaly_panel_context_extracts_text_message_codes_and_sources(spark):
    windows_df = create_sample_windows_df(spark)
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

    panel_df = AnomalyPanelContextFrame.from_raw_and_windows(raw_df=raw_df, windows_df=windows_df).to_dataframe()
    row = panel_df.where("win_id = 1").collect()[0]
    assert "HYD_PRESS_LOW" in list(row["panel_context"]["text"])
    assert "HYD_PRESS_LOW" in list(row["panel_context"]["message_codes"])
    assert "LCD_MSG" in list(row["panel_context"]["source"])


def test_anomaly_models_build_expected_dataframes(spark):
    calibrated_df = create_sample_calibrated_df(spark)
    windows_df = create_sample_windows_df(spark)
    raw_df = create_sample_raw_table_df(spark)
    events_df = create_sample_events_df(spark)
    hierarchy_sensor_map_df = _hierarchy_sensor_map_df(spark)
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
    attribution_context_df = AnomalyAttributionContextFrame.from_context_frames(
        subsystem_context=AnomalySubsystemContextFrame.from_events_and_windows(
            events_df=events_df,
            windows_df=windows_df,
            hierarchy_sensor_map_df=hierarchy_sensor_map_df,
            top_k_per_subsystem=3,
        ),
        panel_context=AnomalyPanelContextFrame.from_raw_and_windows(raw_df=raw_df, windows_df=windows_df),
    ).to_dataframe()

    telemetry_df = AnomalyAttributionPlan(top_k_per_subsystem=3).build_telemetry_attribution(
        calibrated_df=calibrated_df,
        windows_df=windows_df,
        raw_df=raw_df,
        hierarchy_sensor_map_df=hierarchy_sensor_map_df,
    ).to_dataframe()
    event_df = AnomalyAttributionPlan(top_k_per_subsystem=3).build_event_attribution(
        calibrated_df=calibrated_df,
        windows_df=windows_df,
        events_df=events_df,
        hierarchy_sensor_map_df=hierarchy_sensor_map_df,
    ).to_dataframe()
    window_df = AnomalyAttributionPlan(top_k_per_subsystem=3).build_window_attribution(
        calibrated_df=calibrated_df,
        phase_windows_df=phase_windows_df,
        windows_df=windows_df,
        events_df=events_df,
        hierarchy_sensor_map_df=hierarchy_sensor_map_df,
        raw_df=raw_df,
    ).to_dataframe()

    assert telemetry_df.count() > 0
    assert event_df.count() > 0
    row = window_df.where("win_id = 1").collect()[0]
    assert row["artifact_versions"]["graph"] == 1
    assert row["attribution_context"] is not None
