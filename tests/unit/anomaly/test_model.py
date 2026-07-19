from datetime import date, datetime

from libs.anomaly import (
    AnomalyAttributionContextFrame,
    AnomalyAttributionPlan,
    AnomalyPanelContextFrame,
    AnomalyParameterLocalizationFrame,
    AnomalySubsystemContextFrame,
)
from libs.anomaly.frames import ANOMALY_LOCALIZATION_PARAMETER_TOP_K
from libs.scoring.channels import (
    ACCUMULATION_VIOLATION_CHANNEL,
    EVENT_DISCORDANCE_CHANNEL,
    RECONSTRUCTION_ERROR_CHANNEL,
    REGIME_DEVIATION_CHANNEL,
    RESPONSE_VIOLATION_CHANNEL,
    score_component_scores_with_updates,
)
from libs.testing.data import (
    create_sample_calibrated_df,
    create_sample_events_df,
    create_sample_phase_windows_df,
    create_sample_raw_table_df,
    create_sample_windows_df,
)


def _hierarchy_sensor_map_df(spark):
    return spark.createDataFrame(
        [
            {"parameter_name": "ENG_TEMP_1", "system_id": "SYS_0001", "subsystem_id": "SUBSYS_0001", "module_id": "MOD_0001"},
            {"parameter_name": "HYD_PRESS_1", "system_id": "SYS_0001", "subsystem_id": "SUBSYS_0001", "module_id": "MOD_0002"},
            {"parameter_name": "PUMP_STATE", "system_id": "SYS_0001", "subsystem_id": "SUBSYS_0002", "module_id": "MOD_0003"},
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
    phase_windows_df = create_sample_phase_windows_df(spark)
    attribution_context_df = AnomalyAttributionContextFrame.from_context_frames(
        subsystem_context=AnomalySubsystemContextFrame.from_events_and_windows(
            events_df=events_df,
            windows_df=windows_df,
            hierarchy_sensor_map_df=hierarchy_sensor_map_df,
            top_k_per_subsystem=3,
        ),
        panel_context=AnomalyPanelContextFrame.from_raw_and_windows(raw_df=raw_df, windows_df=windows_df),
    ).to_dataframe()
    assert attribution_context_df.count() > 0

    telemetry_df = AnomalyAttributionPlan(top_k_per_subsystem=3).build_telemetry_attribution(
        calibrated_df=calibrated_df,
        phase_windows_df=phase_windows_df,
        windows_df=windows_df,
        events_df=events_df,
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
    assert row["dominant_subsystem_id"] == "SUBSYS_0001"
    assert row["dominant_module_id"] == "MOD_0001"
    assert row["top_subsystem_candidates"][0]["id"] == "SUBSYS_0001"
    assert row["top_module_candidates"][0]["id"] == "MOD_0001"

    telemetry_rows = telemetry_df.where("win_id = 1").select(
        "parameter_name",
        "parameter_localization_selected",
        "parameter_support_rank_in_window",
    ).collect()
    telemetry_row_by_key = {
        (row["parameter_name"], row["parameter_localization_selected"]): row
        for row in telemetry_rows
    }
    assert telemetry_row_by_key[("ENG_TEMP_1", True)]["parameter_support_rank_in_window"] == 1
    assert telemetry_row_by_key[("HYD_PRESS_1", True)]["parameter_support_rank_in_window"] == 2
    pump_ranks = [
        row["parameter_support_rank_in_window"]
        for row in telemetry_rows
        if row["parameter_name"] == "PUMP_STATE" and row["parameter_localization_selected"]
    ]
    if pump_ranks:
        assert min(pump_ranks) >= 3


def test_anomaly_event_attribution_includes_nearby_window_shoulder_events(spark):
    calibrated_df = spark.createDataFrame(
        [
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "win_id": 1,
                "phase_state_detected": "stable",
                "phase_id_detected": 0,
                "phase_confidence_detected": 0.9,
                "distance_to_centroid_detected": 0.1,
                "drift_magnitude": 0.1,
                "breadth": 0.2,
                "global_score": 5.0,
                "p_value": 0.01,
                "severity": "high",
                "dominant_subsystem_id": "SUBSYS_0001",
                "dominant_score_component": REGIME_DEVIATION_CHANNEL,
                "subsystem_scores": {"SUBSYS_0001": 1.0},
                "score_component_scores": score_component_scores_with_updates(
                    {
                        REGIME_DEVIATION_CHANNEL: 5.0,
                    }
                ),
                "warm": True,
                "emit_ready": True,
                "min_warm": 1,
                "date_utc": date(2026, 2, 28),
            }
        ]
    )
    windows_df = spark.createDataFrame(
        [
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "win_id": 1,
                "t_start": datetime(2026, 2, 28, 0, 0, 0, 100000),
                "t_end": datetime(2026, 2, 28, 0, 0, 0, 500000),
                "duration_ms": 400,
                "date_utc": date(2026, 2, 28),
            }
        ]
    )
    events_df = spark.createDataFrame(
        [
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "timestamp_utc": datetime(2026, 2, 28, 0, 0, 0, 850000),
                "parameter_name": "ENG_TEMP_1",
                "event_type_detected": "slope_pos",
                "anomaly_type_detected": "",
                "anomaly_score_detected": 0.0,
                "date_utc": date(2026, 2, 28),
            }
        ]
    )

    event_df = AnomalyAttributionPlan(top_k_per_subsystem=3).build_event_attribution(
        calibrated_df=calibrated_df,
        windows_df=windows_df,
        events_df=events_df,
        hierarchy_sensor_map_df=_hierarchy_sensor_map_df(spark),
    ).to_dataframe()

    rows = event_df.select("win_id", "parameter_name", "event_type_detected").collect()
    assert len(rows) == 1
    assert rows[0]["win_id"] == 1
    assert rows[0]["parameter_name"] == "ENG_TEMP_1"
    assert rows[0]["event_type_detected"] == "slope_pos"


def test_parameter_localization_prefers_channel_supported_parameter_evidence(spark):
    calibrated_df = spark.createDataFrame(
        [
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "win_id": 1,
                "phase_state_detected": "stable",
                "phase_id_detected": 0,
                "phase_confidence_detected": 0.9,
                "distance_to_centroid_detected": 0.1,
                "drift_magnitude": 0.1,
                "breadth": 0.2,
                "global_score": 5.0,
                "p_value": 0.01,
                "severity": "high",
                "dominant_subsystem_id": "SUBSYS_0001",
                "dominant_score_component": EVENT_DISCORDANCE_CHANNEL,
                "subsystem_scores": {"SUBSYS_0001": 1.0},
                "score_component_scores": score_component_scores_with_updates(
                    {
                        REGIME_DEVIATION_CHANNEL: 1.0,
                        EVENT_DISCORDANCE_CHANNEL: 100.0,
                        RESPONSE_VIOLATION_CHANNEL: 25.0,
                    }
                ),
                "warm": True,
                "emit_ready": True,
                "min_warm": 1,
                "date_utc": date(2026, 2, 28),
            }
        ]
    )
    phase_windows_df = spark.createDataFrame(
        [
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "win_id": 1,
                "t_start": datetime(2026, 2, 28, 0, 0, 0, 100000),
                "t_end": datetime(2026, 2, 28, 0, 0, 0, 900000),
                "duration_ms": 800,
                "backbone_residual_by_parameter": {
                    "ENG_TEMP_1": 10.0,
                    "PUMP_STATE": 1.0,
                },
                "date_utc": date(2026, 2, 28),
            }
        ]
    )
    events_df = spark.createDataFrame(
        [
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "timestamp_utc": datetime(2026, 2, 28, 0, 0, 0, 250000),
                "parameter_name": "PUMP_STATE",
                "event_type_detected": "transition",
                "anomaly_type_detected": "",
                "anomaly_score_detected": 0.0,
                "date_utc": date(2026, 2, 28),
            },
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "timestamp_utc": datetime(2026, 2, 28, 0, 0, 0, 450000),
                "parameter_name": "PUMP_STATE",
                "event_type_detected": "state_enter",
                "anomaly_type_detected": "",
                "anomaly_score_detected": 0.0,
                "date_utc": date(2026, 2, 28),
            },
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "timestamp_utc": datetime(2026, 2, 28, 0, 0, 0, 650000),
                "parameter_name": "PUMP_STATE",
                "event_type_detected": "state_exit",
                "anomaly_type_detected": "",
                "anomaly_score_detected": 0.0,
                "date_utc": date(2026, 2, 28),
            },
        ]
    )

    localization_df = AnomalyParameterLocalizationFrame.from_calibrated_phase_windows_events_and_hierarchy(
        calibrated_df=calibrated_df,
        phase_windows_df=phase_windows_df,
        events_df=events_df,
        hierarchy_sensor_map_df=_hierarchy_sensor_map_df(spark),
        top_k_per_window=2,
    ).to_dataframe()

    ranked = localization_df.orderBy("parameter_support_rank_in_window").select(
        "parameter_name",
        "subsystem_id",
        "module_id",
        "parameter_support_rank_in_window",
    ).collect()
    assert [row["parameter_name"] for row in ranked] == ["PUMP_STATE", "ENG_TEMP_1"]

    dominant_targets = AnomalyParameterLocalizationFrame(dataframe=localization_df).dominant_targets_df().collect()[0]
    assert dominant_targets["dominant_subsystem_id"] == "SUBSYS_0002"
    assert dominant_targets["dominant_module_id"] == "MOD_0003"


def test_dominant_targets_prefers_concentrated_top_ranked_module_and_keeps_module_consistent(spark):
    support_df = spark.createDataFrame(
        [
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "win_id": 1,
                "date_utc": date(2026, 2, 28),
                "parameter_name": "P_A1",
                "system_id": "SYS_1",
                "subsystem_id": "SUBSYS_A",
                "module_id": "MOD_A1",
                "parameter_localization_support": 1.0,
                "parameter_support_rank_in_window": 1,
            },
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "win_id": 1,
                "date_utc": date(2026, 2, 28),
                "parameter_name": "P_B1",
                "system_id": "SYS_1",
                "subsystem_id": "SUBSYS_B",
                "module_id": "MOD_B1",
                "parameter_localization_support": 0.7,
                "parameter_support_rank_in_window": 2,
            },
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "win_id": 1,
                "date_utc": date(2026, 2, 28),
                "parameter_name": "P_B2",
                "system_id": "SYS_1",
                "subsystem_id": "SUBSYS_B",
                "module_id": "MOD_B2",
                "parameter_localization_support": 0.6,
                "parameter_support_rank_in_window": 3,
            },
        ]
    )

    dominant_targets = AnomalyParameterLocalizationFrame(dataframe=support_df).dominant_targets_df().collect()[0]
    assert dominant_targets["dominant_subsystem_id"] == "SUBSYS_A"
    assert dominant_targets["dominant_module_id"] == "MOD_A1"


def test_parameter_localization_boosts_coherent_module_over_isolated_context_parameter(spark):
    calibrated_df = spark.createDataFrame(
        [
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "win_id": 1,
                "phase_state_detected": "stable",
                "phase_id_detected": 0,
                "phase_confidence_detected": 0.9,
                "distance_to_centroid_detected": 0.1,
                "drift_magnitude": 0.1,
                "breadth": 0.2,
                "global_score": 5.0,
                "p_value": 0.01,
                "severity": "high",
                "dominant_subsystem_id": "SUBSYS_CONTEXT",
                "dominant_score_component": RECONSTRUCTION_ERROR_CHANNEL,
                "subsystem_scores": {"SUBSYS_CONTEXT": 1.0},
                "score_component_scores": score_component_scores_with_updates({RECONSTRUCTION_ERROR_CHANNEL: 100.0}),
                "warm": True,
                "emit_ready": True,
                "min_warm": 1,
                "date_utc": date(2026, 2, 28),
            }
        ]
    )
    phase_windows_df = spark.createDataFrame(
        [
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "win_id": 1,
                "t_start": datetime(2026, 2, 28, 0, 0, 0, 100000),
                "t_end": datetime(2026, 2, 28, 0, 0, 0, 900000),
                "duration_ms": 800,
                "backbone_residual_by_parameter": {
                    "CTX_PARAM": 3.8,
                    "SRC_PARAM_1": 5.0,
                    "SRC_PARAM_2": 5.0,
                },
                "date_utc": date(2026, 2, 28),
            }
        ]
    )
    events_df = spark.createDataFrame([], schema=create_sample_events_df(spark).schema)
    hierarchy_df = spark.createDataFrame(
        [
            {"parameter_name": "CTX_PARAM", "system_id": "SYS_1", "subsystem_id": "SUBSYS_CONTEXT", "module_id": "MOD_CTX"},
            {"parameter_name": "SRC_PARAM_1", "system_id": "SYS_1", "subsystem_id": "SUBSYS_SOURCE", "module_id": "MOD_SRC"},
            {"parameter_name": "SRC_PARAM_2", "system_id": "SYS_1", "subsystem_id": "SUBSYS_SOURCE", "module_id": "MOD_SRC"},
        ]
    )

    localization_df = AnomalyParameterLocalizationFrame.from_calibrated_phase_windows_events_and_hierarchy(
        calibrated_df=calibrated_df,
        phase_windows_df=phase_windows_df,
        events_df=events_df,
        hierarchy_sensor_map_df=hierarchy_df,
        top_k_per_window=3,
    ).to_dataframe()

    ranked = localization_df.orderBy("parameter_support_rank_in_window").select(
        "parameter_name",
        "subsystem_id",
        "parameter_support_rank_in_window",
    ).collect()
    assert [row["parameter_name"] for row in ranked] == ["SRC_PARAM_1", "SRC_PARAM_2", "CTX_PARAM"]

    dominant_targets = AnomalyParameterLocalizationFrame(dataframe=localization_df).dominant_targets_df().collect()[0]
    assert dominant_targets["dominant_subsystem_id"] == "SUBSYS_SOURCE"
    assert dominant_targets["dominant_module_id"] == "MOD_SRC"

    localized_targets = (
        AnomalyParameterLocalizationFrame(dataframe=localization_df)
        .localized_targets_df(top_k_per_window=2)
        .collect()[0]
    )
    assert localized_targets["top_subsystem_candidates"][0]["id"] == "SUBSYS_SOURCE"
    assert localized_targets["top_subsystem_candidates"][1]["id"] == "SUBSYS_CONTEXT"
    assert localized_targets["top_module_candidates"][0]["id"] == "MOD_SRC"
    assert localized_targets["top_module_candidates"][1]["id"] == "MOD_CTX"


def test_parameter_localization_uses_accumulation_channel_to_promote_accumulative_parameters(spark):
    calibrated_df = spark.createDataFrame(
        [
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "win_id": 1,
                "phase_state_detected": "stable",
                "phase_id_detected": 0,
                "phase_confidence_detected": 0.9,
                "distance_to_centroid_detected": 0.1,
                "drift_magnitude": 0.1,
                "breadth": 0.2,
                "global_score": 5.0,
                "p_value": 0.01,
                "severity": "high",
                "dominant_subsystem_id": "SUBSYS_CONTEXT",
                "dominant_score_component": ACCUMULATION_VIOLATION_CHANNEL,
                "subsystem_scores": {"SUBSYS_CONTEXT": 1.0},
                "score_component_scores": score_component_scores_with_updates({ACCUMULATION_VIOLATION_CHANNEL: 100.0}),
                "warm": True,
                "emit_ready": True,
                "min_warm": 1,
                "date_utc": date(2026, 2, 28),
            }
        ]
    )
    phase_windows_df = spark.createDataFrame(
        [
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "win_id": 1,
                "t_start": datetime(2026, 2, 28, 0, 0, 0, 100000),
                "t_end": datetime(2026, 2, 28, 0, 0, 0, 900000),
                "duration_ms": 800,
                "backbone_residual_by_parameter": {
                    "CTX_PARAM": 4.0,
                    "ACC_PARAM_A": 3.0,
                    "ACC_PARAM_B": 3.0,
                },
                "date_utc": date(2026, 2, 28),
            }
        ]
    )
    events_df = spark.createDataFrame([], schema=create_sample_events_df(spark).schema)
    hierarchy_df = spark.createDataFrame(
        [
            {"parameter_name": "CTX_PARAM", "system_id": "SYS_1", "subsystem_id": "SUBSYS_CONTEXT", "module_id": "MOD_CTX"},
            {"parameter_name": "ACC_PARAM_A", "system_id": "SYS_1", "subsystem_id": "SUBSYS_ACC", "module_id": "MOD_ACC"},
            {"parameter_name": "ACC_PARAM_B", "system_id": "SYS_1", "subsystem_id": "SUBSYS_ACC", "module_id": "MOD_ACC"},
        ]
    )
    parameter_behavior_profile_df = spark.createDataFrame(
        [
            {
                "parameter_name": "CTX_PARAM",
                "persistent_run_strength_profiled": 0.1,
                "run_reinforcement_score_profiled": 0.1,
                "accumulative_score_profiled": 0.0,
                "monotone_accumulation_score_profiled": 0.0,
                "reset_drop_rate_profiled": 0.0,
            },
            {
                "parameter_name": "ACC_PARAM_A",
                "persistent_run_strength_profiled": 0.9,
                "run_reinforcement_score_profiled": 0.8,
                "accumulative_score_profiled": 1.0,
                "monotone_accumulation_score_profiled": 0.9,
                "reset_drop_rate_profiled": 0.4,
            },
            {
                "parameter_name": "ACC_PARAM_B",
                "persistent_run_strength_profiled": 0.85,
                "run_reinforcement_score_profiled": 0.8,
                "accumulative_score_profiled": 1.0,
                "monotone_accumulation_score_profiled": 0.9,
                "reset_drop_rate_profiled": 0.35,
            },
        ]
    )

    localization_df = AnomalyParameterLocalizationFrame.from_calibrated_phase_windows_events_and_hierarchy(
        calibrated_df=calibrated_df,
        phase_windows_df=phase_windows_df,
        events_df=events_df,
        hierarchy_sensor_map_df=hierarchy_df,
        parameter_behavior_profile_df=parameter_behavior_profile_df,
        top_k_per_window=3,
    ).to_dataframe()

    ranked = localization_df.orderBy("parameter_support_rank_in_window").select(
        "parameter_name",
        "subsystem_id",
        "parameter_support_rank_in_window",
    ).collect()
    assert [row["parameter_name"] for row in ranked] == ["ACC_PARAM_A", "ACC_PARAM_B", "CTX_PARAM"]

    dominant_targets = AnomalyParameterLocalizationFrame(dataframe=localization_df).dominant_targets_df().collect()[0]
    assert dominant_targets["dominant_subsystem_id"] == "SUBSYS_ACC"
    assert dominant_targets["dominant_module_id"] == "MOD_ACC"


def test_parameter_localization_retains_broader_selection_than_structural_target_rollup(spark):
    calibrated_df = spark.createDataFrame(
        [
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "win_id": 1,
                "phase_state_detected": "stable",
                "phase_id_detected": 0,
                "phase_confidence_detected": 0.9,
                "distance_to_centroid_detected": 0.1,
                "drift_magnitude": 0.1,
                "breadth": 0.2,
                "global_score": 5.0,
                "p_value": 0.01,
                "severity": "high",
                "dominant_subsystem_id": "SUBSYS_SOURCE",
                "dominant_score_component": RECONSTRUCTION_ERROR_CHANNEL,
                "subsystem_scores": {"SUBSYS_SOURCE": 1.0},
                "score_component_scores": score_component_scores_with_updates({RECONSTRUCTION_ERROR_CHANNEL: 100.0}),
                "warm": True,
                "emit_ready": True,
                "min_warm": 1,
                "date_utc": date(2026, 2, 28),
            }
        ]
    )
    phase_windows_df = spark.createDataFrame(
        [
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "win_id": 1,
                "t_start": datetime(2026, 2, 28, 0, 0, 0, 100000),
                "t_end": datetime(2026, 2, 28, 0, 0, 0, 900000),
                "duration_ms": 800,
                "backbone_residual_by_parameter": {
                    "SRC_PARAM_A": 5.0,
                    "SRC_PARAM_B": 4.5,
                    "CTX_PARAM": 3.0,
                    "SEL_PARAM": 2.5,
                },
                "date_utc": date(2026, 2, 28),
            }
        ]
    )
    events_df = spark.createDataFrame([], schema=create_sample_events_df(spark).schema)
    hierarchy_df = spark.createDataFrame(
        [
            {"parameter_name": "SRC_PARAM_A", "system_id": "SYS_1", "subsystem_id": "SUBSYS_SOURCE", "module_id": "MOD_SRC"},
            {"parameter_name": "SRC_PARAM_B", "system_id": "SYS_1", "subsystem_id": "SUBSYS_SOURCE", "module_id": "MOD_SRC"},
            {"parameter_name": "CTX_PARAM", "system_id": "SYS_1", "subsystem_id": "SUBSYS_CONTEXT", "module_id": "MOD_CTX"},
            {"parameter_name": "SEL_PARAM", "system_id": "SYS_1", "subsystem_id": "SUBSYS_SELECTION", "module_id": "MOD_SEL"},
        ]
    )

    localization = AnomalyParameterLocalizationFrame.from_calibrated_phase_windows_events_and_hierarchy(
        calibrated_df=calibrated_df,
        phase_windows_df=phase_windows_df,
        events_df=events_df,
        hierarchy_sensor_map_df=hierarchy_df,
    )
    localization_df = localization.to_dataframe()

    ranked = localization_df.orderBy("parameter_support_rank_in_window").select("parameter_name").collect()
    assert [row["parameter_name"] for row in ranked] == [
        "SRC_PARAM_A",
        "SRC_PARAM_B",
        "CTX_PARAM",
        "SEL_PARAM",
    ]

    localized_targets = localization.localized_targets_df(
        top_k_per_window=3,
        parameter_support_top_k=ANOMALY_LOCALIZATION_PARAMETER_TOP_K,
    ).collect()[0]
    assert [candidate["id"] for candidate in localized_targets["top_module_candidates"]] == ["MOD_SRC", "MOD_CTX"]
    assert [candidate["id"] for candidate in localized_targets["top_subsystem_candidates"]] == [
        "SUBSYS_SOURCE",
        "SUBSYS_CONTEXT",
    ]
    assert "MOD_SEL" not in [candidate["id"] for candidate in localized_targets["top_module_candidates"]]
    assert "SUBSYS_SELECTION" not in [candidate["id"] for candidate in localized_targets["top_subsystem_candidates"]]
