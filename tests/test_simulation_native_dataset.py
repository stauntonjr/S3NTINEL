from __future__ import annotations

from datetime import datetime, timezone

from libs.behavior import BehaviorStepInput
from libs.simulation import (
    build_native_multibehavior_example,
    simulate_native_backbone_artifacts_from_subsystem_slice,
    native_raw_telemetry_to_events_sdf,
    native_phase_labels_to_table_df,
    native_telemetry_to_raw_telemetry_df,
    simulate_native_dataset_from_assembly,
    simulate_native_event_table_from_subsystem_slice,
    simulate_native_graph_artifacts_from_subsystem_slice,
    simulate_native_phase_artifacts_from_subsystem_slice,
    simulate_native_raw_telemetry_from_assembly,
    simulate_native_dataset_from_subsystem_slice,
    simulate_native_raw_telemetry_from_subsystem_slice,
    simulate_native_window_scores_raw_from_subsystem_slice,
    simulate_native_window_table_from_subsystem_slice,
    simulate_native_window_x_table_from_subsystem_slice,
)


def test_simulate_native_dataset_from_assembly_emits_telemetry_and_phase_rows():
    assembly_spec = build_native_multibehavior_example()
    switch_states = (0, 0, 1, 1, 1)

    telemetry_df, phase_df = simulate_native_dataset_from_assembly(
        assembly_spec=assembly_spec,
        n_steps=5,
        dt_seconds=1.0,
        start_timestamp_utc=None,
        build_step_inputs_by_module=lambda step_index, resolved_dt_seconds: {
            "MOD_SWITCH": {
                "contactor_state": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={"target_state": switch_states[min(step_index, len(switch_states) - 1)]},
                )
            }
        },
        build_initial_state_by_module=lambda: {
            "MOD_SWITCH": {"contactor_state": 0},
            "MOD_SOURCE": {"supply_voltage": 0.0, "fuel_flow_rate": 0.0},
            "MOD_TARGET": {"motor_speed": 0.0},
            "MOD_TANK": {"fuel_used_total": 0.0},
        },
        phase_label_for_step=lambda step_index: "phase_a" if step_index < 2 else "phase_b",
    )

    assert not telemetry_df.empty
    assert len(phase_df) == 5
    assert set(phase_df["phase_label"].astype(str)) == {"phase_a", "phase_b"}
    assert set(telemetry_df["parameter_name"].astype(str)) >= {
        "contactor_state",
        "supply_voltage",
        "fuel_flow_rate",
        "motor_speed",
        "fuel_used_total",
    }


def test_simulate_native_dataset_from_subsystem_slice_uses_named_slice():
    telemetry_df, phase_df = simulate_native_dataset_from_subsystem_slice(
        slice_name="pressurization",
        n_steps=4,
        dt_seconds=1.0,
        start_timestamp_utc=None,
        build_step_inputs_by_module=lambda step_index, resolved_dt_seconds: {
            "MOD_PRESS_MODE": {
                "press_mode_state": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={"target_state": "AUTO" if step_index >= 1 else "GROUND"},
                )
            },
            "MOD_AIRCRAFT_ALT": {
                "aircraft_altitude_ft": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={"target_value": 1000.0 * float(step_index)},
                )
            },
        },
        build_initial_state_by_module=lambda: {
            "MOD_PRESS_MODE": {"press_mode_state": "GROUND"},
            "MOD_AIRCRAFT_ALT": {"aircraft_altitude_ft": 0.0},
            "MOD_PRESS_CTRL": {"outflow_valve_pct": 0.0},
            "MOD_CABIN": {"cabin_altitude_ft": 0.0, "cabin_delta_p_psi": 0.0},
        },
        phase_label_for_step=lambda _step_index: "climb",
    )

    assert not telemetry_df.empty
    assert len(phase_df) == 4
    assert set(telemetry_df["module_id"].astype(str)) == {
        "MOD_PRESS_MODE",
        "MOD_AIRCRAFT_ALT",
        "MOD_PRESS_CTRL",
        "MOD_CABIN",
    }
    assert set(phase_df["phase_label"].astype(str)) == {"climb"}


def test_native_dataset_normalization_to_raw_tables_uses_canonical_columns():
    telemetry_df, phase_df = simulate_native_dataset_from_subsystem_slice(
        slice_name="power_chain",
        n_steps=3,
        dt_seconds=1.0,
        start_timestamp_utc=None,
        build_step_inputs_by_module=lambda step_index, resolved_dt_seconds: {
            "MOD_SWITCH": {
                "contactor_state": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={"target_state": 1 if step_index >= 1 else 0},
                )
            }
        },
        build_initial_state_by_module=lambda: {
            "MOD_SWITCH": {"contactor_state": 0},
            "MOD_SOURCE": {"supply_voltage": 0.0, "fuel_flow_rate": 0.0},
            "MOD_TARGET": {"motor_speed": 0.0},
            "MOD_TANK": {"fuel_used_total": 0.0},
        },
        phase_label_for_step=lambda _step_index: "native_phase",
    )

    raw_df = native_telemetry_to_raw_telemetry_df(telemetry_df, tail_id="TNAT", flight_id="FNAT")
    phase_table_df = native_phase_labels_to_table_df(phase_df, tail_id="TNAT", flight_id="FNAT")

    assert list(raw_df.columns) == [
        "tail_id",
        "flight_id",
        "timestamp_utc",
        "parameter_name",
        "parameter_value",
        "date_utc",
    ]
    assert list(phase_table_df.columns) == [
        "tail_id",
        "flight_id",
        "step_index",
        "timestamp_utc",
        "phase_label",
        "date_utc",
    ]
    assert set(raw_df["tail_id"].astype(str)) == {"TNAT"}
    assert set(raw_df["flight_id"].astype(str)) == {"FNAT"}
    assert set(phase_table_df["phase_label"].astype(str)) == {"native_phase"}


def test_simulate_native_raw_telemetry_from_assembly_returns_profile_ready_tables():
    assembly_spec = build_native_multibehavior_example()

    raw_df, phase_df = simulate_native_raw_telemetry_from_assembly(
        assembly_spec=assembly_spec,
        tail_id="T001",
        flight_id="F001",
        n_steps=4,
        dt_seconds=1.0,
        start_timestamp_utc=None,
        build_step_inputs_by_module=lambda step_index, resolved_dt_seconds: {
            "MOD_SWITCH": {
                "contactor_state": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={"target_state": 1 if step_index >= 1 else 0},
                )
            }
        },
        build_initial_state_by_module=lambda: {
            "MOD_SWITCH": {"contactor_state": 0},
            "MOD_SOURCE": {"supply_voltage": 0.0, "fuel_flow_rate": 0.0},
            "MOD_TARGET": {"motor_speed": 0.0},
            "MOD_TANK": {"fuel_used_total": 0.0},
        },
        phase_label_for_step=lambda _step_index: "native_phase",
    )

    assert not raw_df.empty
    assert not phase_df.empty
    assert set(raw_df["parameter_name"].astype(str)) >= {"contactor_state", "supply_voltage", "motor_speed"}
    assert set(phase_df["phase_label"].astype(str)) == {"native_phase"}


def test_simulate_native_raw_telemetry_from_subsystem_slice_returns_profile_ready_tables():
    raw_df, phase_df = simulate_native_raw_telemetry_from_subsystem_slice(
        slice_name="pressurization",
        tail_id="T002",
        flight_id="F002",
        n_steps=3,
        dt_seconds=1.0,
        start_timestamp_utc=None,
        build_step_inputs_by_module=lambda step_index, resolved_dt_seconds: {
            "MOD_PRESS_MODE": {
                "press_mode_state": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={"target_state": "AUTO" if step_index >= 1 else "GROUND"},
                )
            },
            "MOD_AIRCRAFT_ALT": {
                "aircraft_altitude_ft": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={"target_value": 500.0 * float(step_index)},
                )
            },
        },
        build_initial_state_by_module=lambda: {
            "MOD_PRESS_MODE": {"press_mode_state": "GROUND"},
            "MOD_AIRCRAFT_ALT": {"aircraft_altitude_ft": 0.0},
            "MOD_PRESS_CTRL": {"outflow_valve_pct": 0.0},
            "MOD_CABIN": {"cabin_altitude_ft": 0.0, "cabin_delta_p_psi": 0.0},
        },
        phase_label_for_step=lambda _step_index: "native_phase",
    )

    assert not raw_df.empty
    assert len(phase_df) == 3
    assert set(raw_df["tail_id"].astype(str)) == {"T002"}
    assert set(raw_df["flight_id"].astype(str)) == {"F002"}


def test_native_raw_telemetry_to_events_sdf_uses_canonical_event_columns(spark):
    raw_df, _phase_df = simulate_native_raw_telemetry_from_subsystem_slice(
        slice_name="power_chain",
        tail_id="T003",
        flight_id="F003",
        n_steps=4,
        dt_seconds=1.0,
        start_timestamp_utc=datetime(2025, 1, 1, tzinfo=timezone.utc),
        build_step_inputs_by_module=lambda step_index, resolved_dt_seconds: {
            "MOD_SWITCH": {
                "contactor_state": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={"target_state": 1 if step_index >= 1 else 0},
                )
            }
        },
        build_initial_state_by_module=lambda: {
            "MOD_SWITCH": {"contactor_state": 0},
            "MOD_SOURCE": {"supply_voltage": 0.0, "fuel_flow_rate": 0.0},
            "MOD_TARGET": {"motor_speed": 0.0},
            "MOD_TANK": {"fuel_used_total": 0.0},
        },
        phase_label_for_step=lambda _step_index: "native_phase",
    )

    events_sdf = native_raw_telemetry_to_events_sdf(raw_df, spark=spark)
    assert events_sdf.count() > 0
    assert {"tail_id", "flight_id", "timestamp_utc", "parameter_name", "event_type_detected"}.issubset(events_sdf.columns)
    assert {"anomaly_type_detected", "anomaly_score_detected"}.issubset(events_sdf.columns)


def test_simulate_native_event_table_from_subsystem_slice_uses_active_event_path(spark):
    events_sdf, phase_df = simulate_native_event_table_from_subsystem_slice(
        slice_name="pressurization",
        spark=spark,
        tail_id="T004",
        flight_id="F004",
        n_steps=4,
        dt_seconds=1.0,
        start_timestamp_utc=datetime(2025, 1, 1, tzinfo=timezone.utc),
        build_step_inputs_by_module=lambda step_index, resolved_dt_seconds: {
            "MOD_PRESS_MODE": {
                "press_mode_state": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={"target_state": "AUTO" if step_index >= 1 else "GROUND"},
                )
            },
            "MOD_AIRCRAFT_ALT": {
                "aircraft_altitude_ft": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={"target_value": 500.0 * float(step_index)},
                )
            },
        },
        build_initial_state_by_module=lambda: {
            "MOD_PRESS_MODE": {"press_mode_state": "GROUND"},
            "MOD_AIRCRAFT_ALT": {"aircraft_altitude_ft": 0.0},
            "MOD_PRESS_CTRL": {"outflow_valve_pct": 0.0},
            "MOD_CABIN": {"cabin_altitude_ft": 0.0, "cabin_delta_p_psi": 0.0},
        },
        phase_label_for_step=lambda _step_index: "native_phase",
    )

    assert len(phase_df) == 4
    assert events_sdf.count() > 0
    assert set(events_sdf.select("parameter_name").toPandas()["parameter_name"].astype(str)) >= {
        "press_mode_state",
        "aircraft_altitude_ft",
    }


def test_simulate_native_window_table_from_subsystem_slice_uses_active_window_path(spark):
    windows_sdf, phase_df = simulate_native_window_table_from_subsystem_slice(
        slice_name="power_chain",
        spark=spark,
        tail_id="T005",
        flight_id="F005",
        n_steps=6,
        dt_seconds=1.0,
        start_timestamp_utc=datetime(2025, 1, 1, tzinfo=timezone.utc),
        build_step_inputs_by_module=lambda step_index, resolved_dt_seconds: {
            "MOD_SWITCH": {
                "contactor_state": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={"target_state": 1 if step_index >= 2 else 0},
                )
            }
        },
        build_initial_state_by_module=lambda: {
            "MOD_SWITCH": {"contactor_state": 0},
            "MOD_SOURCE": {"supply_voltage": 0.0, "fuel_flow_rate": 0.0},
            "MOD_TARGET": {"motor_speed": 0.0},
            "MOD_TANK": {"fuel_used_total": 0.0},
        },
        phase_label_for_step=lambda _step_index: "native_phase",
    )

    assert len(phase_df) == 6
    assert windows_sdf.count() > 0
    assert {
        "tail_id",
        "flight_id",
        "win_id",
        "t_start",
        "t_end",
        "duration_ms",
        "event_count",
        "close_reason",
        "date_utc",
    }.issubset(windows_sdf.columns)


def test_simulate_native_window_x_table_from_subsystem_slice_builds_window_x(spark):
    window_x_sdf, phase_df = simulate_native_window_x_table_from_subsystem_slice(
        slice_name="pressurization",
        spark=spark,
        tail_id="T006",
        flight_id="F006",
        n_steps=6,
        dt_seconds=1.0,
        start_timestamp_utc=datetime(2025, 1, 1, tzinfo=timezone.utc),
        build_step_inputs_by_module=lambda step_index, resolved_dt_seconds: {
            "MOD_PRESS_MODE": {
                "press_mode_state": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={"target_state": "AUTO" if step_index >= 1 else "GROUND"},
                )
            },
            "MOD_AIRCRAFT_ALT": {
                "aircraft_altitude_ft": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={"target_value": 750.0 * float(step_index)},
                )
            },
        },
        build_initial_state_by_module=lambda: {
            "MOD_PRESS_MODE": {"press_mode_state": "GROUND"},
            "MOD_AIRCRAFT_ALT": {"aircraft_altitude_ft": 0.0},
            "MOD_PRESS_CTRL": {"outflow_valve_pct": 0.0},
            "MOD_CABIN": {"cabin_altitude_ft": 0.0, "cabin_delta_p_psi": 0.0},
        },
        phase_label_for_step=lambda _step_index: "native_phase",
    )

    assert len(phase_df) == 6
    assert window_x_sdf.count() > 0
    assert {
        "tail_id",
        "flight_id",
        "win_id",
        "t_start",
        "t_end",
        "duration_ms",
        "event_count",
        "continuous_vector_t_end",
        "continuous_vector_t_end_scaled",
        "categorical_state_t_end",
        "drift_magnitude_profiled",
        "date_utc",
    }.issubset(window_x_sdf.columns)


def test_simulate_native_backbone_artifacts_from_subsystem_slice_uses_active_backbone_path(spark):
    artifacts, phase_df = simulate_native_backbone_artifacts_from_subsystem_slice(
        slice_name="power_chain",
        spark=spark,
        tail_id="T007",
        flight_id="F007",
        n_steps=6,
        dt_seconds=1.0,
        start_timestamp_utc=datetime(2025, 1, 1, tzinfo=timezone.utc),
        build_step_inputs_by_module=lambda step_index, resolved_dt_seconds: {
            "MOD_SWITCH": {
                "contactor_state": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={"target_state": 1 if step_index >= 2 else 0},
                )
            }
        },
        build_initial_state_by_module=lambda: {
            "MOD_SWITCH": {"contactor_state": 0},
            "MOD_SOURCE": {"supply_voltage": 0.0, "fuel_flow_rate": 0.0},
            "MOD_TARGET": {"motor_speed": 0.0},
            "MOD_TANK": {"fuel_used_total": 0.0},
        },
        phase_label_for_step=lambda _step_index: "native_phase",
    )

    assert len(phase_df) == 6
    assert set(artifacts.keys()) == {"backbone", "backbone_sensor_energy"}
    assert not artifacts["backbone"].empty
    assert {"selected_sensors_c", "all_sensors", "weights_b"}.issubset(artifacts["backbone"].columns)
    assert {"parameter_name", "energy", "support_count"}.issubset(artifacts["backbone_sensor_energy"].columns)


def test_simulate_native_graph_artifacts_from_subsystem_slice_uses_active_graph_path(spark):
    artifacts, phase_df = simulate_native_graph_artifacts_from_subsystem_slice(
        slice_name="pressurization",
        spark=spark,
        tail_id="T008",
        flight_id="F008",
        n_steps=6,
        dt_seconds=1.0,
        start_timestamp_utc=datetime(2025, 1, 1, tzinfo=timezone.utc),
        build_step_inputs_by_module=lambda step_index, resolved_dt_seconds: {
            "MOD_PRESS_MODE": {
                "press_mode_state": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={"target_state": "AUTO" if step_index >= 1 else "GROUND"},
                )
            },
            "MOD_AIRCRAFT_ALT": {
                "aircraft_altitude_ft": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={"target_value": 750.0 * float(step_index)},
                )
            },
        },
        build_initial_state_by_module=lambda: {
            "MOD_PRESS_MODE": {"press_mode_state": "GROUND"},
            "MOD_AIRCRAFT_ALT": {"aircraft_altitude_ft": 0.0},
            "MOD_PRESS_CTRL": {"outflow_valve_pct": 0.0},
            "MOD_CABIN": {"cabin_altitude_ft": 0.0, "cabin_delta_p_psi": 0.0},
        },
        phase_label_for_step=lambda _step_index: "native_phase",
    )

    assert len(phase_df) == 6
    assert {
        "backbone",
        "backbone_sensor_energy",
        "precision_graph",
        "event_graph",
        "lag_graph",
        "transition_graph",
        "fused_graph",
        "hierarchy_sensor_map",
    } == set(artifacts.keys())
    assert not artifacts["hierarchy_sensor_map"].empty
    assert any(
        not artifacts[artifact_name].empty
        for artifact_name in ("event_graph", "lag_graph", "transition_graph", "fused_graph")
    )
    assert {"parameter_name", "system_id", "subsystem_id", "module_id"}.issubset(
        artifacts["hierarchy_sensor_map"].columns
    )


def test_simulate_native_phase_artifacts_from_subsystem_slice_uses_active_phase_path(spark):
    artifacts, phase_df = simulate_native_phase_artifacts_from_subsystem_slice(
        slice_name="power_chain",
        spark=spark,
        tail_id="T009",
        flight_id="F009",
        n_steps=6,
        dt_seconds=1.0,
        start_timestamp_utc=datetime(2025, 1, 1, tzinfo=timezone.utc),
        build_step_inputs_by_module=lambda step_index, resolved_dt_seconds: {
            "MOD_SWITCH": {
                "contactor_state": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={"target_state": 1 if step_index >= 2 else 0},
                )
            }
        },
        build_initial_state_by_module=lambda: {
            "MOD_SWITCH": {"contactor_state": 0},
            "MOD_SOURCE": {"supply_voltage": 0.0, "fuel_flow_rate": 0.0},
            "MOD_TARGET": {"motor_speed": 0.0},
            "MOD_TANK": {"fuel_used_total": 0.0},
        },
        phase_label_for_step=lambda _step_index: "native_phase",
    )

    assert len(phase_df) == 6
    assert set(artifacts.keys()) == {"phase_windows", "phase_baselines"}
    assert not artifacts["phase_windows"].empty
    assert not artifacts["phase_baselines"].empty
    assert {
        "tail_id",
        "flight_id",
        "win_id",
        "phase_id_detected",
        "phase_state_detected",
        "s_w",
    }.issubset(artifacts["phase_windows"].columns)
    assert {
        "tail_id",
        "phase_id_detected",
        "phase_name_detected",
        "s_w_centroid",
    }.issubset(artifacts["phase_baselines"].columns)


def test_simulate_native_window_scores_raw_from_subsystem_slice_uses_active_score_path(spark):
    artifacts, phase_df = simulate_native_window_scores_raw_from_subsystem_slice(
        slice_name="pressurization",
        spark=spark,
        tail_id="T010",
        flight_id="F010",
        n_steps=6,
        dt_seconds=1.0,
        start_timestamp_utc=datetime(2025, 1, 1, tzinfo=timezone.utc),
        build_step_inputs_by_module=lambda step_index, resolved_dt_seconds: {
            "MOD_PRESS_MODE": {
                "press_mode_state": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={"target_state": "AUTO" if step_index >= 1 else "GROUND"},
                )
            },
            "MOD_AIRCRAFT_ALT": {
                "aircraft_altitude_ft": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={"target_value": 750.0 * float(step_index)},
                )
            },
        },
        build_initial_state_by_module=lambda: {
            "MOD_PRESS_MODE": {"press_mode_state": "GROUND"},
            "MOD_AIRCRAFT_ALT": {"aircraft_altitude_ft": 0.0},
            "MOD_PRESS_CTRL": {"outflow_valve_pct": 0.0},
            "MOD_CABIN": {"cabin_altitude_ft": 0.0, "cabin_delta_p_psi": 0.0},
        },
        phase_label_for_step=lambda _step_index: "native_phase",
    )

    assert len(phase_df) == 6
    assert {
        "phase_windows",
        "phase_baselines",
        "window_scores_raw",
    } == set(artifacts.keys())
    assert not artifacts["window_scores_raw"].empty
    assert {
        "tail_id",
        "flight_id",
        "win_id",
        "global_score",
        "severity",
        "score_component_scores",
    }.issubset(artifacts["window_scores_raw"].columns)
