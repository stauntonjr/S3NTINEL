from __future__ import annotations

from libs.simulation import (
    build_native_coupled_module_example,
    build_native_coupled_module_example_context,
    build_native_multibehavior_example,
    build_native_multibehavior_example_context,
    build_native_pressurization_example,
    build_native_pressurization_example_context,
    simulate_native_coupled_module_example,
    simulate_native_multibehavior_example,
    simulate_native_pressurization_example,
)


def test_build_native_coupled_module_example_uses_native_assembly_path():
    assembly_spec = build_native_coupled_module_example()
    assert tuple(module.module_id for module in assembly_spec.module_specs) == ("MOD_SOURCE", "MOD_TARGET")
    assert len(assembly_spec.inter_module_couplings) == 1
    coupling = assembly_spec.inter_module_couplings[0]
    assert coupling.source_module_id == "MOD_SOURCE"
    assert coupling.target_module_id == "MOD_TARGET"


def test_build_native_coupled_module_example_context_binds_modules_and_runtimes():
    context = build_native_coupled_module_example_context()
    assert set(context.module_bindings_by_id) == {"MOD_SOURCE", "MOD_TARGET"}
    assert set(context.module_runtimes_by_id) == {"MOD_SOURCE", "MOD_TARGET"}
    assert context.module_order == ("MOD_SOURCE", "MOD_TARGET")


def test_simulate_native_coupled_module_example_emits_rows_for_both_modules():
    df = simulate_native_coupled_module_example(n_steps=4, dt_seconds=1.0)
    assert set(df["module_id"].astype(str)) == {"MOD_SOURCE", "MOD_TARGET"}
    assert set(df["parameter_name"].astype(str)) == {"supply_voltage", "motor_speed"}

    source_rows = df[df["parameter_name"] == "supply_voltage"].sort_values("step_index").reset_index(drop=True)
    target_rows = df[df["parameter_name"] == "motor_speed"].sort_values("step_index").reset_index(drop=True)

    assert source_rows["parameter_value_clean"].astype(float).iloc[-1] > 27.0
    assert target_rows["target_source"].astype(str).iloc[-1] == "latent_state"
    assert target_rows["parameter_value_clean"].astype(float).iloc[-1] > target_rows["parameter_value_clean"].astype(float).iloc[0]


def test_build_native_multibehavior_example_uses_all_behavior_families():
    assembly_spec = build_native_multibehavior_example()
    assert tuple(module.module_id for module in assembly_spec.module_specs) == (
        "MOD_SWITCH",
        "MOD_SOURCE",
        "MOD_TARGET",
        "MOD_TANK",
    )
    assert len(assembly_spec.inter_module_couplings) == 3
    behavior_families = {
        parameter.behavior_family_label
        for module in assembly_spec.module_specs
        for parameter in module.parameters
    }
    assert behavior_families == {"discrete_state", "regulated", "inertial", "accumulative"}


def test_build_native_multibehavior_example_context_binds_all_modules():
    context = build_native_multibehavior_example_context()
    assert set(context.module_bindings_by_id) == {"MOD_SWITCH", "MOD_SOURCE", "MOD_TARGET", "MOD_TANK"}
    assert set(context.module_runtimes_by_id) == {"MOD_SWITCH", "MOD_SOURCE", "MOD_TARGET", "MOD_TANK"}
    assert context.module_order == ("MOD_SWITCH", "MOD_SOURCE", "MOD_TARGET", "MOD_TANK")


def test_simulate_native_multibehavior_example_emits_richer_behavior_chain():
    df = simulate_native_multibehavior_example(n_steps=6, dt_seconds=1.0)
    assert set(df["module_id"].astype(str)) == {"MOD_SWITCH", "MOD_SOURCE", "MOD_TARGET", "MOD_TANK"}
    assert set(df["parameter_name"].astype(str)) == {
        "contactor_state",
        "supply_voltage",
        "fuel_flow_rate",
        "motor_speed",
        "fuel_used_total",
    }

    switch_rows = df[df["parameter_name"] == "contactor_state"].sort_values("step_index").reset_index(drop=True)
    voltage_rows = df[df["parameter_name"] == "supply_voltage"].sort_values("step_index").reset_index(drop=True)
    speed_rows = df[df["parameter_name"] == "motor_speed"].sort_values("step_index").reset_index(drop=True)
    fuel_rows = df[df["parameter_name"] == "fuel_used_total"].sort_values("step_index").reset_index(drop=True)

    assert set(df["behavior_family_label"].astype(str)) == {
        "discrete_state",
        "regulated",
        "inertial",
        "accumulative",
    }
    assert set(switch_rows["parameter_value_clean"].astype(str)) == {"0", "1"}
    assert voltage_rows["parameter_value_clean"].astype(float).iloc[-1] > voltage_rows["parameter_value_clean"].astype(float).iloc[0]
    assert speed_rows["target_source"].astype(str).iloc[-1] == "latent_state"
    assert speed_rows["parameter_value_clean"].astype(float).iloc[-1] > speed_rows["parameter_value_clean"].astype(float).iloc[0]
    fuel_values = fuel_rows["parameter_value_clean"].astype(float).tolist()
    assert fuel_values == sorted(fuel_values)
    assert fuel_values[-1] > fuel_values[0]


def test_build_native_pressurization_example_uses_domain_shaped_modules():
    assembly_spec = build_native_pressurization_example()
    assert tuple(module.module_id for module in assembly_spec.module_specs) == (
        "MOD_PRESS_MODE",
        "MOD_AIRCRAFT_ALT",
        "MOD_PRESS_CTRL",
        "MOD_CABIN",
    )
    assert len(assembly_spec.inter_module_couplings) == 3
    assert any(coupling.lag_seconds > 0.0 for coupling in assembly_spec.inter_module_couplings)


def test_build_native_pressurization_example_context_binds_modules_and_runtimes():
    context = build_native_pressurization_example_context()
    assert set(context.module_bindings_by_id) == {
        "MOD_PRESS_MODE",
        "MOD_AIRCRAFT_ALT",
        "MOD_PRESS_CTRL",
        "MOD_CABIN",
    }
    assert set(context.module_runtimes_by_id) == {
        "MOD_PRESS_MODE",
        "MOD_AIRCRAFT_ALT",
        "MOD_PRESS_CTRL",
        "MOD_CABIN",
    }
    assert context.module_order == ("MOD_PRESS_MODE", "MOD_AIRCRAFT_ALT", "MOD_PRESS_CTRL", "MOD_CABIN")


def test_simulate_native_pressurization_example_emits_lagged_cabin_response():
    df = simulate_native_pressurization_example(n_steps=8, dt_seconds=1.0)
    assert set(df["module_id"].astype(str)) == {
        "MOD_PRESS_MODE",
        "MOD_AIRCRAFT_ALT",
        "MOD_PRESS_CTRL",
        "MOD_CABIN",
    }
    assert set(df["parameter_name"].astype(str)) == {
        "press_mode_state",
        "aircraft_altitude_ft",
        "outflow_valve_pct",
        "cabin_altitude_ft",
        "cabin_delta_p_psi",
    }

    mode_rows = df[df["parameter_name"] == "press_mode_state"].sort_values("step_index").reset_index(drop=True)
    altitude_rows = df[df["parameter_name"] == "aircraft_altitude_ft"].sort_values("step_index").reset_index(drop=True)
    valve_rows = df[df["parameter_name"] == "outflow_valve_pct"].sort_values("step_index").reset_index(drop=True)
    cabin_rows = df[df["parameter_name"] == "cabin_altitude_ft"].sort_values("step_index").reset_index(drop=True)
    delta_p_rows = df[df["parameter_name"] == "cabin_delta_p_psi"].sort_values("step_index").reset_index(drop=True)

    assert set(mode_rows["parameter_value_clean"].astype(str)) == {"GROUND", "AUTO"}
    assert altitude_rows["parameter_value_clean"].astype(float).iloc[-1] > altitude_rows["parameter_value_clean"].astype(float).iloc[0]
    assert valve_rows["parameter_value_clean"].astype(float).iloc[-1] > valve_rows["parameter_value_clean"].astype(float).iloc[0]
    assert cabin_rows["parameter_value_clean"].astype(float).iloc[0] == 0.0
    assert cabin_rows["parameter_value_clean"].astype(float).iloc[-1] > cabin_rows["parameter_value_clean"].astype(float).iloc[1]
    assert delta_p_rows["target_source"].astype(str).iloc[-1] == "latent_state"
    assert delta_p_rows["parameter_value_clean"].astype(float).iloc[-1] > 0.0
