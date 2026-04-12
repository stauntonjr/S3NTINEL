from __future__ import annotations

import pandas as pd

from libs.simulation.aircraft.examples import (
    build_coupled_module_aircraft_spec,
    build_power_pressurization_hierarchy_medium_aircraft_spec,
    build_power_pressurization_hierarchy_composite_aircraft_spec,
    build_power_pressurization_hierarchy_smoke_aircraft_spec,
    build_power_chain_aircraft_spec,
    build_pressurization_aircraft_spec,
)
from libs.simulation.flight.examples import (
    build_named_flight_spec,
    build_coupled_module_flight_spec,
    build_power_pressurization_hierarchy_composite_module_localization_flight_spec,
    build_power_pressurization_hierarchy_composite_subsystem_localization_flight_spec,
    build_power_pressurization_hierarchy_medium_flight_spec,
    build_power_pressurization_hierarchy_composite_flight_spec,
    build_power_pressurization_hierarchy_smoke_localization_focus_bias_drift_flight_spec,
    build_power_pressurization_hierarchy_smoke_localization_focus_saturation_local_flight_spec,
    build_power_pressurization_hierarchy_smoke_localization_focus_saturation_flight_spec,
    build_power_pressurization_hierarchy_smoke_localization_focus_flight_spec,
    build_power_pressurization_hierarchy_smoke_flight_spec,
    build_power_chain_flight_spec,
    build_pressurization_flight_spec,
)
from libs.simulation.flight.runtime import Flight


def test_build_coupled_module_aircraft_spec_uses_domain_shape():
    aircraft_spec = build_coupled_module_aircraft_spec()
    assert aircraft_spec.aircraft_id == "coupled_module"
    assert tuple(system.system_id for system in aircraft_spec.systems) == ("SYS_POWER",)
    assert tuple(subsystem.subsystem_id for subsystem in aircraft_spec.systems[0].subsystems) == ("SUB_POWER", "SUB_LOAD")
    assert tuple(module.module_id for subsystem in aircraft_spec.systems[0].subsystems for module in subsystem.modules) == (
        "MOD_SOURCE",
        "MOD_TARGET",
    )
    assert len(aircraft_spec.couplings) == 1


def test_build_power_chain_aircraft_spec_uses_all_behavior_families():
    aircraft_spec = build_power_chain_aircraft_spec()
    all_modules = tuple(
        module
        for system in aircraft_spec.systems
        for subsystem in system.subsystems
        for module in subsystem.modules
    )
    assert tuple(module.module_id for module in all_modules) == (
        "MOD_SWITCH",
        "MOD_SOURCE",
        "MOD_TARGET",
        "MOD_TANK",
    )
    behavior_families = {
        parameter.behavior_family_label
        for module in all_modules
        for parameter in module.parameters
    }
    assert behavior_families == {"discrete_state", "regulated", "inertial", "accumulative"}


def test_build_pressurization_aircraft_spec_uses_domain_shaped_modules():
    aircraft_spec = build_pressurization_aircraft_spec()
    all_modules = tuple(
        module
        for system in aircraft_spec.systems
        for subsystem in system.subsystems
        for module in subsystem.modules
    )
    assert tuple(module.module_id for module in all_modules) == (
        "MOD_PRESS_MODE",
        "MOD_PRESS_CTRL",
        "MOD_CABIN",
        "MOD_AIRCRAFT_ALT",
    )
    assert any(coupling.lag_seconds > 0.0 for coupling in aircraft_spec.couplings)


def test_build_flight_specs_construct_live_flights():
    for builder in (
        build_coupled_module_flight_spec,
        build_power_chain_flight_spec,
        build_power_pressurization_hierarchy_smoke_flight_spec,
        build_power_pressurization_hierarchy_smoke_localization_focus_flight_spec,
        build_power_pressurization_hierarchy_smoke_localization_focus_bias_drift_flight_spec,
        build_power_pressurization_hierarchy_smoke_localization_focus_saturation_flight_spec,
        build_power_pressurization_hierarchy_smoke_localization_focus_saturation_local_flight_spec,
        build_power_pressurization_hierarchy_medium_flight_spec,
        build_power_pressurization_hierarchy_composite_flight_spec,
        build_pressurization_flight_spec,
    ):
        flight = Flight.from_spec(builder())
        assert flight.aircraft.module_ids
        assert flight.step_index == 0


def test_power_chain_flight_emits_richer_behavior_chain():
    flight = Flight.from_spec(build_power_chain_flight_spec())
    rows, _ = flight.simulate_rows(n_steps=6, dt_seconds=1.0)
    df = pd.DataFrame.from_records(rows)

    assert set(df["module_id"].astype(str)) == {"MOD_SWITCH", "MOD_SOURCE", "MOD_TARGET", "MOD_TANK"}
    assert set(df["parameter_name"].astype(str)) == {
        "contactor_state",
        "supply_voltage",
        "fuel_flow_rate",
        "motor_speed",
        "fuel_used_total",
    }


def test_pressurization_flight_emits_lagged_cabin_response():
    flight = Flight.from_spec(build_pressurization_flight_spec())
    rows, _ = flight.simulate_rows(n_steps=8, dt_seconds=1.0)
    df = pd.DataFrame.from_records(rows)

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


def test_composite_aircraft_spec_is_large_enough_for_hierarchy_discovery():
    expectations = {
        "smoke": (build_power_pressurization_hierarchy_smoke_aircraft_spec, 5, 12, 24, 14),
        "medium": (build_power_pressurization_hierarchy_medium_aircraft_spec, 8, 20, 40, 25),
        "composite": (build_power_pressurization_hierarchy_composite_aircraft_spec, 12, 28, 56, 38),
    }

    for _scale, (builder, subsystem_count, module_count, parameter_count, coupling_count) in expectations.items():
        aircraft_spec = builder()
        systems = aircraft_spec.systems
        subsystems = tuple(subsystem for system in systems for subsystem in system.subsystems)
        modules = tuple(module for subsystem in subsystems for module in subsystem.modules)
        parameters = tuple(parameter for module in modules for parameter in module.parameters)

        assert tuple(system.system_id for system in systems) == ("SYS_POWER", "SYS_ECS", "SYS_AIRFRAME")
        assert len(subsystems) == subsystem_count
        assert len(modules) == module_count
        assert len(parameters) == parameter_count
        assert len(aircraft_spec.couplings) == coupling_count
        assert any(coupling.lag_seconds > 0.0 for coupling in aircraft_spec.couplings)
        assert any(coupling.phase_gate for coupling in aircraft_spec.couplings)
        assert any(coupling.source_mode_gate for coupling in aircraft_spec.couplings)
        assert all(coupling.allowed_misbehavior_families for coupling in aircraft_spec.couplings)
        assert {0.5, 1.0, 2.0}.issubset({float(parameter.sampling_rate_hz or 0.0) for parameter in parameters})

    composite = build_power_pressurization_hierarchy_composite_aircraft_spec()
    composite_subsystems = [subsystem for system in composite.systems for subsystem in system.subsystems]
    parameter_count_by_subsystem = {
        subsystem.subsystem_id: sum(len(module.parameters) for module in subsystem.modules)
        for subsystem in composite_subsystems
    }
    source_to_targets: dict[str, set[str]] = {}
    for coupling in composite.couplings:
        source_to_targets.setdefault(str(coupling.source_module_id), set()).add(str(coupling.target_module_id))

    assert len(set(parameter_count_by_subsystem.values())) > 1
    assert any(len(targets) >= 3 for targets in source_to_targets.values())
    assert any(
        ("_AFT" in str(coupling.target_module_id) or "_CTR" in str(coupling.target_module_id))
        and ("_AFT" not in str(coupling.source_module_id) and "_CTR" not in str(coupling.source_module_id))
        for coupling in composite.couplings
    )

    switch_datatypes = {
        parameter.parameter_name: parameter.parameter_datatype_label
        for subsystem in composite_subsystems
        for module in subsystem.modules
        for parameter in module.parameters
        if "master_power_state" in parameter.parameter_name or "generator_tie_state" in parameter.parameter_name
    }
    assert switch_datatypes
    assert set(switch_datatypes.values()) == {"categorical"}


def test_composite_flight_spec_carries_misbehaviors_and_validation_expectations():
    expectations = {
        "smoke": (build_power_pressurization_hierarchy_smoke_flight_spec, 24, 14, 9, 1103),
        "medium": (build_power_pressurization_hierarchy_medium_flight_spec, 40, 25, 16, 2207),
        "composite": (build_power_pressurization_hierarchy_composite_flight_spec, 56, 38, 23, 3301),
    }
    expected_phase_segments = (
        ("gate_turnaround", 480),
        ("takeoff_climb", 720),
        ("cruise", 1440),
        ("descent_approach", 720),
    )

    for scale, (builder, parameter_count, coupling_count, misbehavior_count, default_seed) in expectations.items():
        flight_spec = builder()
        systems = flight_spec.aircraft_spec.systems
        parameters = tuple(
            parameter
            for system in systems
            for subsystem in system.subsystems
            for module in subsystem.modules
            for parameter in module.parameters
        )

        assert flight_spec.metadata["flight_name"] == f"power_pressurization_hierarchy_{scale}"
        assert flight_spec.metadata["scenario_family"] == "power_pressurization_authored_roles_v1"
        assert len(flight_spec.input_program_spec.steps) == 3360
        assert flight_spec.input_program_spec.hold_last_step is False
        assert flight_spec.input_program_spec.metadata["default_dt_seconds"] == 0.5
        assert flight_spec.input_program_spec.metadata["stochasticity"]["seed"] == default_seed
        assert flight_spec.metadata["simulation_defaults"]["dt_seconds"] == 0.5
        assert flight_spec.metadata["simulation_defaults"]["n_steps"] == 3360
        assert flight_spec.metadata["stochasticity"]["seed"] == default_seed
        assert flight_spec.metadata["stochasticity"]["profile_name"] == "seeded_nominal_v1"
        assert flight_spec.phase_program_spec is not None
        assert tuple(
            (str(segment.phase_label), int(segment.duration_steps))
            for segment in flight_spec.phase_program_spec.schedule.segments
        ) == expected_phase_segments
        assert flight_spec.misbehavior_program_spec is not None
        assert len(flight_spec.misbehavior_program_spec.windows) == misbehavior_count
        assert flight_spec.fault_program_spec is not None
        assert len(flight_spec.fault_program_spec.windows) == misbehavior_count
        assert len(parameters) == parameter_count
        assert len(flight_spec.aircraft_spec.couplings) == coupling_count
        assert flight_spec.metadata["validation"]["expected_lag_edges"]
        assert flight_spec.metadata["validation"]["expected_fused_edges"]
        assert flight_spec.metadata["validation"]["expected_coupling_signatures"]
        switch_datatypes = {
            parameter.parameter_name: parameter.parameter_datatype_label
            for parameter in parameters
            if "master_power_state" in parameter.parameter_name or "generator_tie_state" in parameter.parameter_name
        }
        assert switch_datatypes
        assert set(switch_datatypes.values()) == {"categorical"}


def test_seeded_hierarchy_flight_is_reproducible_but_seed_sensitive():
    seeded_a = build_named_flight_spec("power_pressurization_hierarchy_smoke", seed=123)
    seeded_b = build_named_flight_spec("power_pressurization_hierarchy_smoke", seed=123)
    seeded_c = build_named_flight_spec("power_pressurization_hierarchy_smoke", seed=456)

    def parameter_count(flight_spec):
        return sum(
            len(module.parameters)
            for system in flight_spec.aircraft_spec.systems
            for subsystem in system.subsystems
            for module in subsystem.modules
        )

    def window_schedule(flight_spec):
        windows = tuple(flight_spec.misbehavior_program_spec.windows)
        return tuple(
            (
                str(window.subject_kind),
                str(window.module_id or ""),
                str(window.parameter_name or ""),
                str(window.coupling_id or ""),
                int(window.start_step),
                int(window.end_step_exclusive),
            )
            for window in windows
        )

    assert seeded_a.metadata["stochasticity"]["seed"] == 123
    assert seeded_b.metadata["stochasticity"]["seed"] == 123
    assert seeded_c.metadata["stochasticity"]["seed"] == 456
    assert seeded_a.input_program_spec.steps == seeded_b.input_program_spec.steps
    assert seeded_a.misbehavior_program_spec.windows == seeded_b.misbehavior_program_spec.windows
    assert seeded_a.input_program_spec.steps != seeded_c.input_program_spec.steps
    assert len(tuple(seeded_a.misbehavior_program_spec.windows)) == len(tuple(seeded_c.misbehavior_program_spec.windows))
    assert parameter_count(seeded_a) == parameter_count(seeded_c)
    assert window_schedule(seeded_a) == window_schedule(seeded_c)

    raw_rows_a, _ = Flight.from_spec(seeded_a).simulate_rows(n_steps=32, dt_seconds=0.5, apply_faults=True)
    raw_rows_b, _ = Flight.from_spec(seeded_b).simulate_rows(n_steps=32, dt_seconds=0.5, apply_faults=True)
    raw_rows_c, _ = Flight.from_spec(seeded_c).simulate_rows(n_steps=32, dt_seconds=0.5, apply_faults=True)

    assert raw_rows_a == raw_rows_b
    assert raw_rows_a != raw_rows_c


def test_filtered_benchmark_flight_specs_split_module_and_subsystem_suites():
    module_suite = build_power_pressurization_hierarchy_composite_module_localization_flight_spec()
    subsystem_suite = build_power_pressurization_hierarchy_composite_subsystem_localization_flight_spec()

    assert module_suite.metadata["flight_name"] == "power_pressurization_hierarchy_composite_module_localization"
    assert module_suite.metadata["benchmark_suite_name"] == "module_localization"
    assert module_suite.metadata["benchmark_recoverability_targets"] == ["module_recoverable"]
    assert len(module_suite.misbehavior_program_spec.windows) == 14
    assert {
        str(window.metadata["benchmark_recoverability_target"])
        for window in module_suite.misbehavior_program_spec.windows
    } == {"module_recoverable"}

    assert subsystem_suite.metadata["flight_name"] == "power_pressurization_hierarchy_composite_subsystem_localization"
    assert subsystem_suite.metadata["benchmark_suite_name"] == "subsystem_localization"
    assert subsystem_suite.metadata["benchmark_recoverability_targets"] == ["subsystem_recoverable"]
    assert len(subsystem_suite.misbehavior_program_spec.windows) == 9
    assert {
        str(window.metadata["benchmark_recoverability_target"])
        for window in subsystem_suite.misbehavior_program_spec.windows
    } == {"subsystem_recoverable"}


def test_named_builder_resolves_filtered_benchmark_flight_specs():
    module_suite = build_named_flight_spec("power_pressurization_hierarchy_composite_module_localization")
    subsystem_suite = build_named_flight_spec("power_pressurization_hierarchy_composite_subsystem_localization")

    assert module_suite.metadata["flight_name"] == "power_pressurization_hierarchy_composite_module_localization"
    assert subsystem_suite.metadata["flight_name"] == "power_pressurization_hierarchy_composite_subsystem_localization"


def test_smoke_localization_focus_flight_spec_targets_clean_module_faults():
    flight_spec = build_power_pressurization_hierarchy_smoke_localization_focus_flight_spec()

    assert flight_spec.metadata["flight_name"] == "power_pressurization_hierarchy_smoke_localization_focus"
    assert flight_spec.metadata["scale"] == "smoke"
    assert flight_spec.metadata["benchmark_suite_name"] == "localization_focus"
    assert flight_spec.metadata["benchmark_recoverability_targets"] == ["module_recoverable"]
    assert flight_spec.metadata["benchmark_fault_types"] == ["bias", "saturation", "drift"]
    assert flight_spec.metadata["stochasticity"]["profile_name"] == "seeded_localization_focus_v1"
    assert flight_spec.metadata["stochasticity"]["enabled_channels"] == [
        "nominal_observation_noise",
        "role_profile_offsets",
    ]
    assert len(flight_spec.misbehavior_program_spec.windows) == 3
    assert {
        str(window.metadata["benchmark_recoverability_target"])
        for window in flight_spec.misbehavior_program_spec.windows
    } == {"module_recoverable"}
    assert {
        str(window.context["violation_type"])
        for window in flight_spec.misbehavior_program_spec.windows
    } == {"bias", "saturation", "drift"}


def test_named_builder_resolves_smoke_localization_focus_flight_spec():
    focus_suite = build_named_flight_spec("power_pressurization_hierarchy_smoke_localization_focus")

    assert focus_suite.metadata["flight_name"] == "power_pressurization_hierarchy_smoke_localization_focus"


def test_smoke_localization_focus_family_packs_filter_fault_types():
    bias_drift_suite = build_power_pressurization_hierarchy_smoke_localization_focus_bias_drift_flight_spec()
    saturation_suite = build_power_pressurization_hierarchy_smoke_localization_focus_saturation_flight_spec()
    saturation_local_suite = build_power_pressurization_hierarchy_smoke_localization_focus_saturation_local_flight_spec()

    assert bias_drift_suite.metadata["flight_name"] == "power_pressurization_hierarchy_smoke_localization_focus_bias_drift"
    assert bias_drift_suite.metadata["benchmark_suite_name"] == "localization_focus_bias_drift"
    assert bias_drift_suite.metadata["benchmark_fault_types"] == ["bias", "drift"]
    assert len(bias_drift_suite.misbehavior_program_spec.windows) == 2
    assert {
        str(window.context["violation_type"])
        for window in bias_drift_suite.misbehavior_program_spec.windows
    } == {"bias", "drift"}

    assert saturation_suite.metadata["flight_name"] == "power_pressurization_hierarchy_smoke_localization_focus_saturation"
    assert saturation_suite.metadata["benchmark_suite_name"] == "localization_focus_saturation"
    assert saturation_suite.metadata["benchmark_fault_types"] == ["saturation"]
    assert saturation_suite.metadata["benchmark_recoverability_targets"] == ["parameter_visible_only"]
    assert len(saturation_suite.misbehavior_program_spec.windows) == 1
    assert {
        str(window.context["violation_type"])
        for window in saturation_suite.misbehavior_program_spec.windows
    } == {"saturation"}
    assert saturation_suite.misbehavior_program_spec.windows[0].parameter_name == "bleed_supply_psi"
    assert saturation_suite.misbehavior_program_spec.windows[0].metadata["benchmark_recoverability_target"] == "parameter_visible_only"

    assert saturation_local_suite.metadata["flight_name"] == "power_pressurization_hierarchy_smoke_localization_focus_saturation_local"
    assert saturation_local_suite.metadata["benchmark_suite_name"] == "localization_focus_saturation_local"
    assert saturation_local_suite.metadata["benchmark_fault_types"] == ["saturation"]
    assert saturation_local_suite.metadata["benchmark_recoverability_targets"] == ["detection_only"]
    assert saturation_local_suite.metadata["localization_focus_saturation_variant"] == "pack_temp_local"
    assert len(saturation_local_suite.misbehavior_program_spec.windows) == 1
    assert saturation_local_suite.misbehavior_program_spec.windows[0].parameter_name == "pack_temp_c"
    assert saturation_local_suite.misbehavior_program_spec.windows[0].module_id == "MOD_PACK_FLOW"
    assert saturation_local_suite.misbehavior_program_spec.windows[0].metadata["benchmark_recoverability_target"] == "detection_only"
    assert (
        saturation_local_suite.misbehavior_program_spec.windows[0].context["benchmark_fault_variant"]
        == "pack_temp_local"
    )


def test_named_builder_resolves_smoke_localization_focus_family_packs():
    bias_drift_suite = build_named_flight_spec("power_pressurization_hierarchy_smoke_localization_focus_bias_drift")
    saturation_suite = build_named_flight_spec("power_pressurization_hierarchy_smoke_localization_focus_saturation")
    saturation_local_suite = build_named_flight_spec("power_pressurization_hierarchy_smoke_localization_focus_saturation_local")

    assert bias_drift_suite.metadata["flight_name"] == "power_pressurization_hierarchy_smoke_localization_focus_bias_drift"
    assert saturation_suite.metadata["flight_name"] == "power_pressurization_hierarchy_smoke_localization_focus_saturation"
    assert saturation_local_suite.metadata["flight_name"] == "power_pressurization_hierarchy_smoke_localization_focus_saturation_local"
