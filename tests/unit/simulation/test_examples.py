from __future__ import annotations

import pandas as pd

from libs.simulation.aircraft.examples import (
    build_coupled_module_aircraft_spec,
    build_power_pressurization_hierarchy_composite_aircraft_spec,
    build_power_chain_aircraft_spec,
    build_pressurization_aircraft_spec,
)
from libs.simulation.flight.examples import (
    build_coupled_module_flight_spec,
    build_power_pressurization_hierarchy_composite_flight_spec,
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
    aircraft_spec = build_power_pressurization_hierarchy_composite_aircraft_spec()
    systems = aircraft_spec.systems
    subsystems = tuple(subsystem for system in systems for subsystem in system.subsystems)
    modules = tuple(module for subsystem in subsystems for module in subsystem.modules)
    parameters = tuple(parameter for module in modules for parameter in module.parameters)

    assert tuple(system.system_id for system in systems) == ("SYS_POWER", "SYS_ECS", "SYS_AIRFRAME")
    assert len(subsystems) == 6
    assert len(modules) == 12
    assert len(parameters) == 24
    assert any(coupling.lag_seconds > 0.0 for coupling in aircraft_spec.couplings)
    assert any(coupling.phase_gate for coupling in aircraft_spec.couplings)
    assert any(coupling.source_mode_gate for coupling in aircraft_spec.couplings)


def test_composite_flight_spec_carries_faults_and_validation_expectations():
    flight_spec = build_power_pressurization_hierarchy_composite_flight_spec()

    assert flight_spec.metadata["flight_name"] == "power_pressurization_hierarchy_composite"
    assert len(flight_spec.input_program_spec.steps) == 32
    assert flight_spec.phase_program_spec is not None
    assert flight_spec.fault_program_spec is not None
    assert len(flight_spec.fault_program_spec.windows) == 4
    assert flight_spec.metadata["validation"]["expected_lag_edges"]
