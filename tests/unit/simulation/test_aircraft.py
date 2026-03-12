from __future__ import annotations

from libs.behavior import BehaviorStepInput
from libs.simulation import Aircraft, AircraftSpec, SubsystemSpec, SystemSpec
from libs.simulation.aircraft.examples import build_coupled_module_aircraft_spec


def test_aircraft_from_nested_spec_builds_hierarchy_and_steps_modules():
    aircraft_spec = build_coupled_module_aircraft_spec()

    aircraft = Aircraft.from_spec(aircraft_spec)
    samples_by_module = aircraft.step(
        step_inputs_by_module={
            "MOD_SOURCE": {
                "supply_voltage": BehaviorStepInput(
                    dt_seconds=1.0,
                    latent_state={},
                    context={"target_value": 28.0, "reversion_rate": 1.5},
                )
            },
            "MOD_TARGET": {
                "motor_speed": BehaviorStepInput(
                    dt_seconds=1.0,
                    latent_state={},
                    context={
                        "target_value": 0.0,
                        "latent_target_name": "command_target",
                        "time_constant_seconds": 2.0,
                    },
                )
            },
        },
        initial_state_by_module={
            "MOD_SOURCE": {"supply_voltage": 27.0},
            "MOD_TARGET": {"motor_speed": 0.0},
        },
    )

    assert aircraft.id == "coupled_module"
    assert aircraft.system_ids == ("SYS_POWER",)
    assert aircraft.subsystem_ids == ("SUB_POWER", "SUB_LOAD")
    assert aircraft.module_ids == ("MOD_SOURCE", "MOD_TARGET")
    assert tuple(system.id for system in aircraft.systems) == ("SYS_POWER",)
    assert tuple(subsystem.id for subsystem in aircraft.systems[0].subsystems) == (
        "SUB_POWER",
        "SUB_LOAD",
    )
    assert aircraft.system("SYS_POWER").id == "SYS_POWER"
    assert aircraft.subsystem("SUB_POWER").id == "SUB_POWER"
    assert aircraft.module("MOD_SOURCE").id == "MOD_SOURCE"
    assert set(samples_by_module) == {"MOD_SOURCE", "MOD_TARGET"}
    assert not hasattr(aircraft, "step_index")
