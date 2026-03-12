"""Example flight specs."""

from __future__ import annotations

from collections.abc import Callable

from libs.simulation.aircraft.examples import (
    build_coupled_module_aircraft_spec,
    build_power_pressurization_hierarchy_composite_aircraft_spec,
    build_power_chain_aircraft_spec,
    build_pressurization_aircraft_spec,
)
from libs.simulation.fault.examples import build_fault_program_spec, build_fault_window_spec, build_no_fault_program_spec
from libs.simulation.flight.spec import (
    FlightSpec,
    InitialStateSpec,
    InputProgramSpec,
    StepInputSpec,
)
from libs.simulation.phase.examples import build_constant_phase_program_spec
from libs.simulation.phase.spec import (
    PhaseEnvelopeSpec,
    PhaseProgramSpec,
    PhaseScheduleSpec,
    PhaseSegmentSpec,
)


FlightBuilder = Callable[[], FlightSpec]


def _step_inputs(*, module_id: str, parameter_name: str, contexts: tuple[dict[str, object], ...]) -> tuple[dict[str, dict[str, StepInputSpec]], ...]:
    return tuple(
        {
            str(module_id): {
                str(parameter_name): StepInputSpec(context=dict(context)),
            }
        }
        for context in contexts
    )


def build_coupled_module_flight_spec() -> FlightSpec:
    steps = tuple(
        {
            "MOD_SOURCE": {
                "supply_voltage": StepInputSpec(
                    context={"target_value": 28.0, "reversion_rate": 1.5},
                ),
            },
            "MOD_TARGET": {
                "motor_speed": StepInputSpec(
                    context={
                        "target_value": 0.0,
                        "latent_target_name": "command_target",
                        "time_constant_seconds": 2.0,
                    },
                ),
            },
        }
        for _ in range(6)
    )
    return FlightSpec(
        aircraft_spec=build_coupled_module_aircraft_spec(),
        input_program_spec=InputProgramSpec(steps=steps),
        initial_state_spec=InitialStateSpec(
            values_by_module={
                "MOD_SOURCE": {"supply_voltage": 27.0},
                "MOD_TARGET": {"motor_speed": 0.0},
            }
        ),
        phase_program_spec=build_constant_phase_program_spec("takeoff_climb"),
        fault_program_spec=build_no_fault_program_spec(),
        metadata={"flight_name": "coupled_module"},
    )


def build_power_chain_flight_spec() -> FlightSpec:
    switch_states = (0, 0, 1, 1, 1, 1, 0, 0)
    steps = tuple(
        {
            "MOD_SWITCH": {
                "contactor_state": StepInputSpec(context={"target_state": switch_state}),
            },
            "MOD_SOURCE": {
                "supply_voltage": StepInputSpec(
                    context={"target_value": 0.0, "latent_target_name": "setpoint_voltage", "reversion_rate": 1.5},
                ),
                "fuel_flow_rate": StepInputSpec(
                    context={"target_value": 0.0, "latent_target_name": "setpoint_flow", "reversion_rate": 1.0},
                ),
            },
            "MOD_TARGET": {
                "motor_speed": StepInputSpec(
                    context={
                        "target_value": 0.0,
                        "latent_target_name": "command_target",
                        "time_constant_seconds": 2.0,
                    },
                ),
            },
            "MOD_TANK": {
                "fuel_used_total": StepInputSpec(
                    context={"target_value": 0.0, "latent_target_name": "flow_rate"},
                ),
            },
        }
        for switch_state in switch_states
    )
    return FlightSpec(
        aircraft_spec=build_power_chain_aircraft_spec(),
        input_program_spec=InputProgramSpec(steps=steps),
        initial_state_spec=InitialStateSpec(
            values_by_module={
                "MOD_SWITCH": {"contactor_state": 0},
                "MOD_SOURCE": {"supply_voltage": 0.0, "fuel_flow_rate": 0.0},
                "MOD_TARGET": {"motor_speed": 0.0},
                "MOD_TANK": {"fuel_used_total": 0.0},
            }
        ),
        phase_program_spec=PhaseProgramSpec(
            schedule=PhaseScheduleSpec(
                segments=(PhaseSegmentSpec("takeoff_climb", 1),),
                repeat=True,
            ),
        ),
        fault_program_spec=build_no_fault_program_spec(),
        metadata={"flight_name": "power_chain"},
    )


def build_pressurization_flight_spec() -> FlightSpec:
    altitude_sequence = (0.0, 500.0, 1500.0, 3000.0, 5000.0, 6500.0, 8000.0, 8000.0)
    steps = tuple(
        {
            "MOD_AIRCRAFT_ALT": {
                "aircraft_altitude_ft": StepInputSpec(
                    context={
                        "target_value": altitude_value,
                        "time_constant_seconds": 2.0,
                    },
                ),
            },
            "MOD_PRESS_CTRL": {
                "outflow_valve_pct": StepInputSpec(
                    context={
                        "target_value": 0.0,
                        "latent_target_name": "outflow_setpoint",
                        "reversion_rate": 1.2,
                    },
                ),
            },
            "MOD_CABIN": {
                "cabin_altitude_ft": StepInputSpec(
                    context={
                        "target_value": 0.0,
                        "latent_target_name": "cabin_alt_target",
                        "time_constant_seconds": 3.0,
                    },
                ),
                "cabin_delta_p_psi": StepInputSpec(
                    context={
                        "target_value": 0.0,
                        "latent_target_name": "delta_p_target",
                        "reversion_rate": 1.5,
                    },
                ),
            },
        }
        for altitude_value in altitude_sequence
    )
    return FlightSpec(
        aircraft_spec=build_pressurization_aircraft_spec(),
        input_program_spec=InputProgramSpec(steps=steps),
        initial_state_spec=InitialStateSpec(
            values_by_module={
                "MOD_PRESS_MODE": {"press_mode_state": "GROUND"},
                "MOD_AIRCRAFT_ALT": {"aircraft_altitude_ft": 0.0},
                "MOD_PRESS_CTRL": {"outflow_valve_pct": 0.0},
                "MOD_CABIN": {"cabin_altitude_ft": 0.0, "cabin_delta_p_psi": 0.0},
            }
        ),
        phase_program_spec=PhaseProgramSpec(
            schedule=PhaseScheduleSpec(
                segments=(
                    PhaseSegmentSpec("gate_turnaround", 2),
                    PhaseSegmentSpec("takeoff_climb", 6),
                ),
                repeat=False,
            ),
            envelopes=(
                PhaseEnvelopeSpec(
                    phase_label="gate_turnaround",
                    step_input_context_by_module={
                        "MOD_PRESS_MODE": {
                            "press_mode_state": {"target_state": "GROUND"},
                        },
                    },
                    mode_state_by_module={"MOD_PRESS_MODE": {"press_mode": "GROUND"}},
                ),
                PhaseEnvelopeSpec(
                    phase_label="takeoff_climb",
                    step_input_context_by_module={
                        "MOD_PRESS_MODE": {
                            "press_mode_state": {"target_state": "AUTO"},
                        },
                    },
                    mode_state_by_module={"MOD_PRESS_MODE": {"press_mode": "AUTO"}},
                ),
            ),
        ),
        fault_program_spec=build_no_fault_program_spec(),
        metadata={"flight_name": "pressurization"},
    )


def _composite_phase_for_step(step_index: int) -> str:
    if step_index < 8:
        return "gate_turnaround"
    if step_index < 16:
        return "takeoff_climb"
    if step_index < 24:
        return "cruise"
    return "descent_approach"


def _composite_aircraft_state_targets(step_index: int) -> tuple[float, float]:
    phase_label = _composite_phase_for_step(step_index)
    local_index = step_index % 8
    if phase_label == "gate_turnaround":
        altitude_targets = (0.0, 0.0, 25.0, 50.0, 50.0, 0.0, 0.0, 0.0)
        vertical_speed_targets = (0.0, 0.0, 150.0, 150.0, 0.0, -150.0, 0.0, 0.0)
    elif phase_label == "takeoff_climb":
        altitude_targets = (500.0, 2000.0, 5000.0, 9000.0, 14000.0, 20000.0, 28000.0, 35000.0)
        vertical_speed_targets = (1500.0, 1800.0, 2200.0, 2500.0, 2400.0, 2200.0, 1800.0, 1200.0)
    elif phase_label == "cruise":
        altitude_targets = (35000.0, 35100.0, 34950.0, 35020.0, 35000.0, 34980.0, 35010.0, 35000.0)
        vertical_speed_targets = (0.0, 50.0, -30.0, 20.0, 0.0, -20.0, 10.0, 0.0)
    else:
        altitude_targets = (33000.0, 30000.0, 25000.0, 18000.0, 12000.0, 8000.0, 5000.0, 2500.0)
        vertical_speed_targets = (-1200.0, -1800.0, -2400.0, -2600.0, -2200.0, -1800.0, -1200.0, -600.0)
    return float(altitude_targets[local_index]), float(vertical_speed_targets[local_index])


def _composite_power_targets(step_index: int) -> tuple[str, str]:
    if step_index < 2:
        return "0", "0"
    if step_index < 6:
        return "1", "0"
    return "1", "1"


def _build_composite_step(step_index: int) -> dict[str, dict[str, StepInputSpec]]:
    altitude_target, vertical_speed_target = _composite_aircraft_state_targets(step_index)
    master_power_state, generator_tie_state = _composite_power_targets(step_index)
    return {
        "MOD_PWR_SWITCH": {
            "master_power_state": StepInputSpec(context={"target_state": master_power_state}),
            "generator_tie_state": StepInputSpec(context={"target_state": generator_tie_state}),
        },
        "MOD_PWR_SOURCE": {
            "bus_voltage_v": StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "voltage_target", "reversion_rate": 1.6},
            ),
            "bus_current_a": StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "current_target", "reversion_rate": 1.2},
            ),
        },
        "MOD_COMP_DRIVE": {
            "compressor_speed_pct": StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "speed_target", "time_constant_seconds": 2.0},
            ),
            "compressor_energy_total_kwh": StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "energy_rate"},
            ),
        },
        "MOD_PWR_LOAD_MON": {
            "electrical_load_pct": StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "load_target", "reversion_rate": 1.1},
            ),
            "inverter_temp_c": StepInputSpec(
                context={"target_value": 18.0, "latent_target_name": "temp_target", "time_constant_seconds": 3.0},
            ),
        },
        "MOD_AIRCRAFT_STATE": {
            "aircraft_altitude_ft": StepInputSpec(
                context={"target_value": altitude_target, "time_constant_seconds": 2.0},
            ),
            "vertical_speed_fpm": StepInputSpec(
                context={"target_value": vertical_speed_target, "time_constant_seconds": 1.5},
            ),
        },
        "MOD_AMBIENT": {
            "ambient_pressure_kpa": StepInputSpec(
                context={"target_value": 101.3, "latent_target_name": "pressure_target", "reversion_rate": 2.0},
            ),
            "ambient_temp_c": StepInputSpec(
                context={"target_value": 22.0, "latent_target_name": "temperature_target", "reversion_rate": 1.3},
            ),
        },
        "MOD_BLEED_SUPPLY": {
            "bleed_supply_psi": StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "bleed_pressure_target", "reversion_rate": 1.4},
            ),
            "bleed_usage_total": StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "bleed_usage_rate"},
            ),
        },
        "MOD_PACK_FLOW": {
            "pack_flow_rate_pct": StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "pack_flow_target", "time_constant_seconds": 2.5},
            ),
            "pack_temp_c": StepInputSpec(
                context={"target_value": 5.0, "latent_target_name": "pack_temp_target", "reversion_rate": 1.0},
            ),
        },
        "MOD_PRESS_CTRL": {
            "outflow_cmd_pct": StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "outflow_target", "reversion_rate": 1.0},
            ),
            "pack_flow_cmd_pct": StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "pack_flow_target", "reversion_rate": 1.0},
            ),
        },
        "MOD_OUTFLOW_ACT": {
            "actuator_position_pct": StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "actuator_target", "time_constant_seconds": 2.0},
            ),
            "actuator_load_pct": StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "actuator_load_target", "reversion_rate": 1.2},
            ),
        },
        "MOD_CABIN": {
            "cabin_altitude_ft": StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "cabin_alt_target", "time_constant_seconds": 3.0},
            ),
            "cabin_delta_p_psi": StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "delta_p_target", "reversion_rate": 1.3},
            ),
        },
    }


def build_power_pressurization_hierarchy_composite_flight_spec() -> FlightSpec:
    steps = tuple(_build_composite_step(step_index) for step_index in range(32))
    phase_program_spec = PhaseProgramSpec(
        schedule=PhaseScheduleSpec(
            segments=(
                PhaseSegmentSpec("gate_turnaround", 8),
                PhaseSegmentSpec("takeoff_climb", 8),
                    PhaseSegmentSpec("cruise", 8),
                    PhaseSegmentSpec("descent_approach", 8),
            ),
            repeat=False,
        ),
        envelopes=(
            PhaseEnvelopeSpec(
                phase_label="gate_turnaround",
                step_input_context_by_module={
                    "MOD_PRESS_MODE": {
                        "press_mode_state": {"target_state": "GROUND"},
                        "pack_mode_state": {"target_state": "OFF"},
                    },
                },
                mode_state_by_module={
                    "MOD_PRESS_MODE": {"press_mode": "GROUND", "pack_mode": "OFF"},
                    "MOD_BLEED_SUPPLY": {"supply_mode": "CLOSED"},
                    "MOD_PACK_FLOW": {"pack_mode": "OFF"},
                },
            ),
            PhaseEnvelopeSpec(
                phase_label="takeoff_climb",
                step_input_context_by_module={
                    "MOD_PRESS_MODE": {
                        "press_mode_state": {"target_state": "AUTO"},
                        "pack_mode_state": {"target_state": "HIGH"},
                    },
                },
                mode_state_by_module={
                    "MOD_PRESS_MODE": {"press_mode": "AUTO", "pack_mode": "HIGH"},
                    "MOD_BLEED_SUPPLY": {"supply_mode": "OPEN"},
                    "MOD_PACK_FLOW": {"pack_mode": "HIGH"},
                },
            ),
            PhaseEnvelopeSpec(
                phase_label="cruise",
                step_input_context_by_module={
                    "MOD_PRESS_MODE": {
                        "press_mode_state": {"target_state": "AUTO"},
                        "pack_mode_state": {"target_state": "NORM"},
                    },
                },
                mode_state_by_module={
                    "MOD_PRESS_MODE": {"press_mode": "AUTO", "pack_mode": "NORM"},
                    "MOD_BLEED_SUPPLY": {"supply_mode": "OPEN"},
                    "MOD_PACK_FLOW": {"pack_mode": "NORM"},
                },
            ),
            PhaseEnvelopeSpec(
                phase_label="descent_approach",
                step_input_context_by_module={
                    "MOD_PRESS_MODE": {
                        "press_mode_state": {"target_state": "DESCENT"},
                        "pack_mode_state": {"target_state": "LOW"},
                    },
                },
                mode_state_by_module={
                    "MOD_PRESS_MODE": {"press_mode": "DESCENT", "pack_mode": "LOW"},
                    "MOD_BLEED_SUPPLY": {"supply_mode": "OPEN"},
                    "MOD_PACK_FLOW": {"pack_mode": "LOW"},
                },
            ),
        ),
    )
    fault_program_spec = build_fault_program_spec(
        windows=(
            build_fault_window_spec(
                module_id="MOD_OUTFLOW_ACT",
                parameter_name="actuator_position_pct",
                start_step=10,
                end_step_exclusive=15,
                context={"violation_type": "timing_lag", "lag_steps": 2, "anomaly_rate": 1.0},
                metadata={"fault_window_id": "FW_INERTIAL_LAG", "fault_family_label": "inertial"},
            ),
            build_fault_window_spec(
                module_id="MOD_BLEED_SUPPLY",
                parameter_name="bleed_supply_psi",
                start_step=18,
                end_step_exclusive=23,
                context={"violation_type": "saturation", "saturation_max": 6.0, "anomaly_rate": 1.0},
                metadata={"fault_window_id": "FW_REGULATED_SAT", "fault_family_label": "regulated"},
            ),
            build_fault_window_spec(
                module_id="MOD_BLEED_SUPPLY",
                parameter_name="bleed_usage_total",
                start_step=20,
                end_step_exclusive=30,
                context={"violation_type": "drift", "drift_rate": 0.15, "anomaly_rate": 1.0},
                metadata={"fault_window_id": "FW_ACCUM_DRIFT", "fault_family_label": "accumulative"},
            ),
            build_fault_window_spec(
                module_id="MOD_PRESS_MODE",
                parameter_name="pack_mode_state",
                start_step=25,
                end_step_exclusive=29,
                context={"violation_type": "state_chatter", "chatter_states": ("LOW", "OFF"), "anomaly_rate": 1.0},
                metadata={"fault_window_id": "FW_DISCRETE_CHATTER", "fault_family_label": "discrete_state"},
            ),
        ),
        metadata={"fault_program_name": "power_pressurization_hierarchy_composite"},
    )
    return FlightSpec(
        aircraft_spec=build_power_pressurization_hierarchy_composite_aircraft_spec(),
        input_program_spec=InputProgramSpec(steps=steps),
        initial_state_spec=InitialStateSpec(
            values_by_module={
                "MOD_PWR_SWITCH": {"master_power_state": "0", "generator_tie_state": "0"},
                "MOD_PWR_SOURCE": {"bus_voltage_v": 0.0, "bus_current_a": 0.0},
                "MOD_COMP_DRIVE": {"compressor_speed_pct": 0.0, "compressor_energy_total_kwh": 0.0},
                "MOD_PWR_LOAD_MON": {"electrical_load_pct": 0.0, "inverter_temp_c": 18.0},
                "MOD_PRESS_MODE": {"press_mode_state": "GROUND", "pack_mode_state": "OFF"},
                "MOD_PRESS_CTRL": {"outflow_cmd_pct": 0.0, "pack_flow_cmd_pct": 0.0},
                "MOD_OUTFLOW_ACT": {"actuator_position_pct": 0.0, "actuator_load_pct": 0.0},
                "MOD_CABIN": {"cabin_altitude_ft": 0.0, "cabin_delta_p_psi": 0.0},
                "MOD_AIRCRAFT_STATE": {"aircraft_altitude_ft": 0.0, "vertical_speed_fpm": 0.0},
                "MOD_AMBIENT": {"ambient_pressure_kpa": 101.3, "ambient_temp_c": 22.0},
                "MOD_BLEED_SUPPLY": {"bleed_supply_psi": 0.0, "bleed_usage_total": 0.0},
                "MOD_PACK_FLOW": {"pack_flow_rate_pct": 0.0, "pack_temp_c": 5.0},
            }
        ),
        phase_program_spec=phase_program_spec,
        fault_program_spec=fault_program_spec,
        metadata={
            "flight_name": "power_pressurization_hierarchy_composite",
            "validation": {
                "expected_lag_edges": (
                    {"parameter_name_u": "outflow_cmd_pct", "parameter_name_v": "actuator_position_pct"},
                    {"parameter_name_u": "actuator_position_pct", "parameter_name_v": "cabin_altitude_ft"},
                    {"parameter_name_u": "compressor_speed_pct", "parameter_name_v": "bleed_supply_psi"},
                ),
            },
        },
    )


def get_flight_builders() -> dict[str, FlightBuilder]:
    return {
        "coupled_module": build_coupled_module_flight_spec,
        "power_chain": build_power_chain_flight_spec,
        "power_pressurization_hierarchy_composite": build_power_pressurization_hierarchy_composite_flight_spec,
        "pressurization": build_pressurization_flight_spec,
    }


def list_flight_names() -> tuple[str, ...]:
    return tuple(sorted(get_flight_builders()))


def build_named_flight_spec(flight_name: str) -> FlightSpec:
    builders = get_flight_builders()
    try:
        return builders[str(flight_name)]()
    except KeyError as exc:
        raise ValueError(f"unknown flight {flight_name!r}; expected one of {sorted(builders)}") from exc
