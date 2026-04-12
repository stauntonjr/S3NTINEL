"""Example flight specs."""

from __future__ import annotations

from collections.abc import Callable
from math import cos, pi, sin

from libs.simulation.aircraft.examples import (
    build_coupled_module_aircraft_spec,
    build_power_pressurization_hierarchy_medium_aircraft_spec,
    build_power_pressurization_hierarchy_composite_aircraft_spec,
    build_power_pressurization_hierarchy_smoke_aircraft_spec,
    build_power_chain_aircraft_spec,
    build_pressurization_aircraft_spec,
)
from libs.simulation.fault.examples import (
    build_misbehavior_program_spec,
    build_misbehavior_window_spec,
    build_no_misbehavior_program_spec,
)
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
from libs.simulation.scenarios import (
    build_power_pressurization_flight_spec,
    build_power_pressurization_localization_focus_flight_spec,
)


FlightBuilder = Callable[..., FlightSpec]


def _step_inputs(*, module_id: str, parameter_name: str, contexts: tuple[dict[str, object], ...]) -> tuple[dict[str, dict[str, StepInputSpec]], ...]:
    return tuple(
        {
            str(module_id): {
                str(parameter_name): StepInputSpec(context=dict(context)),
            }
        }
        for context in contexts
    )


def _misbehavior_window_metadata(
    *,
    misbehavior_window_id: str,
    fault_window_id: str,
    fault_family_label: str,
    benchmark_recoverability_target: str,
    extra_metadata: dict[str, object] | None = None,
) -> dict[str, object]:
    metadata = {
        "misbehavior_window_id": str(misbehavior_window_id),
        "fault_window_id": str(fault_window_id),
        "fault_family_label": str(fault_family_label),
        "benchmark_recoverability_target": str(benchmark_recoverability_target),
    }
    if extra_metadata:
        metadata.update(dict(extra_metadata))
    return metadata


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
        misbehavior_program_spec=build_no_misbehavior_program_spec(),
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
        misbehavior_program_spec=build_no_misbehavior_program_spec(),
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
        misbehavior_program_spec=build_no_misbehavior_program_spec(),
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
    misbehavior_program_spec = build_misbehavior_program_spec(
        windows=(
            build_misbehavior_window_spec(
                module_id="MOD_OUTFLOW_ACT",
                parameter_name="actuator_position_pct",
                start_step=10,
                end_step_exclusive=15,
                context={"violation_type": "timing_lag", "lag_steps": 2, "anomaly_rate": 1.0},
                metadata=_misbehavior_window_metadata(
                    misbehavior_window_id="MBW_INERTIAL_LAG",
                    fault_window_id="FW_INERTIAL_LAG",
                    fault_family_label="inertial",
                    benchmark_recoverability_target="module_recoverable",
                ),
            ),
            build_misbehavior_window_spec(
                module_id="MOD_BLEED_SUPPLY",
                parameter_name="bleed_supply_psi",
                start_step=18,
                end_step_exclusive=23,
                context={"violation_type": "saturation", "saturation_max": 6.0, "anomaly_rate": 1.0},
                metadata=_misbehavior_window_metadata(
                    misbehavior_window_id="MBW_REGULATED_SAT",
                    fault_window_id="FW_REGULATED_SAT",
                    fault_family_label="regulated",
                    benchmark_recoverability_target="module_recoverable",
                ),
            ),
            build_misbehavior_window_spec(
                module_id="MOD_BLEED_SUPPLY",
                parameter_name="bleed_usage_total",
                start_step=20,
                end_step_exclusive=30,
                context={"violation_type": "drift", "drift_rate": 0.15, "anomaly_rate": 1.0},
                metadata=_misbehavior_window_metadata(
                    misbehavior_window_id="MBW_ACCUM_DRIFT",
                    fault_window_id="FW_ACCUM_DRIFT",
                    fault_family_label="accumulative",
                    benchmark_recoverability_target="module_recoverable",
                ),
            ),
            build_misbehavior_window_spec(
                module_id="MOD_PRESS_MODE",
                parameter_name="pack_mode_state",
                start_step=25,
                end_step_exclusive=29,
                context={"violation_type": "state_chatter", "chatter_states": ("LOW", "OFF"), "anomaly_rate": 1.0},
                metadata=_misbehavior_window_metadata(
                    misbehavior_window_id="MBW_DISCRETE_CHATTER",
                    fault_window_id="FW_DISCRETE_CHATTER",
                    fault_family_label="discrete_state",
                    benchmark_recoverability_target="module_recoverable",
                ),
            ),
        ),
        metadata={"misbehavior_program_name": "power_pressurization_hierarchy_composite"},
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
        misbehavior_program_spec=misbehavior_program_spec,
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


build_legacy_power_pressurization_hierarchy_reference_flight_spec = (
    build_power_pressurization_hierarchy_composite_flight_spec
)
_LEGACY_BUILD_POWER_PRESSURIZATION_HIERARCHY_COMPOSITE_FLIGHT_SPEC = (
    build_legacy_power_pressurization_hierarchy_reference_flight_spec
)

_REALISTIC_BRANCH_COUNT_BY_SCALE = {
    "smoke": 1,
    "medium": 2,
    "composite": 4,
}

_REALISTIC_PHASE_SEGMENTS = (
    ("gate_turnaround", 240.0),
    ("takeoff_climb", 360.0),
    ("cruise", 720.0),
    ("descent_approach", 360.0),
)

_REALISTIC_DT_SECONDS = 0.5
_REALISTIC_ALLOWED_COUPLING_MISBEHAVIORS = (
    "coupling_break",
    "coupling_inversion",
    "timing_lag",
    "timing_jitter",
    "phase_context_violation",
)


def _realistic_branch_suffix(branch_index: int) -> str:
    return "" if int(branch_index) == 0 else f"_B{int(branch_index) + 1}"


def _realistic_branch_parameter_name(parameter_name: str, branch_index: int) -> str:
    return str(parameter_name) if int(branch_index) == 0 else f"{parameter_name}_b{int(branch_index) + 1}"


def _phase_duration_steps(seconds: float) -> int:
    return int(round(float(seconds) / _REALISTIC_DT_SECONDS))


def _realistic_phase_segment_specs() -> tuple[PhaseSegmentSpec, ...]:
    return tuple(
        PhaseSegmentSpec(str(phase_label), _phase_duration_steps(seconds))
        for phase_label, seconds in _REALISTIC_PHASE_SEGMENTS
    )


def _realistic_total_steps() -> int:
    return sum(int(segment.duration_steps) for segment in _realistic_phase_segment_specs())


def _realistic_phase_ranges() -> tuple[tuple[str, int, int], ...]:
    ranges: list[tuple[str, int, int]] = []
    start = 0
    for segment in _realistic_phase_segment_specs():
        end = start + int(segment.duration_steps)
        ranges.append((str(segment.phase_label), start, end))
        start = end
    return tuple(ranges)


def _realistic_phase_progress(step_index: int) -> tuple[str, float]:
    for phase_label, start, end in _realistic_phase_ranges():
        if int(step_index) < end:
            width = max(end - start - 1, 1)
            return phase_label, max(0.0, min((int(step_index) - start) / float(width), 1.0))
    last_phase, start, end = _realistic_phase_ranges()[-1]
    width = max(end - start - 1, 1)
    return last_phase, max(0.0, min((int(step_index) - start) / float(width), 1.0))


def _ease_in_out(progress: float) -> float:
    clipped = max(0.0, min(float(progress), 1.0))
    return 0.5 - (0.5 * cos(pi * clipped))


def _branch_profile(branch_index: int) -> dict[str, float]:
    return {
        "branch_index": float(branch_index),
        "altitude_scale": 1.0 + (0.01 * float(branch_index)),
        "pressure_scale": 1.0 + (0.04 * float(branch_index)),
        "temperature_offset": -0.75 * float(branch_index),
        "ground_altitude_bias": 8.0 * float(branch_index),
    }


def _aircraft_state_targets(step_index: int) -> tuple[float, float]:
    phase_label, progress = _realistic_phase_progress(step_index)
    eased = _ease_in_out(progress)
    if phase_label == "gate_turnaround":
        altitude_target = 35.0 * (1.0 - cos(pi * progress))
        vertical_speed_target = 120.0 * cos(2.0 * pi * progress)
    elif phase_label == "takeoff_climb":
        altitude_target = 35000.0 * eased
        vertical_speed_target = 1400.0 + (1100.0 * sin(pi * progress))
    elif phase_label == "cruise":
        altitude_target = 35000.0 + (120.0 * sin(2.0 * pi * progress)) + (45.0 * sin(6.0 * pi * progress))
        vertical_speed_target = 45.0 * sin(4.0 * pi * progress)
    else:
        altitude_target = (35000.0 * (1.0 - eased)) + (2500.0 * eased)
        vertical_speed_target = -600.0 - (1700.0 * sin(pi * progress))
    return float(max(altitude_target, 0.0)), float(vertical_speed_target)


def _power_states(step_index: int) -> tuple[str, str]:
    phase_label, progress = _realistic_phase_progress(step_index)
    if phase_label == "gate_turnaround":
        if progress < 0.2:
            return "0", "0"
        if progress < 0.6:
            return "1", "0"
    return "1", "1"


def _branch_step(branch_index: int, step_index: int) -> dict[str, dict[str, StepInputSpec]]:
    altitude_target, vertical_speed_target = _aircraft_state_targets(step_index)
    master_power_state, generator_tie_state = _power_states(step_index)
    phase_label, progress = _realistic_phase_progress(step_index)
    profile = _branch_profile(branch_index)
    branch_suffix = _realistic_branch_suffix(branch_index)

    def module_id(base: str) -> str:
        return f"{base}{branch_suffix}" if branch_suffix else str(base)

    def parameter_name(base: str) -> str:
        return _realistic_branch_parameter_name(base, branch_index)

    gate_bias = profile["ground_altitude_bias"] if phase_label == "gate_turnaround" else 0.0
    branch_altitude_target = max((altitude_target * profile["altitude_scale"]) + gate_bias, 0.0)
    branch_vertical_speed_target = vertical_speed_target * profile["altitude_scale"]
    ambient_temp_target = 22.0 - (0.0018 * branch_altitude_target) + profile["temperature_offset"]
    ambient_pressure_target = max(101.3 - (0.002 * branch_altitude_target), 18.0)

    return {
        module_id("MOD_PWR_SWITCH"): {
            parameter_name("master_power_state"): StepInputSpec(context={"target_state": master_power_state}),
            parameter_name("generator_tie_state"): StepInputSpec(context={"target_state": generator_tie_state}),
        },
        module_id("MOD_PWR_SOURCE"): {
            parameter_name("bus_voltage_v"): StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "voltage_target", "reversion_rate": 1.6 + (0.05 * branch_index)},
            ),
            parameter_name("bus_current_a"): StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "current_target", "reversion_rate": 1.15 + (0.04 * branch_index)},
            ),
        },
        module_id("MOD_COMP_DRIVE"): {
            parameter_name("compressor_speed_pct"): StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "speed_target", "time_constant_seconds": 1.8 + (0.15 * branch_index)},
            ),
            parameter_name("compressor_energy_total_kwh"): StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "energy_rate"},
            ),
        },
        module_id("MOD_PWR_LOAD_MON"): {
            parameter_name("electrical_load_pct"): StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "load_target", "reversion_rate": 1.05 + (0.03 * branch_index)},
            ),
            parameter_name("inverter_temp_c"): StepInputSpec(
                context={"target_value": 18.0 + (1.5 * branch_index), "latent_target_name": "temp_target", "time_constant_seconds": 2.5 + (0.2 * branch_index)},
            ),
        },
        module_id("MOD_AIRCRAFT_STATE"): {
            parameter_name("aircraft_altitude_ft"): StepInputSpec(
                context={"target_value": branch_altitude_target, "time_constant_seconds": 1.5},
            ),
            parameter_name("vertical_speed_fpm"): StepInputSpec(
                context={"target_value": branch_vertical_speed_target, "time_constant_seconds": 1.0},
            ),
        },
        module_id("MOD_AMBIENT"): {
            parameter_name("ambient_pressure_kpa"): StepInputSpec(
                context={"target_value": ambient_pressure_target, "latent_target_name": "pressure_target", "reversion_rate": 1.8},
            ),
            parameter_name("ambient_temp_c"): StepInputSpec(
                context={"target_value": ambient_temp_target, "latent_target_name": "temperature_target", "reversion_rate": 1.25},
            ),
        },
        module_id("MOD_BLEED_SUPPLY"): {
            parameter_name("bleed_supply_psi"): StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "bleed_pressure_target", "reversion_rate": 1.25 + (0.06 * branch_index)},
            ),
            parameter_name("bleed_usage_total"): StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "bleed_usage_rate"},
            ),
        },
        module_id("MOD_PACK_FLOW"): {
            parameter_name("pack_flow_rate_pct"): StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "pack_flow_target", "time_constant_seconds": 2.1 + (0.1 * branch_index)},
            ),
            parameter_name("pack_temp_c"): StepInputSpec(
                context={
                    "target_value": 5.0 + profile["temperature_offset"] - (2.0 * progress if phase_label == "cruise" else 0.0),
                    "latent_target_name": "pack_temp_target",
                    "reversion_rate": 1.0,
                },
            ),
        },
        module_id("MOD_PRESS_CTRL"): {
            parameter_name("outflow_cmd_pct"): StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "outflow_target", "reversion_rate": 1.0},
            ),
            parameter_name("pack_flow_cmd_pct"): StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "pack_flow_target", "reversion_rate": 1.0},
            ),
        },
        module_id("MOD_OUTFLOW_ACT"): {
            parameter_name("actuator_position_pct"): StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "actuator_target", "time_constant_seconds": 1.8 + (0.1 * branch_index)},
            ),
            parameter_name("actuator_load_pct"): StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "actuator_load_target", "reversion_rate": 1.15},
            ),
        },
        module_id("MOD_CABIN"): {
            parameter_name("cabin_altitude_ft"): StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "cabin_alt_target", "time_constant_seconds": 2.4 + (0.15 * branch_index)},
            ),
            parameter_name("cabin_delta_p_psi"): StepInputSpec(
                context={"target_value": 0.0, "latent_target_name": "delta_p_target", "reversion_rate": 1.2},
            ),
        },
    }


def _build_realistic_steps(*, branch_count: int) -> tuple[dict[str, dict[str, StepInputSpec]], ...]:
    total_steps = _realistic_total_steps()
    return tuple(
        {
            module_id: parameter_inputs
            for branch_index in range(branch_count)
            for module_id, parameter_inputs in _branch_step(branch_index, step_index).items()
        }
        for step_index in range(total_steps)
    )


def _clone_branch_initial_state(values_by_module: dict[str, dict[str, object]], *, branch_count: int) -> dict[str, dict[str, object]]:
    cloned: dict[str, dict[str, object]] = {}
    for branch_index in range(branch_count):
        branch_suffix = _realistic_branch_suffix(branch_index)
        for module_id, parameter_values in values_by_module.items():
            resolved_module_id = f"{module_id}{branch_suffix}" if branch_suffix else str(module_id)
            cloned[resolved_module_id] = {
                _realistic_branch_parameter_name(parameter_name, branch_index): value
                for parameter_name, value in parameter_values.items()
            }
    return cloned


def _clone_phase_program(legacy_phase_program: PhaseProgramSpec, *, branch_count: int) -> PhaseProgramSpec:
    envelopes = []
    for envelope in legacy_phase_program.envelopes:
        step_input_context_by_module: dict[str, dict[str, dict[str, object]]] = {}
        mode_state_by_module: dict[str, dict[str, object]] = {}
        latent_state_by_module: dict[str, dict[str, float]] = {}
        for branch_index in range(branch_count):
            branch_suffix = _realistic_branch_suffix(branch_index)
            for module_id, parameter_contexts in envelope.step_input_context_by_module.items():
                resolved_module_id = f"{module_id}{branch_suffix}" if branch_suffix else str(module_id)
                step_input_context_by_module[resolved_module_id] = {
                    _realistic_branch_parameter_name(parameter_name, branch_index): dict(context_updates)
                    for parameter_name, context_updates in parameter_contexts.items()
                }
            for module_id, mode_updates in envelope.mode_state_by_module.items():
                resolved_module_id = f"{module_id}{branch_suffix}" if branch_suffix else str(module_id)
                mode_state_by_module[resolved_module_id] = dict(mode_updates)
            for module_id, latent_updates in envelope.latent_state_by_module.items():
                resolved_module_id = f"{module_id}{branch_suffix}" if branch_suffix else str(module_id)
                latent_state_by_module[resolved_module_id] = dict(latent_updates)
        envelopes.append(
            PhaseEnvelopeSpec(
                phase_label=str(envelope.phase_label),
                step_input_context_by_module=step_input_context_by_module,
                mode_state_by_module=mode_state_by_module,
                latent_state_by_module=latent_state_by_module,
                metadata=dict(envelope.metadata),
            )
        )
    return PhaseProgramSpec(
        schedule=PhaseScheduleSpec(
            segments=_realistic_phase_segment_specs(),
            repeat=False,
        ),
        envelopes=tuple(envelopes),
    )


def _branch_coupling_id(base_coupling_id: str, branch_index: int) -> str:
    return str(base_coupling_id) if int(branch_index) == 0 else f"{base_coupling_id}{_realistic_branch_suffix(branch_index)}"


def _mission_step_for_phase(phase_label: str, *, offset_seconds: float) -> int:
    elapsed_seconds = 0.0
    for current_phase_label, duration_seconds in _REALISTIC_PHASE_SEGMENTS:
        if current_phase_label == phase_label:
            return int(round((elapsed_seconds + float(offset_seconds)) / _REALISTIC_DT_SECONDS))
        elapsed_seconds += float(duration_seconds)
    raise ValueError(f"unknown phase label {phase_label!r}")


def _build_realistic_misbehavior_program(*, branch_count: int) -> Any:
    windows = []
    for branch_index in range(branch_count):
        branch_suffix = _realistic_branch_suffix(branch_index)
        branch_code = f"B{branch_index + 1}"

        def module_id(base: str) -> str:
            return f"{base}{branch_suffix}" if branch_suffix else str(base)

        def parameter_name(base: str) -> str:
            return _realistic_branch_parameter_name(base, branch_index)

        windows.extend(
            (
                build_misbehavior_window_spec(
                    module_id=module_id("MOD_OUTFLOW_ACT"),
                    parameter_name=parameter_name("actuator_position_pct"),
                    start_step=_mission_step_for_phase("takeoff_climb", offset_seconds=45.0 + (12.0 * branch_index)),
                    end_step_exclusive=_mission_step_for_phase("takeoff_climb", offset_seconds=105.0 + (12.0 * branch_index)),
                    context={"violation_type": "timing_lag", "lag_steps": 3 + branch_index, "anomaly_rate": 1.0},
                    metadata=_misbehavior_window_metadata(
                        misbehavior_window_id=f"MBW_TIMING_LAG_{branch_code}",
                        fault_window_id=f"FW_TIMING_LAG_{branch_code}",
                        fault_family_label="inertial",
                        benchmark_recoverability_target="module_recoverable",
                    ),
                ),
                build_misbehavior_window_spec(
                    module_id=module_id("MOD_PWR_SOURCE"),
                    parameter_name=parameter_name("bus_voltage_v"),
                    start_step=_mission_step_for_phase("gate_turnaround", offset_seconds=120.0 + (10.0 * branch_index)),
                    end_step_exclusive=_mission_step_for_phase("takeoff_climb", offset_seconds=45.0 + (10.0 * branch_index)),
                    context={"violation_type": "bias", "bias": 1.25 + (0.2 * branch_index), "anomaly_rate": 1.0},
                    metadata=_misbehavior_window_metadata(
                        misbehavior_window_id=f"MBW_REGULATED_BIAS_{branch_code}",
                        fault_window_id=f"FW_REGULATED_BIAS_{branch_code}",
                        fault_family_label="regulated",
                        benchmark_recoverability_target="module_recoverable",
                    ),
                ),
                build_misbehavior_window_spec(
                    module_id=module_id("MOD_BLEED_SUPPLY"),
                    parameter_name=parameter_name("bleed_supply_psi"),
                    start_step=_mission_step_for_phase("cruise", offset_seconds=120.0 + (20.0 * branch_index)),
                    end_step_exclusive=_mission_step_for_phase("cruise", offset_seconds=220.0 + (20.0 * branch_index)),
                    context={"violation_type": "saturation", "saturation_max": 7.0 + (0.25 * branch_index), "anomaly_rate": 1.0},
                    metadata=_misbehavior_window_metadata(
                        misbehavior_window_id=f"MBW_REGULATED_SAT_{branch_code}",
                        fault_window_id=f"FW_REGULATED_SAT_{branch_code}",
                        fault_family_label="regulated",
                        benchmark_recoverability_target="module_recoverable",
                    ),
                ),
                build_misbehavior_window_spec(
                    module_id=module_id("MOD_BLEED_SUPPLY"),
                    parameter_name=parameter_name("bleed_usage_total"),
                    start_step=_mission_step_for_phase("cruise", offset_seconds=420.0 + (25.0 * branch_index)),
                    end_step_exclusive=_mission_step_for_phase("descent_approach", offset_seconds=120.0 + (15.0 * branch_index)),
                    context={"violation_type": "drift", "drift_rate": 0.02 + (0.005 * branch_index), "anomaly_rate": 1.0},
                    metadata=_misbehavior_window_metadata(
                        misbehavior_window_id=f"MBW_ACCUM_DRIFT_{branch_code}",
                        fault_window_id=f"FW_ACCUM_DRIFT_{branch_code}",
                        fault_family_label="accumulative",
                        benchmark_recoverability_target="module_recoverable",
                    ),
                ),
                build_misbehavior_window_spec(
                    module_id=module_id("MOD_PRESS_MODE"),
                    parameter_name=parameter_name("pack_mode_state"),
                    start_step=_mission_step_for_phase("descent_approach", offset_seconds=150.0 + (15.0 * branch_index)),
                    end_step_exclusive=_mission_step_for_phase("descent_approach", offset_seconds=240.0 + (15.0 * branch_index)),
                    context={"violation_type": "state_chatter", "chatter_states": ("LOW", "OFF"), "anomaly_rate": 1.0},
                    metadata=_misbehavior_window_metadata(
                        misbehavior_window_id=f"MBW_STATE_CHATTER_{branch_code}",
                        fault_window_id=f"FW_STATE_CHATTER_{branch_code}",
                        fault_family_label="discrete_state",
                        benchmark_recoverability_target="module_recoverable",
                    ),
                ),
                build_misbehavior_window_spec(
                    module_id=module_id("MOD_PRESS_MODE"),
                    parameter_name=parameter_name("press_mode_state"),
                    start_step=_mission_step_for_phase("descent_approach", offset_seconds=40.0 + (10.0 * branch_index)),
                    end_step_exclusive=_mission_step_for_phase("descent_approach", offset_seconds=110.0 + (10.0 * branch_index)),
                    context={"violation_type": "illegal_transition", "target_state": "GROUND", "anomaly_rate": 1.0},
                    metadata=_misbehavior_window_metadata(
                        misbehavior_window_id=f"MBW_ILLEGAL_TRANSITION_{branch_code}",
                        fault_window_id=f"FW_ILLEGAL_TRANSITION_{branch_code}",
                        fault_family_label="discrete_state",
                        benchmark_recoverability_target="module_recoverable",
                    ),
                ),
                build_misbehavior_window_spec(
                    subject_kind="coupling",
                    coupling_id=_branch_coupling_id("MOD_PRESS_CTRL:outflow_cmd_out:drive:MOD_OUTFLOW_ACT:outflow_cmd_in", branch_index),
                    start_step=_mission_step_for_phase("takeoff_climb", offset_seconds=160.0 + (15.0 * branch_index)),
                    end_step_exclusive=_mission_step_for_phase("takeoff_climb", offset_seconds=245.0 + (15.0 * branch_index)),
                    context={"violation_type": "timing_lag", "extra_lag_seconds": 1.5 + (0.25 * branch_index)},
                    metadata=_misbehavior_window_metadata(
                        misbehavior_window_id=f"MBW_COUPLING_LAG_{branch_code}",
                        fault_window_id=f"FW_COUPLING_LAG_{branch_code}",
                        fault_family_label="coupling",
                        benchmark_recoverability_target="subsystem_recoverable",
                    ),
                ),
                build_misbehavior_window_spec(
                    subject_kind="coupling",
                    coupling_id=_branch_coupling_id("MOD_BLEED_SUPPLY:bleed_psi_out:drive:MOD_PACK_FLOW:bleed_psi_in", branch_index),
                    start_step=_mission_step_for_phase("cruise", offset_seconds=300.0 + (25.0 * branch_index)),
                    end_step_exclusive=_mission_step_for_phase("cruise", offset_seconds=420.0 + (25.0 * branch_index)),
                    context={
                        "violation_type": "coupling_inversion" if branch_index % 2 == 0 else "coupling_break",
                        "anomaly_rate": 1.0,
                    },
                    metadata=_misbehavior_window_metadata(
                        misbehavior_window_id=f"MBW_COUPLING_STRUCTURE_{branch_code}",
                        fault_window_id=f"FW_COUPLING_STRUCTURE_{branch_code}",
                        fault_family_label="coupling",
                        benchmark_recoverability_target="subsystem_recoverable",
                    ),
                ),
                build_misbehavior_window_spec(
                    subject_kind="coupling",
                    coupling_id=_branch_coupling_id("MOD_PACK_FLOW:pack_flow_out:drive:MOD_CABIN:pack_flow_in", branch_index),
                    start_step=_mission_step_for_phase("gate_turnaround", offset_seconds=160.0 + (15.0 * branch_index)),
                    end_step_exclusive=_mission_step_for_phase("gate_turnaround", offset_seconds=220.0 + (15.0 * branch_index)),
                    context={"violation_type": "phase_context_violation", "anomaly_rate": 1.0},
                    metadata=_misbehavior_window_metadata(
                        misbehavior_window_id=f"MBW_COUPLING_PHASE_{branch_code}",
                        fault_window_id=f"FW_COUPLING_PHASE_{branch_code}",
                        fault_family_label="coupling",
                        benchmark_recoverability_target="subsystem_recoverable",
                    ),
                ),
            )
        )
    return build_misbehavior_program_spec(
        windows=tuple(windows),
        metadata={
            "misbehavior_program_name": "power_pressurization_hierarchy_realistic",
            "allowed_coupling_misbehaviors": _REALISTIC_ALLOWED_COUPLING_MISBEHAVIORS,
        },
    )


def _build_realistic_validation_expectations(*, branch_count: int) -> dict[str, object]:
    expected_lag_edges = []
    expected_fused_edges = []
    expected_coupling_signatures = []
    for branch_index in range(branch_count):
        suffix = "" if branch_index == 0 else f"_b{branch_index + 1}"
        expected_lag_edges.extend(
            (
                {"parameter_name_u": f"outflow_cmd_pct{suffix}", "parameter_name_v": f"actuator_position_pct{suffix}"},
                {"parameter_name_u": f"actuator_position_pct{suffix}", "parameter_name_v": f"cabin_altitude_ft{suffix}"},
                {"parameter_name_u": f"compressor_speed_pct{suffix}", "parameter_name_v": f"bleed_supply_psi{suffix}"},
            )
        )
        expected_fused_edges.extend(
            (
                {"parameter_name_u": f"bus_current_a{suffix}", "parameter_name_v": f"electrical_load_pct{suffix}"},
                {"parameter_name_u": f"bleed_supply_psi{suffix}", "parameter_name_v": f"pack_flow_rate_pct{suffix}"},
                {"parameter_name_u": f"cabin_altitude_ft{suffix}", "parameter_name_v": f"cabin_delta_p_psi{suffix}"},
            )
        )
        expected_coupling_signatures.extend(
            (
                {
                    "coupling_id": _branch_coupling_id("MOD_PRESS_CTRL:outflow_cmd_out:drive:MOD_OUTFLOW_ACT:outflow_cmd_in", branch_index),
                    "parameter_name_u": f"outflow_cmd_pct{suffix}",
                    "parameter_name_v": f"actuator_position_pct{suffix}",
                    "signature_type": "lag_shift",
                },
                {
                    "coupling_id": _branch_coupling_id("MOD_BLEED_SUPPLY:bleed_psi_out:drive:MOD_PACK_FLOW:bleed_psi_in", branch_index),
                    "parameter_name_u": f"bleed_supply_psi{suffix}",
                    "parameter_name_v": f"pack_flow_rate_pct{suffix}",
                    "signature_type": "structure_change",
                },
            )
        )
    return {
        "expected_lag_edges": tuple(expected_lag_edges),
        "expected_fused_edges": tuple(expected_fused_edges),
        "expected_coupling_signatures": tuple(expected_coupling_signatures),
    }


def _build_realistic_power_pressurization_hierarchy_flight_spec(*, scale: str) -> FlightSpec:
    try:
        branch_count = int(_REALISTIC_BRANCH_COUNT_BY_SCALE[str(scale)])
    except KeyError as exc:
        raise ValueError(f"unsupported realistic flight scale {scale!r}") from exc

    legacy = _LEGACY_BUILD_POWER_PRESSURIZATION_HIERARCHY_COMPOSITE_FLIGHT_SPEC()
    aircraft_builder = {
        "smoke": build_power_pressurization_hierarchy_smoke_aircraft_spec,
        "medium": build_power_pressurization_hierarchy_medium_aircraft_spec,
        "composite": build_power_pressurization_hierarchy_composite_aircraft_spec,
    }[str(scale)]
    flight_name = f"power_pressurization_hierarchy_{scale}"
    total_steps = _realistic_total_steps()

    return FlightSpec(
        aircraft_spec=aircraft_builder(),
        input_program_spec=InputProgramSpec(
            steps=_build_realistic_steps(branch_count=branch_count),
            hold_last_step=False,
            metadata={
                "default_dt_seconds": _REALISTIC_DT_SECONDS,
                "recommended_n_steps": total_steps,
                "mission_duration_seconds": total_steps * _REALISTIC_DT_SECONDS,
            },
        ),
        initial_state_spec=InitialStateSpec(
            values_by_module=_clone_branch_initial_state(
                legacy.initial_state_spec.values_by_module,
                branch_count=branch_count,
            ),
            metadata={
                "scale": str(scale),
                "branch_count": branch_count,
            },
        ),
        phase_program_spec=_clone_phase_program(legacy.phase_program_spec, branch_count=branch_count),
        misbehavior_program_spec=_build_realistic_misbehavior_program(branch_count=branch_count),
        metadata={
            "flight_name": flight_name,
            "scale": str(scale),
            "branch_count": branch_count,
            "simulation_defaults": {
                "n_steps": total_steps,
                "dt_seconds": _REALISTIC_DT_SECONDS,
            },
            "validation": _build_realistic_validation_expectations(branch_count=branch_count),
        },
    )


def build_power_pressurization_hierarchy_smoke_flight_spec(*, seed: int | None = None) -> FlightSpec:
    return build_power_pressurization_flight_spec(scale="smoke", seed=seed)


def build_power_pressurization_hierarchy_medium_flight_spec(*, seed: int | None = None) -> FlightSpec:
    return build_power_pressurization_flight_spec(scale="medium", seed=seed)


def build_power_pressurization_hierarchy_composite_flight_spec(*, seed: int | None = None) -> FlightSpec:
    return build_power_pressurization_flight_spec(scale="composite", seed=seed)


def build_power_pressurization_hierarchy_composite_module_localization_flight_spec(
    *,
    seed: int | None = None,
) -> FlightSpec:
    return build_power_pressurization_flight_spec(
        scale="composite",
        seed=seed,
        benchmark_recoverability_targets=("module_recoverable",),
        benchmark_suite_name="module_localization",
    )


def build_power_pressurization_hierarchy_composite_subsystem_localization_flight_spec(
    *,
    seed: int | None = None,
) -> FlightSpec:
    return build_power_pressurization_flight_spec(
        scale="composite",
        seed=seed,
        benchmark_recoverability_targets=("subsystem_recoverable",),
        benchmark_suite_name="subsystem_localization",
    )


def build_power_pressurization_hierarchy_smoke_localization_focus_flight_spec(
    *,
    seed: int | None = None,
) -> FlightSpec:
    return build_power_pressurization_localization_focus_flight_spec(seed=seed)


def build_power_pressurization_hierarchy_smoke_localization_focus_bias_drift_flight_spec(
    *,
    seed: int | None = None,
) -> FlightSpec:
    return build_power_pressurization_localization_focus_flight_spec(
        seed=seed,
        benchmark_fault_types=("bias", "drift"),
        benchmark_suite_name="localization_focus_bias_drift",
        flight_name="power_pressurization_hierarchy_smoke_localization_focus_bias_drift",
    )


def build_power_pressurization_hierarchy_smoke_localization_focus_bias_load_monitor_flight_spec(
    *,
    seed: int | None = None,
) -> FlightSpec:
    return build_power_pressurization_localization_focus_flight_spec(
        seed=seed,
        benchmark_fault_types=("bias",),
        benchmark_suite_name="localization_focus_bias_load_monitor",
        flight_name="power_pressurization_hierarchy_smoke_localization_focus_bias_load_monitor",
        bias_variant="load_monitor_local",
    )


def build_power_pressurization_hierarchy_smoke_localization_focus_saturation_flight_spec(
    *,
    seed: int | None = None,
) -> FlightSpec:
    return build_power_pressurization_localization_focus_flight_spec(
        seed=seed,
        benchmark_fault_types=("saturation",),
        benchmark_suite_name="localization_focus_saturation",
        flight_name="power_pressurization_hierarchy_smoke_localization_focus_saturation",
        benchmark_fault_target_overrides={"saturation": "parameter_visible_only"},
    )


def build_power_pressurization_hierarchy_smoke_localization_focus_saturation_local_flight_spec(
    *,
    seed: int | None = None,
) -> FlightSpec:
    return build_power_pressurization_localization_focus_flight_spec(
        seed=seed,
        benchmark_fault_types=("saturation",),
        benchmark_suite_name="localization_focus_saturation_local",
        flight_name="power_pressurization_hierarchy_smoke_localization_focus_saturation_local",
        saturation_variant="pack_temp_local",
        benchmark_fault_target_overrides={"saturation": "detection_only"},
    )


def get_flight_builders() -> dict[str, FlightBuilder]:
    return {
        "coupled_module": build_coupled_module_flight_spec,
        "power_chain": build_power_chain_flight_spec,
        "power_pressurization_hierarchy_smoke": build_power_pressurization_hierarchy_smoke_flight_spec,
        "power_pressurization_hierarchy_smoke_localization_focus": (
            build_power_pressurization_hierarchy_smoke_localization_focus_flight_spec
        ),
        "power_pressurization_hierarchy_smoke_localization_focus_bias_drift": (
            build_power_pressurization_hierarchy_smoke_localization_focus_bias_drift_flight_spec
        ),
        "power_pressurization_hierarchy_smoke_localization_focus_bias_load_monitor": (
            build_power_pressurization_hierarchy_smoke_localization_focus_bias_load_monitor_flight_spec
        ),
        "power_pressurization_hierarchy_smoke_localization_focus_saturation": (
            build_power_pressurization_hierarchy_smoke_localization_focus_saturation_flight_spec
        ),
        "power_pressurization_hierarchy_smoke_localization_focus_saturation_local": (
            build_power_pressurization_hierarchy_smoke_localization_focus_saturation_local_flight_spec
        ),
        "power_pressurization_hierarchy_medium": build_power_pressurization_hierarchy_medium_flight_spec,
        "power_pressurization_hierarchy_composite": build_power_pressurization_hierarchy_composite_flight_spec,
        "power_pressurization_hierarchy_composite_module_localization": (
            build_power_pressurization_hierarchy_composite_module_localization_flight_spec
        ),
        "power_pressurization_hierarchy_composite_subsystem_localization": (
            build_power_pressurization_hierarchy_composite_subsystem_localization_flight_spec
        ),
        "pressurization": build_pressurization_flight_spec,
    }


def list_flight_names() -> tuple[str, ...]:
    return tuple(sorted(get_flight_builders()))


def build_named_flight_spec(flight_name: str, *, seed: int | None = None) -> FlightSpec:
    hierarchy_builders = {
        "power_pressurization_hierarchy_smoke": build_power_pressurization_hierarchy_smoke_flight_spec,
        "power_pressurization_hierarchy_smoke_localization_focus": (
            build_power_pressurization_hierarchy_smoke_localization_focus_flight_spec
        ),
        "power_pressurization_hierarchy_smoke_localization_focus_bias_drift": (
            build_power_pressurization_hierarchy_smoke_localization_focus_bias_drift_flight_spec
        ),
        "power_pressurization_hierarchy_smoke_localization_focus_bias_load_monitor": (
            build_power_pressurization_hierarchy_smoke_localization_focus_bias_load_monitor_flight_spec
        ),
        "power_pressurization_hierarchy_smoke_localization_focus_saturation": (
            build_power_pressurization_hierarchy_smoke_localization_focus_saturation_flight_spec
        ),
        "power_pressurization_hierarchy_smoke_localization_focus_saturation_local": (
            build_power_pressurization_hierarchy_smoke_localization_focus_saturation_local_flight_spec
        ),
        "power_pressurization_hierarchy_medium": build_power_pressurization_hierarchy_medium_flight_spec,
        "power_pressurization_hierarchy_composite": build_power_pressurization_hierarchy_composite_flight_spec,
        "power_pressurization_hierarchy_composite_module_localization": (
            build_power_pressurization_hierarchy_composite_module_localization_flight_spec
        ),
        "power_pressurization_hierarchy_composite_subsystem_localization": (
            build_power_pressurization_hierarchy_composite_subsystem_localization_flight_spec
        ),
    }
    if str(flight_name) in hierarchy_builders:
        return hierarchy_builders[str(flight_name)](seed=seed)
    builders = get_flight_builders()
    try:
        return builders[str(flight_name)]()
    except KeyError as exc:
        raise ValueError(f"unknown flight {flight_name!r}; expected one of {sorted(builders)}") from exc
