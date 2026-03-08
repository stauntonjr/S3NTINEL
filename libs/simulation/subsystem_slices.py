"""Domain-shaped native subsystem slices authored against the V2.1 simulation seam."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from collections.abc import Callable

import pandas as pd

from libs.behavior import BehaviorStepInput
from libs.simulation.assembly import HierarchyAssemblyBuilder
from libs.simulation.example_builders import (
    build_discrete_output_module,
    build_single_parameter_module,
    drive_coupling,
    input_port,
    latent_update_from_input_port,
    module_parameter,
    output_port,
)
from libs.simulation.example_runtime import (
    NativeSimulationExampleContext,
    build_example_context,
    simulate_example,
)
from libs.simulation.specs import HierarchyAssemblySpec, ModuleSpec


NativeSubsystemSliceBuilder = Callable[[], HierarchyAssemblySpec]


@dataclass(frozen=True)
class NativeSubsystemSliceScenario:
    assembly_spec: HierarchyAssemblySpec
    build_step_inputs_by_module: Callable[[int, float], dict[str, dict[str, BehaviorStepInput]]]
    build_initial_state_by_module: Callable[[], dict[str, dict[str, object]]]
    phase_label_for_step: Callable[[int], str | None]


def build_native_multibehavior_example() -> HierarchyAssemblySpec:
    builder = HierarchyAssemblyBuilder(metadata={"example_name": "native_multibehavior"})

    switch_module = build_discrete_output_module(
        module_id="MOD_SWITCH",
        subsystem_id="SUB_POWER",
        system_id="SYS_POWER",
        module_family="switch",
        parameter_name="contactor_state",
        output_port_name="switch_out",
        parameter_datatype_label="binary",
        metadata={"example_role": "switch"},
    )

    source_module = ModuleSpec(
        module_id="MOD_SOURCE",
        subsystem_id="SUB_POWER",
        system_id="SYS_POWER",
        module_family="power_source",
        parameters=(
            module_parameter(
                parameter_name="supply_voltage",
                system_id="SYS_POWER",
                subsystem_id="SUB_POWER",
                module_id="MOD_SOURCE",
                parameter_datatype_label="numeric",
                behavior_family_label="regulated",
                input_port_names=("enable_in",),
                output_port_name="voltage_out",
                metadata={"example_role": "source_voltage"},
            ),
            module_parameter(
                parameter_name="fuel_flow_rate",
                system_id="SYS_POWER",
                subsystem_id="SUB_POWER",
                module_id="MOD_SOURCE",
                parameter_datatype_label="numeric",
                behavior_family_label="regulated",
                input_port_names=("enable_in",),
                output_port_name="flow_out",
                metadata={"example_role": "source_flow"},
            ),
        ),
        input_ports=(input_port(port_name="enable_in", value_datatype_label="numeric", unit="state"),),
        output_ports=(
            output_port(port_name="voltage_out", value_datatype_label="numeric", unit="V"),
            output_port(port_name="flow_out", value_datatype_label="numeric", unit="kg_s"),
        ),
        latent_variables=("setpoint_voltage", "setpoint_flow"),
        latent_update_specs=(
            latent_update_from_input_port(
                latent_name="setpoint_voltage",
                source_name="enable_in",
                gain=28.0,
                sign=1,
                default_value=0.0,
                clamp_min=0.0,
            ),
            latent_update_from_input_port(
                latent_name="setpoint_flow",
                source_name="enable_in",
                gain=2.0,
                sign=1,
                default_value=0.0,
                clamp_min=0.0,
            ),
        ),
    )

    target_module = build_single_parameter_module(
        module_id="MOD_TARGET",
        subsystem_id="SUB_LOAD",
        system_id="SYS_POWER",
        module_family="driven_load",
        parameter_spec=module_parameter(
            parameter_name="motor_speed",
            system_id="SYS_POWER",
            subsystem_id="SUB_LOAD",
            module_id="MOD_TARGET",
            parameter_datatype_label="numeric",
            behavior_family_label="inertial",
            input_port_names=("command_in",),
            metadata={"example_role": "target_speed"},
        ),
        input_ports=(input_port(port_name="command_in", value_datatype_label="numeric", unit="V"),),
        latent_variables=("command_target",),
        latent_update_specs=(
            latent_update_from_input_port(
                latent_name="command_target",
                source_name="command_in",
                gain=1.0,
                sign=1,
                default_value=0.0,
            ),
        ),
    )

    tank_module = build_single_parameter_module(
        module_id="MOD_TANK",
        subsystem_id="SUB_FUEL",
        system_id="SYS_FUEL",
        module_family="accumulator",
        parameter_spec=module_parameter(
            parameter_name="fuel_used_total",
            system_id="SYS_FUEL",
            subsystem_id="SUB_FUEL",
            module_id="MOD_TANK",
            parameter_datatype_label="numeric",
            behavior_family_label="accumulative",
            input_port_names=("flow_in",),
            metadata={"example_role": "fuel_total"},
        ),
        input_ports=(input_port(port_name="flow_in", value_datatype_label="numeric", unit="kg_s"),),
        latent_variables=("flow_rate",),
        latent_update_specs=(
            latent_update_from_input_port(
                latent_name="flow_rate",
                source_name="flow_in",
                gain=1.0,
                sign=1,
                default_value=0.0,
                clamp_min=0.0,
            ),
        ),
    )

    builder.add_module(switch_module)
    builder.add_module(source_module)
    builder.add_module(target_module)
    builder.add_module(tank_module)
    builder.add_inter_module_coupling(
        drive_coupling(
            source_module_id="MOD_SWITCH",
            source_port_name="switch_out",
            target_module_id="MOD_SOURCE",
            target_port_name="enable_in",
            gain=1.0,
        )
    )
    builder.add_inter_module_coupling(
        drive_coupling(
            source_module_id="MOD_SOURCE",
            source_port_name="voltage_out",
            target_module_id="MOD_TARGET",
            target_port_name="command_in",
            gain=0.5,
        )
    )
    builder.add_inter_module_coupling(
        drive_coupling(
            source_module_id="MOD_SOURCE",
            source_port_name="flow_out",
            target_module_id="MOD_TANK",
            target_port_name="flow_in",
            gain=1.0,
        )
    )
    return builder.build()


def build_native_multibehavior_example_context() -> NativeSimulationExampleContext:
    return build_example_context(build_native_multibehavior_example())


def build_native_multibehavior_scenario() -> NativeSubsystemSliceScenario:
    switch_states = (0, 0, 1, 1, 1, 1, 0, 0)
    return NativeSubsystemSliceScenario(
        assembly_spec=build_native_multibehavior_example(),
        build_step_inputs_by_module=lambda step_index, resolved_dt_seconds: {
            "MOD_SWITCH": {
                "contactor_state": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={"target_state": switch_states[min(step_index, len(switch_states) - 1)]},
                )
            },
            "MOD_SOURCE": {
                "supply_voltage": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={"target_value": 0.0, "latent_target_name": "setpoint_voltage", "reversion_rate": 1.5},
                ),
                "fuel_flow_rate": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={"target_value": 0.0, "latent_target_name": "setpoint_flow", "reversion_rate": 1.0},
                ),
            },
            "MOD_TARGET": {
                "motor_speed": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={
                        "target_value": 0.0,
                        "latent_target_name": "command_target",
                        "time_constant_seconds": 2.0,
                    },
                )
            },
            "MOD_TANK": {
                "fuel_used_total": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={"target_value": 0.0, "latent_target_name": "flow_rate"},
                )
            },
        },
        build_initial_state_by_module=lambda: {
            "MOD_SWITCH": {"contactor_state": 0},
            "MOD_SOURCE": {"supply_voltage": 0.0, "fuel_flow_rate": 0.0},
            "MOD_TARGET": {"motor_speed": 0.0},
            "MOD_TANK": {"fuel_used_total": 0.0},
        },
        phase_label_for_step=lambda _step_index: "takeoff_climb",
    )


def simulate_native_multibehavior_example(
    *,
    n_steps: int = 6,
    dt_seconds: float = 1.0,
    start_timestamp_utc: datetime | None = None,
) -> pd.DataFrame:
    scenario = build_native_multibehavior_scenario()
    context = build_example_context(scenario.assembly_spec)
    return simulate_example(
        context=context,
        n_steps=n_steps,
        dt_seconds=dt_seconds,
        start_timestamp_utc=start_timestamp_utc,
        build_step_inputs_by_module=scenario.build_step_inputs_by_module,
        build_initial_state_by_module=scenario.build_initial_state_by_module,
        phase_label_for_step=scenario.phase_label_for_step,
    )


def build_native_pressurization_example() -> HierarchyAssemblySpec:
    builder = HierarchyAssemblyBuilder(metadata={"example_name": "native_pressurization"})

    mode_module = build_discrete_output_module(
        module_id="MOD_PRESS_MODE",
        subsystem_id="SUB_ECS",
        system_id="SYS_ECS",
        module_family="pressurization_mode",
        parameter_name="press_mode_state",
        output_port_name="press_mode_out",
        parameter_datatype_label="categorical",
        metadata={"example_role": "press_mode"},
    )

    altitude_module = build_single_parameter_module(
        module_id="MOD_AIRCRAFT_ALT",
        subsystem_id="SUB_AIRFRAME",
        system_id="SYS_AIRFRAME",
        module_family="aircraft_altitude_source",
        parameter_spec=module_parameter(
            parameter_name="aircraft_altitude_ft",
            system_id="SYS_AIRFRAME",
            subsystem_id="SUB_AIRFRAME",
            module_id="MOD_AIRCRAFT_ALT",
            parameter_datatype_label="numeric",
            behavior_family_label="inertial",
            output_port_name="aircraft_altitude_out",
            metadata={"example_role": "aircraft_altitude"},
        ),
        output_ports=(output_port(port_name="aircraft_altitude_out", value_datatype_label="numeric", unit="ft"),),
    )

    controller_module = build_single_parameter_module(
        module_id="MOD_PRESS_CTRL",
        subsystem_id="SUB_ECS",
        system_id="SYS_ECS",
        module_family="pressurization_controller",
        parameter_spec=module_parameter(
            parameter_name="outflow_valve_pct",
            system_id="SYS_ECS",
            subsystem_id="SUB_ECS",
            module_id="MOD_PRESS_CTRL",
            parameter_datatype_label="numeric",
            behavior_family_label="regulated",
            input_port_names=("aircraft_altitude_in", "press_mode_in"),
            output_port_name="outflow_cmd_out",
            metadata={"example_role": "outflow_valve"},
        ),
        input_ports=(
            input_port(port_name="aircraft_altitude_in", value_datatype_label="numeric", unit="ft"),
            input_port(port_name="press_mode_in", value_datatype_label="categorical", unit="state"),
        ),
        output_ports=(output_port(port_name="outflow_cmd_out", value_datatype_label="numeric", unit="pct"),),
        latent_variables=("outflow_setpoint",),
        latent_update_specs=(
            latent_update_from_input_port(
                latent_name="outflow_setpoint",
                source_name="aircraft_altitude_in",
                gain=0.01,
                sign=1,
                default_value=0.0,
                clamp_min=0.0,
                clamp_max=100.0,
            ),
        ),
    )

    cabin_module = ModuleSpec(
        module_id="MOD_CABIN",
        subsystem_id="SUB_ECS",
        system_id="SYS_ECS",
        module_family="cabin_response",
        parameters=(
            module_parameter(
                parameter_name="cabin_altitude_ft",
                system_id="SYS_ECS",
                subsystem_id="SUB_ECS",
                module_id="MOD_CABIN",
                parameter_datatype_label="numeric",
                behavior_family_label="inertial",
                input_port_names=("outflow_cmd_in",),
                metadata={"example_role": "cabin_altitude"},
            ),
            module_parameter(
                parameter_name="cabin_delta_p_psi",
                system_id="SYS_ECS",
                subsystem_id="SUB_ECS",
                module_id="MOD_CABIN",
                parameter_datatype_label="numeric",
                behavior_family_label="regulated",
                input_port_names=("outflow_cmd_in",),
                metadata={"example_role": "delta_p"},
            ),
        ),
        input_ports=(input_port(port_name="outflow_cmd_in", value_datatype_label="numeric", unit="pct"),),
        latent_variables=("cabin_alt_target", "delta_p_target"),
        latent_update_specs=(
            latent_update_from_input_port(
                latent_name="cabin_alt_target",
                source_name="outflow_cmd_in",
                gain=80.0,
                sign=1,
                default_value=0.0,
                clamp_min=0.0,
                clamp_max=8000.0,
            ),
            latent_update_from_input_port(
                latent_name="delta_p_target",
                source_name="outflow_cmd_in",
                gain=0.08,
                sign=1,
                default_value=0.0,
                clamp_min=0.0,
                clamp_max=8.5,
            ),
        ),
    )

    builder.add_module(mode_module)
    builder.add_module(altitude_module)
    builder.add_module(controller_module)
    builder.add_module(cabin_module)
    builder.add_inter_module_coupling(
        drive_coupling(
            source_module_id="MOD_PRESS_MODE",
            source_port_name="press_mode_out",
            target_module_id="MOD_PRESS_CTRL",
            target_port_name="press_mode_in",
        )
    )
    builder.add_inter_module_coupling(
        drive_coupling(
            source_module_id="MOD_AIRCRAFT_ALT",
            source_port_name="aircraft_altitude_out",
            target_module_id="MOD_PRESS_CTRL",
            target_port_name="aircraft_altitude_in",
        )
    )
    builder.add_inter_module_coupling(
        drive_coupling(
            source_module_id="MOD_PRESS_CTRL",
            source_port_name="outflow_cmd_out",
            target_module_id="MOD_CABIN",
            target_port_name="outflow_cmd_in",
            gain=1.0,
            lag_seconds=1.0,
        )
    )
    return builder.build()


def build_native_pressurization_example_context() -> NativeSimulationExampleContext:
    return build_example_context(build_native_pressurization_example())


def build_native_pressurization_scenario() -> NativeSubsystemSliceScenario:
    mode_sequence = ("GROUND", "GROUND", "AUTO", "AUTO", "AUTO", "AUTO", "AUTO", "AUTO")
    altitude_sequence = (0.0, 500.0, 1500.0, 3000.0, 5000.0, 6500.0, 8000.0, 8000.0)
    return NativeSubsystemSliceScenario(
        assembly_spec=build_native_pressurization_example(),
        build_step_inputs_by_module=lambda step_index, resolved_dt_seconds: {
            "MOD_PRESS_MODE": {
                "press_mode_state": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={"target_state": mode_sequence[min(step_index, len(mode_sequence) - 1)]},
                )
            },
            "MOD_AIRCRAFT_ALT": {
                "aircraft_altitude_ft": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={
                        "target_value": altitude_sequence[min(step_index, len(altitude_sequence) - 1)],
                        "time_constant_seconds": 2.0,
                    },
                )
            },
            "MOD_PRESS_CTRL": {
                "outflow_valve_pct": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={
                        "target_value": 0.0,
                        "latent_target_name": "outflow_setpoint",
                        "reversion_rate": 1.2,
                    },
                )
            },
            "MOD_CABIN": {
                "cabin_altitude_ft": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={
                        "target_value": 0.0,
                        "latent_target_name": "cabin_alt_target",
                        "time_constant_seconds": 3.0,
                    },
                ),
                "cabin_delta_p_psi": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={
                        "target_value": 0.0,
                        "latent_target_name": "delta_p_target",
                        "reversion_rate": 1.5,
                    },
                ),
            },
        },
        build_initial_state_by_module=lambda: {
            "MOD_PRESS_MODE": {"press_mode_state": "GROUND"},
            "MOD_AIRCRAFT_ALT": {"aircraft_altitude_ft": 0.0},
            "MOD_PRESS_CTRL": {"outflow_valve_pct": 0.0},
            "MOD_CABIN": {"cabin_altitude_ft": 0.0, "cabin_delta_p_psi": 0.0},
        },
        phase_label_for_step=lambda step_index: "climb" if step_index >= 2 else "ground",
    )


def simulate_native_pressurization_example(
    *,
    n_steps: int = 8,
    dt_seconds: float = 1.0,
    start_timestamp_utc: datetime | None = None,
) -> pd.DataFrame:
    scenario = build_native_pressurization_scenario()
    context = build_example_context(scenario.assembly_spec)
    return simulate_example(
        context=context,
        n_steps=n_steps,
        dt_seconds=dt_seconds,
        start_timestamp_utc=start_timestamp_utc,
        build_step_inputs_by_module=scenario.build_step_inputs_by_module,
        build_initial_state_by_module=scenario.build_initial_state_by_module,
        phase_label_for_step=scenario.phase_label_for_step,
    )


def get_native_subsystem_slice_builders() -> dict[str, NativeSubsystemSliceBuilder]:
    return {
        "power_chain": build_native_multibehavior_example,
        "pressurization": build_native_pressurization_example,
    }


def list_native_subsystem_slice_names() -> tuple[str, ...]:
    return tuple(sorted(get_native_subsystem_slice_builders()))


def build_native_subsystem_slice(slice_name: str) -> HierarchyAssemblySpec:
    builders = get_native_subsystem_slice_builders()
    try:
        return builders[str(slice_name)]()
    except KeyError as exc:
        raise ValueError(
            f"unknown native subsystem slice {slice_name!r}; expected one of {sorted(builders)}"
        ) from exc


def build_native_subsystem_slice_scenario(slice_name: str) -> NativeSubsystemSliceScenario:
    name = str(slice_name)
    if name == "power_chain":
        return build_native_multibehavior_scenario()
    if name == "pressurization":
        return build_native_pressurization_scenario()
    raise ValueError(
        f"unknown native subsystem slice {slice_name!r}; expected one of {list_native_subsystem_slice_names()}"
    )
