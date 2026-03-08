"""Small generic native V2.1 simulation examples built on the current assembly seam."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from libs.behavior import BehaviorStepInput
from libs.simulation.assembly import HierarchyAssemblyBuilder
from libs.simulation.example_builders import (
    build_single_parameter_module,
    drive_coupling,
    input_port,
    latent_update_from_input_port,
    module_parameter,
    output_port,
    regulate_coupling,
)
from libs.simulation.example_runtime import (
    NativeSimulationExampleContext,
    build_example_context,
    simulate_example,
)
from libs.simulation.specs import HierarchyAssemblySpec
from libs.simulation.subsystem_slices import (
    build_native_multibehavior_example,
    build_native_multibehavior_example_context,
    build_native_multibehavior_scenario,
    build_native_pressurization_example,
    build_native_pressurization_example_context,
    build_native_pressurization_scenario,
    build_native_subsystem_slice_scenario,
    simulate_native_multibehavior_example,
    simulate_native_pressurization_example,
)


def build_native_coupled_module_example() -> HierarchyAssemblySpec:
    builder = HierarchyAssemblyBuilder(metadata={"example_name": "native_coupled_module"})

    source_module = build_single_parameter_module(
        module_id="MOD_SOURCE",
        subsystem_id="SUB_POWER",
        system_id="SYS_POWER",
        module_family="power_source",
        parameter_spec=module_parameter(
            parameter_name="supply_voltage",
            system_id="SYS_POWER",
            subsystem_id="SUB_POWER",
            module_id="MOD_SOURCE",
            parameter_datatype_label="numeric",
            behavior_family_label="regulated",
            output_port_name="voltage_out",
            metadata={"example_role": "source"},
        ),
        output_ports=(output_port(port_name="voltage_out", value_datatype_label="numeric", unit="V"),),
        coupling_edges=(regulate_coupling(source_ref="latent:setpoint", target_ref="parameter:supply_voltage"),),
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
            metadata={"example_role": "target"},
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

    builder.add_module(source_module)
    builder.add_module(target_module)
    builder.add_inter_module_coupling(
        drive_coupling(
            source_module_id="MOD_SOURCE",
            source_port_name="voltage_out",
            target_module_id="MOD_TARGET",
            target_port_name="command_in",
            gain=0.5,
        )
    )
    return builder.build()


def build_native_coupled_module_example_context() -> NativeSimulationExampleContext:
    return build_example_context(build_native_coupled_module_example())


def simulate_native_coupled_module_example(
    *,
    n_steps: int = 6,
    dt_seconds: float = 1.0,
    start_timestamp_utc: datetime | None = None,
) -> pd.DataFrame:
    context = build_native_coupled_module_example_context()
    return simulate_example(
        context=context,
        n_steps=n_steps,
        dt_seconds=dt_seconds,
        start_timestamp_utc=start_timestamp_utc,
        build_step_inputs_by_module=lambda _step_index, resolved_dt_seconds: {
            "MOD_SOURCE": {
                "supply_voltage": BehaviorStepInput(
                    dt_seconds=resolved_dt_seconds,
                    latent_state={},
                    context={"target_value": 28.0, "reversion_rate": 1.5},
                )
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
        },
        build_initial_state_by_module=lambda: {
            "MOD_SOURCE": {"supply_voltage": 27.0},
            "MOD_TARGET": {"motor_speed": 0.0},
        },
        phase_label_for_step=lambda _step_index: "takeoff_climb",
    )


__all__ = [
    "NativeSimulationExampleContext",
    "build_native_coupled_module_example",
    "build_native_coupled_module_example_context",
    "simulate_native_coupled_module_example",
    "build_native_multibehavior_example",
    "build_native_multibehavior_example_context",
    "build_native_multibehavior_scenario",
    "simulate_native_multibehavior_example",
    "build_native_pressurization_example",
    "build_native_pressurization_example_context",
    "build_native_pressurization_scenario",
    "build_native_subsystem_slice_scenario",
    "simulate_native_pressurization_example",
]
