from __future__ import annotations

from datetime import datetime, timezone

from libs.behavior import BehaviorStepInput, build_default_behavior_registry
from libs.simulation import (
    AssemblyModuleStepRequest,
    InterModuleCouplingSpec,
    ModuleRuntime,
    ModuleSpec,
    ParameterSpec,
    PortSpec,
    bind_module_behaviors,
    inject_input_ports_into_step_inputs,
    step_module_and_propagate,
)


def _build_source_module_spec() -> ModuleSpec:
    return ModuleSpec(
        module_id="MOD_SOURCE",
        subsystem_id="SUB_A",
        system_id="SYS_A",
        parameters=(
            ParameterSpec(
                parameter_name="bus_voltage",
                system_id="SYS_A",
                subsystem_id="SUB_A",
                module_id="MOD_SOURCE",
                parameter_datatype_label="numeric",
                behavior_family_label="regulated",
                output_port_name="voltage_out",
            ),
        ),
        output_ports=(PortSpec(port_name="voltage_out", direction="output", value_datatype_label="numeric"),),
    )


def _build_target_module_spec() -> ModuleSpec:
    return ModuleSpec(
        module_id="MOD_TARGET",
        subsystem_id="SUB_A",
        system_id="SYS_A",
        parameters=(
            ParameterSpec(
                parameter_name="spool_speed",
                system_id="SYS_A",
                subsystem_id="SUB_A",
                module_id="MOD_TARGET",
                parameter_datatype_label="numeric",
                behavior_family_label="inertial",
                input_port_names=("command_in",),
            ),
        ),
        input_ports=(PortSpec(port_name="command_in", direction="input", value_datatype_label="numeric"),),
    )


def test_step_module_and_propagate_updates_downstream_input_port():
    source_spec = _build_source_module_spec()
    target_spec = _build_target_module_spec()
    source_runtime = ModuleRuntime.from_spec(source_spec)
    target_runtime = ModuleRuntime.from_spec(target_spec)
    source_binding = bind_module_behaviors(source_spec, build_default_behavior_registry())
    target_binding = bind_module_behaviors(target_spec, build_default_behavior_registry())
    timestamp_utc = datetime(2025, 1, 1, tzinfo=timezone.utc)

    samples = step_module_and_propagate(
        AssemblyModuleStepRequest(
            module_binding=source_binding,
            module_runtime=source_runtime,
            module_runtimes_by_id={
                "MOD_SOURCE": source_runtime,
                "MOD_TARGET": target_runtime,
            },
            step_inputs_by_parameter={
                "bus_voltage": BehaviorStepInput(dt_seconds=1.0, latent_state={}, context={"target_value": 28.0}),
            },
            initial_state_by_parameter={"bus_voltage": 27.5},
            outgoing_inter_module_couplings=(
                InterModuleCouplingSpec(
                    source_module_id="MOD_SOURCE",
                    source_port_name="voltage_out",
                    target_module_id="MOD_TARGET",
                    target_port_name="command_in",
                    relation_type="drive",
                    gain=0.5,
                    phase_gate=("takeoff_climb",),
                ),
            ),
            timestamp_utc=timestamp_utc,
            current_phase_label="takeoff_climb",
        )
    )

    assert len(samples) == 1
    assert target_runtime.input_port_runtime("command_in").current_value == 14.0
    assert target_runtime.input_port_runtime("command_in").timestamp_utc == timestamp_utc

    injected = inject_input_ports_into_step_inputs(
        target_binding,
        target_runtime,
        {
            "spool_speed": BehaviorStepInput(dt_seconds=1.0, latent_state={}, context={"target_value": 0.0}),
        },
    )
    assert injected["spool_speed"].context["command_in"] == 14.0


def test_step_module_and_propagate_uses_runtime_state_as_implicit_next_initial_state():
    target_spec = _build_target_module_spec()
    target_runtime = ModuleRuntime.from_spec(target_spec)
    target_binding = bind_module_behaviors(target_spec, build_default_behavior_registry())
    timestamp_utc = datetime(2025, 1, 1, tzinfo=timezone.utc)

    first_samples = step_module_and_propagate(
        AssemblyModuleStepRequest(
            module_binding=target_binding,
            module_runtime=target_runtime,
            module_runtimes_by_id={"MOD_TARGET": target_runtime},
            step_inputs_by_parameter={
                "spool_speed": BehaviorStepInput(
                    dt_seconds=1.0,
                    latent_state={},
                    context={"target_value": 10.0, "time_constant_seconds": 2.0},
                )
            },
            initial_state_by_parameter={"spool_speed": 0.0},
            timestamp_utc=timestamp_utc,
        )
    )

    second_samples = step_module_and_propagate(
        AssemblyModuleStepRequest(
            module_binding=target_binding,
            module_runtime=target_runtime,
            module_runtimes_by_id={"MOD_TARGET": target_runtime},
            step_inputs_by_parameter={
                "spool_speed": BehaviorStepInput(
                    dt_seconds=1.0,
                    latent_state={},
                    context={"target_value": 10.0, "time_constant_seconds": 2.0},
                )
            },
            timestamp_utc=timestamp_utc,
        )
    )

    first_value = float(first_samples[0].parameter_value_clean)
    second_value = float(second_samples[0].parameter_value_clean)
    assert second_value > first_value
