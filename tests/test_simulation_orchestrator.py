from __future__ import annotations

from datetime import datetime, timezone

import pytest

from libs.behavior import BehaviorStepInput, build_default_behavior_registry
from libs.simulation import (
    InterModuleCouplingSpec,
    ModuleRuntime,
    ModuleSpec,
    ParameterSpec,
    PortSpec,
    build_assembly_tick_request,
    bind_module_behaviors,
    step_assembly_once,
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


def test_step_assembly_once_runs_modules_in_order_and_injects_propagated_ports():
    source_spec = _build_source_module_spec()
    target_spec = _build_target_module_spec()
    source_runtime = ModuleRuntime.from_spec(source_spec)
    target_runtime = ModuleRuntime.from_spec(target_spec)
    registry = build_default_behavior_registry()
    timestamp_utc = datetime(2025, 1, 1, tzinfo=timezone.utc)

    samples_by_module = step_assembly_once(
        build_assembly_tick_request(
            module_bindings_by_id={
                "MOD_SOURCE": bind_module_behaviors(source_spec, registry),
                "MOD_TARGET": bind_module_behaviors(target_spec, registry),
            },
            module_runtimes_by_id={
                "MOD_SOURCE": source_runtime,
                "MOD_TARGET": target_runtime,
            },
            step_inputs_by_module={
                "MOD_SOURCE": {
                    "bus_voltage": BehaviorStepInput(
                        dt_seconds=1.0,
                        latent_state={},
                        context={"target_value": 28.0},
                    ),
                },
                "MOD_TARGET": {
                    "spool_speed": BehaviorStepInput(
                        dt_seconds=1.0,
                        latent_state={},
                        context={"target_value": 0.0, "time_constant_seconds": 1.0},
                    ),
                },
            },
            module_order=("MOD_SOURCE", "MOD_TARGET"),
            inter_module_couplings=(
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
            initial_state_by_module={
                "MOD_SOURCE": {"bus_voltage": 27.5},
                "MOD_TARGET": {"spool_speed": 0.0},
            },
            timestamp_utc=timestamp_utc,
            current_phase_label="takeoff_climb",
        )
    )

    assert set(samples_by_module) == {"MOD_SOURCE", "MOD_TARGET"}
    assert target_runtime.input_port_runtime("command_in").current_value == 14.0
    target_sample = samples_by_module["MOD_TARGET"][0]
    assert target_sample.metadata["command_in"] == 14.0


def test_step_assembly_once_rejects_missing_ordered_module_binding():
    with pytest.raises(KeyError, match="missing module binding"):
        step_assembly_once(
            build_assembly_tick_request(
                module_bindings_by_id={},
                module_runtimes_by_id={},
                step_inputs_by_module={},
                module_order=("MOD_MISSING",),
            )
        )


def test_step_assembly_once_skips_gated_coupling_when_phase_does_not_match():
    source_spec = _build_source_module_spec()
    target_spec = _build_target_module_spec()
    source_runtime = ModuleRuntime.from_spec(source_spec)
    target_runtime = ModuleRuntime.from_spec(target_spec)
    registry = build_default_behavior_registry()

    samples_by_module = step_assembly_once(
        build_assembly_tick_request(
            module_bindings_by_id={
                "MOD_SOURCE": bind_module_behaviors(source_spec, registry),
                "MOD_TARGET": bind_module_behaviors(target_spec, registry),
            },
            module_runtimes_by_id={
                "MOD_SOURCE": source_runtime,
                "MOD_TARGET": target_runtime,
            },
            step_inputs_by_module={
                "MOD_SOURCE": {
                    "bus_voltage": BehaviorStepInput(
                        dt_seconds=1.0,
                        latent_state={},
                        context={"target_value": 28.0},
                    ),
                },
                "MOD_TARGET": {
                    "spool_speed": BehaviorStepInput(
                        dt_seconds=1.0,
                        latent_state={},
                        context={"target_value": 0.0, "time_constant_seconds": 1.0},
                    ),
                },
            },
            module_order=("MOD_SOURCE", "MOD_TARGET"),
            inter_module_couplings=(
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
            initial_state_by_module={
                "MOD_SOURCE": {"bus_voltage": 27.5},
                "MOD_TARGET": {"spool_speed": 0.0},
            },
            current_phase_label="taxi_out",
        )
    )

    assert target_runtime.input_port_runtime("command_in").current_value is None
    assert "command_in" not in samples_by_module["MOD_TARGET"][0].metadata


def test_step_assembly_once_applies_lagged_coupling_on_following_tick():
    source_spec = _build_source_module_spec()
    target_spec = _build_target_module_spec()
    source_runtime = ModuleRuntime.from_spec(source_spec)
    target_runtime = ModuleRuntime.from_spec(target_spec)
    registry = build_default_behavior_registry()
    t0 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2025, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
    coupling = InterModuleCouplingSpec(
        source_module_id="MOD_SOURCE",
        source_port_name="voltage_out",
        target_module_id="MOD_TARGET",
        target_port_name="command_in",
        relation_type="drive",
        gain=0.5,
        lag_seconds=1.0,
    )

    first_samples = step_assembly_once(
        build_assembly_tick_request(
            module_bindings_by_id={
                "MOD_SOURCE": bind_module_behaviors(source_spec, registry),
                "MOD_TARGET": bind_module_behaviors(target_spec, registry),
            },
            module_runtimes_by_id={
                "MOD_SOURCE": source_runtime,
                "MOD_TARGET": target_runtime,
            },
            step_inputs_by_module={
                "MOD_SOURCE": {
                    "bus_voltage": BehaviorStepInput(
                        dt_seconds=1.0,
                        latent_state={},
                        context={"target_value": 28.0},
                    ),
                },
                "MOD_TARGET": {
                    "spool_speed": BehaviorStepInput(
                        dt_seconds=1.0,
                        latent_state={},
                        context={"target_value": 0.0, "time_constant_seconds": 1.0},
                    ),
                },
            },
            module_order=("MOD_SOURCE", "MOD_TARGET"),
            inter_module_couplings=(coupling,),
            initial_state_by_module={
                "MOD_SOURCE": {"bus_voltage": 27.5},
                "MOD_TARGET": {"spool_speed": 0.0},
            },
            timestamp_utc=t0,
            current_phase_label="takeoff_climb",
        )
    )

    assert target_runtime.input_port_runtime("command_in").current_value is None
    assert "command_in" not in first_samples["MOD_TARGET"][0].metadata

    second_samples = step_assembly_once(
        build_assembly_tick_request(
            module_bindings_by_id={
                "MOD_SOURCE": bind_module_behaviors(source_spec, registry),
                "MOD_TARGET": bind_module_behaviors(target_spec, registry),
            },
            module_runtimes_by_id={
                "MOD_SOURCE": source_runtime,
                "MOD_TARGET": target_runtime,
            },
            step_inputs_by_module={
                "MOD_SOURCE": {
                    "bus_voltage": BehaviorStepInput(
                        dt_seconds=1.0,
                        latent_state={},
                        context={"target_value": 28.0},
                    ),
                },
                "MOD_TARGET": {
                    "spool_speed": BehaviorStepInput(
                        dt_seconds=1.0,
                        latent_state={},
                        context={"target_value": 0.0, "time_constant_seconds": 1.0},
                    ),
                },
            },
            module_order=("MOD_SOURCE", "MOD_TARGET"),
            inter_module_couplings=(coupling,),
            timestamp_utc=t1,
            current_phase_label="takeoff_climb",
        )
    )

    assert target_runtime.input_port_runtime("command_in").current_value == 14.0
    assert second_samples["MOD_TARGET"][0].metadata["command_in"] == 14.0


def test_step_assembly_once_supplies_default_tick_input_for_missing_parameter():
    source_spec = _build_source_module_spec()
    target_spec = _build_target_module_spec()
    source_runtime = ModuleRuntime.from_spec(source_spec)
    target_runtime = ModuleRuntime.from_spec(target_spec)
    registry = build_default_behavior_registry()

    samples_by_module = step_assembly_once(
        build_assembly_tick_request(
            module_bindings_by_id={
                "MOD_SOURCE": bind_module_behaviors(source_spec, registry),
                "MOD_TARGET": bind_module_behaviors(target_spec, registry),
            },
            module_runtimes_by_id={
                "MOD_SOURCE": source_runtime,
                "MOD_TARGET": target_runtime,
            },
            step_inputs_by_module={
                "MOD_SOURCE": {
                    "bus_voltage": BehaviorStepInput(
                        dt_seconds=1.0,
                        latent_state={},
                        context={"target_value": 28.0},
                    ),
                },
                "MOD_TARGET": {},
            },
            module_order=("MOD_SOURCE", "MOD_TARGET"),
            inter_module_couplings=(
                InterModuleCouplingSpec(
                    source_module_id="MOD_SOURCE",
                    source_port_name="voltage_out",
                    target_module_id="MOD_TARGET",
                    target_port_name="command_in",
                    relation_type="drive",
                    gain=0.5,
                ),
            ),
            initial_state_by_module={
                "MOD_SOURCE": {"bus_voltage": 27.5},
                "MOD_TARGET": {"spool_speed": 0.0},
            },
        )
    )

    assert len(samples_by_module["MOD_TARGET"]) == 1
    assert samples_by_module["MOD_TARGET"][0].parameter_name == "spool_speed"
