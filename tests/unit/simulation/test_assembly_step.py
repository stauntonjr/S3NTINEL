from __future__ import annotations

from datetime import datetime, timezone

from libs.behavior import BehaviorStepInput
from libs.simulation import Coupling, Module, ModuleSpec, ParameterSpec, PortSpec


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
        output_ports=(PortSpec.output(port_name="voltage_out", value_datatype_label="numeric"),),
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
        input_ports=(PortSpec.input(port_name="command_in", value_datatype_label="numeric"),),
    )


def test_module_step_updates_downstream_input_port():
    source_module = Module.from_spec(_build_source_module_spec())
    target_module = Module.from_spec(_build_target_module_spec())
    timestamp_utc = datetime(2025, 1, 1, tzinfo=timezone.utc)

    samples = source_module.step(
        modules_by_id={"MOD_SOURCE": source_module, "MOD_TARGET": target_module},
        raw_step_inputs={
            "bus_voltage": BehaviorStepInput(dt_seconds=1.0, latent_state={}, context={"target_value": 28.0}),
        },
        initial_state_by_parameter={"bus_voltage": 27.5},
        outgoing_couplings=(
            Coupling.drive(
                source_module_id="MOD_SOURCE",
                source_port_name="voltage_out",
                target_module_id="MOD_TARGET",
                target_port_name="command_in",
                gain=0.5,
                lag_seconds=0.0,
            ),
        ),
        timestamp_utc=timestamp_utc,
        current_phase_label="takeoff_climb",
    )

    assert len(samples) == 1
    assert target_module.input_port("command_in").current_value == 14.0
    injected = target_module.hydrate_step_inputs_from_ports(
        {
            "spool_speed": BehaviorStepInput(dt_seconds=1.0, latent_state={}, context={"target_value": 0.0}),
        },
    )
    assert injected["spool_speed"].context["command_in"] == 14.0


def test_module_step_uses_prior_state_as_implicit_next_initial_state():
    target_module = Module.from_spec(_build_target_module_spec())
    timestamp_utc = datetime(2025, 1, 1, tzinfo=timezone.utc)

    first_samples = target_module.step(
        modules_by_id={"MOD_TARGET": target_module},
        raw_step_inputs={
            "spool_speed": BehaviorStepInput(
                dt_seconds=1.0,
                latent_state={},
                context={"target_value": 10.0, "time_constant_seconds": 2.0},
            )
        },
        initial_state_by_parameter={"spool_speed": 0.0},
        timestamp_utc=timestamp_utc,
    )

    second_samples = target_module.step(
        modules_by_id={"MOD_TARGET": target_module},
        raw_step_inputs={
            "spool_speed": BehaviorStepInput(
                dt_seconds=1.0,
                latent_state={},
                context={"target_value": 10.0, "time_constant_seconds": 2.0},
            )
        },
        timestamp_utc=timestamp_utc,
    )

    assert float(second_samples[0].parameter_value_clean) > float(first_samples[0].parameter_value_clean)
