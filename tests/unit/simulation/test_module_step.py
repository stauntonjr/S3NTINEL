from __future__ import annotations

from datetime import datetime, timezone

from libs.behavior import BehaviorStepInput
from libs.simulation import LatentUpdate, LatentUpdateSpec, Module, ModuleSpec, ParameterSpec, PortSpec


def _build_test_module_spec() -> ModuleSpec:
    return ModuleSpec(
        module_id="MOD_A",
        subsystem_id="SUB_A",
        system_id="SYS_A",
        parameters=(
            ParameterSpec(
                parameter_name="bus_voltage",
                system_id="SYS_A",
                subsystem_id="SUB_A",
                module_id="MOD_A",
                parameter_datatype_label="numeric",
                behavior_family_label="regulated",
                output_port_name="voltage_out",
            ),
            ParameterSpec(
                parameter_name="spool_speed",
                system_id="SYS_A",
                subsystem_id="SUB_A",
                module_id="MOD_A",
                parameter_datatype_label="numeric",
                behavior_family_label="inertial",
                input_port_names=("command_in",),
            ),
        ),
        input_ports=(PortSpec(port_name="command_in", direction="input", value_datatype_label="numeric"),),
        output_ports=(PortSpec(port_name="voltage_out", direction="output", value_datatype_label="numeric"),),
        latent_update_specs=(
            LatentUpdateSpec(
                latent_name="command_state",
                source_name="command_in",
                source_kind="input_port",
                gain=2.0,
            ),
        ),
    )


def test_iter_module_samples_emits_local_behavior_streams():
    module_spec = _build_test_module_spec()
    module = Module.from_spec(module_spec)

    samples = list(
        module.iter_samples(
        step_inputs_by_parameter={
            "bus_voltage": BehaviorStepInput(dt_seconds=1.0, latent_state={}, context={"target_value": 28.0}),
            "spool_speed": BehaviorStepInput(
                dt_seconds=1.0,
                latent_state={},
                context={"target_value": 1.0, "time_constant_seconds": 1.0},
            ),
        },
        initial_state_by_parameter={"bus_voltage": 28.0, "spool_speed": 0.0},
        )
    )

    assert len(samples) == 2
    assert {sample.parameter_name for sample in samples} == {"bus_voltage", "spool_speed"}
    assert all(sample.metadata.get("module_id") == "MOD_A" for sample in samples)
    assert all(sample.metadata.get("system_id") == "SYS_A" for sample in samples)


def test_iter_module_samples_can_apply_violations():
    module_spec = _build_test_module_spec()
    module = Module.from_spec(module_spec)

    samples = list(
        module.iter_samples(
        step_inputs_by_parameter={
            "bus_voltage": BehaviorStepInput(dt_seconds=1.0, latent_state={}, context={"target_value": 28.0}),
        },
        initial_state_by_parameter={"bus_voltage": 28.0},
        fault_context_by_parameter={
            "bus_voltage": {"bias": 2.0, "anomaly_rate": 1.0, "rng_seed": 7},
        },
        apply_faults=True,
        )
    )

    assert len(samples) == 1
    assert samples[0].metadata.get("misbehavior_applied") is True
    assert samples[0].metadata.get("misbehavior_family_label") == "offset"


def test_apply_module_sample_to_runtime_updates_parameter_state():
    module_spec = _build_test_module_spec()
    module = Module.from_spec(module_spec)
    sample = next(
        module.iter_samples(
            step_inputs_by_parameter={
                "bus_voltage": BehaviorStepInput(
                    dt_seconds=1.0,
                    latent_state={},
                    context={"target_value": 28.0},
                ),
            },
            initial_state_by_parameter={"bus_voltage": 27.9},
        )
    )

    timestamp_utc = datetime(2025, 1, 1, tzinfo=timezone.utc)
    module.apply_sample(sample, timestamp_utc=timestamp_utc)

    parameter_runtime = module.parameter("bus_voltage")
    assert parameter_runtime.parameter_value is not None
    assert parameter_runtime.parameter_value_clean is not None
    assert parameter_runtime.timestamp_utc == timestamp_utc
    assert module.output_port("voltage_out").current_value == sample.parameter_value_clean


def test_inject_input_ports_into_step_inputs_merges_port_values_into_context():
    module_spec = _build_test_module_spec()
    module = Module.from_spec(module_spec)
    module.input_port("command_in").current_value = 1.25

    injected = module.hydrate_step_inputs_from_ports(
        {
            "spool_speed": BehaviorStepInput(dt_seconds=1.0, latent_state={}, context={"target_value": 1.0}),
        },
    )

    assert "spool_speed" in injected
    assert injected["spool_speed"].context["command_in"] == 1.25
    assert injected["spool_speed"].context["target_value"] == 1.0


def test_update_module_latent_state_reads_input_port_hooks():
    module_spec = _build_test_module_spec()
    module = Module.from_spec(module_spec)
    module.input_port("command_in").current_value = 1.5

    module.apply_latent_updates(
        {
            "spool_speed": BehaviorStepInput(dt_seconds=1.0, latent_state={}, context={"target_value": 1.0}),
        },
    )

    assert module.latent_state_by_name["command_state"] == 3.0


def test_inject_runtime_latent_state_into_step_inputs_merges_runtime_latent_values():
    module_spec = _build_test_module_spec()
    module = Module.from_spec(module_spec)
    module.latent_state_by_name["command_state"] = 2.5

    injected = module.hydrate_step_inputs_from_latent_state(
        {
            "spool_speed": BehaviorStepInput(dt_seconds=1.0, latent_state={}, context={"target_value": 1.0}),
        },
    )

    assert injected["spool_speed"].latent_state["command_state"] == 2.5


def test_module_builds_runtime_latent_updates_from_specs():
    module = Module.from_spec(_build_test_module_spec())

    assert len(module.latent_updates) == 1
    assert isinstance(module.latent_updates[0], LatentUpdate)
    assert module.latent_updates[0].latent_name == "command_state"


def test_iter_module_samples_can_consume_injected_runtime_latent_state():
    module_spec = _build_test_module_spec()
    module = Module.from_spec(module_spec)
    module.input_port("command_in").current_value = 1.5

    base_inputs = module.hydrate_step_inputs_from_ports(
        {
            "spool_speed": BehaviorStepInput(
                dt_seconds=1.0,
                latent_state={},
                context={"latent_target_name": "command_state", "time_constant_seconds": 1.0},
            ),
        },
    )
    module.apply_latent_updates(base_inputs)
    hydrated_inputs = module.hydrate_step_inputs_from_latent_state(base_inputs)

    samples = list(
        module.iter_samples(
            step_inputs_by_parameter=hydrated_inputs,
            initial_state_by_parameter={"spool_speed": 0.0},
        )
    )

    spool_samples = [sample for sample in samples if sample.parameter_name == "spool_speed"]
    assert len(spool_samples) == 1
    assert spool_samples[0].parameter_value_clean == 3.0
    assert spool_samples[0].metadata["target_source"] == "latent_state"


def test_regulated_module_parameter_can_consume_injected_runtime_latent_state():
    module_spec = ModuleSpec(
        module_id="MOD_REG",
        subsystem_id="SUB_A",
        system_id="SYS_A",
        parameters=(
            ParameterSpec(
                parameter_name="bus_voltage",
                system_id="SYS_A",
                subsystem_id="SUB_A",
                module_id="MOD_REG",
                parameter_datatype_label="numeric",
                behavior_family_label="regulated",
                input_port_names=("setpoint_in",),
            ),
        ),
        input_ports=(PortSpec(port_name="setpoint_in", direction="input", value_datatype_label="numeric"),),
        latent_update_specs=(
            LatentUpdateSpec(
                latent_name="setpoint_state",
                source_name="setpoint_in",
                source_kind="input_port",
            ),
        ),
    )
    module = Module.from_spec(module_spec)
    module.input_port("setpoint_in").current_value = 28.0

    base_inputs = module.hydrate_step_inputs_from_ports(
        {
            "bus_voltage": BehaviorStepInput(
                dt_seconds=1.0,
                latent_state={},
                context={"latent_target_name": "setpoint_state", "reversion_rate": 2.0},
            ),
        },
    )
    module.apply_latent_updates(base_inputs)
    hydrated_inputs = module.hydrate_step_inputs_from_latent_state(base_inputs)

    samples = list(
        module.iter_samples(
            step_inputs_by_parameter=hydrated_inputs,
            initial_state_by_parameter={"bus_voltage": 27.0},
        )
    )

    assert len(samples) == 1
    assert samples[0].parameter_name == "bus_voltage"
    assert samples[0].parameter_value_clean == 28.0
    assert samples[0].metadata["target_source"] == "latent_state"
