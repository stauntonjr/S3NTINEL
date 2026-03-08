from __future__ import annotations

from datetime import datetime, timezone

import pytest

from libs.behavior import BehaviorStepInput, build_default_behavior_registry
from libs.simulation import (
    InterModuleCouplingSpec,
    ModuleRuntime,
    ModuleSpec,
    ModuleStepRequest,
    ParameterSpec,
    PortSpec,
    apply_inter_module_coupling,
    apply_module_sample_to_runtime,
    bind_module_behaviors,
    iter_module_samples,
    propagate_inter_module_couplings,
)


def test_apply_inter_module_coupling_transfers_numeric_output_to_input():
    source_module_spec = ModuleSpec(
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
    target_module_spec = ModuleSpec(
        module_id="MOD_TARGET",
        subsystem_id="SUB_A",
        system_id="SYS_A",
        input_ports=(PortSpec(port_name="voltage_in", direction="input", value_datatype_label="numeric"),),
    )

    source_runtime = ModuleRuntime.from_spec(source_module_spec)
    target_runtime = ModuleRuntime.from_spec(target_module_spec)
    source_binding = bind_module_behaviors(source_module_spec, build_default_behavior_registry())

    sample = next(
        iter_module_samples(
            ModuleStepRequest(
                module_binding=source_binding,
                step_inputs_by_parameter={
                    "bus_voltage": BehaviorStepInput(
                        dt_seconds=1.0,
                        latent_state={},
                        context={"target_value": 28.0},
                    ),
                },
                initial_state_by_parameter={"bus_voltage": 27.5},
            )
        )
    )
    timestamp_utc = datetime(2025, 1, 1, tzinfo=timezone.utc)
    apply_module_sample_to_runtime(
        source_runtime,
        sample,
        module_binding=source_binding,
        timestamp_utc=timestamp_utc,
    )

    apply_inter_module_coupling(
        source_runtime,
        target_runtime,
        InterModuleCouplingSpec(
            source_module_id="MOD_SOURCE",
            source_port_name="voltage_out",
            target_module_id="MOD_TARGET",
            target_port_name="voltage_in",
            relation_type="drive",
            gain=0.5,
            sign=1,
        ),
    )

    assert target_runtime.input_port_runtime("voltage_in").current_value == float(sample.parameter_value_clean) * 0.5
    assert target_runtime.input_port_runtime("voltage_in").timestamp_utc == timestamp_utc


def test_apply_inter_module_coupling_passes_non_numeric_values_through():
    source_module_spec = ModuleSpec(
        module_id="MOD_SOURCE",
        subsystem_id="SUB_A",
        system_id="SYS_A",
        output_ports=(PortSpec(port_name="state_out", direction="output", value_datatype_label="categorical"),),
    )
    target_module_spec = ModuleSpec(
        module_id="MOD_TARGET",
        subsystem_id="SUB_A",
        system_id="SYS_A",
        input_ports=(PortSpec(port_name="state_in", direction="input", value_datatype_label="categorical"),),
    )

    source_runtime = ModuleRuntime.from_spec(source_module_spec)
    target_runtime = ModuleRuntime.from_spec(target_module_spec)
    timestamp_utc = datetime(2025, 1, 1, tzinfo=timezone.utc)
    source_runtime.output_port_runtime("state_out").current_value = "OPEN"
    source_runtime.output_port_runtime("state_out").timestamp_utc = timestamp_utc

    apply_inter_module_coupling(
        source_runtime,
        target_runtime,
        InterModuleCouplingSpec(
            source_module_id="MOD_SOURCE",
            source_port_name="state_out",
            target_module_id="MOD_TARGET",
            target_port_name="state_in",
            relation_type="enable",
        ),
    )

    assert target_runtime.input_port_runtime("state_in").current_value == "OPEN"
    assert target_runtime.input_port_runtime("state_in").timestamp_utc == timestamp_utc


def test_apply_inter_module_coupling_enable_clears_target_when_source_inactive():
    source_module_spec = ModuleSpec(
        module_id="MOD_SOURCE",
        subsystem_id="SUB_A",
        system_id="SYS_A",
        output_ports=(PortSpec(port_name="enable_out", direction="output", value_datatype_label="numeric"),),
    )
    target_module_spec = ModuleSpec(
        module_id="MOD_TARGET",
        subsystem_id="SUB_A",
        system_id="SYS_A",
        input_ports=(PortSpec(port_name="enable_in", direction="input", value_datatype_label="numeric"),),
    )

    source_runtime = ModuleRuntime.from_spec(source_module_spec)
    target_runtime = ModuleRuntime.from_spec(target_module_spec)
    target_runtime.input_port_runtime("enable_in").current_value = 99.0
    source_runtime.output_port_runtime("enable_out").current_value = 0.0

    apply_inter_module_coupling(
        source_runtime,
        target_runtime,
        InterModuleCouplingSpec(
            source_module_id="MOD_SOURCE",
            source_port_name="enable_out",
            target_module_id="MOD_TARGET",
            target_port_name="enable_in",
            relation_type="enable",
        ),
    )

    assert target_runtime.input_port_runtime("enable_in").current_value is None


def test_propagate_inter_module_couplings_updates_target_module_inputs():
    source_module_spec = ModuleSpec(
        module_id="MOD_SOURCE",
        subsystem_id="SUB_A",
        system_id="SYS_A",
        output_ports=(PortSpec(port_name="power_out", direction="output", value_datatype_label="numeric"),),
    )
    target_module_spec = ModuleSpec(
        module_id="MOD_TARGET",
        subsystem_id="SUB_A",
        system_id="SYS_A",
        input_ports=(PortSpec(port_name="power_in", direction="input", value_datatype_label="numeric"),),
    )

    timestamp_utc = datetime(2025, 1, 1, tzinfo=timezone.utc)
    source_runtime = ModuleRuntime.from_spec(source_module_spec)
    target_runtime = ModuleRuntime.from_spec(target_module_spec)
    source_runtime.output_port_runtime("power_out").current_value = 28.0
    source_runtime.output_port_runtime("power_out").timestamp_utc = timestamp_utc

    propagate_inter_module_couplings(
        {
            "MOD_SOURCE": source_runtime,
            "MOD_TARGET": target_runtime,
        },
        (
            InterModuleCouplingSpec(
                source_module_id="MOD_SOURCE",
                source_port_name="power_out",
                target_module_id="MOD_TARGET",
                target_port_name="power_in",
                relation_type="drive",
                gain=0.5,
            ),
        ),
    )

    assert target_runtime.input_port_runtime("power_in").current_value == 14.0
    assert target_runtime.input_port_runtime("power_in").timestamp_utc == timestamp_utc


def test_propagate_inter_module_couplings_rejects_missing_module_runtime():
    source_module_spec = ModuleSpec(
        module_id="MOD_SOURCE",
        subsystem_id="SUB_A",
        system_id="SYS_A",
        output_ports=(PortSpec(port_name="power_out", direction="output", value_datatype_label="numeric"),),
    )
    source_runtime = ModuleRuntime.from_spec(source_module_spec)
    source_runtime.output_port_runtime("power_out").current_value = 28.0

    with pytest.raises(KeyError, match="missing target module runtime"):
        propagate_inter_module_couplings(
            {"MOD_SOURCE": source_runtime},
            (
                InterModuleCouplingSpec(
                    source_module_id="MOD_SOURCE",
                    source_port_name="power_out",
                    target_module_id="MOD_TARGET",
                    target_port_name="power_in",
                    relation_type="drive",
                ),
            ),
        )


def test_apply_inter_module_coupling_respects_phase_gate():
    source_module_spec = ModuleSpec(
        module_id="MOD_SOURCE",
        subsystem_id="SUB_A",
        system_id="SYS_A",
        output_ports=(PortSpec(port_name="power_out", direction="output", value_datatype_label="numeric"),),
    )
    target_module_spec = ModuleSpec(
        module_id="MOD_TARGET",
        subsystem_id="SUB_A",
        system_id="SYS_A",
        input_ports=(PortSpec(port_name="power_in", direction="input", value_datatype_label="numeric"),),
    )
    source_runtime = ModuleRuntime.from_spec(source_module_spec)
    target_runtime = ModuleRuntime.from_spec(target_module_spec)
    source_runtime.output_port_runtime("power_out").current_value = 28.0

    apply_inter_module_coupling(
        source_runtime,
        target_runtime,
        InterModuleCouplingSpec(
            source_module_id="MOD_SOURCE",
            source_port_name="power_out",
            target_module_id="MOD_TARGET",
            target_port_name="power_in",
            relation_type="drive",
            phase_gate=("cruise",),
        ),
        current_phase_label="taxi_out",
    )

    assert target_runtime.input_port_runtime("power_in").current_value is None


def test_apply_inter_module_coupling_delays_transfer_until_due_timestamp():
    source_module_spec = ModuleSpec(
        module_id="MOD_SOURCE",
        subsystem_id="SUB_A",
        system_id="SYS_A",
        output_ports=(PortSpec(port_name="power_out", direction="output", value_datatype_label="numeric"),),
    )
    target_module_spec = ModuleSpec(
        module_id="MOD_TARGET",
        subsystem_id="SUB_A",
        system_id="SYS_A",
        input_ports=(PortSpec(port_name="power_in", direction="input", value_datatype_label="numeric"),),
    )
    source_runtime = ModuleRuntime.from_spec(source_module_spec)
    target_runtime = ModuleRuntime.from_spec(target_module_spec)

    t0 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2025, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
    source_runtime.output_port_runtime("power_out").current_value = 28.0
    source_runtime.output_port_runtime("power_out").timestamp_utc = t0
    coupling = InterModuleCouplingSpec(
        source_module_id="MOD_SOURCE",
        source_port_name="power_out",
        target_module_id="MOD_TARGET",
        target_port_name="power_in",
        relation_type="drive",
        gain=0.5,
        lag_seconds=1.0,
    )

    apply_inter_module_coupling(
        source_runtime,
        target_runtime,
        coupling,
        timestamp_utc=t0,
    )
    assert target_runtime.input_port_runtime("power_in").current_value is None

    apply_inter_module_coupling(
        source_runtime,
        target_runtime,
        coupling,
        timestamp_utc=t1,
    )
    assert target_runtime.input_port_runtime("power_in").current_value == 14.0
    assert target_runtime.input_port_runtime("power_in").timestamp_utc == t1


def test_delayed_transfer_lands_even_if_gate_is_closed_when_due():
    source_module_spec = ModuleSpec(
        module_id="MOD_SOURCE",
        subsystem_id="SUB_A",
        system_id="SYS_A",
        output_ports=(PortSpec(port_name="power_out", direction="output", value_datatype_label="numeric"),),
    )
    target_module_spec = ModuleSpec(
        module_id="MOD_TARGET",
        subsystem_id="SUB_A",
        system_id="SYS_A",
        input_ports=(PortSpec(port_name="power_in", direction="input", value_datatype_label="numeric"),),
    )
    source_runtime = ModuleRuntime.from_spec(source_module_spec)
    target_runtime = ModuleRuntime.from_spec(target_module_spec)

    t0 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2025, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
    source_runtime.output_port_runtime("power_out").current_value = 28.0
    source_runtime.output_port_runtime("power_out").timestamp_utc = t0
    coupling = InterModuleCouplingSpec(
        source_module_id="MOD_SOURCE",
        source_port_name="power_out",
        target_module_id="MOD_TARGET",
        target_port_name="power_in",
        relation_type="drive",
        gain=0.5,
        lag_seconds=1.0,
        phase_gate=("takeoff_climb",),
    )

    apply_inter_module_coupling(
        source_runtime,
        target_runtime,
        coupling,
        timestamp_utc=t0,
        current_phase_label="takeoff_climb",
    )
    assert target_runtime.input_port_runtime("power_in").current_value is None

    apply_inter_module_coupling(
        source_runtime,
        target_runtime,
        coupling,
        timestamp_utc=t1,
        current_phase_label="taxi_out",
    )
    assert target_runtime.input_port_runtime("power_in").current_value == 14.0
    assert target_runtime.input_port_runtime("power_in").timestamp_utc == t1


def test_delayed_transfer_queues_do_not_collapse_distinct_couplings():
    source_module_spec = ModuleSpec(
        module_id="MOD_SOURCE",
        subsystem_id="SUB_A",
        system_id="SYS_A",
        output_ports=(PortSpec(port_name="power_out", direction="output", value_datatype_label="numeric"),),
    )
    target_module_spec = ModuleSpec(
        module_id="MOD_TARGET",
        subsystem_id="SUB_A",
        system_id="SYS_A",
        input_ports=(PortSpec(port_name="power_in", direction="input", value_datatype_label="numeric"),),
    )
    source_runtime = ModuleRuntime.from_spec(source_module_spec)
    target_runtime = ModuleRuntime.from_spec(target_module_spec)
    t0 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    source_runtime.output_port_runtime("power_out").current_value = 28.0
    source_runtime.output_port_runtime("power_out").timestamp_utc = t0

    apply_inter_module_coupling(
        source_runtime,
        target_runtime,
        InterModuleCouplingSpec(
            source_module_id="MOD_SOURCE",
            source_port_name="power_out",
            target_module_id="MOD_TARGET",
            target_port_name="power_in",
            relation_type="drive",
            gain=0.5,
            lag_seconds=1.0,
        ),
        timestamp_utc=t0,
    )
    apply_inter_module_coupling(
        source_runtime,
        target_runtime,
        InterModuleCouplingSpec(
            source_module_id="MOD_SOURCE",
            source_port_name="power_out",
            target_module_id="MOD_TARGET",
            target_port_name="power_in",
            relation_type="drive",
            gain=0.5,
            lag_seconds=2.0,
        ),
        timestamp_utc=t0,
    )

    assert len(target_runtime.delayed_input_transfers_by_key) == 2


def test_apply_inter_module_coupling_respects_mode_gate():
    source_module_spec = ModuleSpec(
        module_id="MOD_SOURCE",
        subsystem_id="SUB_A",
        system_id="SYS_A",
        output_ports=(PortSpec(port_name="power_out", direction="output", value_datatype_label="numeric"),),
    )
    target_module_spec = ModuleSpec(
        module_id="MOD_TARGET",
        subsystem_id="SUB_A",
        system_id="SYS_A",
        input_ports=(PortSpec(port_name="power_in", direction="input", value_datatype_label="numeric"),),
    )
    source_runtime = ModuleRuntime.from_spec(source_module_spec)
    target_runtime = ModuleRuntime.from_spec(target_module_spec)
    source_runtime.output_port_runtime("power_out").current_value = 28.0
    source_runtime.mode_state_by_name["power_mode"] = "normal"

    apply_inter_module_coupling(
        source_runtime,
        target_runtime,
        InterModuleCouplingSpec(
            source_module_id="MOD_SOURCE",
            source_port_name="power_out",
            target_module_id="MOD_TARGET",
            target_port_name="power_in",
            relation_type="drive",
            source_mode_name="power_mode",
            source_mode_gate=("emergency",),
        ),
    )

    assert target_runtime.input_port_runtime("power_in").current_value is None

    apply_inter_module_coupling(
        source_runtime,
        target_runtime,
        InterModuleCouplingSpec(
            source_module_id="MOD_SOURCE",
            source_port_name="power_out",
            target_module_id="MOD_TARGET",
            target_port_name="power_in",
            relation_type="drive",
            source_mode_name="power_mode",
            source_mode_gate=("normal",),
        ),
    )

    assert target_runtime.input_port_runtime("power_in").current_value == 28.0


def test_apply_inter_module_coupling_inhibit_clears_target_when_source_active():
    source_module_spec = ModuleSpec(
        module_id="MOD_SOURCE",
        subsystem_id="SUB_A",
        system_id="SYS_A",
        output_ports=(PortSpec(port_name="inhibit_out", direction="output", value_datatype_label="numeric"),),
    )
    target_module_spec = ModuleSpec(
        module_id="MOD_TARGET",
        subsystem_id="SUB_A",
        system_id="SYS_A",
        input_ports=(PortSpec(port_name="signal_in", direction="input", value_datatype_label="numeric"),),
    )
    source_runtime = ModuleRuntime.from_spec(source_module_spec)
    target_runtime = ModuleRuntime.from_spec(target_module_spec)
    source_runtime.output_port_runtime("inhibit_out").current_value = 1.0
    target_runtime.input_port_runtime("signal_in").current_value = 28.0

    apply_inter_module_coupling(
        source_runtime,
        target_runtime,
        InterModuleCouplingSpec(
            source_module_id="MOD_SOURCE",
            source_port_name="inhibit_out",
            target_module_id="MOD_TARGET",
            target_port_name="signal_in",
            relation_type="inhibit",
        ),
    )

    assert target_runtime.input_port_runtime("signal_in").current_value is None
