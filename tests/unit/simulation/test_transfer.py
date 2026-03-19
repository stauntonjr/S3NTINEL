from __future__ import annotations

from datetime import datetime, timezone

import pytest

from libs.behavior import BehaviorStepInput
from libs.simulation import Coupling, Module, ModuleSpec, ParameterSpec, PortSpec


def _source_module_spec() -> ModuleSpec:
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


def _target_module_spec() -> ModuleSpec:
    return ModuleSpec(
        module_id="MOD_TARGET",
        subsystem_id="SUB_A",
        system_id="SYS_A",
        input_ports=(PortSpec.input(port_name="voltage_in", value_datatype_label="numeric"),),
    )


def _source_module_with_output(*, value: float, timestamp_utc: datetime) -> Module:
    module = Module.from_spec(_source_module_spec())
    sample = next(
        module.iter_samples(
            step_inputs_by_parameter={
                "bus_voltage": BehaviorStepInput(
                    dt_seconds=1.0,
                    latent_state={},
                    context={"target_value": value},
                ),
            },
            initial_state_by_parameter={"bus_voltage": value},
        )
    )
    module.apply_sample(sample, timestamp_utc=timestamp_utc)
    return module


def test_coupling_transfers_numeric_output_to_input():
    timestamp_utc = datetime(2025, 1, 1, tzinfo=timezone.utc)
    source_module = _source_module_with_output(value=28.0, timestamp_utc=timestamp_utc)
    target_module = Module.from_spec(_target_module_spec())

    Coupling.drive(
        source_module_id="MOD_SOURCE",
        source_port_name="voltage_out",
        target_module_id="MOD_TARGET",
        target_port_name="voltage_in",
        gain=0.5,
    ).apply(source_module, target_module)

    assert target_module.input_port("voltage_in").current_value == 14.0
    assert target_module.input_port("voltage_in").timestamp_utc == timestamp_utc


def test_enable_coupling_clears_target_when_source_is_inactive():
    source_module = Module.from_spec(
        ModuleSpec(
            module_id="MOD_SOURCE",
            subsystem_id="SUB_A",
            system_id="SYS_A",
            output_ports=(PortSpec.output(port_name="enable_out", value_datatype_label="numeric"),),
        )
    )
    target_module = Module.from_spec(
        ModuleSpec(
            module_id="MOD_TARGET",
            subsystem_id="SUB_A",
            system_id="SYS_A",
            input_ports=(PortSpec.input(port_name="enable_in", value_datatype_label="numeric"),),
        )
    )
    source_module.output_port("enable_out").current_value = 0.0
    target_module.input_port("enable_in").current_value = 99.0

    Coupling.enable(
        source_module_id="MOD_SOURCE",
        source_port_name="enable_out",
        target_module_id="MOD_TARGET",
        target_port_name="enable_in",
    ).apply(source_module, target_module)

    assert target_module.input_port("enable_in").current_value is None


def test_inhibit_coupling_clears_target_when_source_is_active():
    source_module = Module.from_spec(
        ModuleSpec(
            module_id="MOD_SOURCE",
            subsystem_id="SUB_A",
            system_id="SYS_A",
            output_ports=(PortSpec.output(port_name="inhibit_out", value_datatype_label="numeric"),),
        )
    )
    target_module = Module.from_spec(
        ModuleSpec(
            module_id="MOD_TARGET",
            subsystem_id="SUB_A",
            system_id="SYS_A",
            input_ports=(PortSpec.input(port_name="signal_in", value_datatype_label="numeric"),),
        )
    )
    source_module.output_port("inhibit_out").current_value = 1.0
    target_module.input_port("signal_in").current_value = 28.0

    Coupling.inhibit(
        source_module_id="MOD_SOURCE",
        source_port_name="inhibit_out",
        target_module_id="MOD_TARGET",
        target_port_name="signal_in",
    ).apply(source_module, target_module)

    assert target_module.input_port("signal_in").current_value is None


def test_coupling_respects_phase_gate():
    timestamp_utc = datetime(2025, 1, 1, tzinfo=timezone.utc)
    source_module = _source_module_with_output(value=28.0, timestamp_utc=timestamp_utc)
    target_module = Module.from_spec(_target_module_spec())

    Coupling(
        source_module_id="MOD_SOURCE",
        source_port_name="voltage_out",
        target_module_id="MOD_TARGET",
        target_port_name="voltage_in",
        relation_type="drive",
        phase_gate=("cruise",),
    ).apply(
        source_module,
        target_module,
        current_phase_label="taxi_out",
    )

    assert target_module.input_port("voltage_in").current_value is None


def test_coupling_respects_source_mode_gate():
    timestamp_utc = datetime(2025, 1, 1, tzinfo=timezone.utc)
    source_module = _source_module_with_output(value=28.0, timestamp_utc=timestamp_utc)
    target_module = Module.from_spec(_target_module_spec())
    source_module.mode_state_by_name["power_mode"] = "normal"

    Coupling(
        source_module_id="MOD_SOURCE",
        source_port_name="voltage_out",
        target_module_id="MOD_TARGET",
        target_port_name="voltage_in",
        relation_type="drive",
        source_mode_name="power_mode",
        source_mode_gate=("emergency",),
    ).apply(source_module, target_module)

    assert target_module.input_port("voltage_in").current_value is None

    Coupling(
        source_module_id="MOD_SOURCE",
        source_port_name="voltage_out",
        target_module_id="MOD_TARGET",
        target_port_name="voltage_in",
        relation_type="drive",
        source_mode_name="power_mode",
        source_mode_gate=("normal",),
    ).apply(source_module, target_module)

    assert target_module.input_port("voltage_in").current_value == 28.0


def test_lagged_coupling_delays_transfer_until_due_timestamp():
    t0 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2025, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
    source_module = _source_module_with_output(value=28.0, timestamp_utc=t0)
    target_module = Module.from_spec(_target_module_spec())
    coupling = Coupling.drive(
        source_module_id="MOD_SOURCE",
        source_port_name="voltage_out",
        target_module_id="MOD_TARGET",
        target_port_name="voltage_in",
        gain=0.5,
        lag_seconds=1.0,
    )

    coupling.apply(
        source_module,
        target_module,
        timestamp_utc=t0,
    )
    assert target_module.input_port("voltage_in").current_value is None

    coupling.apply(
        source_module,
        target_module,
        timestamp_utc=t1,
    )
    assert target_module.input_port("voltage_in").current_value == 14.0
    assert target_module.input_port("voltage_in").timestamp_utc == t1


def test_distinct_lagged_couplings_maintain_distinct_transfer_queues():
    t0 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    source_module = _source_module_with_output(value=28.0, timestamp_utc=t0)
    target_module = Module.from_spec(_target_module_spec())

    Coupling.drive(
        source_module_id="MOD_SOURCE",
        source_port_name="voltage_out",
        target_module_id="MOD_TARGET",
        target_port_name="voltage_in",
        gain=0.5,
        lag_seconds=1.0,
    ).apply(
        source_module,
        target_module,
        timestamp_utc=t0,
    )
    Coupling.drive(
        source_module_id="MOD_SOURCE",
        source_port_name="voltage_out",
        target_module_id="MOD_TARGET",
        target_port_name="voltage_in",
        gain=0.5,
        lag_seconds=2.0,
    ).apply(
        source_module,
        target_module,
        timestamp_utc=t0,
    )

    assert len(target_module.delayed_input_transfers_by_key) == 2


def test_coupling_grouping_is_owned_by_coupling_class():
    grouped = Coupling.group_by_source_module(
        (
            Coupling.drive(
                source_module_id="MOD_A",
                source_port_name="out_a",
                target_module_id="MOD_B",
                target_port_name="in_b",
            ),
            Coupling.drive(
                source_module_id="MOD_A",
                source_port_name="out_a2",
                target_module_id="MOD_C",
                target_port_name="in_c",
            ),
            Coupling.drive(
                source_module_id="MOD_X",
                source_port_name="out_x",
                target_module_id="MOD_Y",
                target_port_name="in_y",
            ),
        )
    )

    assert set(grouped) == {"MOD_A", "MOD_X"}
    assert len(grouped["MOD_A"]) == 2


def test_coupling_break_misbehavior_prevents_transfer():
    timestamp_utc = datetime(2025, 1, 1, tzinfo=timezone.utc)
    source_module = _source_module_with_output(value=28.0, timestamp_utc=timestamp_utc)
    target_module = Module.from_spec(_target_module_spec())
    coupling = Coupling.drive(
        source_module_id="MOD_SOURCE",
        source_port_name="voltage_out",
        target_module_id="MOD_TARGET",
        target_port_name="voltage_in",
        gain=0.5,
    )

    coupling.apply(
        source_module,
        target_module,
        timestamp_utc=timestamp_utc,
        misbehavior_context={"misbehavior_detail_label": "coupling_break"},
    )

    assert target_module.input_port("voltage_in").current_value is None


def test_coupling_inversion_misbehavior_reverses_sign():
    timestamp_utc = datetime(2025, 1, 1, tzinfo=timezone.utc)
    source_module = _source_module_with_output(value=28.0, timestamp_utc=timestamp_utc)
    target_module = Module.from_spec(_target_module_spec())
    coupling = Coupling.drive(
        source_module_id="MOD_SOURCE",
        source_port_name="voltage_out",
        target_module_id="MOD_TARGET",
        target_port_name="voltage_in",
        gain=0.5,
    )

    coupling.apply(
        source_module,
        target_module,
        timestamp_utc=timestamp_utc,
        misbehavior_context={"misbehavior_detail_label": "coupling_inversion"},
    )

    assert target_module.input_port("voltage_in").current_value == -14.0


def test_coupling_timing_lag_misbehavior_delays_transfer():
    t0 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2025, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
    source_module = _source_module_with_output(value=28.0, timestamp_utc=t0)
    target_module = Module.from_spec(_target_module_spec())
    coupling = Coupling.drive(
        source_module_id="MOD_SOURCE",
        source_port_name="voltage_out",
        target_module_id="MOD_TARGET",
        target_port_name="voltage_in",
        gain=0.5,
    )

    coupling.apply(
        source_module,
        target_module,
        timestamp_utc=t0,
        misbehavior_context={"misbehavior_detail_label": "timing_lag", "lag_delta_seconds": 1.0},
    )
    assert target_module.input_port("voltage_in").current_value is None

    coupling.apply(
        source_module,
        target_module,
        timestamp_utc=t1,
        misbehavior_context={"misbehavior_detail_label": "timing_lag", "lag_delta_seconds": 1.0},
    )
    assert target_module.input_port("voltage_in").current_value == 14.0


def test_coupling_timing_jitter_misbehavior_uses_override_lag():
    t0 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2025, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
    source_module = _source_module_with_output(value=28.0, timestamp_utc=t0)
    target_module = Module.from_spec(_target_module_spec())
    coupling = Coupling.drive(
        source_module_id="MOD_SOURCE",
        source_port_name="voltage_out",
        target_module_id="MOD_TARGET",
        target_port_name="voltage_in",
        gain=0.5,
    )

    coupling.apply(
        source_module,
        target_module,
        timestamp_utc=t0,
        misbehavior_context={"misbehavior_detail_label": "timing_jitter", "jitter_seconds": 1.0},
    )
    assert target_module.input_port("voltage_in").current_value is None

    coupling.apply(
        source_module,
        target_module,
        timestamp_utc=t1,
        misbehavior_context={"misbehavior_detail_label": "timing_jitter", "jitter_seconds": 1.0},
    )
    assert target_module.input_port("voltage_in").current_value == 14.0


def test_module_step_raises_for_missing_target_module_on_coupling():
    timestamp_utc = datetime(2025, 1, 1, tzinfo=timezone.utc)
    source_module = Module.from_spec(_source_module_spec())

    with pytest.raises(KeyError, match="missing target module"):
        source_module.step(
            modules_by_id={"MOD_SOURCE": source_module},
            raw_step_inputs={
                "bus_voltage": BehaviorStepInput(
                    dt_seconds=1.0,
                    latent_state={},
                    context={"target_value": 28.0},
                ),
            },
            initial_state_by_parameter={"bus_voltage": 28.0},
            outgoing_couplings=(
                Coupling.drive(
                    source_module_id="MOD_SOURCE",
                    source_port_name="voltage_out",
                    target_module_id="MOD_TARGET",
                    target_port_name="voltage_in",
                ),
            ),
            timestamp_utc=timestamp_utc,
        )
