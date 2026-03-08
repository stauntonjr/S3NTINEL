from __future__ import annotations

from datetime import datetime, timezone

from libs.simulation import (
    ModuleSpec,
    ModuleRuntime,
    ParameterSpec,
    PortSpec,
    module_runtimes_from_specs,
)


def test_module_runtime_from_spec_initializes_parameter_and_port_state():
    module_spec = ModuleSpec(
        module_id="mod_a",
        subsystem_id="sub_a",
        system_id="sys_a",
        parameters=(
            ParameterSpec(
                parameter_name="p_num",
                system_id="sys_a",
                subsystem_id="sub_a",
                module_id="mod_a",
                parameter_datatype_label="numeric",
                behavior_family_label="inertial",
            ),
        ),
        input_ports=(PortSpec(port_name="cmd_in", direction="input", value_datatype_label="numeric"),),
        output_ports=(PortSpec(port_name="sig_out", direction="output", value_datatype_label="numeric"),),
        latent_variables=("plant_state",),
        controllers=("ctrl",),
        state_machines=("mode",),
    )

    module_runtime = ModuleRuntime.from_spec(module_spec)

    assert set(module_runtime.parameters) == {"p_num"}
    assert set(module_runtime.input_ports) == {"cmd_in"}
    assert set(module_runtime.output_ports) == {"sig_out"}
    assert module_runtime.latent_state_by_name == {"plant_state": 0.0}
    assert module_runtime.controller_state_by_name == {"ctrl": None}
    assert module_runtime.mode_state_by_name == {"mode": ""}
    assert module_runtime.parameter_runtime("p_num").behavior_family_label == "inertial"


def test_parameter_runtime_observation_is_simulation_local_state_only():
    module_spec = ModuleSpec(
        module_id="mod_a",
        subsystem_id="sub_a",
        system_id="sys_a",
        parameters=(
            ParameterSpec(
                parameter_name="p_num",
                system_id="sys_a",
                subsystem_id="sub_a",
                module_id="mod_a",
                parameter_datatype_label="numeric",
                behavior_family_label="inertial",
            ),
        ),
    )
    module_runtime = ModuleRuntime.from_spec(module_spec)
    parameter_runtime = module_runtime.parameter_runtime("p_num")

    timestamp_utc = datetime(2025, 1, 1, tzinfo=timezone.utc)
    parameter_runtime.update_observation(
        parameter_value=12.5,
        parameter_value_clean=12.0,
        timestamp_utc=timestamp_utc,
    )

    assert parameter_runtime.parameter_value == "12.5"
    assert parameter_runtime.parameter_value_clean == "12.0"
    assert parameter_runtime.timestamp_utc == timestamp_utc

    assert parameter_runtime.behavior_family_label == "inertial"


def test_module_runtimes_from_specs_builds_tuple():
    module_specs = (
        ModuleSpec(
            module_id="mod_a",
            subsystem_id="sub_a",
            system_id="sys_a",
            parameters=(
                ParameterSpec(
                    parameter_name="p_a",
                    system_id="sys_a",
                    subsystem_id="sub_a",
                    module_id="mod_a",
                    parameter_datatype_label="numeric",
                ),
            ),
        ),
        ModuleSpec(
            module_id="mod_b",
            subsystem_id="sub_b",
            system_id="sys_a",
            parameters=(
                ParameterSpec(
                    parameter_name="p_b",
                    system_id="sys_a",
                    subsystem_id="sub_b",
                    module_id="mod_b",
                    parameter_datatype_label="categorical",
                ),
            ),
        ),
    )

    runtimes = module_runtimes_from_specs(module_specs)

    assert len(runtimes) == 2
    assert runtimes[0].spec.module_id == "mod_a"
    assert runtimes[1].spec.module_id == "mod_b"
