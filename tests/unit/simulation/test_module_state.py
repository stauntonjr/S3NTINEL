from __future__ import annotations

from datetime import datetime, timezone

from libs.simulation import Module, ModuleSpec, ParameterSpec, PortSpec


def test_module_from_spec_initializes_parameter_and_port_state():
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

    module = Module.from_spec(module_spec)

    assert module.id == "mod_a"
    assert set(module.parameters) == {"p_num"}
    assert set(module.input_ports) == {"cmd_in"}
    assert set(module.output_ports) == {"sig_out"}
    assert module.latent_state_by_name == {"plant_state": 0.0}
    assert module.controller_state_by_name == {"ctrl": None}
    assert module.mode_state_by_name == {"mode": ""}
    assert module.parameter("p_num").behavior_family_label == "inertial"


def test_module_parameter_observation_is_local_state_only():
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
    module = Module.from_spec(module_spec)
    parameter = module.parameter("p_num")

    timestamp_utc = datetime(2025, 1, 1, tzinfo=timezone.utc)
    parameter.step(
        parameter_value=12.5,
        parameter_value_clean=12.0,
        timestamp_utc=timestamp_utc,
    )

    assert parameter.parameter_value == 12.5
    assert parameter.parameter_value_clean == 12.0
    assert parameter.timestamp_utc == timestamp_utc
    assert parameter.behavior_family_label == "inertial"


def test_module_from_spec_builds_independent_state_per_module():
    module_a = Module.from_spec(
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
                    behavior_family_label="regulated",
                ),
            ),
        )
    )
    module_b = Module.from_spec(
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
                    behavior_family_label="discrete_state",
                ),
            ),
        )
    )

    assert module_a.id == "mod_a"
    assert module_b.id == "mod_b"
    assert set(module_a.parameters) == {"p_a"}
    assert set(module_b.parameters) == {"p_b"}
