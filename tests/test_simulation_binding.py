from __future__ import annotations

import pytest

from libs.behavior import build_default_behavior_registry
from libs.simulation import (
    HierarchyAssemblySpec,
    ModuleSpec,
    ParameterSpec,
    bind_assembly_behaviors,
    bind_module_behaviors,
    bind_parameter_behavior,
)


def test_bind_parameter_behavior_resolves_registry_entry():
    registry = build_default_behavior_registry()
    parameter_spec = ParameterSpec(
        parameter_name="bus_voltage",
        system_id="SYS_A",
        subsystem_id="SUB_A",
        module_id="MOD_A",
        parameter_datatype_label="numeric",
        behavior_family_label="regulated",
    )

    binding = bind_parameter_behavior(parameter_spec, registry)

    assert binding.parameter_spec.parameter_name == "bus_voltage"
    assert binding.behavior.contract.behavior_family == "regulated"


def test_bind_parameter_behavior_rejects_missing_behavior_label():
    registry = build_default_behavior_registry()
    parameter_spec = ParameterSpec(
        parameter_name="bus_voltage",
        system_id="SYS_A",
        subsystem_id="SUB_A",
        module_id="MOD_A",
        parameter_datatype_label="numeric",
        behavior_family_label=None,
    )

    with pytest.raises(ValueError, match="has no behavior_family_label"):
        bind_parameter_behavior(parameter_spec, registry)


def test_bind_module_behaviors_binds_all_parameters():
    registry = build_default_behavior_registry()
    module_spec = ModuleSpec(
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
            ),
            ParameterSpec(
                parameter_name="spool_speed",
                system_id="SYS_A",
                subsystem_id="SUB_A",
                module_id="MOD_A",
                parameter_datatype_label="numeric",
                behavior_family_label="inertial",
            ),
        ),
    )

    binding = bind_module_behaviors(module_spec, registry)

    assert binding.module_spec.module_id == "MOD_A"
    assert [item.behavior.contract.behavior_family for item in binding.parameter_bindings] == [
        "regulated",
        "inertial",
    ]


def test_bind_assembly_behaviors_binds_each_module():
    registry = build_default_behavior_registry()
    assembly_spec = HierarchyAssemblySpec(
        module_specs=(
            ModuleSpec(
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
                    ),
                ),
            ),
            ModuleSpec(
                module_id="MOD_B",
                subsystem_id="SUB_A",
                system_id="SYS_A",
                parameters=(
                    ParameterSpec(
                        parameter_name="spool_speed",
                        system_id="SYS_A",
                        subsystem_id="SUB_A",
                        module_id="MOD_B",
                        parameter_datatype_label="numeric",
                        behavior_family_label="inertial",
                    ),
                ),
            ),
        ),
    )

    bindings = bind_assembly_behaviors(assembly_spec, registry)

    assert len(bindings) == 2
    assert bindings[0].module_spec.module_id == "MOD_A"
    assert bindings[1].module_spec.module_id == "MOD_B"
