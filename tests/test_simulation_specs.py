from __future__ import annotations

from libs.simulation import (
    build_hierarchy_assembly_spec,
    assembly_spec_from_hierarchy_spec,
    CouplingSpec,
    HierarchyAssemblySpec,
    HierarchyAssemblyBuilder,
    InterModuleCouplingSpec,
    ModuleSpec,
    PortSpec,
    default_behavior_family_for_datatype,
    flatten_inter_module_couplings,
    flatten_module_specs,
    inter_module_coupling_spec_from_row,
    module_specs_from_hierarchy_spec,
    parameter_spec_from_legacy_sensor,
    validate_assembly_spec,
)
import pytest


def test_default_behavior_family_for_datatype_returns_expected_defaults():
    assert default_behavior_family_for_datatype("numeric") == "inertial"
    assert default_behavior_family_for_datatype("categorical") == "discrete_state"
    assert default_behavior_family_for_datatype("binary") == "discrete_state"
    assert default_behavior_family_for_datatype("constant") == "regulated"


def test_parameter_spec_from_legacy_sensor_normalizes_datatype_and_defaults_behavior():
    spec = parameter_spec_from_legacy_sensor(
        system_id="SYS_A",
        subsystem_id="SUB_A",
        module_id="MOD_A",
        sensor_obj={
            "sensor": "ias_kt",
            "datatype": "continuous",
            "unit": "kt",
            "sampling_rate_hz": 4.0,
        },
    )

    assert spec.parameter_name == "ias_kt"
    assert spec.parameter_datatype_label == "numeric"
    assert spec.behavior_family_label == "inertial"
    assert spec.sampling_rate_hz == 4.0


def test_module_specs_from_hierarchy_spec_builds_parameter_specs():
    hierarchy_spec = {
        "systems": {
            "SYS_A": {
                "subsystems": {
                    "SUB_A": {
                        "modules": {
                            "MOD_A": [
                                {"sensor": "eng_temp", "datatype": "numeric", "unit": "c"},
                                {"sensor": "pump_state", "datatype": "categorical", "unit": ""},
                            ]
                        }
                    }
                }
            }
        }
    }

    module_specs = module_specs_from_hierarchy_spec(hierarchy_spec)
    assert len(module_specs) == 1
    module_spec = module_specs[0]
    assert isinstance(module_spec, ModuleSpec)
    assert module_spec.module_id == "MOD_A"
    assert [parameter.parameter_name for parameter in module_spec.parameters] == ["eng_temp", "pump_state"]
    assert module_spec.parameters[0].behavior_family_label == "inertial"
    assert module_spec.parameters[1].behavior_family_label == "discrete_state"


def test_flatten_module_specs_returns_canonical_rows():
    module_specs = (
        ModuleSpec(
            module_id="MOD_A",
            subsystem_id="SUB_A",
            system_id="SYS_A",
            parameters=(
                parameter_spec_from_legacy_sensor(
                    system_id="SYS_A",
                    subsystem_id="SUB_A",
                    module_id="MOD_A",
                    sensor_obj={"sensor": "eng_temp", "datatype": "numeric", "unit": "c"},
                ),
            ),
        ),
    )

    rows = flatten_module_specs(module_specs)
    assert rows == [
        {
            "system_id": "SYS_A",
            "subsystem_id": "SUB_A",
            "module_id": "MOD_A",
            "parameter_name": "eng_temp",
            "parameter_datatype_label": "numeric",
            "behavior_family_label": "inertial",
            "unit": "c",
            "sampling_rate_hz": None,
            "input_port_names": [],
            "output_port_name": None,
        }
    ]


def test_port_and_coupling_specs_are_instantiable():
    port = PortSpec(port_name="bleed_pressure_out", direction="output", value_datatype_label="numeric", unit="psi")
    coupling = CouplingSpec(source_ref="bleed_pressure_out", target_ref="pack_pressure_in", relation_type="drive", gain=0.9, lag_seconds=0.5)

    assert port.port_name == "bleed_pressure_out"
    assert coupling.relation_type == "drive"
    assert coupling.lag_seconds == 0.5


def test_inter_module_coupling_and_assembly_specs_are_instantiable():
    module_spec = ModuleSpec(
        module_id="mod_source",
        subsystem_id="sub_a",
        system_id="sys_a",
        output_ports=(PortSpec(port_name="bleed_out", direction="output", value_datatype_label="numeric"),),
    )
    coupling = InterModuleCouplingSpec(
        source_module_id="mod_source",
        source_port_name="bleed_out",
        target_module_id="mod_target",
        target_port_name="bleed_in",
        relation_type="drive",
        gain=0.8,
        lag_seconds=0.2,
    )
    assembly = HierarchyAssemblySpec(module_specs=(module_spec,), inter_module_couplings=(coupling,))

    assert assembly.module_specs[0].module_id == "mod_source"
    assert assembly.inter_module_couplings[0].target_port_name == "bleed_in"


def test_flatten_inter_module_couplings_returns_canonical_rows():
    rows = flatten_inter_module_couplings(
        (
            InterModuleCouplingSpec(
                source_module_id="mod_source",
                source_port_name="power_out",
                target_module_id="mod_target",
                target_port_name="power_in",
                relation_type="enable",
                gain=1.0,
                sign=1,
                lag_seconds=0.0,
                phase_gate=("cruise",),
            ),
        )
    )

    assert rows == [
        {
            "source_module_id": "mod_source",
            "source_port_name": "power_out",
            "target_module_id": "mod_target",
            "target_port_name": "power_in",
            "relation_type": "enable",
            "gain": 1.0,
            "sign": 1,
            "lag_seconds": 0.0,
            "time_constant_seconds": None,
            "phase_gate": ["cruise"],
            "mode_gate": [],
            "source_mode_name": None,
            "source_mode_gate": [],
            "target_mode_name": None,
            "target_mode_gate": [],
            "shared_noise_group": None,
        }
    ]


def test_inter_module_coupling_spec_from_row_captures_known_fields_and_metadata():
    coupling = inter_module_coupling_spec_from_row(
        {
            "source_module_id": "mod_source",
            "source_port_name": "power_out",
            "target_module_id": "mod_target",
            "target_port_name": "power_in",
            "relation_type": "enable",
            "gain": 0.75,
            "sign": 1,
            "lag_seconds": 0.1,
            "phase_gate": ["taxi_out", "takeoff_climb"],
            "note": "example",
        }
    )

    assert coupling.source_module_id == "mod_source"
    assert coupling.target_port_name == "power_in"
    assert coupling.phase_gate == ("taxi_out", "takeoff_climb")
    assert coupling.metadata == {"note": "example"}


def test_assembly_spec_from_hierarchy_spec_builds_modules_and_wiring():
    hierarchy_spec = {
        "systems": {
            "SYS_A": {
                "subsystems": {
                    "SUB_A": {
                        "modules": {
                            "MOD_A": [{"sensor": "eng_temp", "datatype": "numeric"}],
                            "MOD_B": [{"sensor": "pack_temp", "datatype": "numeric"}],
                        }
                    }
                }
            }
        }
    }

    assembly = assembly_spec_from_hierarchy_spec(
        hierarchy_spec,
        inter_module_coupling_rows=[
            {
                "source_module_id": "MOD_A",
                "source_port_name": "bleed_out",
                "target_module_id": "MOD_B",
                "target_port_name": "bleed_in",
                "relation_type": "drive",
                "gain": 0.9,
            }
        ],
        metadata={"aircraft_model": "demo"},
    )

    assert isinstance(assembly, HierarchyAssemblySpec)
    assert len(assembly.module_specs) == 2
    assert len(assembly.inter_module_couplings) == 1
    assert assembly.inter_module_couplings[0].relation_type == "drive"
    assert assembly.metadata == {"aircraft_model": "demo"}


def test_validate_assembly_spec_rejects_unknown_module_reference():
    assembly = HierarchyAssemblySpec(
        module_specs=(
            ModuleSpec(
                module_id="MOD_A",
                subsystem_id="SUB_A",
                system_id="SYS_A",
            ),
        ),
        inter_module_couplings=(
            InterModuleCouplingSpec(
                source_module_id="MOD_A",
                source_port_name="out",
                target_module_id="MOD_MISSING",
                target_port_name="in",
                relation_type="drive",
            ),
        ),
    )

    with pytest.raises(ValueError, match="unknown target module_id"):
        validate_assembly_spec(assembly)


def test_validate_assembly_spec_rejects_unscoped_mode_gate():
    assembly = HierarchyAssemblySpec(
        module_specs=(
            ModuleSpec(
                module_id="MOD_A",
                subsystem_id="SUB_A",
                system_id="SYS_A",
            ),
            ModuleSpec(
                module_id="MOD_B",
                subsystem_id="SUB_A",
                system_id="SYS_A",
            ),
        ),
        inter_module_couplings=(
            InterModuleCouplingSpec(
                source_module_id="MOD_A",
                source_port_name="out",
                target_module_id="MOD_B",
                target_port_name="in",
                relation_type="drive",
                source_mode_gate=("normal",),
            ),
        ),
    )

    with pytest.raises(ValueError, match="source_mode_gate without source_mode_name"):
        validate_assembly_spec(assembly)


def test_validate_assembly_spec_rejects_unknown_port_when_ports_are_declared():
    assembly = HierarchyAssemblySpec(
        module_specs=(
            ModuleSpec(
                module_id="MOD_A",
                subsystem_id="SUB_A",
                system_id="SYS_A",
                output_ports=(PortSpec(port_name="out_ok", direction="output", value_datatype_label="numeric"),),
            ),
            ModuleSpec(
                module_id="MOD_B",
                subsystem_id="SUB_A",
                system_id="SYS_A",
                input_ports=(PortSpec(port_name="in_ok", direction="input", value_datatype_label="numeric"),),
            ),
        ),
        inter_module_couplings=(
            InterModuleCouplingSpec(
                source_module_id="MOD_A",
                source_port_name="out_missing",
                target_module_id="MOD_B",
                target_port_name="in_ok",
                relation_type="drive",
            ),
        ),
    )

    with pytest.raises(ValueError, match="unknown source port"):
        validate_assembly_spec(assembly)


def test_hierarchy_assembly_builder_builds_native_spec():
    builder = HierarchyAssemblyBuilder(metadata={"aircraft_model": "native"})
    builder.add_module(
        ModuleSpec(
            module_id="MOD_A",
            subsystem_id="SUB_A",
            system_id="SYS_A",
            output_ports=(PortSpec(port_name="bleed_out", direction="output", value_datatype_label="numeric"),),
        )
    )
    builder.add_module(
        ModuleSpec(
            module_id="MOD_B",
            subsystem_id="SUB_A",
            system_id="SYS_A",
            input_ports=(PortSpec(port_name="bleed_in", direction="input", value_datatype_label="numeric"),),
        )
    )
    builder.add_inter_module_coupling(
        InterModuleCouplingSpec(
            source_module_id="MOD_A",
            source_port_name="bleed_out",
            target_module_id="MOD_B",
            target_port_name="bleed_in",
            relation_type="drive",
        )
    )

    assembly = builder.build()

    assert isinstance(assembly, HierarchyAssemblySpec)
    assert assembly.metadata == {"aircraft_model": "native"}
    assert len(assembly.module_specs) == 2
    assert len(assembly.inter_module_couplings) == 1


def test_build_hierarchy_assembly_spec_validates_duplicate_modules():
    module_spec = ModuleSpec(module_id="MOD_A", subsystem_id="SUB_A", system_id="SYS_A")

    with pytest.raises(ValueError, match="duplicate module_id"):
        build_hierarchy_assembly_spec(
            module_specs=(module_spec, module_spec),
        )
