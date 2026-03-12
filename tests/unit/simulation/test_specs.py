from __future__ import annotations

import pytest

from libs.simulation import AircraftSpec, CouplingSpec, ModuleSpec, ParameterSpec, PortSpec, SubsystemSpec, SystemSpec


def test_aircraft_spec_iter_module_specs_exposes_nested_modules():
    aircraft = AircraftSpec(
        aircraft_id="A1",
        systems=(
            SystemSpec(
                system_id="SYS_A",
                subsystems=(
                    SubsystemSpec(
                        subsystem_id="SUB_A",
                        system_id="SYS_A",
                        modules=(
                            ModuleSpec(
                                module_id="MOD_A",
                                subsystem_id="SUB_A",
                                system_id="SYS_A",
                                parameters=(
                                    ParameterSpec(
                                        parameter_name="eng_temp",
                                        system_id="SYS_A",
                                        subsystem_id="SUB_A",
                                        module_id="MOD_A",
                                        parameter_datatype_label="numeric",
                                        unit="c",
                                        behavior_family_label="inertial",
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        ),
    )

    module_specs = aircraft.iter_module_specs()
    assert len(module_specs) == 1
    assert module_specs[0].module_id == "MOD_A"
    assert module_specs[0].parameters[0].parameter_name == "eng_temp"


def test_port_and_coupling_specs_are_instantiable():
    port = PortSpec.output(port_name="bleed_pressure_out", value_datatype_label="numeric", unit="psi")
    coupling = CouplingSpec.drive(
        source_module_id="MOD_A",
        source_port_name="bleed_pressure_out",
        target_module_id="MOD_B",
        target_port_name="pack_pressure_in",
        gain=0.9,
        lag_seconds=0.5,
    )

    assert port.port_name == "bleed_pressure_out"
    assert coupling.relation_type == "drive"
    assert coupling.lag_seconds == 0.5


def test_aircraft_coupling_and_aircraft_specs_are_instantiable():
    module_spec = ModuleSpec(
        module_id="mod_source",
        subsystem_id="sub_a",
        system_id="sys_a",
        output_ports=(PortSpec.output(port_name="bleed_out", value_datatype_label="numeric"),),
    )
    coupling = CouplingSpec.drive(
        source_module_id="mod_source",
        source_port_name="bleed_out",
        target_module_id="mod_target",
        target_port_name="bleed_in",
        gain=0.8,
        lag_seconds=0.2,
    )
    aircraft = AircraftSpec(
        aircraft_id="A1",
        systems=(
            SystemSpec(
                system_id="sys_a",
                subsystems=(
                    SubsystemSpec(
                        subsystem_id="sub_a",
                        system_id="sys_a",
                        modules=(module_spec,),
                    ),
                ),
            ),
        ),
        couplings=(coupling,),
    )

    assert aircraft.iter_module_specs()[0].module_id == "mod_source"
    assert aircraft.couplings[0].target_port_name == "bleed_in"


def test_aircraft_spec_validate_rejects_unknown_module_reference():
    aircraft = AircraftSpec(
        aircraft_id="A1",
        systems=(
            SystemSpec(
                system_id="SYS_A",
                subsystems=(
                    SubsystemSpec(
                        subsystem_id="SUB_A",
                        system_id="SYS_A",
                        modules=(
                            ModuleSpec(
                                module_id="MOD_A",
                                subsystem_id="SUB_A",
                                system_id="SYS_A",
                            ),
                        ),
                    ),
                ),
            ),
        ),
        couplings=(
            CouplingSpec.drive(
                source_module_id="MOD_A",
                source_port_name="out",
                target_module_id="MOD_MISSING",
                target_port_name="in",
            ),
        ),
    )

    with pytest.raises(ValueError, match="unknown target module_id"):
        aircraft.validate()


def test_aircraft_spec_validate_rejects_unscoped_mode_gate():
    aircraft = AircraftSpec(
        aircraft_id="A1",
        systems=(
            SystemSpec(
                system_id="SYS_A",
                subsystems=(
                    SubsystemSpec(
                        subsystem_id="SUB_A",
                        system_id="SYS_A",
                        modules=(
                            ModuleSpec(module_id="MOD_A", subsystem_id="SUB_A", system_id="SYS_A"),
                            ModuleSpec(module_id="MOD_B", subsystem_id="SUB_A", system_id="SYS_A"),
                        ),
                    ),
                ),
            ),
        ),
        couplings=(
            CouplingSpec(
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
        aircraft.validate()


def test_aircraft_spec_validate_rejects_unknown_port_when_ports_are_declared():
    aircraft = AircraftSpec(
        aircraft_id="A1",
        systems=(
            SystemSpec(
                system_id="SYS_A",
                subsystems=(
                    SubsystemSpec(
                        subsystem_id="SUB_A",
                        system_id="SYS_A",
                        modules=(
                            ModuleSpec(
                                module_id="MOD_A",
                                subsystem_id="SUB_A",
                                system_id="SYS_A",
                                output_ports=(PortSpec.output(port_name="out_ok", value_datatype_label="numeric"),),
                            ),
                            ModuleSpec(
                                module_id="MOD_B",
                                subsystem_id="SUB_A",
                                system_id="SYS_A",
                                input_ports=(PortSpec.input(port_name="in_ok", value_datatype_label="numeric"),),
                            ),
                        ),
                    ),
                ),
            ),
        ),
        couplings=(
            CouplingSpec.drive(
                source_module_id="MOD_A",
                source_port_name="out_missing",
                target_module_id="MOD_B",
                target_port_name="in_ok",
            ),
        ),
    )

    with pytest.raises(ValueError, match="unknown source port"):
        aircraft.validate()


def test_aircraft_spec_validate_rejects_duplicate_modules():
    module_spec = ModuleSpec(module_id="MOD_A", subsystem_id="SUB_A", system_id="SYS_A")
    aircraft = AircraftSpec(
        aircraft_id="A1",
        systems=(
            SystemSpec(
                system_id="SYS_A",
                subsystems=(
                    SubsystemSpec(
                        subsystem_id="SUB_A",
                        system_id="SYS_A",
                        modules=(module_spec, module_spec),
                    ),
                ),
            ),
        ),
    )

    with pytest.raises(ValueError, match="duplicate module_id"):
        aircraft.validate()
