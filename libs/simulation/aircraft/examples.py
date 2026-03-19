"""Example aircraft specs."""

from __future__ import annotations

from dataclasses import replace

from libs.simulation.aircraft.spec import AircraftSpec
from libs.simulation.coupling.examples import build_drive_coupling_spec, build_enable_coupling_spec
from libs.simulation.module.examples import (
    build_discrete_output_module_spec,
    build_single_parameter_module_spec,
)
from libs.simulation.module.spec import LatentUpdateSpec, ModuleSpec
from libs.simulation.parameter.examples import (
    build_categorical_parameter_spec,
    build_numeric_parameter_spec,
)
from libs.simulation.port.examples import (
    build_categorical_input_port_spec,
    build_categorical_output_port_spec,
    build_numeric_input_port_spec,
    build_numeric_output_port_spec,
)
from libs.simulation.subsystem.examples import build_subsystem_spec
from libs.simulation.system.examples import build_system_spec


def build_coupled_module_aircraft_spec() -> AircraftSpec:
    source_module = build_single_parameter_module_spec(
        module_id="MOD_SOURCE",
        subsystem_id="SUB_POWER",
        system_id="SYS_POWER",
        module_family="power_source",
        parameter_spec=build_numeric_parameter_spec(
            parameter_name="supply_voltage",
            system_id="SYS_POWER",
            subsystem_id="SUB_POWER",
            module_id="MOD_SOURCE",
            behavior_family_label="regulated",
            output_port_name="voltage_out",
            metadata={"example_role": "source"},
        ),
        output_ports=(build_numeric_output_port_spec(port_name="voltage_out", unit="V"),),
    )
    target_module = build_single_parameter_module_spec(
        module_id="MOD_TARGET",
        subsystem_id="SUB_LOAD",
        system_id="SYS_POWER",
        module_family="driven_load",
        parameter_spec=build_numeric_parameter_spec(
            parameter_name="motor_speed",
            system_id="SYS_POWER",
            subsystem_id="SUB_LOAD",
            module_id="MOD_TARGET",
            behavior_family_label="inertial",
            input_port_names=("command_in",),
            metadata={"example_role": "target"},
        ),
        input_ports=(build_numeric_input_port_spec(port_name="command_in", unit="V"),),
        latent_variables=("command_target",),
        latent_update_specs=(
            LatentUpdateSpec.from_input_port(
                latent_name="command_target",
                source_name="command_in",
                gain=1.0,
                sign=1,
                default_value=0.0,
            ),
        ),
    )
    return AircraftSpec(
        aircraft_id="coupled_module",
        systems=(
            build_system_spec(
                system_id="SYS_POWER",
                subsystems=(
                    build_subsystem_spec(
                        subsystem_id="SUB_POWER",
                        system_id="SYS_POWER",
                        modules=(source_module,),
                    ),
                    build_subsystem_spec(
                        subsystem_id="SUB_LOAD",
                        system_id="SYS_POWER",
                        modules=(target_module,),
                    ),
                ),
            ),
        ),
        couplings=(
            build_drive_coupling_spec(
                source_module_id="MOD_SOURCE",
                source_port_name="voltage_out",
                target_module_id="MOD_TARGET",
                target_port_name="command_in",
                gain=0.5,
            ),
        ),
        metadata={"example_name": "coupled_module"},
    )


def build_power_chain_aircraft_spec() -> AircraftSpec:
    switch_module = build_discrete_output_module_spec(
        module_id="MOD_SWITCH",
        subsystem_id="SUB_POWER",
        system_id="SYS_POWER",
        module_family="switch",
        parameter_spec=build_categorical_parameter_spec(
            parameter_name="contactor_state",
            system_id="SYS_POWER",
            subsystem_id="SUB_POWER",
            module_id="MOD_SWITCH",
            behavior_family_label="discrete_state",
            output_port_name="switch_out",
            metadata={"example_role": "switch"},
        ),
        output_ports=(build_categorical_output_port_spec(port_name="switch_out", unit="state"),),
    )
    source_module = ModuleSpec(
        module_id="MOD_SOURCE",
        subsystem_id="SUB_POWER",
        system_id="SYS_POWER",
        module_family="power_source",
        parameters=(
            build_numeric_parameter_spec(
                parameter_name="supply_voltage",
                system_id="SYS_POWER",
                subsystem_id="SUB_POWER",
                module_id="MOD_SOURCE",
                behavior_family_label="regulated",
                input_port_names=("enable_in",),
                output_port_name="voltage_out",
                metadata={"example_role": "source_voltage"},
            ),
            build_numeric_parameter_spec(
                parameter_name="fuel_flow_rate",
                system_id="SYS_POWER",
                subsystem_id="SUB_POWER",
                module_id="MOD_SOURCE",
                behavior_family_label="regulated",
                input_port_names=("enable_in",),
                output_port_name="flow_out",
                metadata={"example_role": "source_flow"},
            ),
        ),
        input_ports=(build_numeric_input_port_spec(port_name="enable_in", unit="state"),),
        output_ports=(
            build_numeric_output_port_spec(port_name="voltage_out", unit="V"),
            build_numeric_output_port_spec(port_name="flow_out", unit="kg_s"),
        ),
        latent_variables=("setpoint_voltage", "setpoint_flow"),
        latent_update_specs=(
            LatentUpdateSpec.from_input_port(
                latent_name="setpoint_voltage",
                source_name="enable_in",
                gain=28.0,
                sign=1,
                default_value=0.0,
                clamp_min=0.0,
            ),
            LatentUpdateSpec.from_input_port(
                latent_name="setpoint_flow",
                source_name="enable_in",
                gain=2.0,
                sign=1,
                default_value=0.0,
                clamp_min=0.0,
            ),
        ),
    )
    target_module = build_single_parameter_module_spec(
        module_id="MOD_TARGET",
        subsystem_id="SUB_LOAD",
        system_id="SYS_POWER",
        module_family="driven_load",
        parameter_spec=build_numeric_parameter_spec(
            parameter_name="motor_speed",
            system_id="SYS_POWER",
            subsystem_id="SUB_LOAD",
            module_id="MOD_TARGET",
            behavior_family_label="inertial",
            input_port_names=("command_in",),
            metadata={"example_role": "target_speed"},
        ),
        input_ports=(build_numeric_input_port_spec(port_name="command_in", unit="V"),),
        latent_variables=("command_target",),
        latent_update_specs=(
            LatentUpdateSpec.from_input_port(
                latent_name="command_target",
                source_name="command_in",
                gain=1.0,
                sign=1,
                default_value=0.0,
            ),
        ),
    )
    tank_module = build_single_parameter_module_spec(
        module_id="MOD_TANK",
        subsystem_id="SUB_FUEL",
        system_id="SYS_FUEL",
        module_family="accumulator",
        parameter_spec=build_numeric_parameter_spec(
            parameter_name="fuel_used_total",
            system_id="SYS_FUEL",
            subsystem_id="SUB_FUEL",
            module_id="MOD_TANK",
            behavior_family_label="accumulative",
            input_port_names=("flow_in",),
            metadata={"example_role": "fuel_total"},
        ),
        input_ports=(build_numeric_input_port_spec(port_name="flow_in", unit="kg_s"),),
        latent_variables=("flow_rate",),
        latent_update_specs=(
            LatentUpdateSpec.from_input_port(
                latent_name="flow_rate",
                source_name="flow_in",
                gain=1.0,
                sign=1,
                default_value=0.0,
                clamp_min=0.0,
            ),
        ),
    )
    return AircraftSpec(
        aircraft_id="multibehavior",
        systems=(
            build_system_spec(
                system_id="SYS_POWER",
                subsystems=(
                    build_subsystem_spec(
                        subsystem_id="SUB_POWER",
                        system_id="SYS_POWER",
                        modules=(switch_module, source_module),
                    ),
                    build_subsystem_spec(
                        subsystem_id="SUB_LOAD",
                        system_id="SYS_POWER",
                        modules=(target_module,),
                    ),
                ),
            ),
            build_system_spec(
                system_id="SYS_FUEL",
                subsystems=(
                    build_subsystem_spec(
                        subsystem_id="SUB_FUEL",
                        system_id="SYS_FUEL",
                        modules=(tank_module,),
                    ),
                ),
            ),
        ),
        couplings=(
            build_drive_coupling_spec(
                source_module_id="MOD_SWITCH",
                source_port_name="switch_out",
                target_module_id="MOD_SOURCE",
                target_port_name="enable_in",
                gain=1.0,
            ),
            build_drive_coupling_spec(
                source_module_id="MOD_SOURCE",
                source_port_name="voltage_out",
                target_module_id="MOD_TARGET",
                target_port_name="command_in",
                gain=0.5,
            ),
            build_drive_coupling_spec(
                source_module_id="MOD_SOURCE",
                source_port_name="flow_out",
                target_module_id="MOD_TANK",
                target_port_name="flow_in",
                gain=1.0,
            ),
        ),
        metadata={"example_name": "multibehavior"},
    )


def build_pressurization_aircraft_spec() -> AircraftSpec:
    mode_module = build_discrete_output_module_spec(
        module_id="MOD_PRESS_MODE",
        subsystem_id="SUB_ECS",
        system_id="SYS_ECS",
        module_family="pressurization_mode",
        parameter_spec=build_categorical_parameter_spec(
            parameter_name="press_mode_state",
            system_id="SYS_ECS",
            subsystem_id="SUB_ECS",
            module_id="MOD_PRESS_MODE",
            behavior_family_label="discrete_state",
            output_port_name="press_mode_out",
            metadata={"example_role": "press_mode"},
        ),
        output_ports=(build_categorical_output_port_spec(port_name="press_mode_out", unit="state"),),
    )
    altitude_module = build_single_parameter_module_spec(
        module_id="MOD_AIRCRAFT_ALT",
        subsystem_id="SUB_AIRFRAME",
        system_id="SYS_AIRFRAME",
        module_family="aircraft_altitude_source",
        parameter_spec=build_numeric_parameter_spec(
            parameter_name="aircraft_altitude_ft",
            system_id="SYS_AIRFRAME",
            subsystem_id="SUB_AIRFRAME",
            module_id="MOD_AIRCRAFT_ALT",
            behavior_family_label="inertial",
            output_port_name="aircraft_altitude_out",
            metadata={"example_role": "aircraft_altitude"},
        ),
        output_ports=(build_numeric_output_port_spec(port_name="aircraft_altitude_out", unit="ft"),),
    )
    controller_module = build_single_parameter_module_spec(
        module_id="MOD_PRESS_CTRL",
        subsystem_id="SUB_ECS",
        system_id="SYS_ECS",
        module_family="pressurization_controller",
        parameter_spec=build_numeric_parameter_spec(
            parameter_name="outflow_valve_pct",
            system_id="SYS_ECS",
            subsystem_id="SUB_ECS",
            module_id="MOD_PRESS_CTRL",
            behavior_family_label="regulated",
            input_port_names=("aircraft_altitude_in", "press_mode_in"),
            output_port_name="outflow_cmd_out",
            metadata={"example_role": "outflow_valve"},
        ),
        input_ports=(
            build_numeric_input_port_spec(port_name="aircraft_altitude_in", unit="ft"),
            build_categorical_input_port_spec(port_name="press_mode_in", unit="state"),
        ),
        output_ports=(build_numeric_output_port_spec(port_name="outflow_cmd_out", unit="pct"),),
        latent_variables=("outflow_setpoint",),
        latent_update_specs=(
            LatentUpdateSpec.from_input_port(
                latent_name="outflow_setpoint",
                source_name="aircraft_altitude_in",
                gain=0.01,
                sign=1,
                default_value=0.0,
                clamp_min=0.0,
                clamp_max=100.0,
            ),
        ),
    )
    cabin_module = ModuleSpec(
        module_id="MOD_CABIN",
        subsystem_id="SUB_ECS",
        system_id="SYS_ECS",
        module_family="cabin_response",
        parameters=(
            build_numeric_parameter_spec(
                parameter_name="cabin_altitude_ft",
                system_id="SYS_ECS",
                subsystem_id="SUB_ECS",
                module_id="MOD_CABIN",
                behavior_family_label="inertial",
                input_port_names=("outflow_cmd_in",),
                metadata={"example_role": "cabin_altitude"},
            ),
            build_numeric_parameter_spec(
                parameter_name="cabin_delta_p_psi",
                system_id="SYS_ECS",
                subsystem_id="SUB_ECS",
                module_id="MOD_CABIN",
                behavior_family_label="regulated",
                input_port_names=("outflow_cmd_in",),
                metadata={"example_role": "delta_p"},
            ),
        ),
        input_ports=(build_numeric_input_port_spec(port_name="outflow_cmd_in", unit="pct"),),
        latent_variables=("cabin_alt_target", "delta_p_target"),
        latent_update_specs=(
            LatentUpdateSpec.from_input_port(
                latent_name="cabin_alt_target",
                source_name="outflow_cmd_in",
                gain=80.0,
                sign=1,
                default_value=0.0,
                clamp_min=0.0,
                clamp_max=8000.0,
            ),
            LatentUpdateSpec.from_input_port(
                latent_name="delta_p_target",
                source_name="outflow_cmd_in",
                gain=0.08,
                sign=1,
                default_value=0.0,
                clamp_min=0.0,
                clamp_max=8.5,
            ),
        ),
    )
    return AircraftSpec(
        aircraft_id="pressurization",
        systems=(
            build_system_spec(
                system_id="SYS_ECS",
                subsystems=(
                    build_subsystem_spec(
                        subsystem_id="SUB_ECS",
                        system_id="SYS_ECS",
                        modules=(mode_module, controller_module, cabin_module),
                    ),
                ),
            ),
            build_system_spec(
                system_id="SYS_AIRFRAME",
                subsystems=(
                    build_subsystem_spec(
                        subsystem_id="SUB_AIRFRAME",
                        system_id="SYS_AIRFRAME",
                        modules=(altitude_module,),
                    ),
                ),
            ),
        ),
        couplings=(
            build_drive_coupling_spec(
                source_module_id="MOD_PRESS_MODE",
                source_port_name="press_mode_out",
                target_module_id="MOD_PRESS_CTRL",
                target_port_name="press_mode_in",
            ),
            build_drive_coupling_spec(
                source_module_id="MOD_AIRCRAFT_ALT",
                source_port_name="aircraft_altitude_out",
                target_module_id="MOD_PRESS_CTRL",
                target_port_name="aircraft_altitude_in",
            ),
            build_drive_coupling_spec(
                source_module_id="MOD_PRESS_CTRL",
                source_port_name="outflow_cmd_out",
                target_module_id="MOD_CABIN",
                target_port_name="outflow_cmd_in",
                gain=1.0,
                lag_seconds=1.0,
            ),
        ),
        metadata={"example_name": "pressurization"},
    )


def build_power_pressurization_hierarchy_composite_aircraft_spec() -> AircraftSpec:
    power_switch_module = ModuleSpec(
        module_id="MOD_PWR_SWITCH",
        subsystem_id="SUB_POWER_DIST",
        system_id="SYS_POWER",
        module_family="power_switching",
        parameters=(
            build_categorical_parameter_spec(
                parameter_name="master_power_state",
                system_id="SYS_POWER",
                subsystem_id="SUB_POWER_DIST",
                module_id="MOD_PWR_SWITCH",
                behavior_family_label="discrete_state",
                output_port_name="master_power_out",
                allowed_fault_families=("illegal_transition", "dwell_violation", "state_chatter", "stuck_state"),
                metadata={"example_role": "master_power"},
            ),
            build_categorical_parameter_spec(
                parameter_name="generator_tie_state",
                system_id="SYS_POWER",
                subsystem_id="SUB_POWER_DIST",
                module_id="MOD_PWR_SWITCH",
                behavior_family_label="discrete_state",
                output_port_name="generator_tie_out",
                allowed_fault_families=("illegal_transition", "dwell_violation", "state_chatter", "stuck_state"),
                metadata={"example_role": "generator_tie"},
            ),
        ),
        output_ports=(
            build_categorical_output_port_spec(port_name="master_power_out"),
            build_categorical_output_port_spec(port_name="generator_tie_out"),
        ),
        state_machines=("master_power", "generator_tie"),
    )
    power_source_module = ModuleSpec(
        module_id="MOD_PWR_SOURCE",
        subsystem_id="SUB_POWER_DIST",
        system_id="SYS_POWER",
        module_family="power_source",
        parameters=(
            build_numeric_parameter_spec(
                parameter_name="bus_voltage_v",
                system_id="SYS_POWER",
                subsystem_id="SUB_POWER_DIST",
                module_id="MOD_PWR_SOURCE",
                behavior_family_label="regulated",
                unit="V",
                input_port_names=("master_enable_in",),
                output_port_name="bus_voltage_out",
                allowed_fault_families=("offset", "saturation", "tracking_degradation", "oscillation"),
                metadata={"example_role": "bus_voltage"},
            ),
            build_numeric_parameter_spec(
                parameter_name="bus_current_a",
                system_id="SYS_POWER",
                subsystem_id="SUB_POWER_DIST",
                module_id="MOD_PWR_SOURCE",
                behavior_family_label="regulated",
                unit="A",
                input_port_names=("master_enable_in",),
                output_port_name="bus_current_out",
                allowed_fault_families=("offset", "saturation", "tracking_degradation", "oscillation"),
                metadata={"example_role": "bus_current"},
            ),
        ),
        input_ports=(build_numeric_input_port_spec(port_name="master_enable_in", unit="state"),),
        output_ports=(
            build_numeric_output_port_spec(port_name="bus_voltage_out", unit="V"),
            build_numeric_output_port_spec(port_name="bus_current_out", unit="A"),
        ),
        latent_variables=("voltage_target", "current_target"),
        latent_update_specs=(
            LatentUpdateSpec.from_input_port(
                latent_name="voltage_target",
                source_name="master_enable_in",
                gain=28.0,
                sign=1,
                default_value=0.0,
                clamp_min=0.0,
                clamp_max=30.0,
            ),
            LatentUpdateSpec.from_input_port(
                latent_name="current_target",
                source_name="master_enable_in",
                gain=48.0,
                sign=1,
                default_value=0.0,
                clamp_min=0.0,
                clamp_max=60.0,
            ),
        ),
    )
    compressor_drive_module = ModuleSpec(
        module_id="MOD_COMP_DRIVE",
        subsystem_id="SUB_POWER_LOAD",
        system_id="SYS_POWER",
        module_family="compressor_drive",
        parameters=(
            build_numeric_parameter_spec(
                parameter_name="compressor_speed_pct",
                system_id="SYS_POWER",
                subsystem_id="SUB_POWER_LOAD",
                module_id="MOD_COMP_DRIVE",
                behavior_family_label="inertial",
                unit="pct",
                input_port_names=("voltage_in",),
                output_port_name="compressor_speed_out",
                allowed_fault_families=("timing_lag", "increased_time_constant", "stuck_value", "ramp_distortion"),
                metadata={"example_role": "compressor_speed"},
            ),
            build_numeric_parameter_spec(
                parameter_name="compressor_energy_total_kwh",
                system_id="SYS_POWER",
                subsystem_id="SUB_POWER_LOAD",
                module_id="MOD_COMP_DRIVE",
                behavior_family_label="accumulative",
                unit="kWh",
                input_port_names=("current_in",),
                allowed_fault_families=("reset_drop", "leak_rate", "drift", "bias"),
                metadata={"example_role": "compressor_energy"},
            ),
        ),
        input_ports=(
            build_numeric_input_port_spec(port_name="voltage_in", unit="V"),
            build_numeric_input_port_spec(port_name="current_in", unit="A"),
        ),
        output_ports=(build_numeric_output_port_spec(port_name="compressor_speed_out", unit="pct"),),
        latent_variables=("speed_target", "energy_rate"),
        latent_update_specs=(
            LatentUpdateSpec.from_input_port(
                latent_name="speed_target",
                source_name="voltage_in",
                gain=3.0,
                sign=1,
                default_value=0.0,
                clamp_min=0.0,
                clamp_max=100.0,
            ),
            LatentUpdateSpec.from_input_port(
                latent_name="energy_rate",
                source_name="current_in",
                gain=0.02,
                sign=1,
                default_value=0.0,
                clamp_min=0.0,
            ),
        ),
    )
    power_load_module = ModuleSpec(
        module_id="MOD_PWR_LOAD_MON",
        subsystem_id="SUB_POWER_LOAD",
        system_id="SYS_POWER",
        module_family="power_load_monitor",
        parameters=(
            build_numeric_parameter_spec(
                parameter_name="electrical_load_pct",
                system_id="SYS_POWER",
                subsystem_id="SUB_POWER_LOAD",
                module_id="MOD_PWR_LOAD_MON",
                behavior_family_label="regulated",
                unit="pct",
                input_port_names=("current_in",),
                output_port_name="load_pct_out",
                allowed_fault_families=("offset", "saturation", "tracking_degradation", "oscillation"),
                metadata={"example_role": "electrical_load"},
            ),
            build_numeric_parameter_spec(
                parameter_name="inverter_temp_c",
                system_id="SYS_POWER",
                subsystem_id="SUB_POWER_LOAD",
                module_id="MOD_PWR_LOAD_MON",
                behavior_family_label="inertial",
                unit="C",
                input_port_names=("current_in",),
                output_port_name="inverter_temp_out",
                allowed_fault_families=("timing_lag", "increased_time_constant", "stuck_value", "ramp_distortion"),
                metadata={"example_role": "inverter_temp"},
            ),
        ),
        input_ports=(build_numeric_input_port_spec(port_name="current_in", unit="A"),),
        output_ports=(
            build_numeric_output_port_spec(port_name="load_pct_out", unit="pct"),
            build_numeric_output_port_spec(port_name="inverter_temp_out", unit="C"),
        ),
        latent_variables=("load_target", "temp_target"),
        latent_update_specs=(
            LatentUpdateSpec.from_input_port(
                latent_name="load_target",
                source_name="current_in",
                gain=1.5,
                sign=1,
                default_value=0.0,
                clamp_min=0.0,
                clamp_max=100.0,
            ),
            LatentUpdateSpec.from_input_port(
                latent_name="temp_target",
                source_name="current_in",
                gain=0.8,
                sign=1,
                offset=18.0,
                default_value=18.0,
                clamp_min=-20.0,
                clamp_max=120.0,
            ),
        ),
    )
    press_mode_module = ModuleSpec(
        module_id="MOD_PRESS_MODE",
        subsystem_id="SUB_ECS_CONTROL",
        system_id="SYS_ECS",
        module_family="pressurization_mode",
        parameters=(
            build_categorical_parameter_spec(
                parameter_name="press_mode_state",
                system_id="SYS_ECS",
                subsystem_id="SUB_ECS_CONTROL",
                module_id="MOD_PRESS_MODE",
                behavior_family_label="discrete_state",
                output_port_name="press_mode_out",
                allowed_fault_families=("illegal_transition", "dwell_violation", "state_chatter", "stuck_state"),
                metadata={"example_role": "press_mode"},
            ),
            build_categorical_parameter_spec(
                parameter_name="pack_mode_state",
                system_id="SYS_ECS",
                subsystem_id="SUB_ECS_CONTROL",
                module_id="MOD_PRESS_MODE",
                behavior_family_label="discrete_state",
                output_port_name="pack_mode_out",
                allowed_fault_families=("illegal_transition", "dwell_violation", "state_chatter", "stuck_state"),
                metadata={"example_role": "pack_mode"},
            ),
        ),
        output_ports=(
            build_categorical_output_port_spec(port_name="press_mode_out"),
            build_categorical_output_port_spec(port_name="pack_mode_out"),
        ),
        state_machines=("press_mode", "pack_mode"),
    )
    press_controller_module = ModuleSpec(
        module_id="MOD_PRESS_CTRL",
        subsystem_id="SUB_ECS_CONTROL",
        system_id="SYS_ECS",
        module_family="pressurization_controller",
        parameters=(
            build_numeric_parameter_spec(
                parameter_name="outflow_cmd_pct",
                system_id="SYS_ECS",
                subsystem_id="SUB_ECS_CONTROL",
                module_id="MOD_PRESS_CTRL",
                behavior_family_label="regulated",
                unit="pct",
                input_port_names=("aircraft_altitude_in", "press_mode_in"),
                output_port_name="outflow_cmd_out",
                allowed_fault_families=("offset", "saturation", "tracking_degradation", "oscillation"),
                metadata={"example_role": "outflow_command"},
            ),
            build_numeric_parameter_spec(
                parameter_name="pack_flow_cmd_pct",
                system_id="SYS_ECS",
                subsystem_id="SUB_ECS_CONTROL",
                module_id="MOD_PRESS_CTRL",
                behavior_family_label="regulated",
                unit="pct",
                input_port_names=("aircraft_altitude_in", "press_mode_in"),
                output_port_name="pack_flow_cmd_out",
                allowed_fault_families=("offset", "saturation", "tracking_degradation", "oscillation"),
                metadata={"example_role": "pack_flow_command"},
            ),
        ),
        input_ports=(
            build_numeric_input_port_spec(port_name="aircraft_altitude_in", unit="ft"),
            build_categorical_input_port_spec(port_name="press_mode_in"),
        ),
        output_ports=(
            build_numeric_output_port_spec(port_name="outflow_cmd_out", unit="pct"),
            build_numeric_output_port_spec(port_name="pack_flow_cmd_out", unit="pct"),
        ),
        latent_variables=("outflow_target", "pack_flow_target"),
        latent_update_specs=(
            LatentUpdateSpec.from_input_port(
                latent_name="outflow_target",
                source_name="aircraft_altitude_in",
                gain=0.0025,
                sign=1,
                default_value=5.0,
                clamp_min=0.0,
                clamp_max=100.0,
            ),
            LatentUpdateSpec.from_input_port(
                latent_name="pack_flow_target",
                source_name="aircraft_altitude_in",
                gain=0.002,
                sign=1,
                default_value=10.0,
                clamp_min=0.0,
                clamp_max=100.0,
            ),
        ),
    )
    outflow_actuator_module = ModuleSpec(
        module_id="MOD_OUTFLOW_ACT",
        subsystem_id="SUB_ECS_CABIN",
        system_id="SYS_ECS",
        module_family="outflow_actuator",
        parameters=(
            build_numeric_parameter_spec(
                parameter_name="actuator_position_pct",
                system_id="SYS_ECS",
                subsystem_id="SUB_ECS_CABIN",
                module_id="MOD_OUTFLOW_ACT",
                behavior_family_label="inertial",
                unit="pct",
                input_port_names=("outflow_cmd_in",),
                output_port_name="actuator_pos_out",
                allowed_fault_families=("timing_lag", "increased_time_constant", "stuck_value", "ramp_distortion"),
                metadata={"example_role": "actuator_position"},
            ),
            build_numeric_parameter_spec(
                parameter_name="actuator_load_pct",
                system_id="SYS_ECS",
                subsystem_id="SUB_ECS_CABIN",
                module_id="MOD_OUTFLOW_ACT",
                behavior_family_label="regulated",
                unit="pct",
                input_port_names=("outflow_cmd_in",),
                output_port_name="actuator_load_out",
                allowed_fault_families=("offset", "saturation", "tracking_degradation", "oscillation"),
                metadata={"example_role": "actuator_load"},
            ),
        ),
        input_ports=(build_numeric_input_port_spec(port_name="outflow_cmd_in", unit="pct"),),
        output_ports=(
            build_numeric_output_port_spec(port_name="actuator_pos_out", unit="pct"),
            build_numeric_output_port_spec(port_name="actuator_load_out", unit="pct"),
        ),
        latent_variables=("actuator_target", "actuator_load_target"),
        latent_update_specs=(
            LatentUpdateSpec.from_input_port(
                latent_name="actuator_target",
                source_name="outflow_cmd_in",
                gain=1.0,
                sign=1,
                default_value=0.0,
                clamp_min=0.0,
                clamp_max=100.0,
            ),
            LatentUpdateSpec.from_input_port(
                latent_name="actuator_load_target",
                source_name="outflow_cmd_in",
                gain=0.7,
                sign=1,
                default_value=0.0,
                clamp_min=0.0,
                clamp_max=100.0,
            ),
        ),
    )
    cabin_module = ModuleSpec(
        module_id="MOD_CABIN",
        subsystem_id="SUB_ECS_CABIN",
        system_id="SYS_ECS",
        module_family="cabin_response",
        parameters=(
            build_numeric_parameter_spec(
                parameter_name="cabin_altitude_ft",
                system_id="SYS_ECS",
                subsystem_id="SUB_ECS_CABIN",
                module_id="MOD_CABIN",
                behavior_family_label="inertial",
                unit="ft",
                input_port_names=("actuator_pos_in", "ambient_pressure_in"),
                output_port_name="cabin_altitude_out",
                allowed_fault_families=("timing_lag", "increased_time_constant", "stuck_value", "ramp_distortion"),
                metadata={"example_role": "cabin_altitude"},
            ),
            build_numeric_parameter_spec(
                parameter_name="cabin_delta_p_psi",
                system_id="SYS_ECS",
                subsystem_id="SUB_ECS_CABIN",
                module_id="MOD_CABIN",
                behavior_family_label="regulated",
                unit="psi",
                input_port_names=("bleed_psi_in", "pack_flow_in"),
                output_port_name="cabin_delta_p_out",
                allowed_fault_families=("offset", "saturation", "tracking_degradation", "oscillation"),
                metadata={"example_role": "cabin_delta_p"},
            ),
        ),
        input_ports=(
            build_numeric_input_port_spec(port_name="actuator_pos_in", unit="pct"),
            build_numeric_input_port_spec(port_name="ambient_pressure_in", unit="kPa"),
            build_numeric_input_port_spec(port_name="bleed_psi_in", unit="psi"),
            build_numeric_input_port_spec(port_name="pack_flow_in", unit="pct"),
        ),
        output_ports=(
            build_numeric_output_port_spec(port_name="cabin_altitude_out", unit="ft"),
            build_numeric_output_port_spec(port_name="cabin_delta_p_out", unit="psi"),
        ),
        latent_variables=("cabin_alt_target", "delta_p_target"),
        latent_update_specs=(
            LatentUpdateSpec.from_input_port(
                latent_name="cabin_alt_target",
                source_name="actuator_pos_in",
                gain=95.0,
                sign=1,
                default_value=0.0,
                clamp_min=0.0,
                clamp_max=12000.0,
            ),
            LatentUpdateSpec.from_input_port(
                latent_name="delta_p_target",
                source_name="bleed_psi_in",
                gain=0.25,
                sign=1,
                default_value=0.0,
                clamp_min=0.0,
                clamp_max=9.0,
            ),
        ),
    )
    aircraft_state_module = ModuleSpec(
        module_id="MOD_AIRCRAFT_STATE",
        subsystem_id="SUB_AIR_ENV",
        system_id="SYS_AIRFRAME",
        module_family="aircraft_state",
        parameters=(
            build_numeric_parameter_spec(
                parameter_name="aircraft_altitude_ft",
                system_id="SYS_AIRFRAME",
                subsystem_id="SUB_AIR_ENV",
                module_id="MOD_AIRCRAFT_STATE",
                behavior_family_label="inertial",
                unit="ft",
                output_port_name="aircraft_altitude_out",
                allowed_fault_families=("timing_lag", "increased_time_constant", "stuck_value", "ramp_distortion"),
                metadata={"example_role": "aircraft_altitude"},
            ),
            build_numeric_parameter_spec(
                parameter_name="vertical_speed_fpm",
                system_id="SYS_AIRFRAME",
                subsystem_id="SUB_AIR_ENV",
                module_id="MOD_AIRCRAFT_STATE",
                behavior_family_label="inertial",
                unit="fpm",
                output_port_name="vertical_speed_out",
                allowed_fault_families=("timing_lag", "increased_time_constant", "stuck_value", "ramp_distortion"),
                metadata={"example_role": "vertical_speed"},
            ),
        ),
        output_ports=(
            build_numeric_output_port_spec(port_name="aircraft_altitude_out", unit="ft"),
            build_numeric_output_port_spec(port_name="vertical_speed_out", unit="fpm"),
        ),
    )
    ambient_module = ModuleSpec(
        module_id="MOD_AMBIENT",
        subsystem_id="SUB_AIR_ENV",
        system_id="SYS_AIRFRAME",
        module_family="ambient_reference",
        parameters=(
            build_numeric_parameter_spec(
                parameter_name="ambient_pressure_kpa",
                system_id="SYS_AIRFRAME",
                subsystem_id="SUB_AIR_ENV",
                module_id="MOD_AMBIENT",
                behavior_family_label="regulated",
                unit="kPa",
                input_port_names=("aircraft_altitude_in",),
                output_port_name="ambient_pressure_out",
                allowed_fault_families=("offset", "saturation", "tracking_degradation", "oscillation"),
                metadata={"example_role": "ambient_pressure"},
            ),
            build_numeric_parameter_spec(
                parameter_name="ambient_temp_c",
                system_id="SYS_AIRFRAME",
                subsystem_id="SUB_AIR_ENV",
                module_id="MOD_AMBIENT",
                behavior_family_label="regulated",
                unit="C",
                input_port_names=("aircraft_altitude_in",),
                output_port_name="ambient_temp_out",
                allowed_fault_families=("offset", "saturation", "tracking_degradation", "oscillation"),
                metadata={"example_role": "ambient_temperature"},
            ),
        ),
        input_ports=(build_numeric_input_port_spec(port_name="aircraft_altitude_in", unit="ft"),),
        output_ports=(
            build_numeric_output_port_spec(port_name="ambient_pressure_out", unit="kPa"),
            build_numeric_output_port_spec(port_name="ambient_temp_out", unit="C"),
        ),
        latent_variables=("pressure_target", "temperature_target"),
        latent_update_specs=(
            LatentUpdateSpec.from_input_port(
                latent_name="pressure_target",
                source_name="aircraft_altitude_in",
                gain=0.002,
                sign=-1,
                offset=101.3,
                default_value=101.3,
                clamp_min=18.0,
                clamp_max=101.3,
            ),
            LatentUpdateSpec.from_input_port(
                latent_name="temperature_target",
                source_name="aircraft_altitude_in",
                gain=0.0018,
                sign=-1,
                offset=22.0,
                default_value=22.0,
                clamp_min=-60.0,
                clamp_max=30.0,
            ),
        ),
    )
    bleed_supply_module = ModuleSpec(
        module_id="MOD_BLEED_SUPPLY",
        subsystem_id="SUB_AIR_BLEED",
        system_id="SYS_AIRFRAME",
        module_family="bleed_supply",
        parameters=(
            build_numeric_parameter_spec(
                parameter_name="bleed_supply_psi",
                system_id="SYS_AIRFRAME",
                subsystem_id="SUB_AIR_BLEED",
                module_id="MOD_BLEED_SUPPLY",
                behavior_family_label="regulated",
                unit="psi",
                input_port_names=("compressor_speed_in",),
                output_port_name="bleed_psi_out",
                allowed_fault_families=("offset", "saturation", "tracking_degradation", "oscillation"),
                metadata={"example_role": "bleed_supply"},
            ),
            build_numeric_parameter_spec(
                parameter_name="bleed_usage_total",
                system_id="SYS_AIRFRAME",
                subsystem_id="SUB_AIR_BLEED",
                module_id="MOD_BLEED_SUPPLY",
                behavior_family_label="accumulative",
                unit="arb",
                input_port_names=("compressor_speed_in",),
                output_port_name="bleed_usage_out",
                allowed_fault_families=("reset_drop", "leak_rate", "drift", "bias"),
                metadata={"example_role": "bleed_usage"},
            ),
        ),
        input_ports=(build_numeric_input_port_spec(port_name="compressor_speed_in", unit="pct"),),
        output_ports=(
            build_numeric_output_port_spec(port_name="bleed_psi_out", unit="psi"),
            build_numeric_output_port_spec(port_name="bleed_usage_out", unit="arb"),
        ),
        latent_variables=("bleed_pressure_target", "bleed_usage_rate"),
        latent_update_specs=(
            LatentUpdateSpec.from_input_port(
                latent_name="bleed_pressure_target",
                source_name="compressor_speed_in",
                gain=0.24,
                sign=1,
                default_value=0.0,
                clamp_min=0.0,
                clamp_max=35.0,
            ),
            LatentUpdateSpec.from_input_port(
                latent_name="bleed_usage_rate",
                source_name="compressor_speed_in",
                gain=0.03,
                sign=1,
                default_value=0.0,
                clamp_min=0.0,
            ),
        ),
        state_machines=("supply_mode",),
    )
    pack_flow_module = ModuleSpec(
        module_id="MOD_PACK_FLOW",
        subsystem_id="SUB_AIR_BLEED",
        system_id="SYS_AIRFRAME",
        module_family="pack_flow",
        parameters=(
            build_numeric_parameter_spec(
                parameter_name="pack_flow_rate_pct",
                system_id="SYS_AIRFRAME",
                subsystem_id="SUB_AIR_BLEED",
                module_id="MOD_PACK_FLOW",
                behavior_family_label="inertial",
                unit="pct",
                input_port_names=("bleed_psi_in",),
                output_port_name="pack_flow_out",
                allowed_fault_families=("timing_lag", "increased_time_constant", "stuck_value", "ramp_distortion"),
                metadata={"example_role": "pack_flow"},
            ),
            build_numeric_parameter_spec(
                parameter_name="pack_temp_c",
                system_id="SYS_AIRFRAME",
                subsystem_id="SUB_AIR_BLEED",
                module_id="MOD_PACK_FLOW",
                behavior_family_label="regulated",
                unit="C",
                input_port_names=("bleed_psi_in",),
                output_port_name="pack_temp_out",
                allowed_fault_families=("offset", "saturation", "tracking_degradation", "oscillation"),
                metadata={"example_role": "pack_temperature"},
            ),
        ),
        input_ports=(build_numeric_input_port_spec(port_name="bleed_psi_in", unit="psi"),),
        output_ports=(
            build_numeric_output_port_spec(port_name="pack_flow_out", unit="pct"),
            build_numeric_output_port_spec(port_name="pack_temp_out", unit="C"),
        ),
        latent_variables=("pack_flow_target", "pack_temp_target"),
        latent_update_specs=(
            LatentUpdateSpec.from_input_port(
                latent_name="pack_flow_target",
                source_name="bleed_psi_in",
                gain=3.0,
                sign=1,
                default_value=0.0,
                clamp_min=0.0,
                clamp_max=100.0,
            ),
            LatentUpdateSpec.from_input_port(
                latent_name="pack_temp_target",
                source_name="bleed_psi_in",
                gain=0.9,
                sign=1,
                offset=5.0,
                default_value=5.0,
                clamp_min=-20.0,
                clamp_max=80.0,
            ),
        ),
        state_machines=("pack_mode",),
    )
    return AircraftSpec(
        aircraft_id="power_pressurization_hierarchy_composite",
        systems=(
            build_system_spec(
                system_id="SYS_POWER",
                subsystems=(
                    build_subsystem_spec(
                        subsystem_id="SUB_POWER_DIST",
                        system_id="SYS_POWER",
                        modules=(power_switch_module, power_source_module),
                    ),
                    build_subsystem_spec(
                        subsystem_id="SUB_POWER_LOAD",
                        system_id="SYS_POWER",
                        modules=(compressor_drive_module, power_load_module),
                    ),
                ),
            ),
            build_system_spec(
                system_id="SYS_ECS",
                subsystems=(
                    build_subsystem_spec(
                        subsystem_id="SUB_ECS_CONTROL",
                        system_id="SYS_ECS",
                        modules=(press_mode_module, press_controller_module),
                    ),
                    build_subsystem_spec(
                        subsystem_id="SUB_ECS_CABIN",
                        system_id="SYS_ECS",
                        modules=(outflow_actuator_module, cabin_module),
                    ),
                ),
            ),
            build_system_spec(
                system_id="SYS_AIRFRAME",
                subsystems=(
                    build_subsystem_spec(
                        subsystem_id="SUB_AIR_ENV",
                        system_id="SYS_AIRFRAME",
                        modules=(aircraft_state_module, ambient_module),
                    ),
                    build_subsystem_spec(
                        subsystem_id="SUB_AIR_BLEED",
                        system_id="SYS_AIRFRAME",
                        modules=(bleed_supply_module, pack_flow_module),
                    ),
                ),
            ),
        ),
        couplings=(
            build_enable_coupling_spec(
                source_module_id="MOD_PWR_SWITCH",
                source_port_name="master_power_out",
                target_module_id="MOD_PWR_SOURCE",
                target_port_name="master_enable_in",
                gain=1.0,
            ),
            build_drive_coupling_spec(
                source_module_id="MOD_PWR_SOURCE",
                source_port_name="bus_voltage_out",
                target_module_id="MOD_COMP_DRIVE",
                target_port_name="voltage_in",
                gain=1.0,
            ),
            build_drive_coupling_spec(
                source_module_id="MOD_PWR_SOURCE",
                source_port_name="bus_current_out",
                target_module_id="MOD_COMP_DRIVE",
                target_port_name="current_in",
                gain=1.0,
            ),
            build_drive_coupling_spec(
                source_module_id="MOD_PWR_SOURCE",
                source_port_name="bus_current_out",
                target_module_id="MOD_PWR_LOAD_MON",
                target_port_name="current_in",
                gain=1.0,
            ),
            build_drive_coupling_spec(
                source_module_id="MOD_COMP_DRIVE",
                source_port_name="compressor_speed_out",
                target_module_id="MOD_BLEED_SUPPLY",
                target_port_name="compressor_speed_in",
                gain=1.0,
                lag_seconds=1.0,
            ),
            build_drive_coupling_spec(
                source_module_id="MOD_AIRCRAFT_STATE",
                source_port_name="aircraft_altitude_out",
                target_module_id="MOD_AMBIENT",
                target_port_name="aircraft_altitude_in",
                gain=1.0,
            ),
            build_drive_coupling_spec(
                source_module_id="MOD_AIRCRAFT_STATE",
                source_port_name="aircraft_altitude_out",
                target_module_id="MOD_PRESS_CTRL",
                target_port_name="aircraft_altitude_in",
                gain=1.0,
            ),
            build_drive_coupling_spec(
                source_module_id="MOD_PRESS_MODE",
                source_port_name="press_mode_out",
                target_module_id="MOD_PRESS_CTRL",
                target_port_name="press_mode_in",
                gain=1.0,
            ),
            build_drive_coupling_spec(
                source_module_id="MOD_PRESS_CTRL",
                source_port_name="outflow_cmd_out",
                target_module_id="MOD_OUTFLOW_ACT",
                target_port_name="outflow_cmd_in",
                gain=1.0,
                lag_seconds=1.0,
                phase_gate=("takeoff_climb", "cruise", "descent_approach"),
            ),
            build_drive_coupling_spec(
                source_module_id="MOD_OUTFLOW_ACT",
                source_port_name="actuator_pos_out",
                target_module_id="MOD_CABIN",
                target_port_name="actuator_pos_in",
                gain=1.0,
                lag_seconds=2.0,
                phase_gate=("takeoff_climb", "cruise", "descent_approach"),
            ),
            build_drive_coupling_spec(
                source_module_id="MOD_AMBIENT",
                source_port_name="ambient_pressure_out",
                target_module_id="MOD_CABIN",
                target_port_name="ambient_pressure_in",
                gain=1.0,
            ),
            build_drive_coupling_spec(
                source_module_id="MOD_BLEED_SUPPLY",
                source_port_name="bleed_psi_out",
                target_module_id="MOD_PACK_FLOW",
                target_port_name="bleed_psi_in",
                gain=1.0,
                lag_seconds=1.0,
                phase_gate=("takeoff_climb", "cruise", "descent_approach"),
                source_mode_name="supply_mode",
                source_mode_gate=("OPEN",),
            ),
            build_drive_coupling_spec(
                source_module_id="MOD_BLEED_SUPPLY",
                source_port_name="bleed_psi_out",
                target_module_id="MOD_CABIN",
                target_port_name="bleed_psi_in",
                gain=1.0,
                lag_seconds=1.0,
                phase_gate=("takeoff_climb", "cruise", "descent_approach"),
                source_mode_name="supply_mode",
                source_mode_gate=("OPEN",),
            ),
            build_drive_coupling_spec(
                source_module_id="MOD_PACK_FLOW",
                source_port_name="pack_flow_out",
                target_module_id="MOD_CABIN",
                target_port_name="pack_flow_in",
                gain=1.0,
                lag_seconds=1.0,
                phase_gate=("takeoff_climb", "cruise", "descent_approach"),
                source_mode_name="pack_mode",
                source_mode_gate=("HIGH", "NORM", "LOW"),
            ),
        ),
        metadata={"example_name": "power_pressurization_hierarchy_composite"},
    )


_LEGACY_BUILD_POWER_PRESSURIZATION_HIERARCHY_COMPOSITE_AIRCRAFT_SPEC = build_power_pressurization_hierarchy_composite_aircraft_spec

_COMPOSITE_RATE_HZ_BY_PARAMETER = {
    "master_power_state": 0.5,
    "generator_tie_state": 0.5,
    "bus_voltage_v": 1.0,
    "bus_current_a": 1.0,
    "compressor_speed_pct": 2.0,
    "compressor_energy_total_kwh": 0.5,
    "electrical_load_pct": 1.0,
    "inverter_temp_c": 1.0,
    "press_mode_state": 0.5,
    "pack_mode_state": 0.5,
    "outflow_cmd_pct": 2.0,
    "pack_flow_cmd_pct": 2.0,
    "actuator_position_pct": 2.0,
    "actuator_load_pct": 1.0,
    "cabin_altitude_ft": 2.0,
    "cabin_delta_p_psi": 1.0,
    "aircraft_altitude_ft": 2.0,
    "vertical_speed_fpm": 2.0,
    "ambient_pressure_kpa": 1.0,
    "ambient_temp_c": 1.0,
    "bleed_supply_psi": 2.0,
    "bleed_usage_total": 0.5,
    "pack_flow_rate_pct": 2.0,
    "pack_temp_c": 1.0,
}

_COUPLING_ALLOWED_MISBEHAVIORS = (
    "coupling_break",
    "coupling_inversion",
    "timing_lag",
    "timing_jitter",
    "phase_context_violation",
)

_BRANCH_COUNT_BY_SCALE = {
    "smoke": 1,
    "medium": 2,
    "composite": 4,
}


def _branch_suffix(branch_index: int) -> str:
    return "" if int(branch_index) == 0 else f"_B{int(branch_index) + 1}"


def _branch_metadata(*, branch_index: int, base_id: str) -> dict[str, object]:
    return {
        "branch_index": int(branch_index),
        "base_id": str(base_id),
    }


def _branch_parameter_name(parameter_name: str, branch_index: int) -> str:
    return str(parameter_name) if int(branch_index) == 0 else f"{parameter_name}_b{int(branch_index) + 1}"


def _scaled_parameter_spec(parameter_spec, *, branch_index: int, module_id: str, subsystem_id: str):
    branch_suffix = _branch_suffix(branch_index)
    rate_hz = _COMPOSITE_RATE_HZ_BY_PARAMETER.get(str(parameter_spec.parameter_name), parameter_spec.sampling_rate_hz)
    allowed_fault_families = tuple(str(name) for name in parameter_spec.allowed_fault_families)
    if str(parameter_spec.behavior_family_label or "") == "regulated" and "bias" not in allowed_fault_families:
        allowed_fault_families = (*allowed_fault_families, "bias")
    metadata = {
        **dict(parameter_spec.metadata),
        **_branch_metadata(branch_index=branch_index, base_id=parameter_spec.parameter_name),
    }
    return replace(
        parameter_spec,
        parameter_name=_branch_parameter_name(str(parameter_spec.parameter_name), branch_index),
        module_id=f"{parameter_spec.module_id}{branch_suffix}" if branch_suffix else str(parameter_spec.module_id),
        subsystem_id=f"{parameter_spec.subsystem_id}{branch_suffix}" if branch_suffix else str(parameter_spec.subsystem_id),
        sampling_rate_hz=(None if rate_hz is None else float(rate_hz)),
        allowed_fault_families=allowed_fault_families,
        metadata=metadata,
    )


def _scaled_module_spec(module_spec, *, branch_index: int):
    branch_suffix = _branch_suffix(branch_index)
    module_id = f"{module_spec.module_id}{branch_suffix}" if branch_suffix else str(module_spec.module_id)
    subsystem_id = f"{module_spec.subsystem_id}{branch_suffix}" if branch_suffix else str(module_spec.subsystem_id)
    gain_scale = 1.0 + (0.04 * float(branch_index))
    offset_scale = float(branch_index) * 0.25
    return replace(
        module_spec,
        module_id=module_id,
        subsystem_id=subsystem_id,
        parameters=tuple(
            _scaled_parameter_spec(
                parameter_spec,
                branch_index=branch_index,
                module_id=module_id,
                subsystem_id=subsystem_id,
            )
            for parameter_spec in module_spec.parameters
        ),
        latent_update_specs=tuple(
            replace(
                latent_update,
                gain=float(latent_update.gain) * gain_scale,
                offset=float(latent_update.offset) + offset_scale,
                metadata={
                    **dict(latent_update.metadata),
                    **_branch_metadata(branch_index=branch_index, base_id=latent_update.latent_name),
                },
            )
            for latent_update in module_spec.latent_update_specs
        ),
        metadata={
            **dict(module_spec.metadata),
            **_branch_metadata(branch_index=branch_index, base_id=module_spec.module_id),
        },
    )


def _scaled_coupling_spec(coupling_spec, *, branch_index: int):
    branch_suffix = _branch_suffix(branch_index)
    lag_scale = 1.0 + (0.15 * float(branch_index))
    gain_scale = 1.0 + (0.03 * float(branch_index))
    metadata = {
        **dict(coupling_spec.metadata),
        **_branch_metadata(branch_index=branch_index, base_id=coupling_spec.coupling_id),
        "coupling_id": (
            str(coupling_spec.coupling_id)
            if not branch_suffix
            else f"{coupling_spec.coupling_id}{branch_suffix}"
        ),
    }
    return replace(
        coupling_spec,
        source_module_id=f"{coupling_spec.source_module_id}{branch_suffix}" if branch_suffix else str(coupling_spec.source_module_id),
        target_module_id=f"{coupling_spec.target_module_id}{branch_suffix}" if branch_suffix else str(coupling_spec.target_module_id),
        gain=float(coupling_spec.gain) * gain_scale,
        lag_seconds=float(coupling_spec.lag_seconds) * lag_scale,
        allowed_misbehavior_families=_COUPLING_ALLOWED_MISBEHAVIORS,
        metadata=metadata,
    )


def _build_scaled_composite_aircraft_spec(*, scale: str) -> AircraftSpec:
    try:
        branch_count = int(_BRANCH_COUNT_BY_SCALE[str(scale)])
    except KeyError as exc:
        raise ValueError(f"unsupported composite aircraft scale {scale!r}") from exc

    legacy = _LEGACY_BUILD_POWER_PRESSURIZATION_HIERARCHY_COMPOSITE_AIRCRAFT_SPEC()
    systems_by_id: dict[str, list] = {system.system_id: [] for system in legacy.systems}
    couplings = []

    for branch_index in range(branch_count):
        subsystem_specs = []
        for system_spec in legacy.systems:
            for subsystem_spec in system_spec.subsystems:
                subsystem_specs.append(
                    replace(
                        subsystem_spec,
                        subsystem_id=(
                            f"{subsystem_spec.subsystem_id}{_branch_suffix(branch_index)}"
                            if _branch_suffix(branch_index)
                            else str(subsystem_spec.subsystem_id)
                        ),
                        modules=tuple(
                            _scaled_module_spec(module_spec, branch_index=branch_index)
                            for module_spec in subsystem_spec.modules
                        ),
                        metadata={
                            **dict(subsystem_spec.metadata),
                            **_branch_metadata(branch_index=branch_index, base_id=subsystem_spec.subsystem_id),
                        },
                    )
                )
            systems_by_id[system_spec.system_id].extend(
                item for item in subsystem_specs if item.system_id == system_spec.system_id
            )
            subsystem_specs = []
        couplings.extend(
            _scaled_coupling_spec(coupling_spec, branch_index=branch_index)
            for coupling_spec in legacy.couplings
        )

    systems = tuple(
        replace(
            system_spec,
            subsystems=tuple(systems_by_id[system_spec.system_id]),
            metadata={
                **dict(system_spec.metadata),
                "scale": str(scale),
                "branch_count": branch_count,
            },
        )
        for system_spec in legacy.systems
    )
    return AircraftSpec(
        aircraft_id=f"power_pressurization_hierarchy_{scale}",
        systems=systems,
        couplings=tuple(couplings),
        metadata={
            **dict(legacy.metadata),
            "example_name": f"power_pressurization_hierarchy_{scale}",
            "scale": str(scale),
            "branch_count": branch_count,
        },
    )


def build_power_pressurization_hierarchy_smoke_aircraft_spec() -> AircraftSpec:
    return _build_scaled_composite_aircraft_spec(scale="smoke")


def build_power_pressurization_hierarchy_medium_aircraft_spec() -> AircraftSpec:
    return _build_scaled_composite_aircraft_spec(scale="medium")


def build_power_pressurization_hierarchy_composite_aircraft_spec() -> AircraftSpec:
    return _build_scaled_composite_aircraft_spec(scale="composite")
