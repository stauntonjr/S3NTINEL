"""Authored realistic power/pressurization scenario family."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, replace
from math import cos, pi, sin
from typing import Any, Literal

import numpy as np

from libs.simulation.aircraft.spec import AircraftSpec
from libs.simulation.coupling.examples import build_drive_coupling_spec, build_enable_coupling_spec
from libs.simulation.coupling.spec import CouplingSpec
from libs.simulation.fault.examples import build_misbehavior_program_spec, build_misbehavior_window_spec
from libs.simulation.fault.spec import (
    resolve_window_benchmark_recoverability_target,
    resolve_window_fault_type,
)
from libs.simulation.flight.spec import FlightSpec, InitialStateSpec, InputProgramSpec, StepInputSpec
from libs.simulation.module.spec import LatentUpdateSpec, ModuleSpec
from libs.simulation.parameter.examples import (
    build_categorical_parameter_spec,
    build_numeric_parameter_spec,
)
from libs.simulation.phase.spec import PhaseEnvelopeSpec, PhaseProgramSpec, PhaseScheduleSpec, PhaseSegmentSpec
from libs.simulation.port.examples import (
    build_categorical_input_port_spec,
    build_categorical_output_port_spec,
    build_numeric_input_port_spec,
    build_numeric_output_port_spec,
)
from libs.simulation.subsystem.examples import build_subsystem_spec
from libs.simulation.system.examples import build_system_spec


ScenarioScale = Literal["smoke", "medium", "composite"]
LocalizationFocusSaturationVariant = Literal["shared_supply", "pack_temp_local"]

_MISSION_PHASE_SEGMENTS = (
    ("gate_turnaround", 480),
    ("takeoff_climb", 720),
    ("cruise", 1440),
    ("descent_approach", 720),
)
_DEFAULT_DT_SECONDS = 0.5
_DEFAULT_SEED_BY_SCALE = {
    "smoke": 1103,
    "medium": 2207,
    "composite": 3301,
}
_RATE_HZ_BY_PARAMETER = {
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
_NUMERIC_PARAMETER_NAMES = frozenset(_RATE_HZ_BY_PARAMETER)
_PARTIAL_ACTIVATION_BY_DETAIL = {
    "bias": 0.8,
    "saturation": 0.85,
    "drift": 0.75,
    "state_chatter": 0.7,
    "illegal_transition": 0.7,
}
_COUPLING_ALLOWED_MISBEHAVIORS = (
    "coupling_break",
    "coupling_inversion",
    "timing_lag",
    "timing_jitter",
    "phase_context_violation",
)


@dataclass(frozen=True, slots=True)
class StructuralRoleSpec:
    role_name: str
    role_kind: str
    system_id: str
    subsystem_id: str
    module_kinds: tuple[str, ...]
    module_suffix: str = ""
    parameter_suffix: str = ""
    shared: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MissionProfileSpec:
    dt_seconds: float
    phase_segments: tuple[tuple[str, int], ...]
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def total_steps(self) -> int:
        return sum(int(duration_steps) for _label, duration_steps in self.phase_segments)


@dataclass(frozen=True, slots=True)
class ScenarioStochasticSpec:
    seed: int
    profile_name: str = "seeded_nominal_v1"
    profile_version: str = "v1"
    enabled_channels: tuple[str, ...] = (
        "nominal_observation_noise",
        "role_profile_offsets",
        "probabilistic_parameter_misbehavior",
        "coupling_lag_jitter",
    )
    nominal_noise_scale_by_behavior: dict[str, float] = field(
        default_factory=lambda: {
            "regulated": 0.08,
            "tracking": 0.05,
            "inertial": 0.06,
            "accumulative": 0.02,
            "discrete_state": 0.0,
        }
    )
    role_offset_scale: float = 0.06
    coupling_lag_jitter_seconds: float = 0.4
    misbehavior_activation_probability_by_detail: dict[str, float] = field(
        default_factory=lambda: dict(_PARTIAL_ACTIVATION_BY_DETAIL)
    )

    def to_payload(self) -> dict[str, Any]:
        return {
            "seed": int(self.seed),
            "profile_name": self.profile_name,
            "profile_version": self.profile_version,
            "enabled_channels": list(self.enabled_channels),
            "nominal_noise_scale_by_behavior": dict(self.nominal_noise_scale_by_behavior),
            "role_offset_scale": float(self.role_offset_scale),
            "coupling_lag_jitter_seconds": float(self.coupling_lag_jitter_seconds),
            "misbehavior_activation_probability_by_detail": dict(self.misbehavior_activation_probability_by_detail),
        }


@dataclass(frozen=True, slots=True)
class PowerPressurizationScenarioSpec:
    scale: ScenarioScale
    flight_name: str
    aircraft_id: str
    mission_profile: MissionProfileSpec
    stochasticity: ScenarioStochasticSpec
    structural_roles: tuple[StructuralRoleSpec, ...]

    def to_metadata(self) -> dict[str, Any]:
        return {
            "flight_name": self.flight_name,
            "scenario_family": "power_pressurization_authored_roles_v1",
            "scale": self.scale,
            "simulation_defaults": {
                "n_steps": self.mission_profile.total_steps,
                "dt_seconds": self.mission_profile.dt_seconds,
            },
            "stochasticity": self.stochasticity.to_payload(),
            "structural_roles": [
                {
                    "role_name": role.role_name,
                    "role_kind": role.role_kind,
                    "system_id": role.system_id,
                    "subsystem_id": role.subsystem_id,
                    "module_kinds": list(role.module_kinds),
                    "module_suffix": role.module_suffix,
                    "parameter_suffix": role.parameter_suffix,
                    "shared": role.shared,
                    "metadata": dict(role.metadata),
                }
                for role in self.structural_roles
            ],
        }


@dataclass(frozen=True, slots=True)
class _RoleInstance:
    spec: StructuralRoleSpec
    module_ids_by_kind: dict[str, str]
    parameter_suffix: str


def _stable_seed(seed: int, *parts: str) -> int:
    digest = hashlib.blake2b(
        "|".join((str(seed), *[str(part) for part in parts])).encode("utf-8"),
        digest_size=8,
    ).digest()
    return int.from_bytes(digest, byteorder="big", signed=False) % (2**31 - 1)


def _rng(seed: int, *parts: str) -> np.random.Generator:
    return np.random.default_rng(_stable_seed(seed, *parts))


def _replace_numeric_parameter(
    *,
    parameter_name: str,
    system_id: str,
    subsystem_id: str,
    module_id: str,
    behavior_family_label: str,
    unit: str,
    input_port_names: tuple[str, ...] = (),
    output_port_name: str | None = None,
    allowed_fault_families: tuple[str, ...] = (),
    metadata: dict[str, Any] | None = None,
) -> Any:
    return build_numeric_parameter_spec(
        parameter_name=parameter_name,
        system_id=system_id,
        subsystem_id=subsystem_id,
        module_id=module_id,
        behavior_family_label=behavior_family_label,
        unit=unit,
        input_port_names=input_port_names,
        output_port_name=output_port_name,
        allowed_fault_families=allowed_fault_families,
        metadata=dict(metadata or {}),
    )


def _replace_categorical_parameter(
    *,
    parameter_name: str,
    system_id: str,
    subsystem_id: str,
    module_id: str,
    behavior_family_label: str,
    output_port_name: str | None = None,
    allowed_fault_families: tuple[str, ...] = (),
    metadata: dict[str, Any] | None = None,
) -> Any:
    return build_categorical_parameter_spec(
        parameter_name=parameter_name,
        system_id=system_id,
        subsystem_id=subsystem_id,
        module_id=module_id,
        behavior_family_label=behavior_family_label,
        output_port_name=output_port_name,
        allowed_fault_families=allowed_fault_families,
        metadata=dict(metadata or {}),
    )


def _build_module_templates() -> dict[str, ModuleSpec]:
    return {
        "MOD_PWR_SWITCH": ModuleSpec(
            module_id="MOD_PWR_SWITCH",
            subsystem_id="SUB_POWER_DIST",
            system_id="SYS_POWER",
            module_family="power_switching",
            parameters=(
                _replace_categorical_parameter(
                    parameter_name="master_power_state",
                    system_id="SYS_POWER",
                    subsystem_id="SUB_POWER_DIST",
                    module_id="MOD_PWR_SWITCH",
                    behavior_family_label="discrete_state",
                    output_port_name="master_power_out",
                    allowed_fault_families=("illegal_transition", "dwell_violation", "state_chatter", "stuck_state"),
                    metadata={"example_role": "master_power"},
                ),
                _replace_categorical_parameter(
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
        ),
        "MOD_PWR_SOURCE": ModuleSpec(
            module_id="MOD_PWR_SOURCE",
            subsystem_id="SUB_POWER_DIST",
            system_id="SYS_POWER",
            module_family="power_source",
            parameters=(
                _replace_numeric_parameter(
                    parameter_name="bus_voltage_v",
                    system_id="SYS_POWER",
                    subsystem_id="SUB_POWER_DIST",
                    module_id="MOD_PWR_SOURCE",
                    behavior_family_label="regulated",
                    unit="V",
                    input_port_names=("master_enable_in",),
                    output_port_name="bus_voltage_out",
                    allowed_fault_families=("offset", "saturation", "tracking_degradation", "oscillation", "bias"),
                    metadata={"example_role": "bus_voltage"},
                ),
                _replace_numeric_parameter(
                    parameter_name="bus_current_a",
                    system_id="SYS_POWER",
                    subsystem_id="SUB_POWER_DIST",
                    module_id="MOD_PWR_SOURCE",
                    behavior_family_label="regulated",
                    unit="A",
                    input_port_names=("master_enable_in",),
                    output_port_name="bus_current_out",
                    allowed_fault_families=("offset", "saturation", "tracking_degradation", "oscillation", "bias"),
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
        ),
        "MOD_COMP_DRIVE": ModuleSpec(
            module_id="MOD_COMP_DRIVE",
            subsystem_id="SUB_POWER_LOAD",
            system_id="SYS_POWER",
            module_family="compressor_drive",
            parameters=(
                _replace_numeric_parameter(
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
                _replace_numeric_parameter(
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
        ),
        "MOD_PWR_LOAD_MON": ModuleSpec(
            module_id="MOD_PWR_LOAD_MON",
            subsystem_id="SUB_POWER_LOAD",
            system_id="SYS_POWER",
            module_family="power_load_monitor",
            parameters=(
                _replace_numeric_parameter(
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
                _replace_numeric_parameter(
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
        ),
        "MOD_PRESS_MODE": ModuleSpec(
            module_id="MOD_PRESS_MODE",
            subsystem_id="SUB_ECS_CONTROL",
            system_id="SYS_ECS",
            module_family="pressurization_mode",
            parameters=(
                _replace_categorical_parameter(
                    parameter_name="press_mode_state",
                    system_id="SYS_ECS",
                    subsystem_id="SUB_ECS_CONTROL",
                    module_id="MOD_PRESS_MODE",
                    behavior_family_label="discrete_state",
                    output_port_name="press_mode_out",
                    allowed_fault_families=("illegal_transition", "dwell_violation", "state_chatter", "stuck_state"),
                    metadata={"example_role": "press_mode"},
                ),
                _replace_categorical_parameter(
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
        ),
        "MOD_PRESS_CTRL": ModuleSpec(
            module_id="MOD_PRESS_CTRL",
            subsystem_id="SUB_ECS_CONTROL",
            system_id="SYS_ECS",
            module_family="pressurization_controller",
            parameters=(
                _replace_numeric_parameter(
                    parameter_name="outflow_cmd_pct",
                    system_id="SYS_ECS",
                    subsystem_id="SUB_ECS_CONTROL",
                    module_id="MOD_PRESS_CTRL",
                    behavior_family_label="tracking",
                    unit="pct",
                    input_port_names=("aircraft_altitude_in", "press_mode_in"),
                    output_port_name="outflow_cmd_out",
                    allowed_fault_families=("offset", "saturation", "tracking_degradation", "oscillation"),
                    metadata={"example_role": "outflow_command", "bound_min": 0.0, "bound_max": 100.0},
                ),
                _replace_numeric_parameter(
                    parameter_name="pack_flow_cmd_pct",
                    system_id="SYS_ECS",
                    subsystem_id="SUB_ECS_CONTROL",
                    module_id="MOD_PRESS_CTRL",
                    behavior_family_label="tracking",
                    unit="pct",
                    input_port_names=("aircraft_altitude_in", "press_mode_in"),
                    output_port_name="pack_flow_cmd_out",
                    allowed_fault_families=("offset", "saturation", "tracking_degradation", "oscillation"),
                    metadata={"example_role": "pack_flow_command", "bound_min": 0.0, "bound_max": 100.0},
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
        ),
        "MOD_OUTFLOW_ACT": ModuleSpec(
            module_id="MOD_OUTFLOW_ACT",
            subsystem_id="SUB_ECS_CABIN",
            system_id="SYS_ECS",
            module_family="outflow_actuator",
            parameters=(
                _replace_numeric_parameter(
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
                _replace_numeric_parameter(
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
        ),
        "MOD_CABIN": ModuleSpec(
            module_id="MOD_CABIN",
            subsystem_id="SUB_ECS_CABIN",
            system_id="SYS_ECS",
            module_family="cabin_response",
            parameters=(
                _replace_numeric_parameter(
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
                _replace_numeric_parameter(
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
        ),
        "MOD_AIRCRAFT_STATE": ModuleSpec(
            module_id="MOD_AIRCRAFT_STATE",
            subsystem_id="SUB_AIR_ENV",
            system_id="SYS_AIRFRAME",
            module_family="aircraft_state",
            parameters=(
                _replace_numeric_parameter(
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
                _replace_numeric_parameter(
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
        ),
        "MOD_AMBIENT": ModuleSpec(
            module_id="MOD_AMBIENT",
            subsystem_id="SUB_AIR_ENV",
            system_id="SYS_AIRFRAME",
            module_family="ambient_reference",
            parameters=(
                _replace_numeric_parameter(
                    parameter_name="ambient_pressure_kpa",
                    system_id="SYS_AIRFRAME",
                    subsystem_id="SUB_AIR_ENV",
                    module_id="MOD_AMBIENT",
                    behavior_family_label="tracking",
                    unit="kPa",
                    input_port_names=("aircraft_altitude_in",),
                    output_port_name="ambient_pressure_out",
                    allowed_fault_families=("offset", "saturation", "tracking_degradation", "oscillation"),
                    metadata={"example_role": "ambient_pressure"},
                ),
                _replace_numeric_parameter(
                    parameter_name="ambient_temp_c",
                    system_id="SYS_AIRFRAME",
                    subsystem_id="SUB_AIR_ENV",
                    module_id="MOD_AMBIENT",
                    behavior_family_label="tracking",
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
        ),
        "MOD_BLEED_SUPPLY": ModuleSpec(
            module_id="MOD_BLEED_SUPPLY",
            subsystem_id="SUB_AIR_BLEED",
            system_id="SYS_AIRFRAME",
            module_family="bleed_supply",
            parameters=(
                _replace_numeric_parameter(
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
                _replace_numeric_parameter(
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
        ),
        "MOD_PACK_FLOW": ModuleSpec(
            module_id="MOD_PACK_FLOW",
            subsystem_id="SUB_AIR_BLEED",
            system_id="SYS_AIRFRAME",
            module_family="pack_flow",
            parameters=(
                _replace_numeric_parameter(
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
                _replace_numeric_parameter(
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
        ),
    }


def _parameter_name(base_name: str, suffix: str) -> str:
    return f"{base_name}{suffix}" if suffix else str(base_name)


def _module_id(base_name: str, suffix: str) -> str:
    return f"{base_name}{suffix}" if suffix else str(base_name)


def _instantiate_role(role: StructuralRoleSpec, templates: dict[str, ModuleSpec]) -> _RoleInstance:
    module_ids_by_kind = {
        module_kind: _module_id(module_kind, role.module_suffix)
        for module_kind in role.module_kinds
    }
    return _RoleInstance(
        spec=role,
        module_ids_by_kind=module_ids_by_kind,
        parameter_suffix=role.parameter_suffix,
    )


def _instantiate_module_template(instance: _RoleInstance, template: ModuleSpec) -> ModuleSpec:
    module_id = instance.module_ids_by_kind[template.module_id]
    subsystem_id = instance.spec.subsystem_id
    system_id = instance.spec.system_id
    parameter_suffix = instance.parameter_suffix
    role_metadata = {
        "scenario_role": instance.spec.role_name,
        "shared_role": bool(instance.spec.shared),
        **dict(instance.spec.metadata),
    }
    parameters = []
    for parameter in template.parameters:
        parameter_name = _parameter_name(str(parameter.parameter_name), parameter_suffix)
        parameter_metadata = {
            **dict(parameter.metadata),
            **role_metadata,
            "base_parameter_name": str(parameter.parameter_name),
        }
        parameters.append(
            replace(
                parameter,
                parameter_name=parameter_name,
                system_id=system_id,
                subsystem_id=subsystem_id,
                module_id=module_id,
                sampling_rate_hz=float(_RATE_HZ_BY_PARAMETER.get(str(parameter.parameter_name), parameter.sampling_rate_hz or 1.0)),
                metadata=parameter_metadata,
            )
        )
    return replace(
        template,
        module_id=module_id,
        subsystem_id=subsystem_id,
        system_id=system_id,
        parameters=tuple(parameters),
        metadata={
            **dict(template.metadata),
            **role_metadata,
            "base_module_id": template.module_id,
        },
    )


def _smoke_roles() -> tuple[StructuralRoleSpec, ...]:
    return (
        StructuralRoleSpec(
            role_name="backbone_shared",
            role_kind="backbone",
            system_id="SYS_AIRFRAME",
            subsystem_id="SUB_AIR_ENV_SHARED",
            module_kinds=("MOD_AIRCRAFT_STATE", "MOD_AMBIENT"),
            shared=True,
        ),
        StructuralRoleSpec(
            role_name="control_shared",
            role_kind="control",
            system_id="SYS_ECS",
            subsystem_id="SUB_ECS_CONTROL_SHARED",
            module_kinds=("MOD_PRESS_MODE", "MOD_PRESS_CTRL"),
            shared=True,
        ),
        StructuralRoleSpec(
            role_name="power_primary",
            role_kind="power",
            system_id="SYS_POWER",
            subsystem_id="SUB_POWER_PRIMARY",
            module_kinds=("MOD_PWR_SWITCH", "MOD_PWR_SOURCE", "MOD_COMP_DRIVE", "MOD_PWR_LOAD_MON"),
            metadata={"power_role": "primary"},
        ),
        StructuralRoleSpec(
            role_name="bleed_forward",
            role_kind="bleed",
            system_id="SYS_AIRFRAME",
            subsystem_id="SUB_AIR_BLEED_FORWARD",
            module_kinds=("MOD_BLEED_SUPPLY", "MOD_PACK_FLOW"),
            metadata={"zone": "forward", "power_role": "power_primary"},
        ),
        StructuralRoleSpec(
            role_name="cabin_forward",
            role_kind="cabin",
            system_id="SYS_ECS",
            subsystem_id="SUB_ECS_CABIN_FORWARD",
            module_kinds=("MOD_OUTFLOW_ACT", "MOD_CABIN"),
            metadata={"zone": "forward", "bleed_role": "bleed_forward", "control_role": "control_shared"},
        ),
    )


def _medium_roles() -> tuple[StructuralRoleSpec, ...]:
    return (
        *_smoke_roles()[:2],
        StructuralRoleSpec(
            role_name="power_primary",
            role_kind="power",
            system_id="SYS_POWER",
            subsystem_id="SUB_POWER_PRIMARY",
            module_kinds=("MOD_PWR_SWITCH", "MOD_PWR_SOURCE", "MOD_COMP_DRIVE", "MOD_PWR_LOAD_MON"),
            metadata={"power_role": "primary"},
        ),
        StructuralRoleSpec(
            role_name="power_essential",
            role_kind="power",
            system_id="SYS_POWER",
            subsystem_id="SUB_POWER_ESSENTIAL",
            module_kinds=("MOD_PWR_SWITCH", "MOD_PWR_SOURCE", "MOD_COMP_DRIVE", "MOD_PWR_LOAD_MON"),
            module_suffix="_ESS",
            parameter_suffix="_ess",
            metadata={"power_role": "essential"},
        ),
        StructuralRoleSpec(
            role_name="bleed_forward",
            role_kind="bleed",
            system_id="SYS_AIRFRAME",
            subsystem_id="SUB_AIR_BLEED_FORWARD",
            module_kinds=("MOD_BLEED_SUPPLY", "MOD_PACK_FLOW"),
            metadata={"zone": "forward", "power_role": "power_primary"},
        ),
        StructuralRoleSpec(
            role_name="bleed_aft",
            role_kind="bleed",
            system_id="SYS_AIRFRAME",
            subsystem_id="SUB_AIR_BLEED_AFT",
            module_kinds=("MOD_BLEED_SUPPLY", "MOD_PACK_FLOW"),
            module_suffix="_AFT",
            parameter_suffix="_aft",
            metadata={"zone": "aft", "power_role": "power_essential"},
        ),
        StructuralRoleSpec(
            role_name="cabin_forward",
            role_kind="cabin",
            system_id="SYS_ECS",
            subsystem_id="SUB_ECS_CABIN_FORWARD",
            module_kinds=("MOD_OUTFLOW_ACT", "MOD_CABIN"),
            metadata={"zone": "forward", "bleed_role": "bleed_forward", "control_role": "control_shared"},
        ),
        StructuralRoleSpec(
            role_name="cabin_aft",
            role_kind="cabin",
            system_id="SYS_ECS",
            subsystem_id="SUB_ECS_CABIN_AFT",
            module_kinds=("MOD_OUTFLOW_ACT", "MOD_CABIN"),
            module_suffix="_AFT",
            parameter_suffix="_aft",
            metadata={"zone": "aft", "bleed_role": "bleed_aft", "control_role": "control_shared"},
        ),
    )


def _composite_roles() -> tuple[StructuralRoleSpec, ...]:
    return (
        *_medium_roles()[:2],
        StructuralRoleSpec(
            role_name="control_aft",
            role_kind="control",
            system_id="SYS_ECS",
            subsystem_id="SUB_ECS_CONTROL_AFT",
            module_kinds=("MOD_PRESS_CTRL",),
            module_suffix="_AFT",
            parameter_suffix="_aft",
            metadata={"control_target_zones": ("aft", "center")},
        ),
        StructuralRoleSpec(
            role_name="power_primary",
            role_kind="power",
            system_id="SYS_POWER",
            subsystem_id="SUB_POWER_PRIMARY",
            module_kinds=("MOD_PWR_SWITCH", "MOD_PWR_SOURCE", "MOD_COMP_DRIVE", "MOD_PWR_LOAD_MON"),
            metadata={"power_role": "primary"},
        ),
        StructuralRoleSpec(
            role_name="power_essential",
            role_kind="power",
            system_id="SYS_POWER",
            subsystem_id="SUB_POWER_ESSENTIAL",
            module_kinds=("MOD_PWR_SWITCH", "MOD_PWR_SOURCE", "MOD_COMP_DRIVE", "MOD_PWR_LOAD_MON"),
            module_suffix="_ESS",
            parameter_suffix="_ess",
            metadata={"power_role": "essential"},
        ),
        StructuralRoleSpec(
            role_name="power_standby",
            role_kind="power",
            system_id="SYS_POWER",
            subsystem_id="SUB_POWER_STANDBY",
            module_kinds=("MOD_PWR_SWITCH", "MOD_PWR_SOURCE", "MOD_COMP_DRIVE"),
            module_suffix="_STBY",
            parameter_suffix="_stby",
            metadata={"power_role": "standby"},
        ),
        StructuralRoleSpec(
            role_name="bleed_forward",
            role_kind="bleed",
            system_id="SYS_AIRFRAME",
            subsystem_id="SUB_AIR_BLEED_FORWARD",
            module_kinds=("MOD_BLEED_SUPPLY", "MOD_PACK_FLOW"),
            metadata={"zone": "forward", "power_role": "power_primary"},
        ),
        StructuralRoleSpec(
            role_name="bleed_aft",
            role_kind="bleed",
            system_id="SYS_AIRFRAME",
            subsystem_id="SUB_AIR_BLEED_AFT",
            module_kinds=("MOD_BLEED_SUPPLY", "MOD_PACK_FLOW"),
            module_suffix="_AFT",
            parameter_suffix="_aft",
            metadata={"zone": "aft", "power_role": "power_essential"},
        ),
        StructuralRoleSpec(
            role_name="bleed_center",
            role_kind="bleed",
            system_id="SYS_AIRFRAME",
            subsystem_id="SUB_AIR_BLEED_CENTER",
            module_kinds=("MOD_BLEED_SUPPLY", "MOD_PACK_FLOW"),
            module_suffix="_CTR",
            parameter_suffix="_ctr",
            metadata={"zone": "center", "power_role": "power_standby"},
        ),
        StructuralRoleSpec(
            role_name="cabin_forward",
            role_kind="cabin",
            system_id="SYS_ECS",
            subsystem_id="SUB_ECS_CABIN_FORWARD",
            module_kinds=("MOD_OUTFLOW_ACT", "MOD_CABIN"),
            metadata={"zone": "forward", "bleed_role": "bleed_forward", "control_role": "control_shared"},
        ),
        StructuralRoleSpec(
            role_name="cabin_aft",
            role_kind="cabin",
            system_id="SYS_ECS",
            subsystem_id="SUB_ECS_CABIN_AFT",
            module_kinds=("MOD_OUTFLOW_ACT", "MOD_CABIN"),
            module_suffix="_AFT",
            parameter_suffix="_aft",
            metadata={"zone": "aft", "bleed_role": "bleed_aft", "control_role": "control_aft"},
        ),
        StructuralRoleSpec(
            role_name="cabin_center",
            role_kind="cabin",
            system_id="SYS_ECS",
            subsystem_id="SUB_ECS_CABIN_CENTER",
            module_kinds=("MOD_OUTFLOW_ACT", "MOD_CABIN"),
            module_suffix="_CTR",
            parameter_suffix="_ctr",
            metadata={"zone": "center", "bleed_role": "bleed_center", "control_role": "control_aft"},
        ),
    )


def _roles_for_scale(scale: ScenarioScale) -> tuple[StructuralRoleSpec, ...]:
    if scale == "smoke":
        return _smoke_roles()
    if scale == "medium":
        return _medium_roles()
    if scale == "composite":
        return _composite_roles()
    raise ValueError(f"unsupported power pressurization scale {scale!r}")


def build_power_pressurization_scenario_spec(*, scale: ScenarioScale, seed: int | None = None) -> PowerPressurizationScenarioSpec:
    resolved_seed = int(_DEFAULT_SEED_BY_SCALE[str(scale)] if seed is None else seed)
    return PowerPressurizationScenarioSpec(
        scale=scale,
        flight_name=f"power_pressurization_hierarchy_{scale}",
        aircraft_id=f"power_pressurization_hierarchy_{scale}",
        mission_profile=MissionProfileSpec(
            dt_seconds=_DEFAULT_DT_SECONDS,
            phase_segments=tuple((str(label), int(duration_steps)) for label, duration_steps in _MISSION_PHASE_SEGMENTS),
            metadata={"mission_duration_seconds": sum(duration_steps for _label, duration_steps in _MISSION_PHASE_SEGMENTS) * _DEFAULT_DT_SECONDS},
        ),
        stochasticity=ScenarioStochasticSpec(seed=resolved_seed),
        structural_roles=_roles_for_scale(scale),
    )


def _role_instances(scenario: PowerPressurizationScenarioSpec) -> dict[str, _RoleInstance]:
    templates = _build_module_templates()
    return {
        role.role_name: _instantiate_role(role, templates)
        for role in scenario.structural_roles
    }


def _build_aircraft_systems(role_instances: dict[str, _RoleInstance], templates: dict[str, ModuleSpec]) -> tuple[Any, ...]:
    subsystems_by_system: dict[str, dict[str, list[ModuleSpec]]] = {}
    for role_name in [role_name for role_name in role_instances]:
        instance = role_instances[role_name]
        subsystem_modules = subsystems_by_system.setdefault(instance.spec.system_id, {}).setdefault(instance.spec.subsystem_id, [])
        for module_kind in instance.spec.module_kinds:
            subsystem_modules.append(_instantiate_module_template(instance, templates[module_kind]))
    systems = []
    for system_id in ("SYS_POWER", "SYS_ECS", "SYS_AIRFRAME"):
        subsystem_map = subsystems_by_system.get(system_id, {})
        subsystems = tuple(
            build_subsystem_spec(
                subsystem_id=subsystem_id,
                system_id=system_id,
                modules=tuple(modules),
            )
            for subsystem_id, modules in subsystem_map.items()
        )
        if subsystems:
            systems.append(build_system_spec(system_id=system_id, subsystems=subsystems))
    return tuple(systems)


def _build_power_role_couplings(instance: _RoleInstance) -> tuple[CouplingSpec, ...]:
    ids = instance.module_ids_by_kind
    return (
        build_enable_coupling_spec(
            source_module_id=ids["MOD_PWR_SWITCH"],
            source_port_name="master_power_out",
            target_module_id=ids["MOD_PWR_SOURCE"],
            target_port_name="master_enable_in",
            gain=1.0,
            allowed_misbehavior_families=_COUPLING_ALLOWED_MISBEHAVIORS,
            metadata={"scenario_role": instance.spec.role_name},
        ),
        build_drive_coupling_spec(
            source_module_id=ids["MOD_PWR_SOURCE"],
            source_port_name="bus_voltage_out",
            target_module_id=ids["MOD_COMP_DRIVE"],
            target_port_name="voltage_in",
            gain=1.0,
            allowed_misbehavior_families=_COUPLING_ALLOWED_MISBEHAVIORS,
            metadata={"scenario_role": instance.spec.role_name},
        ),
        build_drive_coupling_spec(
            source_module_id=ids["MOD_PWR_SOURCE"],
            source_port_name="bus_current_out",
            target_module_id=ids["MOD_COMP_DRIVE"],
            target_port_name="current_in",
            gain=1.0,
            allowed_misbehavior_families=_COUPLING_ALLOWED_MISBEHAVIORS,
            metadata={"scenario_role": instance.spec.role_name},
        ),
        build_drive_coupling_spec(
            source_module_id=ids["MOD_PWR_SOURCE"],
            source_port_name="bus_current_out",
            target_module_id=ids.get("MOD_PWR_LOAD_MON", ids["MOD_COMP_DRIVE"]),
            target_port_name="current_in",
            gain=1.0,
            allowed_misbehavior_families=_COUPLING_ALLOWED_MISBEHAVIORS,
            metadata={"scenario_role": instance.spec.role_name},
        ),
    )


def _build_backbone_couplings(
    *,
    backbone: _RoleInstance,
    control: _RoleInstance,
) -> tuple[CouplingSpec, ...]:
    return (
        build_drive_coupling_spec(
            source_module_id=backbone.module_ids_by_kind["MOD_AIRCRAFT_STATE"],
            source_port_name="aircraft_altitude_out",
            target_module_id=backbone.module_ids_by_kind["MOD_AMBIENT"],
            target_port_name="aircraft_altitude_in",
            gain=1.0,
            allowed_misbehavior_families=_COUPLING_ALLOWED_MISBEHAVIORS,
            metadata={"scenario_role": "backbone_to_ambient"},
        ),
        build_drive_coupling_spec(
            source_module_id=backbone.module_ids_by_kind["MOD_AIRCRAFT_STATE"],
            source_port_name="aircraft_altitude_out",
            target_module_id=control.module_ids_by_kind["MOD_PRESS_CTRL"],
            target_port_name="aircraft_altitude_in",
            gain=1.0,
            allowed_misbehavior_families=_COUPLING_ALLOWED_MISBEHAVIORS,
            metadata={"scenario_role": "backbone_to_control"},
        ),
        build_drive_coupling_spec(
            source_module_id=control.module_ids_by_kind["MOD_PRESS_MODE"],
            source_port_name="press_mode_out",
            target_module_id=control.module_ids_by_kind["MOD_PRESS_CTRL"],
            target_port_name="press_mode_in",
            gain=1.0,
            allowed_misbehavior_families=_COUPLING_ALLOWED_MISBEHAVIORS,
            metadata={"scenario_role": "mode_to_control"},
        ),
    )


def _build_cabin_zone_couplings(
    *,
    backbone: _RoleInstance,
    control: _RoleInstance,
    bleed: _RoleInstance,
    cabin: _RoleInstance,
    controller_module_kind: str = "MOD_PRESS_CTRL",
) -> tuple[CouplingSpec, ...]:
    return (
        build_drive_coupling_spec(
            source_module_id=control.module_ids_by_kind[controller_module_kind],
            source_port_name="outflow_cmd_out",
            target_module_id=cabin.module_ids_by_kind["MOD_OUTFLOW_ACT"],
            target_port_name="outflow_cmd_in",
            gain=1.0,
            lag_seconds=1.0,
            phase_gate=("takeoff_climb", "cruise", "descent_approach"),
            allowed_misbehavior_families=_COUPLING_ALLOWED_MISBEHAVIORS,
            metadata={"scenario_role": f"{cabin.spec.role_name}_controller"},
        ),
        build_drive_coupling_spec(
            source_module_id=cabin.module_ids_by_kind["MOD_OUTFLOW_ACT"],
            source_port_name="actuator_pos_out",
            target_module_id=cabin.module_ids_by_kind["MOD_CABIN"],
            target_port_name="actuator_pos_in",
            gain=1.0,
            lag_seconds=2.0,
            phase_gate=("takeoff_climb", "cruise", "descent_approach"),
            allowed_misbehavior_families=_COUPLING_ALLOWED_MISBEHAVIORS,
            metadata={"scenario_role": f"{cabin.spec.role_name}_actuator"},
        ),
        build_drive_coupling_spec(
            source_module_id=backbone.module_ids_by_kind["MOD_AMBIENT"],
            source_port_name="ambient_pressure_out",
            target_module_id=cabin.module_ids_by_kind["MOD_CABIN"],
            target_port_name="ambient_pressure_in",
            gain=1.0,
            allowed_misbehavior_families=_COUPLING_ALLOWED_MISBEHAVIORS,
            metadata={"scenario_role": f"{cabin.spec.role_name}_ambient"},
        ),
        build_drive_coupling_spec(
            source_module_id=bleed.module_ids_by_kind["MOD_BLEED_SUPPLY"],
            source_port_name="bleed_psi_out",
            target_module_id=bleed.module_ids_by_kind["MOD_PACK_FLOW"],
            target_port_name="bleed_psi_in",
            gain=1.0,
            lag_seconds=1.0,
            phase_gate=("takeoff_climb", "cruise", "descent_approach"),
            source_mode_name="supply_mode",
            source_mode_gate=("OPEN",),
            allowed_misbehavior_families=_COUPLING_ALLOWED_MISBEHAVIORS,
            metadata={"scenario_role": f"{bleed.spec.role_name}_pack"},
        ),
        build_drive_coupling_spec(
            source_module_id=bleed.module_ids_by_kind["MOD_BLEED_SUPPLY"],
            source_port_name="bleed_psi_out",
            target_module_id=cabin.module_ids_by_kind["MOD_CABIN"],
            target_port_name="bleed_psi_in",
            gain=1.0,
            lag_seconds=1.0,
            phase_gate=("takeoff_climb", "cruise", "descent_approach"),
            source_mode_name="supply_mode",
            source_mode_gate=("OPEN",),
            allowed_misbehavior_families=_COUPLING_ALLOWED_MISBEHAVIORS,
            metadata={"scenario_role": f"{cabin.spec.role_name}_bleed"},
        ),
        build_drive_coupling_spec(
            source_module_id=bleed.module_ids_by_kind["MOD_PACK_FLOW"],
            source_port_name="pack_flow_out",
            target_module_id=cabin.module_ids_by_kind["MOD_CABIN"],
            target_port_name="pack_flow_in",
            gain=1.0,
            lag_seconds=1.0,
            phase_gate=("takeoff_climb", "cruise", "descent_approach"),
            source_mode_name="pack_mode",
            source_mode_gate=("HIGH", "NORM", "LOW"),
            allowed_misbehavior_families=_COUPLING_ALLOWED_MISBEHAVIORS,
            metadata={"scenario_role": f"{cabin.spec.role_name}_pack_flow"},
        ),
    )


def _build_power_to_bleed_coupling(
    *,
    power: _RoleInstance,
    bleed: _RoleInstance,
) -> CouplingSpec:
    return build_drive_coupling_spec(
        source_module_id=power.module_ids_by_kind["MOD_COMP_DRIVE"],
        source_port_name="compressor_speed_out",
        target_module_id=bleed.module_ids_by_kind["MOD_BLEED_SUPPLY"],
        target_port_name="compressor_speed_in",
        gain=1.0,
        lag_seconds=1.0,
        allowed_misbehavior_families=_COUPLING_ALLOWED_MISBEHAVIORS,
        metadata={"scenario_role": f"{power.spec.role_name}_to_{bleed.spec.role_name}"},
    )


def build_power_pressurization_aircraft_spec(*, scale: ScenarioScale) -> AircraftSpec:
    scenario = build_power_pressurization_scenario_spec(scale=scale)
    templates = _build_module_templates()
    instances = _role_instances(scenario)
    systems = _build_aircraft_systems(instances, templates)
    couplings: list[CouplingSpec] = []
    backbone = instances["backbone_shared"]
    shared_control = instances["control_shared"]
    couplings.extend(_build_backbone_couplings(backbone=backbone, control=shared_control))

    for instance in instances.values():
        if instance.spec.role_kind == "power":
            couplings.extend(_build_power_role_couplings(instance))

    for bleed_name, cabin_name in (
        ("bleed_forward", "cabin_forward"),
        ("bleed_aft", "cabin_aft"),
        ("bleed_center", "cabin_center"),
    ):
        if bleed_name not in instances or cabin_name not in instances:
            continue
        bleed = instances[bleed_name]
        cabin = instances[cabin_name]
        power = instances[str(bleed.spec.metadata["power_role"])]
        control_name = str(cabin.spec.metadata["control_role"])
        control = instances[control_name]
        controller_module_kind = "MOD_PRESS_CTRL"
        couplings.append(_build_power_to_bleed_coupling(power=power, bleed=bleed))
        couplings.extend(
            _build_cabin_zone_couplings(
                backbone=backbone,
                control=control,
                bleed=bleed,
                cabin=cabin,
                controller_module_kind=controller_module_kind,
            )
        )
    if "control_aft" in instances:
        couplings.extend(
            (
                build_drive_coupling_spec(
                    source_module_id=backbone.module_ids_by_kind["MOD_AIRCRAFT_STATE"],
                    source_port_name="aircraft_altitude_out",
                    target_module_id=instances["control_aft"].module_ids_by_kind["MOD_PRESS_CTRL"],
                    target_port_name="aircraft_altitude_in",
                    gain=1.0,
                    allowed_misbehavior_families=_COUPLING_ALLOWED_MISBEHAVIORS,
                    metadata={"scenario_role": "backbone_to_control_aft"},
                ),
                build_drive_coupling_spec(
                    source_module_id=shared_control.module_ids_by_kind["MOD_PRESS_MODE"],
                    source_port_name="press_mode_out",
                    target_module_id=instances["control_aft"].module_ids_by_kind["MOD_PRESS_CTRL"],
                    target_port_name="press_mode_in",
                    gain=1.0,
                    allowed_misbehavior_families=_COUPLING_ALLOWED_MISBEHAVIORS,
                    metadata={"scenario_role": "mode_to_control_aft"},
                ),
            )
        )
    return AircraftSpec(
        aircraft_id=scenario.aircraft_id,
        systems=systems,
        couplings=tuple(couplings),
        metadata={
            "example_name": scenario.flight_name,
            "scenario_family": "power_pressurization_authored_roles_v1",
            "scale": scenario.scale,
            "structural_roles": [role.role_name for role in scenario.structural_roles],
        },
    )


def _role_profile(scenario: PowerPressurizationScenarioSpec, role: _RoleInstance) -> dict[str, float]:
    rng = _rng(scenario.stochasticity.seed, "role_profile", role.spec.role_name)
    return {
        "altitude_scale": 1.0 + float(rng.uniform(-0.015, 0.02)),
        "temperature_offset": float(rng.normal(0.0, 0.7)),
        "pressure_scale": 1.0 + float(rng.uniform(-0.04, 0.05)),
        "controller_offset": float(rng.normal(0.0, 3.0)),
        "pack_temp_offset": float(rng.normal(0.0, 1.2)),
        "bleed_scale": 1.0 + float(rng.uniform(-0.08, 0.1)),
        "noise_scale_multiplier": max(0.25, 1.0 + float(rng.normal(0.0, scenario.stochasticity.role_offset_scale))),
    }


def _phase_progress(mission: MissionProfileSpec, step_index: int) -> tuple[str, float]:
    start = 0
    for phase_label, duration_steps in mission.phase_segments:
        end = start + int(duration_steps)
        if int(step_index) < end:
            width = max(end - start - 1, 1)
            return str(phase_label), max(0.0, min((int(step_index) - start) / float(width), 1.0))
        start = end
    last_phase_label, last_duration_steps = mission.phase_segments[-1]
    width = max(int(last_duration_steps) - 1, 1)
    return str(last_phase_label), max(0.0, min((int(step_index) - start) / float(width), 1.0))


def _ease_in_out(progress: float) -> float:
    clipped = max(0.0, min(float(progress), 1.0))
    return 0.5 - (0.5 * cos(pi * clipped))


def _aircraft_targets(mission: MissionProfileSpec, step_index: int) -> tuple[str, float, float]:
    phase_label, progress = _phase_progress(mission, step_index)
    eased = _ease_in_out(progress)
    if phase_label == "gate_turnaround":
        altitude_target = 35.0 * (1.0 - cos(pi * progress))
        vertical_speed_target = 120.0 * cos(2.0 * pi * progress)
    elif phase_label == "takeoff_climb":
        altitude_target = 35000.0 * eased
        vertical_speed_target = 1400.0 + (1100.0 * sin(pi * progress))
    elif phase_label == "cruise":
        altitude_target = 35000.0 + (120.0 * sin(2.0 * pi * progress)) + (45.0 * sin(6.0 * pi * progress))
        vertical_speed_target = 45.0 * sin(4.0 * pi * progress)
    else:
        altitude_target = (35000.0 * (1.0 - eased)) + (2500.0 * eased)
        vertical_speed_target = -600.0 - (1700.0 * sin(pi * progress))
    return phase_label, float(max(altitude_target, 0.0)), float(vertical_speed_target)


def _power_states(phase_label: str, progress: float, role_name: str) -> tuple[str, str]:
    if role_name == "power_primary":
        if phase_label == "gate_turnaround":
            if progress < 0.15:
                return "0", "0"
            if progress < 0.55:
                return "1", "0"
        return "1", "1"
    if role_name == "power_essential":
        if phase_label == "gate_turnaround" and progress < 0.35:
            return "0", "0"
        return "1", "1"
    if phase_label == "cruise":
        return "1", "1"
    return "0", "0"


def _numeric_context(
    *,
    scenario: PowerPressurizationScenarioSpec,
    role: _RoleInstance,
    parameter_name: str,
    behavior_family: str,
    base_context: dict[str, Any],
    step_index: int,
) -> dict[str, Any]:
    noise_scale = float(scenario.stochasticity.nominal_noise_scale_by_behavior.get(behavior_family, 0.0))
    if noise_scale > 0.0:
        rng = _rng(scenario.stochasticity.seed, "noise", role.spec.role_name, parameter_name, str(step_index))
        base_context["noise_value"] = float(rng.normal(0.0, noise_scale))
    return base_context


def _role_step_inputs(
    *,
    scenario: PowerPressurizationScenarioSpec,
    role: _RoleInstance,
    step_index: int,
    profile: dict[str, float],
) -> dict[str, dict[str, StepInputSpec]]:
    phase_label, altitude_target, vertical_speed_target = _aircraft_targets(scenario.mission_profile, step_index)
    _phase_label, progress = _phase_progress(scenario.mission_profile, step_index)
    master_power_state, generator_tie_state = _power_states(phase_label, progress, role.spec.role_name)
    ambient_pressure_target = max(101.3 - (0.002 * altitude_target * profile["pressure_scale"]), 18.0)
    ambient_temp_target = 22.0 - (0.0018 * altitude_target) + profile["temperature_offset"]
    result: dict[str, dict[str, StepInputSpec]] = {}
    for module_kind, module_id in role.module_ids_by_kind.items():
        parameter_inputs: dict[str, StepInputSpec] = {}
        if module_kind == "MOD_PWR_SWITCH":
            parameter_inputs = {
                _parameter_name("master_power_state", role.parameter_suffix): StepInputSpec(
                    context={"target_state": master_power_state},
                ),
                _parameter_name("generator_tie_state", role.parameter_suffix): StepInputSpec(
                    context={"target_state": generator_tie_state},
                ),
            }
        elif module_kind == "MOD_PWR_SOURCE":
            parameter_inputs = {
                _parameter_name("bus_voltage_v", role.parameter_suffix): StepInputSpec(
                    context=_numeric_context(
                        scenario=scenario,
                        role=role,
                        parameter_name="bus_voltage_v",
                        behavior_family="regulated",
                        base_context={"target_value": 0.0, "latent_target_name": "voltage_target", "reversion_rate": 1.4 + (0.12 * profile["noise_scale_multiplier"])},
                        step_index=step_index,
                    )
                ),
                _parameter_name("bus_current_a", role.parameter_suffix): StepInputSpec(
                    context=_numeric_context(
                        scenario=scenario,
                        role=role,
                        parameter_name="bus_current_a",
                        behavior_family="regulated",
                        base_context={"target_value": 0.0, "latent_target_name": "current_target", "reversion_rate": 1.0 + (0.08 * profile["noise_scale_multiplier"])},
                        step_index=step_index,
                    )
                ),
            }
        elif module_kind == "MOD_COMP_DRIVE":
            parameter_inputs = {
                _parameter_name("compressor_speed_pct", role.parameter_suffix): StepInputSpec(
                    context=_numeric_context(
                        scenario=scenario,
                        role=role,
                        parameter_name="compressor_speed_pct",
                        behavior_family="inertial",
                        base_context={"target_value": 0.0, "latent_target_name": "speed_target", "time_constant_seconds": 1.6 + (0.2 * profile["noise_scale_multiplier"])},
                        step_index=step_index,
                    )
                ),
                _parameter_name("compressor_energy_total_kwh", role.parameter_suffix): StepInputSpec(
                    context=_numeric_context(
                        scenario=scenario,
                        role=role,
                        parameter_name="compressor_energy_total_kwh",
                        behavior_family="accumulative",
                        base_context={"target_value": 0.0, "latent_target_name": "energy_rate"},
                        step_index=step_index,
                    )
                ),
            }
        elif module_kind == "MOD_PWR_LOAD_MON":
            parameter_inputs = {
                _parameter_name("electrical_load_pct", role.parameter_suffix): StepInputSpec(
                    context=_numeric_context(
                        scenario=scenario,
                        role=role,
                        parameter_name="electrical_load_pct",
                        behavior_family="regulated",
                        base_context={"target_value": 0.0, "latent_target_name": "load_target", "reversion_rate": 1.0 + (0.08 * profile["noise_scale_multiplier"])},
                        step_index=step_index,
                    )
                ),
                _parameter_name("inverter_temp_c", role.parameter_suffix): StepInputSpec(
                    context=_numeric_context(
                        scenario=scenario,
                        role=role,
                        parameter_name="inverter_temp_c",
                        behavior_family="inertial",
                        base_context={"target_value": 18.0 + profile["temperature_offset"], "latent_target_name": "temp_target", "time_constant_seconds": 2.2},
                        step_index=step_index,
                    )
                ),
            }
        elif module_kind == "MOD_AIRCRAFT_STATE":
            parameter_inputs = {
                _parameter_name("aircraft_altitude_ft", role.parameter_suffix): StepInputSpec(
                    context=_numeric_context(
                        scenario=scenario,
                        role=role,
                        parameter_name="aircraft_altitude_ft",
                        behavior_family="inertial",
                        base_context={"target_value": max(altitude_target * profile["altitude_scale"], 0.0), "time_constant_seconds": 1.5},
                        step_index=step_index,
                    )
                ),
                _parameter_name("vertical_speed_fpm", role.parameter_suffix): StepInputSpec(
                    context=_numeric_context(
                        scenario=scenario,
                        role=role,
                        parameter_name="vertical_speed_fpm",
                        behavior_family="inertial",
                        base_context={"target_value": vertical_speed_target * profile["altitude_scale"], "time_constant_seconds": 1.0},
                        step_index=step_index,
                    )
                ),
            }
        elif module_kind == "MOD_AMBIENT":
            parameter_inputs = {
                _parameter_name("ambient_pressure_kpa", role.parameter_suffix): StepInputSpec(
                    context=_numeric_context(
                        scenario=scenario,
                        role=role,
                        parameter_name="ambient_pressure_kpa",
                        behavior_family="tracking",
                        base_context={"target_value": ambient_pressure_target, "latent_target_name": "pressure_target", "reversion_rate": 1.8},
                        step_index=step_index,
                    )
                ),
                _parameter_name("ambient_temp_c", role.parameter_suffix): StepInputSpec(
                    context=_numeric_context(
                        scenario=scenario,
                        role=role,
                        parameter_name="ambient_temp_c",
                        behavior_family="tracking",
                        base_context={"target_value": ambient_temp_target, "latent_target_name": "temperature_target", "reversion_rate": 1.25},
                        step_index=step_index,
                    )
                ),
            }
        elif module_kind == "MOD_BLEED_SUPPLY":
            parameter_inputs = {
                _parameter_name("bleed_supply_psi", role.parameter_suffix): StepInputSpec(
                    context=_numeric_context(
                        scenario=scenario,
                        role=role,
                        parameter_name="bleed_supply_psi",
                        behavior_family="regulated",
                        base_context={"target_value": 0.0, "latent_target_name": "bleed_pressure_target", "reversion_rate": 1.15 + (0.08 * profile["bleed_scale"])},
                        step_index=step_index,
                    )
                ),
                _parameter_name("bleed_usage_total", role.parameter_suffix): StepInputSpec(
                    context=_numeric_context(
                        scenario=scenario,
                        role=role,
                        parameter_name="bleed_usage_total",
                        behavior_family="accumulative",
                        base_context={"target_value": 0.0, "latent_target_name": "bleed_usage_rate"},
                        step_index=step_index,
                    )
                ),
            }
        elif module_kind == "MOD_PACK_FLOW":
            parameter_inputs = {
                _parameter_name("pack_flow_rate_pct", role.parameter_suffix): StepInputSpec(
                    context=_numeric_context(
                        scenario=scenario,
                        role=role,
                        parameter_name="pack_flow_rate_pct",
                        behavior_family="inertial",
                        base_context={"target_value": 0.0, "latent_target_name": "pack_flow_target", "time_constant_seconds": 2.0 + (0.1 * profile["noise_scale_multiplier"])},
                        step_index=step_index,
                    )
                ),
                _parameter_name("pack_temp_c", role.parameter_suffix): StepInputSpec(
                    context=_numeric_context(
                        scenario=scenario,
                        role=role,
                        parameter_name="pack_temp_c",
                        behavior_family="regulated",
                        base_context={
                            "target_value": 5.0 + profile["pack_temp_offset"] - (1.5 * progress if phase_label == "cruise" else 0.0),
                            "latent_target_name": "pack_temp_target",
                            "reversion_rate": 1.0,
                        },
                        step_index=step_index,
                    )
                ),
            }
        elif module_kind == "MOD_PRESS_MODE":
            parameter_inputs = {
                _parameter_name("press_mode_state", role.parameter_suffix): StepInputSpec(context={}),
                _parameter_name("pack_mode_state", role.parameter_suffix): StepInputSpec(context={}),
            }
        elif module_kind == "MOD_PRESS_CTRL":
            parameter_inputs = {
                _parameter_name("outflow_cmd_pct", role.parameter_suffix): StepInputSpec(
                    context=_numeric_context(
                        scenario=scenario,
                        role=role,
                        parameter_name="outflow_cmd_pct",
                        behavior_family="tracking",
                        base_context={"target_value": 0.0, "latent_target_name": "outflow_target", "reversion_rate": 1.0, "tracking_offset": profile["controller_offset"]},
                        step_index=step_index,
                    )
                ),
                _parameter_name("pack_flow_cmd_pct", role.parameter_suffix): StepInputSpec(
                    context=_numeric_context(
                        scenario=scenario,
                        role=role,
                        parameter_name="pack_flow_cmd_pct",
                        behavior_family="tracking",
                        base_context={"target_value": 0.0, "latent_target_name": "pack_flow_target", "reversion_rate": 1.0},
                        step_index=step_index,
                    )
                ),
            }
        elif module_kind == "MOD_OUTFLOW_ACT":
            parameter_inputs = {
                _parameter_name("actuator_position_pct", role.parameter_suffix): StepInputSpec(
                    context=_numeric_context(
                        scenario=scenario,
                        role=role,
                        parameter_name="actuator_position_pct",
                        behavior_family="inertial",
                        base_context={"target_value": 0.0, "latent_target_name": "actuator_target", "time_constant_seconds": 1.8},
                        step_index=step_index,
                    )
                ),
                _parameter_name("actuator_load_pct", role.parameter_suffix): StepInputSpec(
                    context=_numeric_context(
                        scenario=scenario,
                        role=role,
                        parameter_name="actuator_load_pct",
                        behavior_family="regulated",
                        base_context={"target_value": 0.0, "latent_target_name": "actuator_load_target", "reversion_rate": 1.1},
                        step_index=step_index,
                    )
                ),
            }
        elif module_kind == "MOD_CABIN":
            parameter_inputs = {
                _parameter_name("cabin_altitude_ft", role.parameter_suffix): StepInputSpec(
                    context=_numeric_context(
                        scenario=scenario,
                        role=role,
                        parameter_name="cabin_altitude_ft",
                        behavior_family="inertial",
                        base_context={"target_value": 0.0, "latent_target_name": "cabin_alt_target", "time_constant_seconds": 2.6},
                        step_index=step_index,
                    )
                ),
                _parameter_name("cabin_delta_p_psi", role.parameter_suffix): StepInputSpec(
                    context=_numeric_context(
                        scenario=scenario,
                        role=role,
                        parameter_name="cabin_delta_p_psi",
                        behavior_family="regulated",
                        base_context={"target_value": 0.0, "latent_target_name": "delta_p_target", "reversion_rate": 1.15},
                        step_index=step_index,
                    )
                ),
            }
        result[module_id] = parameter_inputs
    return result


def _build_input_program(scenario: PowerPressurizationScenarioSpec) -> InputProgramSpec:
    role_instances = _role_instances(scenario)
    role_profiles = {name: _role_profile(scenario, instance) for name, instance in role_instances.items()}
    steps = []
    for step_index in range(scenario.mission_profile.total_steps):
        step_payload: dict[str, dict[str, StepInputSpec]] = {}
        for role_name, role in role_instances.items():
            for module_id, parameter_inputs in _role_step_inputs(
                scenario=scenario,
                role=role,
                step_index=step_index,
                profile=role_profiles[role_name],
            ).items():
                step_payload[module_id] = parameter_inputs
        steps.append(step_payload)
    return InputProgramSpec(
        steps=tuple(steps),
        hold_last_step=False,
        metadata={
            "default_dt_seconds": scenario.mission_profile.dt_seconds,
            "recommended_n_steps": scenario.mission_profile.total_steps,
            "mission_duration_seconds": scenario.mission_profile.total_steps * scenario.mission_profile.dt_seconds,
            "scenario_family": "power_pressurization_authored_roles_v1",
            "stochasticity": scenario.stochasticity.to_payload(),
        },
    )


def _phase_mode_targets(phase_label: str) -> tuple[str, str, str, str]:
    if phase_label == "gate_turnaround":
        return ("GROUND", "OFF", "CLOSED", "OFF")
    if phase_label == "takeoff_climb":
        return ("AUTO", "HIGH", "OPEN", "HIGH")
    if phase_label == "cruise":
        return ("AUTO", "NORM", "OPEN", "NORM")
    return ("DESCENT", "LOW", "OPEN", "LOW")


def _build_phase_program(scenario: PowerPressurizationScenarioSpec) -> PhaseProgramSpec:
    instances = _role_instances(scenario)
    envelopes = []
    press_mode_modules = [
        instance
        for instance in instances.values()
        if "MOD_PRESS_MODE" in instance.module_ids_by_kind
    ]
    bleed_modules = [
        instance
        for instance in instances.values()
        if "MOD_BLEED_SUPPLY" in instance.module_ids_by_kind
    ]
    pack_modules = [
        instance
        for instance in instances.values()
        if "MOD_PACK_FLOW" in instance.module_ids_by_kind
    ]
    for phase_label, duration_steps in scenario.mission_profile.phase_segments:
        press_mode_state, pack_mode_state, supply_mode, pack_mode = _phase_mode_targets(str(phase_label))
        step_input_context_by_module: dict[str, dict[str, dict[str, object]]] = {}
        mode_state_by_module: dict[str, dict[str, object]] = {}
        for instance in press_mode_modules:
            step_input_context_by_module[instance.module_ids_by_kind["MOD_PRESS_MODE"]] = {
                _parameter_name("press_mode_state", instance.parameter_suffix): {"target_state": press_mode_state},
                _parameter_name("pack_mode_state", instance.parameter_suffix): {"target_state": pack_mode_state},
            }
            mode_state_by_module[instance.module_ids_by_kind["MOD_PRESS_MODE"]] = {
                "press_mode": press_mode_state,
                "pack_mode": pack_mode_state,
            }
        for instance in bleed_modules:
            mode_state_by_module[instance.module_ids_by_kind["MOD_BLEED_SUPPLY"]] = {"supply_mode": supply_mode}
        for instance in pack_modules:
            mode_state_by_module[instance.module_ids_by_kind["MOD_PACK_FLOW"]] = {"pack_mode": pack_mode}
        envelopes.append(
            PhaseEnvelopeSpec(
                phase_label=str(phase_label),
                step_input_context_by_module=step_input_context_by_module,
                mode_state_by_module=mode_state_by_module,
            )
        )
    return PhaseProgramSpec(
        schedule=PhaseScheduleSpec(
            segments=tuple(
                PhaseSegmentSpec(str(phase_label), int(duration_steps))
                for phase_label, duration_steps in scenario.mission_profile.phase_segments
            ),
            repeat=False,
        ),
        envelopes=tuple(envelopes),
    )


def _initial_parameter_defaults(module_kind: str) -> dict[str, object]:
    return {
        "MOD_PWR_SWITCH": {"master_power_state": "0", "generator_tie_state": "0"},
        "MOD_PWR_SOURCE": {"bus_voltage_v": 0.0, "bus_current_a": 0.0},
        "MOD_COMP_DRIVE": {"compressor_speed_pct": 0.0, "compressor_energy_total_kwh": 0.0},
        "MOD_PWR_LOAD_MON": {"electrical_load_pct": 0.0, "inverter_temp_c": 18.0},
        "MOD_PRESS_MODE": {"press_mode_state": "GROUND", "pack_mode_state": "OFF"},
        "MOD_PRESS_CTRL": {"outflow_cmd_pct": 0.0, "pack_flow_cmd_pct": 0.0},
        "MOD_OUTFLOW_ACT": {"actuator_position_pct": 0.0, "actuator_load_pct": 0.0},
        "MOD_CABIN": {"cabin_altitude_ft": 0.0, "cabin_delta_p_psi": 0.0},
        "MOD_AIRCRAFT_STATE": {"aircraft_altitude_ft": 0.0, "vertical_speed_fpm": 0.0},
        "MOD_AMBIENT": {"ambient_pressure_kpa": 101.3, "ambient_temp_c": 22.0},
        "MOD_BLEED_SUPPLY": {"bleed_supply_psi": 0.0, "bleed_usage_total": 0.0},
        "MOD_PACK_FLOW": {"pack_flow_rate_pct": 0.0, "pack_temp_c": 5.0},
    }[module_kind]


def _build_initial_state(scenario: PowerPressurizationScenarioSpec) -> InitialStateSpec:
    values_by_module: dict[str, dict[str, object]] = {}
    for instance in _role_instances(scenario).values():
        for module_kind, module_id in instance.module_ids_by_kind.items():
            defaults = _initial_parameter_defaults(module_kind)
            values_by_module[module_id] = {
                _parameter_name(parameter_name, instance.parameter_suffix): value
                for parameter_name, value in defaults.items()
            }
    return InitialStateSpec(
        values_by_module=values_by_module,
        metadata={"scenario_family": "power_pressurization_authored_roles_v1"},
    )


def _mission_step_for_phase(scenario: PowerPressurizationScenarioSpec, phase_label: str, offset_seconds: float) -> int:
    elapsed_steps = 0
    for current_phase_label, duration_steps in scenario.mission_profile.phase_segments:
        if str(current_phase_label) == str(phase_label):
            return int(elapsed_steps + round(float(offset_seconds) / scenario.mission_profile.dt_seconds))
        elapsed_steps += int(duration_steps)
    raise ValueError(f"unknown phase label {phase_label!r}")


def _probability(scenario: PowerPressurizationScenarioSpec, detail_label: str) -> float:
    return float(scenario.stochasticity.misbehavior_activation_probability_by_detail.get(detail_label, 1.0))


def _window_rng_seed(scenario: PowerPressurizationScenarioSpec, window_id: str) -> int:
    return _stable_seed(scenario.stochasticity.seed, "window", window_id)


def _coupling_id(couplings: tuple[CouplingSpec, ...], *, source_module_id: str, source_port_name: str, target_module_id: str, target_port_name: str) -> str:
    for coupling in couplings:
        if (
            str(coupling.source_module_id) == str(source_module_id)
            and str(coupling.source_port_name) == str(source_port_name)
            and str(coupling.target_module_id) == str(target_module_id)
            and str(coupling.target_port_name) == str(target_port_name)
        ):
            return str(coupling.coupling_id)
    raise KeyError(f"missing coupling {source_module_id}:{source_port_name}->{target_module_id}:{target_port_name}")


def _build_validation_expectations(*, scenario: PowerPressurizationScenarioSpec, aircraft: AircraftSpec) -> dict[str, object]:
    couplings = tuple(aircraft.couplings)
    instances = _role_instances(scenario)
    expected_lag_edges = []
    expected_fused_edges = []
    expected_coupling_signatures = []
    for cabin_role_name in ("cabin_forward", "cabin_aft", "cabin_center"):
        if cabin_role_name not in instances:
            continue
        cabin = instances[cabin_role_name]
        bleed = instances[str(cabin.spec.metadata["bleed_role"])]
        power = instances[str(bleed.spec.metadata["power_role"])]
        control = instances[str(cabin.spec.metadata["control_role"])]
        actuator_position = _parameter_name("actuator_position_pct", cabin.parameter_suffix)
        cabin_altitude = _parameter_name("cabin_altitude_ft", cabin.parameter_suffix)
        bleed_supply = _parameter_name("bleed_supply_psi", bleed.parameter_suffix)
        pack_flow = _parameter_name("pack_flow_rate_pct", bleed.parameter_suffix)
        compressor_speed = _parameter_name("compressor_speed_pct", power.parameter_suffix)
        outflow_cmd = _parameter_name("outflow_cmd_pct", control.parameter_suffix)
        electrical_load = _parameter_name("electrical_load_pct", power.parameter_suffix)
        bus_current = _parameter_name("bus_current_a", power.parameter_suffix)
        cabin_delta_p = _parameter_name("cabin_delta_p_psi", cabin.parameter_suffix)
        expected_lag_edges.extend(
            (
                {"parameter_name_u": outflow_cmd, "parameter_name_v": actuator_position},
                {"parameter_name_u": actuator_position, "parameter_name_v": cabin_altitude},
                {"parameter_name_u": compressor_speed, "parameter_name_v": bleed_supply},
            )
        )
        expected_fused_edges.extend(
            (
                {"parameter_name_u": bus_current, "parameter_name_v": electrical_load},
                {"parameter_name_u": bleed_supply, "parameter_name_v": pack_flow},
                {"parameter_name_u": cabin_altitude, "parameter_name_v": cabin_delta_p},
            )
        )
        expected_coupling_signatures.extend(
            (
                {
                    "coupling_id": _coupling_id(
                        couplings,
                        source_module_id=control.module_ids_by_kind["MOD_PRESS_CTRL"],
                        source_port_name="outflow_cmd_out",
                        target_module_id=cabin.module_ids_by_kind["MOD_OUTFLOW_ACT"],
                        target_port_name="outflow_cmd_in",
                    ),
                    "parameter_name_u": outflow_cmd,
                    "parameter_name_v": actuator_position,
                    "signature_type": "lag_shift",
                },
                {
                    "coupling_id": _coupling_id(
                        couplings,
                        source_module_id=bleed.module_ids_by_kind["MOD_BLEED_SUPPLY"],
                        source_port_name="bleed_psi_out",
                        target_module_id=bleed.module_ids_by_kind["MOD_PACK_FLOW"],
                        target_port_name="bleed_psi_in",
                    ),
                    "parameter_name_u": bleed_supply,
                    "parameter_name_v": pack_flow,
                    "signature_type": "structure_change",
                },
            )
        )
    return {
        "expected_lag_edges": tuple(expected_lag_edges),
        "expected_fused_edges": tuple(expected_fused_edges),
        "expected_coupling_signatures": tuple(expected_coupling_signatures),
    }


def _misbehavior_window_metadata(
    *,
    misbehavior_window_id: str,
    fault_window_id: str,
    fault_family_label: str,
    benchmark_recoverability_target: str,
) -> dict[str, str]:
    return {
        "misbehavior_window_id": str(misbehavior_window_id),
        "fault_window_id": str(fault_window_id),
        "fault_family_label": str(fault_family_label),
        "benchmark_recoverability_target": str(benchmark_recoverability_target),
    }


def _filter_misbehavior_program_by_benchmark_targets(
    *,
    program: Any,
    benchmark_recoverability_targets: tuple[str, ...] | None,
    benchmark_suite_name: str | None,
    fault_types: tuple[str, ...] | None = None,
) -> Any:
    if not benchmark_recoverability_targets and not benchmark_suite_name and not fault_types:
        return program
    allowed_targets = {
        str(target)
        for target in tuple(benchmark_recoverability_targets or ())
        if str(target)
    }
    allowed_fault_types = {
        str(fault_type)
        for fault_type in tuple(fault_types or ())
        if str(fault_type)
    }
    windows = tuple(
        window
        for window in tuple(getattr(program, "windows", ()) or ())
        if (
            (not allowed_targets or str(resolve_window_benchmark_recoverability_target(window) or "") in allowed_targets)
            and (not allowed_fault_types or str(resolve_window_fault_type(window) or "") in allowed_fault_types)
        )
    )
    metadata = dict(getattr(program, "metadata", {}) or {})
    if allowed_targets:
        metadata["benchmark_recoverability_targets"] = sorted(allowed_targets)
    if allowed_fault_types:
        metadata["benchmark_fault_types"] = sorted(allowed_fault_types)
    if benchmark_suite_name:
        metadata["benchmark_suite_name"] = str(benchmark_suite_name)
        if metadata.get("misbehavior_program_name"):
            metadata["misbehavior_program_name"] = f"{metadata['misbehavior_program_name']}_{benchmark_suite_name}"
    return build_misbehavior_program_spec(
        windows=windows,
        metadata=metadata,
    )


def _build_misbehavior_program(*, scenario: PowerPressurizationScenarioSpec, aircraft: AircraftSpec) -> Any:
    instances = _role_instances(scenario)
    couplings = tuple(aircraft.couplings)
    windows = []
    for control_role_name in ("control_shared", "control_aft"):
        if control_role_name not in instances or "MOD_PRESS_MODE" not in instances[control_role_name].module_ids_by_kind:
            continue
        control = instances[control_role_name]
        windows.extend(
            (
                build_misbehavior_window_spec(
                    module_id=control.module_ids_by_kind["MOD_PRESS_MODE"],
                    parameter_name=_parameter_name("pack_mode_state", control.parameter_suffix),
                    start_step=_mission_step_for_phase(scenario, "descent_approach", 120.0),
                    end_step_exclusive=_mission_step_for_phase(scenario, "descent_approach", 210.0),
                    context={
                        "violation_type": "state_chatter",
                        "chatter_states": ("LOW", "OFF"),
                        "anomaly_rate": _probability(scenario, "state_chatter"),
                        "rng_seed": _window_rng_seed(scenario, f"MBW_STATE_CHATTER_{control_role_name.upper()}"),
                    },
                    metadata=_misbehavior_window_metadata(
                        misbehavior_window_id=f"MBW_STATE_CHATTER_{control_role_name.upper()}",
                        fault_window_id=f"FW_STATE_CHATTER_{control_role_name.upper()}",
                        fault_family_label="discrete_state",
                        benchmark_recoverability_target="module_recoverable",
                    ),
                ),
                build_misbehavior_window_spec(
                    module_id=control.module_ids_by_kind["MOD_PRESS_MODE"],
                    parameter_name=_parameter_name("press_mode_state", control.parameter_suffix),
                    start_step=_mission_step_for_phase(scenario, "descent_approach", 40.0),
                    end_step_exclusive=_mission_step_for_phase(scenario, "descent_approach", 100.0),
                    context={
                        "violation_type": "illegal_transition",
                        "violating_state": "GROUND",
                        "anomaly_rate": _probability(scenario, "illegal_transition"),
                        "rng_seed": _window_rng_seed(scenario, f"MBW_ILLEGAL_TRANSITION_{control_role_name.upper()}"),
                    },
                    metadata=_misbehavior_window_metadata(
                        misbehavior_window_id=f"MBW_ILLEGAL_TRANSITION_{control_role_name.upper()}",
                        fault_window_id=f"FW_ILLEGAL_TRANSITION_{control_role_name.upper()}",
                        fault_family_label="discrete_state",
                        benchmark_recoverability_target="module_recoverable",
                    ),
                ),
            )
        )
    for power_role_name in ("power_primary", "power_essential", "power_standby"):
        if power_role_name not in instances:
            continue
        power = instances[power_role_name]
        windows.append(
            build_misbehavior_window_spec(
                module_id=power.module_ids_by_kind["MOD_PWR_SOURCE"],
                parameter_name=_parameter_name("bus_voltage_v", power.parameter_suffix),
                start_step=_mission_step_for_phase(scenario, "gate_turnaround", 90.0),
                end_step_exclusive=_mission_step_for_phase(scenario, "takeoff_climb", 35.0),
                context={
                    "violation_type": "bias",
                    "bias": float(_rng(scenario.stochasticity.seed, "bias", power_role_name).uniform(0.9, 1.4)),
                    "anomaly_rate": _probability(scenario, "bias"),
                    "rng_seed": _window_rng_seed(scenario, f"MBW_REGULATED_BIAS_{power_role_name.upper()}"),
                },
                metadata=_misbehavior_window_metadata(
                    misbehavior_window_id=f"MBW_REGULATED_BIAS_{power_role_name.upper()}",
                    fault_window_id=f"FW_REGULATED_BIAS_{power_role_name.upper()}",
                    fault_family_label="regulated",
                    benchmark_recoverability_target="module_recoverable",
                ),
            )
        )
    for bleed_role_name, cabin_role_name in (
        ("bleed_forward", "cabin_forward"),
        ("bleed_aft", "cabin_aft"),
        ("bleed_center", "cabin_center"),
    ):
        if bleed_role_name not in instances or cabin_role_name not in instances:
            continue
        bleed = instances[bleed_role_name]
        cabin = instances[cabin_role_name]
        control = instances[str(cabin.spec.metadata["control_role"])]
        zone_code = str(cabin.spec.metadata["zone"]).upper()
        windows.extend(
            (
                build_misbehavior_window_spec(
                    module_id=cabin.module_ids_by_kind["MOD_OUTFLOW_ACT"],
                    parameter_name=_parameter_name("actuator_position_pct", cabin.parameter_suffix),
                    start_step=_mission_step_for_phase(scenario, "takeoff_climb", 45.0),
                    end_step_exclusive=_mission_step_for_phase(scenario, "takeoff_climb", 105.0),
                    context={"violation_type": "timing_lag", "lag_steps": 3, "anomaly_rate": 1.0},
                    metadata=_misbehavior_window_metadata(
                        misbehavior_window_id=f"MBW_TIMING_LAG_{zone_code}",
                        fault_window_id=f"FW_TIMING_LAG_{zone_code}",
                        fault_family_label="inertial",
                        benchmark_recoverability_target="module_recoverable",
                    ),
                ),
                build_misbehavior_window_spec(
                    module_id=bleed.module_ids_by_kind["MOD_BLEED_SUPPLY"],
                    parameter_name=_parameter_name("bleed_supply_psi", bleed.parameter_suffix),
                    start_step=_mission_step_for_phase(scenario, "cruise", 120.0),
                    end_step_exclusive=_mission_step_for_phase(scenario, "cruise", 220.0),
                    context={
                        "violation_type": "saturation",
                        "saturation_max": float(_rng(scenario.stochasticity.seed, "sat", zone_code).uniform(6.0, 7.5)),
                        "anomaly_rate": _probability(scenario, "saturation"),
                        "rng_seed": _window_rng_seed(scenario, f"MBW_REGULATED_SAT_{zone_code}"),
                    },
                    metadata=_misbehavior_window_metadata(
                        misbehavior_window_id=f"MBW_REGULATED_SAT_{zone_code}",
                        fault_window_id=f"FW_REGULATED_SAT_{zone_code}",
                        fault_family_label="regulated",
                        benchmark_recoverability_target="module_recoverable",
                    ),
                ),
                build_misbehavior_window_spec(
                    module_id=bleed.module_ids_by_kind["MOD_BLEED_SUPPLY"],
                    parameter_name=_parameter_name("bleed_usage_total", bleed.parameter_suffix),
                    start_step=_mission_step_for_phase(scenario, "cruise", 360.0),
                    end_step_exclusive=_mission_step_for_phase(scenario, "descent_approach", 90.0),
                    context={
                        "violation_type": "drift",
                        "drift_rate": float(_rng(scenario.stochasticity.seed, "drift", zone_code).uniform(0.015, 0.03)),
                        "anomaly_rate": _probability(scenario, "drift"),
                        "rng_seed": _window_rng_seed(scenario, f"MBW_ACCUM_DRIFT_{zone_code}"),
                    },
                    metadata=_misbehavior_window_metadata(
                        misbehavior_window_id=f"MBW_ACCUM_DRIFT_{zone_code}",
                        fault_window_id=f"FW_ACCUM_DRIFT_{zone_code}",
                        fault_family_label="accumulative",
                        benchmark_recoverability_target="module_recoverable",
                    ),
                ),
                build_misbehavior_window_spec(
                    subject_kind="coupling",
                    coupling_id=_coupling_id(
                        couplings,
                        source_module_id=control.module_ids_by_kind["MOD_PRESS_CTRL"],
                        source_port_name="outflow_cmd_out",
                        target_module_id=cabin.module_ids_by_kind["MOD_OUTFLOW_ACT"],
                        target_port_name="outflow_cmd_in",
                    ),
                    start_step=_mission_step_for_phase(scenario, "takeoff_climb", 160.0),
                    end_step_exclusive=_mission_step_for_phase(scenario, "takeoff_climb", 245.0),
                    context={
                        "violation_type": "timing_jitter",
                        "jitter_seconds": float(_rng(scenario.stochasticity.seed, "coupling_jitter", zone_code).uniform(0.15, scenario.stochasticity.coupling_lag_jitter_seconds)),
                    },
                    metadata=_misbehavior_window_metadata(
                        misbehavior_window_id=f"MBW_COUPLING_JITTER_{zone_code}",
                        fault_window_id=f"FW_COUPLING_JITTER_{zone_code}",
                        fault_family_label="coupling",
                        benchmark_recoverability_target="subsystem_recoverable",
                    ),
                ),
                build_misbehavior_window_spec(
                    subject_kind="coupling",
                    coupling_id=_coupling_id(
                        couplings,
                        source_module_id=bleed.module_ids_by_kind["MOD_BLEED_SUPPLY"],
                        source_port_name="bleed_psi_out",
                        target_module_id=bleed.module_ids_by_kind["MOD_PACK_FLOW"],
                        target_port_name="bleed_psi_in",
                    ),
                    start_step=_mission_step_for_phase(scenario, "cruise", 280.0),
                    end_step_exclusive=_mission_step_for_phase(scenario, "cruise", 420.0),
                    context={
                        "violation_type": "coupling_inversion" if zone_code in {"FORWARD", "CENTER"} else "coupling_break",
                        "anomaly_rate": 1.0,
                    },
                    metadata=_misbehavior_window_metadata(
                        misbehavior_window_id=f"MBW_COUPLING_STRUCTURE_{zone_code}",
                        fault_window_id=f"FW_COUPLING_STRUCTURE_{zone_code}",
                        fault_family_label="coupling",
                        benchmark_recoverability_target="subsystem_recoverable",
                    ),
                ),
                build_misbehavior_window_spec(
                    subject_kind="coupling",
                    coupling_id=_coupling_id(
                        couplings,
                        source_module_id=bleed.module_ids_by_kind["MOD_PACK_FLOW"],
                        source_port_name="pack_flow_out",
                        target_module_id=cabin.module_ids_by_kind["MOD_CABIN"],
                        target_port_name="pack_flow_in",
                    ),
                    start_step=_mission_step_for_phase(scenario, "gate_turnaround", 140.0),
                    end_step_exclusive=_mission_step_for_phase(scenario, "gate_turnaround", 220.0),
                    context={"violation_type": "phase_context_violation", "anomaly_rate": 1.0},
                    metadata=_misbehavior_window_metadata(
                        misbehavior_window_id=f"MBW_COUPLING_PHASE_{zone_code}",
                        fault_window_id=f"FW_COUPLING_PHASE_{zone_code}",
                        fault_family_label="coupling",
                        benchmark_recoverability_target="subsystem_recoverable",
                    ),
                ),
            )
        )
    return build_misbehavior_program_spec(
        windows=tuple(windows),
        metadata={
            "misbehavior_program_name": scenario.flight_name,
            "stochasticity": scenario.stochasticity.to_payload(),
        },
    )


def _build_power_pressurization_flight_from_scenario(
    *,
    scenario: PowerPressurizationScenarioSpec,
    benchmark_recoverability_targets: tuple[str, ...] | None = None,
    benchmark_suite_name: str | None = None,
    benchmark_fault_types: tuple[str, ...] | None = None,
    flight_name_override: str | None = None,
    localization_focus_saturation_variant: LocalizationFocusSaturationVariant = "shared_supply",
    benchmark_fault_target_overrides: dict[str, str] | None = None,
) -> FlightSpec:
    aircraft = build_power_pressurization_aircraft_spec(scale=scenario.scale)
    validation_expectations = _build_validation_expectations(scenario=scenario, aircraft=aircraft)
    misbehavior_program = _filter_misbehavior_program_by_benchmark_targets(
        program=_build_misbehavior_program(scenario=scenario, aircraft=aircraft),
        benchmark_recoverability_targets=benchmark_recoverability_targets,
        benchmark_suite_name=benchmark_suite_name,
        fault_types=benchmark_fault_types,
    )
    misbehavior_program = _rewrite_localization_focus_saturation_windows(
        program=misbehavior_program,
        scenario=scenario,
        saturation_variant=localization_focus_saturation_variant,
    )
    misbehavior_program = _override_benchmark_targets_by_fault_type(
        program=misbehavior_program,
        benchmark_fault_target_overrides=benchmark_fault_target_overrides,
    )
    metadata = {
        **scenario.to_metadata(),
        "validation": validation_expectations,
    }
    effective_targets = sorted(
        {
            str(target)
            for target in (
                resolve_window_benchmark_recoverability_target(window)
                for window in misbehavior_program.windows
            )
            if target
        }
    )
    if effective_targets:
        metadata["benchmark_recoverability_targets"] = effective_targets
    elif benchmark_recoverability_targets:
        metadata["benchmark_recoverability_targets"] = [str(target) for target in benchmark_recoverability_targets]
    if benchmark_fault_types:
        metadata["benchmark_fault_types"] = [str(fault_type) for fault_type in benchmark_fault_types]
    if benchmark_suite_name:
        metadata["benchmark_suite_name"] = str(benchmark_suite_name)
    if localization_focus_saturation_variant != "shared_supply":
        metadata["localization_focus_saturation_variant"] = str(localization_focus_saturation_variant)
    if flight_name_override:
        metadata["flight_name"] = str(flight_name_override)
    elif benchmark_suite_name:
        metadata["flight_name"] = f"{scenario.flight_name}_{benchmark_suite_name}"
    return FlightSpec(
        aircraft_spec=aircraft,
        input_program_spec=_build_input_program(scenario),
        initial_state_spec=_build_initial_state(scenario),
        phase_program_spec=_build_phase_program(scenario),
        misbehavior_program_spec=misbehavior_program,
        metadata=metadata,
    )


def _localization_focus_stochasticity(stochasticity: ScenarioStochasticSpec) -> ScenarioStochasticSpec:
    return replace(
        stochasticity,
        profile_name="seeded_localization_focus_v1",
        enabled_channels=("nominal_observation_noise", "role_profile_offsets"),
        nominal_noise_scale_by_behavior={
            "regulated": 0.04,
            "tracking": 0.025,
            "inertial": 0.03,
            "accumulative": 0.01,
            "discrete_state": 0.0,
        },
        role_offset_scale=0.025,
        coupling_lag_jitter_seconds=0.2,
        misbehavior_activation_probability_by_detail={
            key: 1.0
            for key in stochasticity.misbehavior_activation_probability_by_detail
        },
    )


def _find_role_instance_for_parameter_name(
    *,
    scenario: PowerPressurizationScenarioSpec,
    base_parameter_name: str,
    parameter_name: str,
    required_module_kind: str | None = None,
) -> _RoleInstance | None:
    for instance in _role_instances(scenario).values():
        if required_module_kind and required_module_kind not in instance.module_ids_by_kind:
            continue
        if _parameter_name(base_parameter_name, instance.parameter_suffix) == str(parameter_name):
            return instance
    return None


def _rewrite_localization_focus_saturation_windows(
    *,
    program,
    scenario: PowerPressurizationScenarioSpec,
    saturation_variant: LocalizationFocusSaturationVariant,
):
    if saturation_variant == "shared_supply":
        return program
    if saturation_variant != "pack_temp_local":
        raise ValueError(f"unsupported localization focus saturation variant {saturation_variant!r}")

    rewritten_windows = []
    for window in program.windows:
        if resolve_window_fault_type(window) != "saturation":
            rewritten_windows.append(window)
            continue
        if window.subject_kind != "parameter" or not window.parameter_name:
            rewritten_windows.append(window)
            continue
        role_instance = _find_role_instance_for_parameter_name(
            scenario=scenario,
            base_parameter_name="bleed_supply_psi",
            parameter_name=str(window.parameter_name),
            required_module_kind="MOD_BLEED_SUPPLY",
        )
        if role_instance is None:
            raise KeyError(f"could not resolve saturation role for parameter {window.parameter_name!r}")
        rewritten_context = dict(window.context)
        rewritten_context["saturation_max"] = float(
            _rng(scenario.stochasticity.seed, "local_sat_pack_temp", role_instance.spec.role_name).uniform(1.75, 2.5)
        )
        rewritten_context["benchmark_fault_variant"] = "pack_temp_local"
        rewritten_metadata = dict(window.metadata)
        rewritten_metadata["benchmark_fault_variant"] = "pack_temp_local"
        rewritten_metadata["fault_variant_label"] = "pack_temp_local"
        rewritten_windows.append(
            replace(
                window,
                module_id=role_instance.module_ids_by_kind["MOD_PACK_FLOW"],
                parameter_name=_parameter_name("pack_temp_c", role_instance.parameter_suffix),
                context=rewritten_context,
                metadata=rewritten_metadata,
            )
        )
    rewritten_metadata = dict(program.metadata)
    rewritten_metadata["localization_focus_saturation_variant"] = str(saturation_variant)
    return replace(program, windows=tuple(rewritten_windows), metadata=rewritten_metadata)


def _override_benchmark_targets_by_fault_type(
    *,
    program,
    benchmark_fault_target_overrides: dict[str, str] | None,
):
    if not benchmark_fault_target_overrides:
        return program
    rewritten_windows = []
    for window in program.windows:
        override_target = benchmark_fault_target_overrides.get(str(resolve_window_fault_type(window) or ""))
        if not override_target:
            rewritten_windows.append(window)
            continue
        rewritten_metadata = dict(window.metadata)
        rewritten_metadata["benchmark_recoverability_target"] = str(override_target)
        rewritten_windows.append(replace(window, metadata=rewritten_metadata))
    return replace(program, windows=tuple(rewritten_windows))


def build_power_pressurization_flight_spec(
    *,
    scale: ScenarioScale,
    seed: int | None = None,
    benchmark_recoverability_targets: tuple[str, ...] | None = None,
    benchmark_suite_name: str | None = None,
    benchmark_fault_types: tuple[str, ...] | None = None,
) -> FlightSpec:
    scenario = build_power_pressurization_scenario_spec(scale=scale, seed=seed)
    return _build_power_pressurization_flight_from_scenario(
        scenario=scenario,
        benchmark_recoverability_targets=benchmark_recoverability_targets,
        benchmark_suite_name=benchmark_suite_name,
        benchmark_fault_types=benchmark_fault_types,
    )


def build_power_pressurization_localization_focus_flight_spec(
    *,
    seed: int | None = None,
    benchmark_fault_types: tuple[str, ...] | None = ("bias", "saturation", "drift"),
    benchmark_suite_name: str = "localization_focus",
    flight_name: str = "power_pressurization_hierarchy_smoke_localization_focus",
    saturation_variant: LocalizationFocusSaturationVariant = "shared_supply",
    benchmark_fault_target_overrides: dict[str, str] | None = None,
) -> FlightSpec:
    base_scenario = build_power_pressurization_scenario_spec(scale="smoke", seed=seed)
    focus_scenario = replace(
        base_scenario,
        stochasticity=_localization_focus_stochasticity(base_scenario.stochasticity),
    )
    return _build_power_pressurization_flight_from_scenario(
        scenario=focus_scenario,
        benchmark_recoverability_targets=("module_recoverable",),
        benchmark_suite_name=benchmark_suite_name,
        benchmark_fault_types=benchmark_fault_types,
        flight_name_override=flight_name,
        localization_focus_saturation_variant=saturation_variant,
        benchmark_fault_target_overrides=benchmark_fault_target_overrides,
    )
