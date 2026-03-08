"""Static native simulation specification objects for hierarchy, parameters, and coupling."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


PortDirection = Literal["input", "output"]
LatentSourceKind = Literal["input_port", "context"]


@dataclass(frozen=True, slots=True)
class PortSpec:
    port_name: str
    direction: PortDirection
    value_datatype_label: str
    unit: str = ""
    description: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CouplingSpec:
    source_ref: str
    target_ref: str
    relation_type: str
    gain: float = 1.0
    sign: int = 1
    lag_seconds: float = 0.0
    time_constant_seconds: float | None = None
    nonlinearity: str | None = None
    phase_gate: tuple[str, ...] = ()
    mode_gate: tuple[str, ...] = ()
    shared_noise_group: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class LatentUpdateSpec:
    latent_name: str
    source_name: str
    source_kind: LatentSourceKind = "input_port"
    gain: float = 1.0
    sign: int = 1
    offset: float = 0.0
    default_value: float = 0.0
    clamp_min: float | None = None
    clamp_max: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ParameterSpec:
    parameter_name: str
    system_id: str
    subsystem_id: str
    module_id: str
    parameter_datatype_label: str
    unit: str = ""
    behavior_family_label: str | None = None
    latent_group: str | None = None
    sampling_rate_hz: float | None = None
    noise_scale: float = 0.0
    quantization: float | None = None
    delay_class: str | None = None
    phase_envelope_id: str | None = None
    allowed_fault_families: tuple[str, ...] = ()
    input_port_names: tuple[str, ...] = ()
    output_port_name: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ModuleSpec:
    module_id: str
    subsystem_id: str
    system_id: str
    module_family: str | None = None
    parameters: tuple[ParameterSpec, ...] = ()
    input_ports: tuple[PortSpec, ...] = ()
    output_ports: tuple[PortSpec, ...] = ()
    latent_variables: tuple[str, ...] = ()
    latent_update_specs: tuple[LatentUpdateSpec, ...] = ()
    controllers: tuple[str, ...] = ()
    coupling_edges: tuple[CouplingSpec, ...] = ()
    state_machines: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class InterModuleCouplingSpec:
    source_module_id: str
    source_port_name: str
    target_module_id: str
    target_port_name: str
    relation_type: str
    gain: float = 1.0
    sign: int = 1
    lag_seconds: float = 0.0
    time_constant_seconds: float | None = None
    phase_gate: tuple[str, ...] = ()
    mode_gate: tuple[str, ...] = ()
    source_mode_name: str | None = None
    source_mode_gate: tuple[str, ...] = ()
    target_mode_name: str | None = None
    target_mode_gate: tuple[str, ...] = ()
    shared_noise_group: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class HierarchyAssemblySpec:
    module_specs: tuple[ModuleSpec, ...]
    inter_module_couplings: tuple[InterModuleCouplingSpec, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)
