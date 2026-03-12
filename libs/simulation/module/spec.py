"""Module specification objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from libs.simulation.parameter.spec import ParameterSpec
from libs.simulation.port.spec import PortSpec


LatentSourceKind = Literal["input_port", "context"]


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

    @classmethod
    def from_input_port(
        cls,
        *,
        latent_name: str,
        source_name: str,
        gain: float = 1.0,
        sign: int = 1,
        offset: float = 0.0,
        default_value: float = 0.0,
        clamp_min: float | None = None,
        clamp_max: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "LatentUpdateSpec":
        return cls(
            latent_name=latent_name,
            source_name=source_name,
            source_kind="input_port",
            gain=float(gain),
            sign=int(sign),
            offset=float(offset),
            default_value=float(default_value),
            clamp_min=(None if clamp_min is None else float(clamp_min)),
            clamp_max=(None if clamp_max is None else float(clamp_max)),
            metadata=dict(metadata or {}),
        )


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
    state_machines: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)
