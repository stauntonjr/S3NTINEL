"""Small helper builders for native simulation examples."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from libs.simulation.specs import (
    CouplingSpec,
    InterModuleCouplingSpec,
    LatentUpdateSpec,
    ModuleSpec,
    ParameterSpec,
    PortSpec,
)


def input_port(*, port_name: str, value_datatype_label: str, unit: str = "") -> PortSpec:
    return PortSpec(
        port_name=port_name,
        direction="input",
        value_datatype_label=value_datatype_label,
        unit=unit,
    )


def output_port(*, port_name: str, value_datatype_label: str, unit: str = "") -> PortSpec:
    return PortSpec(
        port_name=port_name,
        direction="output",
        value_datatype_label=value_datatype_label,
        unit=unit,
    )


def module_parameter(
    *,
    parameter_name: str,
    module_id: str,
    subsystem_id: str,
    system_id: str,
    parameter_datatype_label: str,
    behavior_family_label: str,
    unit: str = "",
    input_port_names: Iterable[str] = (),
    output_port_name: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> ParameterSpec:
    return ParameterSpec(
        parameter_name=parameter_name,
        system_id=system_id,
        subsystem_id=subsystem_id,
        module_id=module_id,
        parameter_datatype_label=parameter_datatype_label,
        unit=unit,
        behavior_family_label=behavior_family_label,
        input_port_names=tuple(str(name) for name in input_port_names),
        output_port_name=(str(output_port_name) if output_port_name else None),
        metadata=dict(metadata or {}),
    )


def latent_update_from_input_port(
    *,
    latent_name: str,
    source_name: str,
    gain: float = 1.0,
    sign: int = 1,
    offset: float = 0.0,
    default_value: float = 0.0,
    clamp_min: float | None = None,
    clamp_max: float | None = None,
) -> LatentUpdateSpec:
    return LatentUpdateSpec(
        latent_name=latent_name,
        source_name=source_name,
        source_kind="input_port",
        gain=float(gain),
        sign=int(sign),
        offset=float(offset),
        default_value=float(default_value),
        clamp_min=(None if clamp_min is None else float(clamp_min)),
        clamp_max=(None if clamp_max is None else float(clamp_max)),
    )


def drive_coupling(
    *,
    source_module_id: str,
    source_port_name: str,
    target_module_id: str,
    target_port_name: str,
    gain: float = 1.0,
    sign: int = 1,
    lag_seconds: float = 0.0,
) -> InterModuleCouplingSpec:
    return InterModuleCouplingSpec(
        source_module_id=source_module_id,
        source_port_name=source_port_name,
        target_module_id=target_module_id,
        target_port_name=target_port_name,
        relation_type="drive",
        gain=float(gain),
        sign=int(sign),
        lag_seconds=float(lag_seconds),
    )


def regulate_coupling(*, source_ref: str, target_ref: str) -> CouplingSpec:
    return CouplingSpec(
        source_ref=source_ref,
        target_ref=target_ref,
        relation_type="regulate",
    )


def build_single_parameter_module(
    *,
    module_id: str,
    subsystem_id: str,
    system_id: str,
    module_family: str,
    parameter_spec: ParameterSpec,
    input_ports: Iterable[PortSpec] = (),
    output_ports: Iterable[PortSpec] = (),
    latent_variables: Iterable[str] = (),
    latent_update_specs: Iterable[LatentUpdateSpec] = (),
    coupling_edges: Iterable[CouplingSpec] = (),
) -> ModuleSpec:
    return ModuleSpec(
        module_id=module_id,
        subsystem_id=subsystem_id,
        system_id=system_id,
        module_family=module_family,
        parameters=(parameter_spec,),
        input_ports=tuple(input_ports),
        output_ports=tuple(output_ports),
        latent_variables=tuple(str(name) for name in latent_variables),
        latent_update_specs=tuple(latent_update_specs),
        coupling_edges=tuple(coupling_edges),
    )


def build_discrete_output_module(
    *,
    module_id: str,
    subsystem_id: str,
    system_id: str,
    module_family: str,
    parameter_name: str,
    output_port_name: str,
    parameter_datatype_label: str = "categorical",
    unit: str = "state",
    metadata: dict[str, Any] | None = None,
) -> ModuleSpec:
    return build_single_parameter_module(
        module_id=module_id,
        subsystem_id=subsystem_id,
        system_id=system_id,
        module_family=module_family,
        parameter_spec=module_parameter(
            parameter_name=parameter_name,
            module_id=module_id,
            subsystem_id=subsystem_id,
            system_id=system_id,
            parameter_datatype_label=parameter_datatype_label,
            behavior_family_label="discrete_state",
            output_port_name=output_port_name,
            metadata=metadata,
        ),
        output_ports=(output_port(port_name=output_port_name, value_datatype_label=parameter_datatype_label, unit=unit),),
    )
