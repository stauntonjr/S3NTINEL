"""Example module specs."""

from __future__ import annotations

from collections.abc import Iterable

from libs.simulation.module.spec import LatentUpdateSpec, ModuleSpec
from libs.simulation.parameter.spec import ParameterSpec
from libs.simulation.port.spec import PortSpec


def build_single_parameter_module_spec(
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
    )


def build_discrete_output_module_spec(
    *,
    module_id: str,
    subsystem_id: str,
    system_id: str,
    module_family: str,
    parameter_spec: ParameterSpec,
    output_ports: Iterable[PortSpec] = (),
) -> ModuleSpec:
    return ModuleSpec(
        module_id=module_id,
        subsystem_id=subsystem_id,
        system_id=system_id,
        module_family=module_family,
        parameters=(parameter_spec,),
        output_ports=tuple(output_ports),
    )
