"""Aircraft specification objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from libs.simulation.coupling.spec import CouplingSpec
from libs.simulation.system.spec import SystemSpec


@dataclass(frozen=True, slots=True)
class AircraftSpec:
    aircraft_id: str
    systems: tuple[SystemSpec, ...]
    couplings: tuple[CouplingSpec, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)

    def iter_module_specs(self) -> tuple[object, ...]:
        return tuple(
            module
            for system in self.systems
            for subsystem in system.subsystems
            for module in subsystem.modules
        )

    def validate(self) -> None:
        module_specs = self.iter_module_specs()
        module_by_id = {module_spec.module_id: module_spec for module_spec in module_specs}
        if len(module_by_id) != len(module_specs):
            raise ValueError("aircraft spec contains duplicate module_id values")

        for coupling in self.couplings:
            source_module = module_by_id.get(coupling.source_module_id)
            target_module = module_by_id.get(coupling.target_module_id)
            if source_module is None:
                raise ValueError(
                    f"coupling references unknown source module_id={coupling.source_module_id!r}"
                )
            if target_module is None:
                raise ValueError(
                    f"coupling references unknown target module_id={coupling.target_module_id!r}"
                )
            source_ports = {port.port_name for port in source_module.output_ports}
            target_ports = {port.port_name for port in target_module.input_ports}
            if source_ports and coupling.source_port_name not in source_ports:
                raise ValueError(
                    f"coupling references unknown source port "
                    f"{coupling.source_port_name!r} on module_id={coupling.source_module_id!r}"
                )
            if target_ports and coupling.target_port_name not in target_ports:
                raise ValueError(
                    f"coupling references unknown target port "
                    f"{coupling.target_port_name!r} on module_id={coupling.target_module_id!r}"
                )
            if coupling.source_mode_gate and not coupling.source_mode_name:
                raise ValueError("coupling declares source_mode_gate without source_mode_name")
            if coupling.target_mode_gate and not coupling.target_mode_name:
                raise ValueError("coupling declares target_mode_gate without target_mode_name")
