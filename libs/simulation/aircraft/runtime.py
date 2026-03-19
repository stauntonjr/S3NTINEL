"""Live aircraft runtime objects."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from libs.behavior import BehaviorRegistry, BehaviorSample, BehaviorStepInput, build_default_behavior_registry
from libs.simulation.aircraft.spec import AircraftSpec
from libs.simulation.coupling.runtime import Coupling
from libs.simulation.module.runtime import Module
from libs.simulation.system.runtime import System


@dataclass(frozen=True, slots=True)
class AircraftIndex:
    systems_by_id: Mapping[str, System]
    subsystems_by_id: Mapping[str, object]
    modules_by_id: Mapping[str, Module]


@dataclass(slots=True)
class Aircraft:
    id: str
    systems: tuple[System, ...]
    _index: AircraftIndex = field(repr=False)
    _outgoing_couplings_by_source_module: Mapping[str, tuple[Coupling, ...]] = field(repr=False)

    @classmethod
    def from_spec(
        cls,
        spec: AircraftSpec,
        *,
        behavior_registry: BehaviorRegistry | None = None,
    ) -> "Aircraft":
        aircraft_spec = spec
        aircraft_spec.validate()
        resolved_behavior_registry = behavior_registry or build_default_behavior_registry()
        couplings = tuple(Coupling.from_spec(spec) for spec in aircraft_spec.couplings)
        modules_by_id = {
            module_spec.module_id: Module.from_spec(
                module_spec,
                behavior_registry=resolved_behavior_registry,
            )
            for module_spec in aircraft_spec.iter_module_specs()
        }
        systems = tuple(
            System.from_spec(system_spec, modules_by_id=modules_by_id)
            for system_spec in aircraft_spec.systems
        )
        index = AircraftIndex(
            systems_by_id={system.id: system for system in systems},
            subsystems_by_id={
                subsystem.id: subsystem
                for system in systems
                for subsystem in system.subsystems
            },
            modules_by_id=modules_by_id,
        )
        return cls(
            id=str(aircraft_spec.aircraft_id),
            systems=systems,
            _index=index,
            _outgoing_couplings_by_source_module=Coupling.group_by_source_module(couplings),
        )

    def system(self, system_id: str) -> System:
        return self._index.systems_by_id[str(system_id)]

    def subsystem(self, subsystem_id: str):
        return self._index.subsystems_by_id[str(subsystem_id)]

    def module(self, module_id: str) -> Module:
        return self._index.modules_by_id[str(module_id)]

    @property
    def modules(self) -> Mapping[str, Module]:
        return self._index.modules_by_id

    @property
    def system_ids(self) -> tuple[str, ...]:
        return tuple(system.id for system in self.systems)

    @property
    def subsystem_ids(self) -> tuple[str, ...]:
        return tuple(subsystem.id for system in self.systems for subsystem in system.subsystems)

    @property
    def module_ids(self) -> tuple[str, ...]:
        return tuple(module.id for system in self.systems for subsystem in system.subsystems for module in subsystem.modules)

    @property
    def coupling_source_module_ids(self) -> tuple[str, ...]:
        return tuple(self._outgoing_couplings_by_source_module)

    def step(
        self,
        *,
        step_inputs_by_module: Mapping[str, Mapping[str, BehaviorStepInput]],
        initial_state_by_module: Mapping[str, Mapping[str, Any]] | None = None,
        fault_context_by_module: Mapping[str, Mapping[str, Mapping[str, Any]]] | None = None,
        coupling_misbehavior_context_by_id: Mapping[str, Mapping[str, Any]] | None = None,
        apply_faults: bool = True,
        timestamp_utc: datetime | None = None,
        current_phase_label: str | None = None,
    ) -> dict[str, list[BehaviorSample]]:
        samples_by_module_id: dict[str, list[BehaviorSample]] = {}
        for module in self._index.modules_by_id.values():
            for port in module.input_ports.values():
                port.metadata = {}
        for system in self.systems:
            samples_by_module_id.update(
                system.step(
                    modules_by_id=self._index.modules_by_id,
                    step_inputs_by_module=step_inputs_by_module,
                    outgoing_couplings_by_source_module=self._outgoing_couplings_by_source_module,
                    initial_state_by_module=initial_state_by_module or {},
                    fault_context_by_module=fault_context_by_module or {},
                    coupling_misbehavior_context_by_id=coupling_misbehavior_context_by_id or {},
                    apply_faults=apply_faults,
                    timestamp_utc=timestamp_utc,
                    current_phase_label=current_phase_label,
                )
            )
        return samples_by_module_id
