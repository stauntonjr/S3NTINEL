"""Live subsystem runtime objects."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from libs.behavior import BehaviorSample, BehaviorStepInput
from libs.simulation.coupling.runtime import Coupling
from libs.simulation.module.runtime import Module
from libs.simulation.subsystem.spec import SubsystemSpec


@dataclass(slots=True)
class Subsystem:
    id: str
    system_id: str
    modules: tuple[Module, ...]

    @classmethod
    def from_spec(
        cls,
        spec: SubsystemSpec,
        *,
        modules_by_id: Mapping[str, Module],
    ) -> "Subsystem":
        modules = tuple(modules_by_id[module_spec.module_id] for module_spec in spec.modules)
        return cls(id=str(spec.subsystem_id), system_id=str(spec.system_id), modules=modules)

    def step(
        self,
        *,
        modules_by_id: Mapping[str, Module],
        step_inputs_by_module: Mapping[str, Mapping[str, BehaviorStepInput]],
        outgoing_couplings_by_source_module: Mapping[str, tuple[Coupling, ...]],
        initial_state_by_module: Mapping[str, Mapping[str, Any]],
        fault_context_by_module: Mapping[str, Mapping[str, Mapping[str, Any]]],
        coupling_misbehavior_context_by_id: Mapping[str, Mapping[str, Any]],
        apply_faults: bool,
        timestamp_utc: datetime | None,
        current_phase_label: str | None,
    ) -> dict[str, list[BehaviorSample]]:
        samples_by_module_id: dict[str, list[BehaviorSample]] = {}
        for module in self.modules:
            module_id = module.id
            samples_by_module_id[module_id] = module.step(
                modules_by_id=modules_by_id,
                raw_step_inputs=step_inputs_by_module.get(module_id, {}),
                outgoing_couplings=outgoing_couplings_by_source_module.get(module_id, ()),
                initial_state_by_parameter=initial_state_by_module.get(module_id, {}),
                fault_context_by_parameter=fault_context_by_module.get(module_id, {}),
                coupling_misbehavior_context_by_id=coupling_misbehavior_context_by_id,
                apply_faults=apply_faults,
                timestamp_utc=timestamp_utc,
                current_phase_label=current_phase_label,
            )
        return samples_by_module_id
