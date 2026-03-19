"""Live system runtime objects."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from libs.behavior import BehaviorSample, BehaviorStepInput
from libs.simulation.coupling.runtime import Coupling
from libs.simulation.module.runtime import Module
from libs.simulation.subsystem.runtime import Subsystem
from libs.simulation.system.spec import SystemSpec


@dataclass(slots=True)
class System:
    id: str
    subsystems: tuple[Subsystem, ...]

    @classmethod
    def from_spec(
        cls,
        spec: SystemSpec,
        *,
        modules_by_id: Mapping[str, Module],
    ) -> "System":
        subsystems = tuple(
            Subsystem.from_spec(subsystem_spec, modules_by_id=modules_by_id)
            for subsystem_spec in spec.subsystems
        )
        return cls(id=str(spec.system_id), subsystems=subsystems)

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
        for subsystem in self.subsystems:
            samples_by_module_id.update(
                subsystem.step(
                    modules_by_id=modules_by_id,
                    step_inputs_by_module=step_inputs_by_module,
                    outgoing_couplings_by_source_module=outgoing_couplings_by_source_module,
                    initial_state_by_module=initial_state_by_module,
                    fault_context_by_module=fault_context_by_module,
                    coupling_misbehavior_context_by_id=coupling_misbehavior_context_by_id,
                    apply_faults=apply_faults,
                    timestamp_utc=timestamp_utc,
                    current_phase_label=current_phase_label,
                )
            )
        return samples_by_module_id
