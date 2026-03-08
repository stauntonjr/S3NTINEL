"""Precomputed native assembly runtime context."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from libs.behavior import BehaviorRegistry, BehaviorStepInput, build_default_behavior_registry
from libs.simulation.binding import ModuleBehaviorBinding, bind_assembly_behaviors
from libs.simulation.orchestrator import AssemblyTickRequest, build_assembly_tick_request, group_inter_module_couplings_by_source_module
from libs.simulation.runtime import ModuleRuntime, module_runtimes_from_specs
from libs.simulation.specs import HierarchyAssemblySpec, InterModuleCouplingSpec


@dataclass(slots=True)
class AssemblyRuntime:
    assembly_spec: HierarchyAssemblySpec
    module_bindings_by_id: dict[str, ModuleBehaviorBinding]
    module_runtimes_by_id: dict[str, ModuleRuntime]
    module_order: tuple[str, ...]
    outgoing_inter_module_couplings_by_source_module: dict[str, tuple[InterModuleCouplingSpec, ...]]

    @classmethod
    def from_spec(
        cls,
        assembly_spec: HierarchyAssemblySpec,
        *,
        behavior_registry: BehaviorRegistry | None = None,
    ) -> "AssemblyRuntime":
        resolved_behavior_registry = behavior_registry or build_default_behavior_registry()
        module_bindings = bind_assembly_behaviors(assembly_spec, resolved_behavior_registry)
        module_runtimes = module_runtimes_from_specs(assembly_spec.module_specs)
        return cls(
            assembly_spec=assembly_spec,
            module_bindings_by_id={binding.module_spec.module_id: binding for binding in module_bindings},
            module_runtimes_by_id={runtime.spec.module_id: runtime for runtime in module_runtimes},
            module_order=tuple(module_spec.module_id for module_spec in assembly_spec.module_specs),
            outgoing_inter_module_couplings_by_source_module=group_inter_module_couplings_by_source_module(
                assembly_spec.inter_module_couplings
            ),
        )

    def build_tick_request(
        self,
        *,
        step_inputs_by_module: Mapping[str, Mapping[str, BehaviorStepInput]],
        initial_state_by_module: Mapping[str, Mapping[str, Any]] | None = None,
        violation_context_by_module: Mapping[str, Mapping[str, Mapping[str, Any]]] | None = None,
        apply_violations: bool = False,
        timestamp_utc: datetime | None = None,
        current_phase_label: str | None = None,
    ) -> AssemblyTickRequest:
        return build_assembly_tick_request(
            module_bindings_by_id=self.module_bindings_by_id,
            module_runtimes_by_id=self.module_runtimes_by_id,
            step_inputs_by_module=step_inputs_by_module,
            module_order=self.module_order,
            outgoing_inter_module_couplings_by_source_module=self.outgoing_inter_module_couplings_by_source_module,
            initial_state_by_module=initial_state_by_module or {},
            violation_context_by_module=violation_context_by_module or {},
            apply_violations=apply_violations,
            timestamp_utc=timestamp_utc,
            current_phase_label=current_phase_label,
        )
