"""Deterministic single-pass assembly stepping helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from libs.behavior import BehaviorSample, BehaviorStepInput
from libs.simulation.assembly_step import AssemblyModuleStepRequest, step_module_and_propagate
from libs.simulation.binding import ModuleBehaviorBinding
from libs.simulation.module_step import inject_input_ports_into_step_inputs
from libs.simulation.runtime import ModuleRuntime
from libs.simulation.specs import InterModuleCouplingSpec


@dataclass(frozen=True, slots=True)
class AssemblyTickRequest:
    module_bindings_by_id: Mapping[str, ModuleBehaviorBinding]
    module_runtimes_by_id: Mapping[str, ModuleRuntime]
    step_inputs_by_module: Mapping[str, Mapping[str, BehaviorStepInput]]
    module_order: tuple[str, ...]
    outgoing_inter_module_couplings_by_source_module: Mapping[str, tuple[InterModuleCouplingSpec, ...]] = field(default_factory=dict)
    initial_state_by_module: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    violation_context_by_module: Mapping[str, Mapping[str, Mapping[str, Any]]] = field(default_factory=dict)
    apply_violations: bool = False
    timestamp_utc: datetime | None = None
    current_phase_label: str | None = None
    

def resolve_step_inputs_for_module(
    module_binding: ModuleBehaviorBinding,
    raw_step_inputs: Mapping[str, BehaviorStepInput],
) -> dict[str, BehaviorStepInput]:
    resolved = {str(parameter_name): step_input for parameter_name, step_input in raw_step_inputs.items()}
    default_dt_seconds = next(iter((step_input.dt_seconds for step_input in resolved.values())), 1.0)
    for parameter_binding in module_binding.parameter_bindings:
        parameter_name = parameter_binding.parameter_spec.parameter_name
        if parameter_name in resolved:
            continue
        resolved[parameter_name] = BehaviorStepInput(
            dt_seconds=default_dt_seconds,
            latent_state={},
            context={},
        )
    return resolved


def group_inter_module_couplings_by_source_module(
    inter_module_couplings: tuple[InterModuleCouplingSpec, ...],
) -> dict[str, tuple[InterModuleCouplingSpec, ...]]:
    grouped: dict[str, list[InterModuleCouplingSpec]] = {}
    for coupling in inter_module_couplings:
        grouped.setdefault(str(coupling.source_module_id), []).append(coupling)
    return {
        module_id: tuple(couplings)
        for module_id, couplings in grouped.items()
    }


def build_assembly_tick_request(
    *,
    module_bindings_by_id: Mapping[str, ModuleBehaviorBinding],
    module_runtimes_by_id: Mapping[str, ModuleRuntime],
    step_inputs_by_module: Mapping[str, Mapping[str, BehaviorStepInput]],
    module_order: tuple[str, ...],
    outgoing_inter_module_couplings_by_source_module: Mapping[str, tuple[InterModuleCouplingSpec, ...]],
    initial_state_by_module: Mapping[str, Mapping[str, Any]] | None = None,
    violation_context_by_module: Mapping[str, Mapping[str, Mapping[str, Any]]] | None = None,
    apply_violations: bool = False,
    timestamp_utc: datetime | None = None,
    current_phase_label: str | None = None,
) -> AssemblyTickRequest:
    return AssemblyTickRequest(
        module_bindings_by_id=module_bindings_by_id,
        module_runtimes_by_id=module_runtimes_by_id,
        step_inputs_by_module=step_inputs_by_module,
        module_order=module_order,
        outgoing_inter_module_couplings_by_source_module=outgoing_inter_module_couplings_by_source_module,
        initial_state_by_module=initial_state_by_module or {},
        violation_context_by_module=violation_context_by_module or {},
        apply_violations=apply_violations,
        timestamp_utc=timestamp_utc,
        current_phase_label=current_phase_label,
    )


def step_assembly_once(request: AssemblyTickRequest) -> dict[str, list[BehaviorSample]]:
    samples_by_module_id: dict[str, list[BehaviorSample]] = {}
    for module_id in request.module_order:
        module_binding = request.module_bindings_by_id.get(module_id)
        if module_binding is None:
            raise KeyError(f"missing module binding for ordered module: {module_id}")
        module_runtime = request.module_runtimes_by_id.get(module_id)
        if module_runtime is None:
            raise KeyError(f"missing module runtime for ordered module: {module_id}")

        raw_step_inputs = resolve_step_inputs_for_module(
            module_binding,
            request.step_inputs_by_module.get(module_id, {}),
        )
        hydrated_step_inputs = inject_input_ports_into_step_inputs(
            module_binding,
            module_runtime,
            raw_step_inputs,
        )
        samples_by_module_id[module_id] = step_module_and_propagate(
            AssemblyModuleStepRequest(
                module_binding=module_binding,
                module_runtime=module_runtime,
                module_runtimes_by_id=request.module_runtimes_by_id,
                step_inputs_by_parameter=hydrated_step_inputs,
                outgoing_inter_module_couplings=request.outgoing_inter_module_couplings_by_source_module.get(
                    module_id, ()
                ),
                initial_state_by_parameter=request.initial_state_by_module.get(module_id, {}),
                violation_context_by_parameter=request.violation_context_by_module.get(module_id, {}),
                apply_violations=request.apply_violations,
                timestamp_utc=request.timestamp_utc,
                current_phase_label=request.current_phase_label,
            )
        )
    return samples_by_module_id
