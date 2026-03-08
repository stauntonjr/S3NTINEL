"""Minimal assembly-level stepping helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from libs.behavior import BehaviorSample, BehaviorStepInput
from libs.simulation.binding import ModuleBehaviorBinding
from libs.simulation.module_step import (
    ModuleStepRequest,
    apply_module_sample_to_runtime,
    inject_runtime_latent_state_into_step_inputs,
    iter_module_samples,
    resolve_initial_state_by_parameter,
    update_module_latent_state,
)
from libs.simulation.runtime import ModuleRuntime
from libs.simulation.specs import InterModuleCouplingSpec
from libs.simulation.transfer import propagate_inter_module_couplings


@dataclass(frozen=True, slots=True)
class AssemblyModuleStepRequest:
    module_binding: ModuleBehaviorBinding
    module_runtime: ModuleRuntime
    module_runtimes_by_id: Mapping[str, ModuleRuntime]
    step_inputs_by_parameter: Mapping[str, BehaviorStepInput]
    outgoing_inter_module_couplings: tuple[InterModuleCouplingSpec, ...] = ()
    initial_state_by_parameter: Mapping[str, Any] = field(default_factory=dict)
    violation_context_by_parameter: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    apply_violations: bool = False
    timestamp_utc: datetime | None = None
    current_phase_label: str | None = None


def step_module_and_propagate(request: AssemblyModuleStepRequest) -> list[BehaviorSample]:
    update_module_latent_state(
        request.module_binding,
        request.module_runtime,
        request.step_inputs_by_parameter,
    )
    hydrated_step_inputs = inject_runtime_latent_state_into_step_inputs(
        request.module_runtime,
        request.step_inputs_by_parameter,
    )
    resolved_initial_state_by_parameter = resolve_initial_state_by_parameter(
        request.module_binding,
        request.module_runtime,
        request.initial_state_by_parameter,
    )
    local_request = ModuleStepRequest(
        module_binding=request.module_binding,
        step_inputs_by_parameter=hydrated_step_inputs,
        initial_state_by_parameter=resolved_initial_state_by_parameter,
        violation_context_by_parameter=request.violation_context_by_parameter,
        apply_violations=request.apply_violations,
    )
    samples = list(iter_module_samples(local_request))
    for sample in samples:
        apply_module_sample_to_runtime(
            request.module_runtime,
            sample,
            module_binding=request.module_binding,
            timestamp_utc=request.timestamp_utc,
        )
    if request.outgoing_inter_module_couplings:
        propagate_inter_module_couplings(
            request.module_runtimes_by_id,
            request.outgoing_inter_module_couplings,
            timestamp_utc=request.timestamp_utc,
            current_phase_label=request.current_phase_label,
        )
    return samples
