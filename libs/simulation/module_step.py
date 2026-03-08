"""Local module-level behavior stepping helpers.

This layer coordinates one local module tick at a time. Behavior generators remain
stream-capable, but module stepping itself consumes exactly one `BehaviorStepInput`
per parameter for the current tick.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from libs.behavior import BehaviorSample, BehaviorStepInput, iter_tick_samples
from libs.simulation.binding import ModuleBehaviorBinding
from libs.simulation.runtime import ModuleRuntime


@dataclass(frozen=True, slots=True)
class ModuleStepRequest:
    module_binding: ModuleBehaviorBinding
    step_inputs_by_parameter: Mapping[str, BehaviorStepInput]
    initial_state_by_parameter: Mapping[str, Any] = field(default_factory=dict)
    violation_context_by_parameter: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    apply_violations: bool = False


def _iter_parameter_samples_for_tick(
    request: ModuleStepRequest,
    *,
    parameter_name: str,
    step_input: BehaviorStepInput,
) -> Iterator[BehaviorSample]:
    parameter_binding = request.module_binding.parameter_bindings_by_name.get(parameter_name)
    if parameter_binding is None:
        return

    yield from iter_tick_samples(
        parameter_name=parameter_name,
        generator=parameter_binding.behavior.generator,
        step_input=step_input,
        initial_state=request.initial_state_by_parameter.get(parameter_name),
        violator=parameter_binding.behavior.violator if request.apply_violations else None,
        violation_context=request.violation_context_by_parameter.get(parameter_name),
    )


def inject_input_ports_into_step_inputs(
    module_binding: ModuleBehaviorBinding,
    module_runtime: ModuleRuntime,
    step_inputs_by_parameter: Mapping[str, BehaviorStepInput],
) -> dict[str, BehaviorStepInput]:
    hydrated: dict[str, BehaviorStepInput] = {}
    for parameter_name, step_input in step_inputs_by_parameter.items():
        parameter_binding = module_binding.parameter_bindings_by_name.get(str(parameter_name))
        if parameter_binding is None:
            continue
        parameter_spec = parameter_binding.parameter_spec
        injected_context: dict[str, Any] = {}
        for port_name in parameter_spec.input_port_names:
            if port_name not in module_runtime.input_ports:
                continue
            port_runtime = module_runtime.input_ports[port_name]
            if port_runtime.current_value is not None:
                injected_context[str(port_name)] = port_runtime.current_value

        hydrated[str(parameter_name)] = BehaviorStepInput(
            dt_seconds=step_input.dt_seconds,
            latent_state=step_input.latent_state,
            context={**injected_context, **dict(step_input.context)},
        )
    return hydrated


def _coerce_float(value: object, *, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def update_module_latent_state(
    module_binding: ModuleBehaviorBinding,
    module_runtime: ModuleRuntime,
    step_inputs_by_parameter: Mapping[str, BehaviorStepInput],
) -> None:
    module_spec = module_binding.module_spec
    if not module_spec.latent_update_specs:
        return
    context_values: dict[str, object] = {}
    for step_input in step_inputs_by_parameter.values():
        context_values.update(dict(step_input.context))
    for latent_update_spec in module_spec.latent_update_specs:
        if latent_update_spec.source_kind == "input_port":
            source_value = (
                module_runtime.input_ports.get(latent_update_spec.source_name).current_value
                if latent_update_spec.source_name in module_runtime.input_ports
                else None
            )
        else:
            source_value = context_values.get(latent_update_spec.source_name)
        value = (
            latent_update_spec.sign
            * latent_update_spec.gain
            * _coerce_float(source_value, default=latent_update_spec.default_value)
            + latent_update_spec.offset
        )
        if latent_update_spec.clamp_min is not None:
            value = max(value, latent_update_spec.clamp_min)
        if latent_update_spec.clamp_max is not None:
            value = min(value, latent_update_spec.clamp_max)
        module_runtime.latent_state_by_name[str(latent_update_spec.latent_name)] = value


def inject_runtime_latent_state_into_step_inputs(
    module_runtime: ModuleRuntime,
    step_inputs_by_parameter: Mapping[str, BehaviorStepInput],
) -> dict[str, BehaviorStepInput]:
    hydrated: dict[str, BehaviorStepInput] = {}
    runtime_latent_state = dict(module_runtime.latent_state_by_name)
    for parameter_name, step_input in step_inputs_by_parameter.items():
        hydrated[str(parameter_name)] = BehaviorStepInput(
            dt_seconds=step_input.dt_seconds,
            latent_state={**runtime_latent_state, **dict(step_input.latent_state)},
            context=dict(step_input.context),
        )
    return hydrated


def resolve_initial_state_by_parameter(
    module_binding: ModuleBehaviorBinding,
    module_runtime: ModuleRuntime,
    initial_state_by_parameter: Mapping[str, Any],
) -> dict[str, Any]:
    resolved: dict[str, Any] = dict(initial_state_by_parameter)
    for parameter_binding in module_binding.parameter_bindings:
        parameter_name = parameter_binding.parameter_spec.parameter_name
        if parameter_name in resolved:
            continue
        parameter_runtime = module_runtime.parameter_runtime(parameter_name)
        if parameter_runtime.parameter_value_clean is not None:
            resolved[parameter_name] = parameter_runtime.parameter_value_clean
        elif parameter_runtime.parameter_value is not None:
            resolved[parameter_name] = parameter_runtime.parameter_value
    return resolved


def iter_module_samples(request: ModuleStepRequest) -> Iterator[BehaviorSample]:
    module_spec = request.module_binding.module_spec
    for parameter_binding in request.module_binding.parameter_bindings:
        parameter_spec = parameter_binding.parameter_spec
        parameter_name = parameter_spec.parameter_name
        step_input = request.step_inputs_by_parameter.get(parameter_name)
        if step_input is None:
            continue
        for sample in _iter_parameter_samples_for_tick(
            request,
            parameter_name=parameter_name,
            step_input=step_input,
        ):
            metadata = dict(sample.metadata)
            metadata.setdefault("system_id", module_spec.system_id)
            metadata.setdefault("subsystem_id", module_spec.subsystem_id)
            metadata.setdefault("module_id", module_spec.module_id)
            metadata.setdefault("behavior_family_label", parameter_spec.behavior_family_label)
            yield BehaviorSample(
                parameter_name=sample.parameter_name,
                parameter_value_clean=sample.parameter_value_clean,
                parameter_value=sample.parameter_value,
                state=sample.state,
                metadata=metadata,
            )


def apply_module_sample_to_output_ports(
    module_runtime: ModuleRuntime,
    sample: BehaviorSample,
    module_binding: ModuleBehaviorBinding,
    *,
    timestamp_utc: datetime | None = None,
) -> None:
    parameter_binding = module_binding.parameter_bindings_by_name.get(sample.parameter_name)
    if parameter_binding is None:
        return
    output_port_name = parameter_binding.parameter_spec.output_port_name
    if not output_port_name or output_port_name not in module_runtime.output_ports:
        return
    port_runtime = module_runtime.output_port_runtime(output_port_name)
    port_runtime.current_value = (
        sample.parameter_value_clean if sample.parameter_value_clean is not None else sample.parameter_value
    )
    port_runtime.timestamp_utc = timestamp_utc


def apply_module_sample_to_runtime(
    module_runtime: ModuleRuntime,
    sample: BehaviorSample,
    *,
    module_binding: ModuleBehaviorBinding | None = None,
    timestamp_utc: datetime | None = None,
) -> None:
    parameter_runtime = module_runtime.parameter_runtime(sample.parameter_name)
    parameter_runtime.update_observation(
        parameter_value=sample.parameter_value,
        parameter_value_clean=sample.parameter_value_clean,
        timestamp_utc=timestamp_utc,
    )
    if module_binding is not None:
        apply_module_sample_to_output_ports(
            module_runtime,
            sample,
            module_binding,
            timestamp_utc=timestamp_utc,
        )
