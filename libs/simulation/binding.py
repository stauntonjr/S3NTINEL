"""Declarative binding of simulation specs to behavior bundles."""

from __future__ import annotations

from dataclasses import dataclass, field

from libs.behavior import Behavior, BehaviorRegistry
from libs.simulation.specs import HierarchyAssemblySpec, ModuleSpec, ParameterSpec


@dataclass(frozen=True, slots=True)
class ParameterBehaviorBinding:
    parameter_spec: ParameterSpec
    behavior: Behavior


@dataclass(frozen=True, slots=True)
class ModuleBehaviorBinding:
    module_spec: ModuleSpec
    parameter_bindings: tuple[ParameterBehaviorBinding, ...]
    parameter_bindings_by_name: dict[str, ParameterBehaviorBinding] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "parameter_bindings_by_name",
            {
                parameter_binding.parameter_spec.parameter_name: parameter_binding
                for parameter_binding in self.parameter_bindings
            },
        )


def bind_parameter_behavior(
    parameter_spec: ParameterSpec,
    behavior_registry: BehaviorRegistry,
) -> ParameterBehaviorBinding:
    behavior_family_label = str(parameter_spec.behavior_family_label or "").strip()
    if not behavior_family_label:
        raise ValueError(
            f"parameter {parameter_spec.parameter_name!r} has no behavior_family_label for binding"
        )
    try:
        behavior = behavior_registry.get(behavior_family_label)
    except KeyError as exc:
        raise ValueError(
            f"parameter {parameter_spec.parameter_name!r} references unknown behavior_family_label="
            f"{behavior_family_label!r}"
        ) from exc
    return ParameterBehaviorBinding(parameter_spec=parameter_spec, behavior=behavior)


def bind_module_behaviors(
    module_spec: ModuleSpec,
    behavior_registry: BehaviorRegistry,
) -> ModuleBehaviorBinding:
    return ModuleBehaviorBinding(
        module_spec=module_spec,
        parameter_bindings=tuple(
            bind_parameter_behavior(parameter_spec, behavior_registry)
            for parameter_spec in module_spec.parameters
        ),
    )


def bind_assembly_behaviors(
    assembly_spec: HierarchyAssemblySpec,
    behavior_registry: BehaviorRegistry,
) -> tuple[ModuleBehaviorBinding, ...]:
    return tuple(
        bind_module_behaviors(module_spec, behavior_registry)
        for module_spec in assembly_spec.module_specs
    )
