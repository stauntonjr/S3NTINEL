"""Native assembly authoring helpers for V2.1 simulation specs."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from libs.simulation.compiler import validate_assembly_spec
from libs.simulation.specs import HierarchyAssemblySpec, InterModuleCouplingSpec, ModuleSpec


@dataclass(slots=True)
class HierarchyAssemblyBuilder:
    metadata: dict[str, Any] = field(default_factory=dict)
    _module_specs: dict[str, ModuleSpec] = field(default_factory=dict, init=False, repr=False)
    _inter_module_couplings: list[InterModuleCouplingSpec] = field(default_factory=list, init=False, repr=False)

    def add_module(self, module_spec: ModuleSpec) -> "HierarchyAssemblyBuilder":
        module_id = str(module_spec.module_id)
        if module_id in self._module_specs:
            raise ValueError(f"duplicate module_id in assembly builder: {module_id!r}")
        self._module_specs[module_id] = module_spec
        return self

    def add_inter_module_coupling(
        self, inter_module_coupling_spec: InterModuleCouplingSpec
    ) -> "HierarchyAssemblyBuilder":
        self._inter_module_couplings.append(inter_module_coupling_spec)
        return self

    def build(self) -> HierarchyAssemblySpec:
        assembly_spec = HierarchyAssemblySpec(
            module_specs=tuple(self._module_specs.values()),
            inter_module_couplings=tuple(self._inter_module_couplings),
            metadata=dict(self.metadata),
        )
        validate_assembly_spec(assembly_spec)
        return assembly_spec


def build_hierarchy_assembly_spec(
    *,
    module_specs: tuple[ModuleSpec, ...],
    inter_module_couplings: tuple[InterModuleCouplingSpec, ...] = (),
    metadata: dict[str, Any] | None = None,
) -> HierarchyAssemblySpec:
    builder = HierarchyAssemblyBuilder(metadata=dict(metadata or {}))
    for module_spec in module_specs:
        builder.add_module(module_spec)
    for inter_module_coupling in inter_module_couplings:
        builder.add_inter_module_coupling(inter_module_coupling)
    return builder.build()
