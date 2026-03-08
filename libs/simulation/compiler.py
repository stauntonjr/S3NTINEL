"""Helpers for compiling legacy hierarchy inputs into assembly-level simulation specs."""

from __future__ import annotations

from typing import Any

from libs.simulation.legacy import module_specs_from_hierarchy_spec
from libs.simulation.specs import (
    HierarchyAssemblySpec,
    InterModuleCouplingSpec,
    ModuleSpec,
)


def inter_module_coupling_spec_from_row(row: dict[str, Any]) -> InterModuleCouplingSpec:
    return InterModuleCouplingSpec(
        source_module_id=str(row.get("source_module_id", "")),
        source_port_name=str(row.get("source_port_name", "")),
        target_module_id=str(row.get("target_module_id", "")),
        target_port_name=str(row.get("target_port_name", "")),
        relation_type=str(row.get("relation_type", "")),
        gain=float(row.get("gain", 1.0)),
        sign=int(row.get("sign", 1)),
        lag_seconds=float(row.get("lag_seconds", 0.0)),
        time_constant_seconds=(
            None if row.get("time_constant_seconds") is None else float(row.get("time_constant_seconds"))
        ),
        phase_gate=tuple(str(item) for item in (row.get("phase_gate") or ())),
        mode_gate=tuple(str(item) for item in (row.get("mode_gate") or ())),
        source_mode_name=(None if row.get("source_mode_name") is None else str(row.get("source_mode_name"))),
        source_mode_gate=tuple(str(item) for item in (row.get("source_mode_gate") or ())),
        target_mode_name=(None if row.get("target_mode_name") is None else str(row.get("target_mode_name"))),
        target_mode_gate=tuple(str(item) for item in (row.get("target_mode_gate") or ())),
        shared_noise_group=(
            None if row.get("shared_noise_group") is None else str(row.get("shared_noise_group"))
        ),
        metadata={
            key: value
            for key, value in row.items()
            if key
            not in {
                "source_module_id",
                "source_port_name",
                "target_module_id",
                "target_port_name",
                "relation_type",
                "gain",
                "sign",
                "lag_seconds",
                "time_constant_seconds",
                "phase_gate",
                "mode_gate",
                "source_mode_name",
                "source_mode_gate",
                "target_mode_name",
                "target_mode_gate",
                "shared_noise_group",
            }
        },
    )


def validate_assembly_spec(assembly_spec: HierarchyAssemblySpec) -> None:
    module_by_id: dict[str, ModuleSpec] = {
        module_spec.module_id: module_spec for module_spec in assembly_spec.module_specs
    }
    if len(module_by_id) != len(assembly_spec.module_specs):
        raise ValueError("assembly spec contains duplicate module_id values")

    for coupling in assembly_spec.inter_module_couplings:
        source_module = module_by_id.get(coupling.source_module_id)
        target_module = module_by_id.get(coupling.target_module_id)
        if source_module is None:
            raise ValueError(
                f"inter-module coupling references unknown source module_id={coupling.source_module_id!r}"
            )
        if target_module is None:
            raise ValueError(
                f"inter-module coupling references unknown target module_id={coupling.target_module_id!r}"
            )

        source_ports = {port.port_name for port in source_module.output_ports}
        target_ports = {port.port_name for port in target_module.input_ports}

        if source_ports and coupling.source_port_name not in source_ports:
            raise ValueError(
                f"inter-module coupling references unknown source port "
                f"{coupling.source_port_name!r} on module_id={coupling.source_module_id!r}"
            )
        if target_ports and coupling.target_port_name not in target_ports:
            raise ValueError(
                f"inter-module coupling references unknown target port "
                f"{coupling.target_port_name!r} on module_id={coupling.target_module_id!r}"
            )
        if coupling.source_mode_gate and not coupling.source_mode_name:
            raise ValueError(
                "inter-module coupling declares source_mode_gate without source_mode_name"
            )
        if coupling.target_mode_gate and not coupling.target_mode_name:
            raise ValueError(
                "inter-module coupling declares target_mode_gate without target_mode_name"
            )


def assembly_spec_from_hierarchy_spec(
    hierarchy_spec: dict[str, Any],
    *,
    inter_module_coupling_rows: list[dict[str, Any]] | None = None,
    metadata: dict[str, Any] | None = None,
) -> HierarchyAssemblySpec:
    assembly_spec = HierarchyAssemblySpec(
        module_specs=module_specs_from_hierarchy_spec(hierarchy_spec),
        inter_module_couplings=tuple(
            inter_module_coupling_spec_from_row(row) for row in (inter_module_coupling_rows or [])
        ),
        metadata=dict(metadata or {}),
    )
    validate_assembly_spec(assembly_spec)
    return assembly_spec
