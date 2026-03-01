"""Synthetic hierarchy profile generation for simulation-only correlation structure."""

from __future__ import annotations

from dataclasses import dataclass
import random


@dataclass(frozen=True)
class HierarchyShape:
    system_count: int = 3
    subsystems_per_system: int = 2
    modules_per_subsystem: int = 3


def synthesize_hierarchy(
    parameter_names: list[str],
    hierarchy_profile_id: str,
    shape: HierarchyShape,
    seed: int,
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    rng = random.Random(seed)

    systems = [f"SYS_{index + 1:02d}" for index in range(max(int(shape.system_count), 1))]
    subsystems: list[str] = []
    modules: list[str] = []

    subsystem_parent: dict[str, str] = {}
    module_parent: dict[str, str] = {}

    for system_id in systems:
        for sub_index in range(max(int(shape.subsystems_per_system), 1)):
            subsystem_id = f"{system_id}_SUB_{sub_index + 1:02d}"
            subsystems.append(subsystem_id)
            subsystem_parent[subsystem_id] = system_id
            for module_index in range(max(int(shape.modules_per_subsystem), 1)):
                module_id = f"{subsystem_id}_MOD_{module_index + 1:02d}"
                modules.append(module_id)
                module_parent[module_id] = subsystem_id

    unique_parameters = sorted({str(name).strip() for name in parameter_names if str(name).strip()})
    if not unique_parameters:
        raise ValueError("parameter_names cannot be empty")

    assignment_rows: list[dict[str, object]] = []
    if not modules:
        raise ValueError("hierarchy shape produced no modules")

    for index, parameter_name in enumerate(unique_parameters):
        base_module = modules[index % len(modules)]
        if rng.random() < 0.30:
            module_id = rng.choice(modules)
        else:
            module_id = base_module

        subsystem_id = module_parent[module_id]
        system_id = subsystem_parent[subsystem_id]
        assignment_rows.append(
            {
                "parameter_name": parameter_name,
                "system_id": system_id,
                "subsystem_id": subsystem_id,
                "module_id": module_id,
                "hierarchy_profile_id": hierarchy_profile_id,
                "hierarchy_source": "synthetic_injected",
            }
        )

    node_rows: list[dict[str, object]] = [
        {
            "node_id": "GLOBAL",
            "parent_node_id": None,
            "node_type": "global",
            "node_name": "GLOBAL",
            "hierarchy_profile_id": hierarchy_profile_id,
            "hierarchy_source": "synthetic_injected",
        }
    ]
    edge_rows: list[dict[str, object]] = []

    for system_id in systems:
        node_rows.append(
            {
                "node_id": system_id,
                "parent_node_id": "GLOBAL",
                "node_type": "system",
                "node_name": system_id,
                "hierarchy_profile_id": hierarchy_profile_id,
                "hierarchy_source": "synthetic_injected",
            }
        )
        edge_rows.append(
            {
                "parent_node_id": "GLOBAL",
                "child_node_id": system_id,
                "edge_type": "contains",
                "hierarchy_profile_id": hierarchy_profile_id,
                "hierarchy_source": "synthetic_injected",
            }
        )

    for subsystem_id in subsystems:
        parent = subsystem_parent[subsystem_id]
        node_rows.append(
            {
                "node_id": subsystem_id,
                "parent_node_id": parent,
                "node_type": "subsystem",
                "node_name": subsystem_id,
                "hierarchy_profile_id": hierarchy_profile_id,
                "hierarchy_source": "synthetic_injected",
            }
        )
        edge_rows.append(
            {
                "parent_node_id": parent,
                "child_node_id": subsystem_id,
                "edge_type": "contains",
                "hierarchy_profile_id": hierarchy_profile_id,
                "hierarchy_source": "synthetic_injected",
            }
        )

    for module_id in modules:
        parent = module_parent[module_id]
        node_rows.append(
            {
                "node_id": module_id,
                "parent_node_id": parent,
                "node_type": "module",
                "node_name": module_id,
                "hierarchy_profile_id": hierarchy_profile_id,
                "hierarchy_source": "synthetic_injected",
            }
        )
        edge_rows.append(
            {
                "parent_node_id": parent,
                "child_node_id": module_id,
                "edge_type": "contains",
                "hierarchy_profile_id": hierarchy_profile_id,
                "hierarchy_source": "synthetic_injected",
            }
        )

    for assignment in assignment_rows:
        sensor_node_id = f"SENSOR::{assignment['parameter_name']}"
        parent = str(assignment["module_id"])
        node_rows.append(
            {
                "node_id": sensor_node_id,
                "parent_node_id": parent,
                "node_type": "sensor",
                "node_name": str(assignment["parameter_name"]),
                "hierarchy_profile_id": hierarchy_profile_id,
                "hierarchy_source": "synthetic_injected",
            }
        )
        edge_rows.append(
            {
                "parent_node_id": parent,
                "child_node_id": sensor_node_id,
                "edge_type": "contains",
                "hierarchy_profile_id": hierarchy_profile_id,
                "hierarchy_source": "synthetic_injected",
            }
        )

    return node_rows, edge_rows, assignment_rows
