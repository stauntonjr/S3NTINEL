"""Subsystem discovery helpers from weighted sensor graphs."""

from __future__ import annotations


def _connected_components_from_edges(
    node_ids: list[str],
    edges: list[tuple[str, str, float]],
    *,
    min_edge_weight: float,
) -> list[list[str]]:
    nodes = sorted({str(node).strip() for node in node_ids if str(node).strip()})
    parent: dict[str, str] = {node: node for node in nodes}
    rank: dict[str, int] = {node: 0 for node in nodes}

    def find(item: str) -> str:
        root = item
        while parent[root] != root:
            root = parent[root]
        while parent[item] != item:
            nxt = parent[item]
            parent[item] = root
            item = nxt
        return root

    def union(left: str, right: str) -> None:
        root_left = find(left)
        root_right = find(right)
        if root_left == root_right:
            return
        if rank[root_left] < rank[root_right]:
            parent[root_left] = root_right
            return
        if rank[root_left] > rank[root_right]:
            parent[root_right] = root_left
            return
        parent[root_right] = root_left
        rank[root_left] += 1

    threshold = float(min_edge_weight)
    for left, right, weight in edges:
        left_node = str(left).strip()
        right_node = str(right).strip()
        if not left_node or not right_node or left_node == right_node:
            continue
        if left_node not in parent or right_node not in parent:
            continue
        if float(weight) < threshold:
            continue
        union(left_node, right_node)

    groups: dict[str, list[str]] = {}
    for node in nodes:
        root = find(node)
        groups.setdefault(root, []).append(node)
    return sorted(groups.values(), key=lambda items: (-len(items), items[0]))


def assign_subsystems_from_weighted_edges(
    sensor_ids: list[str],
    edges: list[tuple[str, str, float]],
    *,
    min_edge_weight: float,
) -> dict[str, str]:
    ordered_groups = _connected_components_from_edges(
        sensor_ids,
        edges,
        min_edge_weight=min_edge_weight,
    )
    out: dict[str, str] = {}
    for index, members in enumerate(ordered_groups, start=1):
        subsystem_id = f"SUBSYS_{index:04d}"
        for sensor in members:
            out[sensor] = subsystem_id
    return out


def assign_hierarchy_from_weighted_edges(
    sensor_ids: list[str],
    edges: list[tuple[str, str, float]],
    *,
    module_min_edge_weight: float,
    subsystem_min_edge_weight: float | None = None,
    system_min_edge_weight: float | None = None,
    rollup_edges: list[tuple[str, str, float]] | None = None,
) -> list[dict[str, str]]:
    sensors = sorted({str(sensor).strip() for sensor in sensor_ids if str(sensor).strip()})
    module_groups = _connected_components_from_edges(
        sensors,
        edges,
        min_edge_weight=float(module_min_edge_weight),
    )
    module_by_sensor: dict[str, str] = {}
    modules: list[dict[str, object]] = []
    for index, members in enumerate(module_groups, start=1):
        module_id = f"MOD_{index:04d}"
        modules.append({"module_id": module_id, "members": list(members)})
        for sensor in members:
            module_by_sensor[sensor] = module_id

    subsystem_threshold = float(subsystem_min_edge_weight) if subsystem_min_edge_weight is not None else max(float(module_min_edge_weight) * 0.75, 1e-6)
    module_edge_weights: dict[tuple[str, str], list[float]] = {}
    inter_module_edges = rollup_edges if rollup_edges is not None else edges
    for left, right, weight in inter_module_edges:
        left_sensor = str(left).strip()
        right_sensor = str(right).strip()
        if left_sensor == right_sensor:
            continue
        left_module = module_by_sensor.get(left_sensor)
        right_module = module_by_sensor.get(right_sensor)
        if not left_module or not right_module or left_module == right_module:
            continue
        key = tuple(sorted((left_module, right_module)))
        module_edge_weights.setdefault(key, []).append(float(weight))
    module_edges = [
        (left_module, right_module, float(sum(weights) / max(len(weights), 1)))
        for (left_module, right_module), weights in sorted(module_edge_weights.items())
    ]
    module_ids = [str(item["module_id"]) for item in modules]
    subsystem_groups = _connected_components_from_edges(
        module_ids,
        module_edges,
        min_edge_weight=subsystem_threshold,
    )
    subsystem_by_module: dict[str, str] = {}
    subsystem_membership: list[dict[str, object]] = []
    for index, members in enumerate(subsystem_groups, start=1):
        subsystem_id = f"SUBSYS_{index:04d}"
        subsystem_membership.append({"subsystem_id": subsystem_id, "members": list(members)})
        for module_id in members:
            subsystem_by_module[module_id] = subsystem_id

    system_threshold = float(system_min_edge_weight) if system_min_edge_weight is not None else max(subsystem_threshold * 0.75, 1e-6)
    subsystem_edge_weights: dict[tuple[str, str], list[float]] = {}
    for left_module, right_module, weight in module_edges:
        left_subsystem = subsystem_by_module.get(left_module)
        right_subsystem = subsystem_by_module.get(right_module)
        if not left_subsystem or not right_subsystem or left_subsystem == right_subsystem:
            continue
        key = tuple(sorted((left_subsystem, right_subsystem)))
        subsystem_edge_weights.setdefault(key, []).append(float(weight))
    subsystem_edges = [
        (left_subsystem, right_subsystem, float(sum(weights) / max(len(weights), 1)))
        for (left_subsystem, right_subsystem), weights in sorted(subsystem_edge_weights.items())
    ]
    subsystem_ids = [str(item["subsystem_id"]) for item in subsystem_membership]
    system_groups = _connected_components_from_edges(
        subsystem_ids,
        subsystem_edges,
        min_edge_weight=system_threshold,
    )
    system_by_subsystem: dict[str, str] = {}
    for index, members in enumerate(system_groups, start=1):
        system_id = f"SYS_{index:04d}"
        for subsystem_id in members:
            system_by_subsystem[subsystem_id] = system_id

    rows: list[dict[str, str]] = []
    for sensor in sensors:
        module_id = module_by_sensor[sensor]
        subsystem_id = subsystem_by_module[module_id]
        system_id = system_by_subsystem[subsystem_id]
        rows.append(
            {
                "parameter_name": sensor,
                "system_id": system_id,
                "subsystem_id": subsystem_id,
                "module_id": module_id,
            }
        )
    return rows
