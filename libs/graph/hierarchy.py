"""Subsystem discovery helpers from weighted sensor graphs."""

from __future__ import annotations


def assign_subsystems_from_weighted_edges(
    sensor_ids: list[str],
    edges: list[tuple[str, str, float]],
    *,
    min_edge_weight: float,
) -> dict[str, str]:
    sensors = sorted({str(sensor).strip() for sensor in sensor_ids if str(sensor).strip()})
    parent: dict[str, str] = {sensor: sensor for sensor in sensors}
    rank: dict[str, int] = {sensor: 0 for sensor in sensors}

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
        left_sensor = str(left).strip()
        right_sensor = str(right).strip()
        if not left_sensor or not right_sensor or left_sensor == right_sensor:
            continue
        if left_sensor not in parent or right_sensor not in parent:
            continue
        if float(weight) < threshold:
            continue
        union(left_sensor, right_sensor)

    groups: dict[str, list[str]] = {}
    for sensor in sensors:
        root = find(sensor)
        groups.setdefault(root, []).append(sensor)

    ordered_groups = sorted(
        groups.values(),
        key=lambda items: (-len(items), items[0]),
    )

    out: dict[str, str] = {}
    for index, members in enumerate(ordered_groups, start=1):
        subsystem_id = f"SUBSYS_{index:04d}"
        for sensor in members:
            out[sensor] = subsystem_id
    return out
