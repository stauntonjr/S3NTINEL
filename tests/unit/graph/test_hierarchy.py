from libs.graph.hierarchy import assign_hierarchy_from_weighted_edges, assign_subsystems_from_weighted_edges


def test_assign_subsystems_groups_connected_components():
    sensors = ["S1", "S2", "S3", "S4"]
    edges = [
        ("S1", "S2", 0.9),
        ("S2", "S3", 0.8),
        ("S3", "S4", 0.05),
    ]

    mapping = assign_subsystems_from_weighted_edges(sensors, edges, min_edge_weight=0.2)

    assert mapping["S1"] == mapping["S2"] == mapping["S3"]
    assert mapping["S4"] != mapping["S1"]


def test_assign_subsystems_keeps_isolated_when_no_edges_above_threshold():
    sensors = ["A", "B"]
    edges = [("A", "B", 0.1)]

    mapping = assign_subsystems_from_weighted_edges(sensors, edges, min_edge_weight=0.2)

    assert mapping["A"] != mapping["B"]


def test_assign_hierarchy_from_weighted_edges_builds_two_levels():
    sensors = ["S1", "S2", "S3", "S4", "S5"]
    edges = [
        ("S1", "S2", 0.95),
        ("S2", "S3", 0.70),
        ("S4", "S5", 0.92),
        ("S3", "S4", 0.55),
    ]

    rows = assign_hierarchy_from_weighted_edges(
        sensors,
        edges,
        module_min_edge_weight=0.8,
        subsystem_min_edge_weight=0.5,
        system_min_edge_weight=0.4,
    )
    by_sensor = {row["parameter_name"]: row for row in rows}

    assert by_sensor["S1"]["module_id"] == by_sensor["S2"]["module_id"]
    assert by_sensor["S4"]["module_id"] == by_sensor["S5"]["module_id"]
    assert by_sensor["S1"]["module_id"] != by_sensor["S4"]["module_id"]

    assert by_sensor["S1"]["subsystem_id"] == by_sensor["S4"]["subsystem_id"]
    assert by_sensor["S1"]["system_id"] == by_sensor["S4"]["system_id"]
