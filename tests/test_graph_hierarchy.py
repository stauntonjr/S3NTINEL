from libs.graph.hierarchy import assign_subsystems_from_weighted_edges


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
