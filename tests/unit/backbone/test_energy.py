from libs.backbone import aggregate_sensor_energy_over_corpus, compute_window_sensor_energy


def test_compute_window_sensor_energy_from_scaled_vectors():
    sampled_windows = [
        {"continuous_vector_t_end_scaled": {"s1": 1.0, "s2": 2.0}},
        {"continuous_vector_t_end_scaled": {"s1": 3.0}},
        {"continuous_vector_t_end_scaled": {"s2": -1.0}},
    ]
    out = compute_window_sensor_energy(sampled_windows, vector_field="continuous_vector_t_end_scaled")
    by_sensor = {str(item["parameter_name"]): float(item["energy"]) for item in out}
    support = {str(item["parameter_name"]): int(item["support_count"]) for item in out}

    assert by_sensor["s1"] == 10.0
    assert by_sensor["s2"] == 5.0
    assert support["s1"] == 2
    assert support["s2"] == 2


def test_aggregate_sensor_energy_over_corpus():
    per_flight = [
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "s1", "energy": 10.0, "support_count": 2},
        {"tail_id": "T1", "flight_id": "F1", "parameter_name": "s2", "energy": 5.0, "support_count": 2},
        {"tail_id": "T2", "flight_id": "F2", "parameter_name": "s1", "energy": 3.0, "support_count": 1},
    ]
    out = aggregate_sensor_energy_over_corpus(per_flight)
    assert out[0]["parameter_name"] == "s1"
    assert float(out[0]["energy"]) == 13.0
    assert int(out[0]["flight_support_count"]) == 2
    assert out[1]["parameter_name"] == "s2"
    assert float(out[1]["energy"]) == 5.0
