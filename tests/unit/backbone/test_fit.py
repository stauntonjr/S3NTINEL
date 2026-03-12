from __future__ import annotations

import numpy as np

from libs.backbone import (
    BackboneModel,
    BackboneSpec,
    aggregate_backbone_gh,
    compute_backbone_gh_by_flight,
    reconstruct_window_vector,
    reconstruction_error,
    select_backbone_sensors_by_energy,
    solve_backbone_weights,
)


def test_select_backbone_sensors_by_energy_orders_descending():
    rows = [
        {"parameter_name": "s1", "energy": 5.0},
        {"parameter_name": "s2", "energy": 2.0},
        {"parameter_name": "s3", "energy": 1.0},
    ]
    assert select_backbone_sensors_by_energy(rows, k=2) == ["s1", "s2"]


def test_compute_gh_and_solve_weights_reconstructs_simple_system():
    windows = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "continuous_vector_t_end_scaled": {
                "s1": 1.0,
                "s2": 2.0,
                "s3": 3.0,
            },
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "continuous_vector_t_end_scaled": {
                "s1": 2.0,
                "s2": 4.0,
                "s3": 6.0,
            },
        },
    ]

    gh_rows, all_sensors = compute_backbone_gh_by_flight(
        windows,
        selected_sensors=["s1"],
        all_sensors=["s1", "s2", "s3"],
    )
    g, h, total_windows = aggregate_backbone_gh(gh_rows)
    weights = solve_backbone_weights(g, h, ridge_lambda=0.0)

    assert all_sensors == ["s1", "s2", "s3"]
    assert total_windows == 2
    assert g.shape == (1, 1)
    assert h.shape == (1, 3)
    assert np.allclose(weights, np.asarray([[1.0, 2.0, 3.0]]))

    x_hat = reconstruct_window_vector(
        {"s1": 3.0},
        selected_sensors=["s1"],
        all_sensors=all_sensors,
        weights_b=weights,
    )
    error, residuals = reconstruction_error(
        {"s1": 3.0, "s2": 6.0, "s3": 9.0},
        x_hat,
        sensor_order=all_sensors,
    )

    assert np.isclose(error, 0.0)
    assert residuals == {"s1": 0.0, "s2": 0.0, "s3": 0.0}


def test_backbone_model_from_window_x_rows_builds_model_and_energy_rows():
    window_x_rows = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "continuous_vector_t_end_scaled": {
                "s1": 1.0,
                "s2": 2.0,
                "s3": 3.0,
            },
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "continuous_vector_t_end_scaled": {
                "s1": 2.0,
                "s2": 4.0,
                "s3": 6.0,
            },
        },
    ]

    model, sensor_energies = BackboneModel.from_window_x_rows(
        window_x_rows,
        spec=BackboneSpec(sensor_count=1, ridge_lambda=0.0),
    )

    assert model.selected_sensors_c == ["s3"]
    assert model.all_sensors == ["s1", "s2", "s3"]
    assert model.training_window_count == 2
    assert len(sensor_energies) == 3
    assert sensor_energies[0].parameter_name == "s3"
    assert sensor_energies[0].selected_backbone is True
