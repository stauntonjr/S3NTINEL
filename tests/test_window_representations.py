from __future__ import annotations

import pandas as pd

from libs.windows import build_continuous_robust_scaler, build_window_s_rows, build_window_x_row, top_window_cooccurrence_sensor_pairs


def test_build_window_x_row_extracts_scaled_continuous_and_categorical_state():
    telemetry_df = pd.DataFrame(
        [
            {"parameter_name": "s_num", "parameter_value_clean": "0.0"},
            {"parameter_name": "s_num", "parameter_value_clean": "2.0"},
        ]
    )
    scaler = build_continuous_robust_scaler(telemetry_df)
    previous = {}
    window = {
        "tail_id": "T1",
        "flight_id": "F1",
        "win_id": 1,
        "t_start": "2026-03-01T00:00:00Z",
        "t_end": "2026-03-01T00:00:01Z",
        "duration_ms": 1000,
        "event_count": 2,
        "event_type_counts": {"transition": 1},
        "zoh_snapshot": {"s_num": "2.0", "s_cat": "ON"},
    }
    window_events = [
        {"parameter_name": "s_num", "payload": {"value": 2.0}},
        {"parameter_name": "s_cat", "payload": {"to": "ON"}},
    ]

    row = build_window_x_row(
        window=window,
        window_events=window_events,
        scaler_by_sensor=scaler,
        previous_scaled_by_flight=previous,
        phase_label="phase_a",
    )

    assert row["continuous_vector_t_end"] == {"s_num": 2.0}
    assert row["categorical_state_t_end"] == {"s_cat": "ON"}
    assert row["phase_label"] == "phase_a"


def test_build_window_s_rows_adds_x_c_and_s_w():
    window_x_rows = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "win_id": 1,
            "duration_ms": 1000,
            "event_count": 2,
            "event_type_counts": {"transition": 1, "slope_pos": 1},
            "continuous_vector_t_end_scaled": {"s_num": 1.0},
            "categorical_state_t_end": {"s_cat": "ON"},
        }
    ]

    rows, feature_names = build_window_s_rows(
        window_x_rows,
        selected_sensors_c=["s_num"],
        selected_event_types=["transition"],
        selected_categorical_state_pairs=[("s_cat", "ON")],
    )

    assert feature_names[0] == "parameter_name::s_num"
    assert rows[0]["x_c"] == [1.0]
    assert rows[0]["s_w"][0:3] == [1.0, 0.5, 1.0]


def test_top_window_cooccurrence_sensor_pairs_counts_joint_presence():
    rows = [
        {"continuous_vector_t_end_scaled": {"a": 1.0}, "categorical_state_t_end": {"b": "ON"}},
        {"continuous_vector_t_end_scaled": {"a": 1.0}, "categorical_state_t_end": {"b": "ON", "c": "OFF"}},
    ]
    pairs = top_window_cooccurrence_sensor_pairs(rows, k=2)
    assert ("a", "b") in pairs
