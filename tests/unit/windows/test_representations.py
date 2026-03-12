from __future__ import annotations

import pandas as pd

from libs.windows import WindowFeatureSelection, WindowFeatures, WindowScaler


def test_build_window_x_row_extracts_scaled_continuous_and_categorical_state():
    telemetry_df = pd.DataFrame(
        [
            {"parameter_name": "s_num", "parameter_value_clean": "0.0"},
            {"parameter_name": "s_num", "parameter_value_clean": "2.0"},
        ]
    )
    scaler = WindowScaler.from_telemetry_df(telemetry_df)
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
        "window_events": [
            {"parameter_name": "s_num", "payload": {"value": 2.0}},
            {"parameter_name": "s_cat", "payload": {"to": "ON"}},
        ],
    }
    row = WindowFeatures.from_window_row(
        window=window,
        scaler=scaler,
        previous_scaled_by_flight=previous,
        phase_label="phase_a",
    ).row

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

    selection = WindowFeatureSelection(
        selected_sensors_c=["s_num"],
        selected_event_types=["transition"],
        selected_categorical_state_pairs=[("s_cat", "ON")],
    )
    rows, feature_names = selection.encode_rows(window_x_rows)

    assert feature_names[0] == "parameter_name::s_num"
    assert rows[0]["x_c"] == [1.0]
    assert rows[0]["s_w"][0:3] == [1.0, 0.5, 1.0]


def test_top_window_cooccurrence_sensor_pairs_counts_joint_presence():
    rows = [
        {"continuous_vector_t_end_scaled": {"a": 1.0}, "categorical_state_t_end": {"b": "ON"}},
        {"continuous_vector_t_end_scaled": {"a": 1.0}, "categorical_state_t_end": {"b": "ON", "c": "OFF"}},
    ]
    pairs = WindowFeatures.top_cooccurrence_sensor_pairs(rows, k=2)
    assert ("a", "b") in pairs
