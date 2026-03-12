from __future__ import annotations

from libs.phase import evaluate_detected_phases
from libs.phase.model import PhaseFeatureConfig
from libs.phase.runtime import PhaseDetectionPolicy


def test_build_structure_vectors_uses_selected_sensors_and_event_types():
    windows = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "win_id": 1,
            "continuous_vector_t_end_scaled": {"s1": 1.5, "s2": -0.5},
            "event_type_counts": {"transition": 2, "slope_up": 1},
            "event_count": 4,
            "duration_ms": 1000,
        }
    ]

    structured, feature_names = PhaseFeatureConfig.build_structure_vectors(
        windows,
        selected_sensors=["s1", "s3"],
        selected_event_types=["transition", "switch"],
    )

    assert feature_names == [
        "parameter_name::s1",
        "parameter_name::s3",
        "event_type::transition",
        "event_type::switch",
        "summary::event_density_hz",
        "summary::continuous_event_fraction",
        "summary::categorical_event_fraction",
        "summary::active_sensor_fraction",
    ]
    assert structured[0]["s_w"] == [1.5, 0.0, 0.5, 0.0, 4.0, 0.0, 0.5, 1.0]


def test_detect_phases_from_windows_separates_low_drift_clusters():
    windows = []
    for win_id in range(1, 5):
        windows.append(
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": win_id,
                "t_end": f"2026-03-01T00:00:0{win_id}Z",
                "s_w": [0.0 + (0.01 * win_id), 0.0],
                "drift_magnitude_profiled": 0.05,
                "phase_label": "climb",
            }
        )
    for win_id in range(5, 9):
        windows.append(
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": win_id,
                "t_end": f"2026-03-01T00:00:{win_id:02d}Z",
                "s_w": [10.0 + (0.01 * win_id), 0.0],
                "drift_magnitude_profiled": 0.05,
                "phase_label": "cruise",
            }
        )

    assignments, baselines = PhaseDetectionPolicy(phase_count=2, stable_drift_quantile=0.5).detect(windows)

    assert len(assignments) == 8
    assert len(baselines) == 2

    climb_phase_ids = {item["phase_id_detected"] for item in assignments if item["phase_label"] == "climb"}
    cruise_phase_ids = {item["phase_id_detected"] for item in assignments if item["phase_label"] == "cruise"}

    assert len(climb_phase_ids) == 1
    assert len(cruise_phase_ids) == 1
    assert climb_phase_ids != cruise_phase_ids


def test_evaluate_detected_phases_uses_best_tail_local_mapping():
    assignments = [
        {"tail_id": "T1", "flight_id": "F1", "win_id": 1, "phase_id_detected": 1, "phase_label": "climb"},
        {"tail_id": "T1", "flight_id": "F1", "win_id": 2, "phase_id_detected": 1, "phase_label": "climb"},
        {"tail_id": "T1", "flight_id": "F1", "win_id": 3, "phase_id_detected": 0, "phase_label": "cruise"},
        {"tail_id": "T1", "flight_id": "F1", "win_id": 4, "phase_id_detected": 0, "phase_label": "cruise"},
    ]

    metrics = evaluate_detected_phases(assignments)

    assert metrics["overall_accuracy"] == 1.0
    assert metrics["by_tail"][0]["accuracy"] == 1.0
    assert metrics["by_tail_flight"][0]["accuracy"] == 1.0


def test_detect_phases_from_windows_enforces_minimum_dwell():
    windows = []
    for win_id, value in enumerate([0.0, 0.1, 5.0, 0.2, 0.0], start=1):
        windows.append(
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": win_id,
                "t_end": f"2026-03-01T00:00:{win_id:02d}Z",
                "s_w": [value],
                "drift_magnitude_profiled": 0.01,
                "phase_label": "steady",
            }
        )

    assignments, _ = PhaseDetectionPolicy(
        phase_count=2,
        stable_drift_quantile=1.0,
        transition_penalty=2.0,
        min_dwell_windows=2,
    ).detect(windows)

    phase_ids = [int(item["phase_id_detected"]) for item in assignments]
    # The middle singleton spike should be absorbed into a neighboring segment.
    assert phase_ids[1] == phase_ids[2] or phase_ids[2] == phase_ids[3]


def test_detect_phases_from_windows_robust_scales_structure_dimensions():
    windows = []
    for win_id in range(1, 5):
        windows.append(
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": win_id,
                "t_end": f"2026-03-01T00:00:{win_id:02d}Z",
                "s_w": [0.0, 1000.0 + win_id],
                "drift_magnitude_profiled": 0.02,
                "phase_label": "alpha",
            }
        )
    for win_id in range(5, 9):
        windows.append(
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": win_id,
                "t_end": f"2026-03-01T00:00:{win_id:02d}Z",
                "s_w": [10.0, 1000.0 + win_id],
                "drift_magnitude_profiled": 0.02,
                "phase_label": "beta",
            }
        )

    assignments, baselines = PhaseDetectionPolicy(phase_count=2, stable_drift_quantile=0.5).detect(windows)

    assert len(assignments) == 8
    assert len(baselines) == 2
    alpha_phase_ids = {item["phase_id_detected"] for item in assignments if item["phase_label"] == "alpha"}
    beta_phase_ids = {item["phase_id_detected"] for item in assignments if item["phase_label"] == "beta"}
    assert len(alpha_phase_ids) == 1
    assert len(beta_phase_ids) == 1
    assert alpha_phase_ids != beta_phase_ids
