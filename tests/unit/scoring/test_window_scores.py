from __future__ import annotations

from libs.scoring import WindowScoreArtifacts, build_phase_window_score_baselines, score_phase_window_rows


def test_scoring_v2_builds_baselines_and_scores():
    window_s_rows = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "win_id": 1,
            "s_w": [0.0, 0.0],
            "backbone_reconstruction_error": 0.1,
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "win_id": 2,
            "s_w": [0.1, 0.0],
            "backbone_reconstruction_error": 0.2,
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "win_id": 3,
            "s_w": [5.0, 0.0],
            "backbone_reconstruction_error": 2.0,
        },
    ]
    phase_assignments = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "win_id": 1,
            "phase_id_detected": 0,
            "phase_state_detected": "stable",
            "phase_confidence_detected": 0.9,
            "distance_to_centroid_detected": 0.1,
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "win_id": 2,
            "phase_id_detected": 0,
            "phase_state_detected": "stable",
            "phase_confidence_detected": 0.8,
            "distance_to_centroid_detected": 0.2,
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "win_id": 3,
            "phase_id_detected": 0,
            "phase_state_detected": "transition_region",
            "phase_confidence_detected": 0.1,
            "distance_to_centroid_detected": 5.0,
        },
    ]

    baselines = build_phase_window_score_baselines(window_s_rows, phase_assignments)
    scores = score_phase_window_rows(window_s_rows, phase_assignments, baselines)

    assert len(baselines) == 1
    assert baselines[0]["stable_window_count"] == 2
    scored = {int(item["win_id"]): item for item in scores}
    assert scored[3]["global_score"] > scored[1]["global_score"]
    assert scored[3]["severity"] in {"low", "medium", "high"}


def test_window_score_artifacts_build_from_phase_rows():
    phase_window_rows = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "win_id": 1,
            "s_w": [0.0, 0.0],
            "backbone_reconstruction_error": 0.1,
            "backbone_residual_by_parameter": {"p1": 0.2, "p2": 0.1},
            "phase_id_detected": 0,
            "phase_state_detected": "stable",
            "phase_confidence_detected": 0.9,
            "distance_to_centroid_detected": 0.1,
            "drift_magnitude": 0.1,
            "breadth": 0.2,
            "date_utc": "2025-01-01",
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "win_id": 2,
            "s_w": [5.0, 0.0],
            "backbone_reconstruction_error": 2.0,
            "backbone_residual_by_parameter": {"p1": 2.0, "p2": 0.2},
            "phase_id_detected": 0,
            "phase_state_detected": "transition_region",
            "phase_confidence_detected": 0.1,
            "distance_to_centroid_detected": 5.0,
            "drift_magnitude": 1.2,
            "breadth": 0.8,
            "date_utc": "2025-01-01",
        },
    ]
    phase_baseline_rows = [
        {
            "tail_id": "T1",
            "phase_id_detected": 0,
            "s_w_centroid": [0.0, 0.0],
            "reconstruction_median": 0.1,
            "reconstruction_mad": 0.1,
            "distance_median": 0.1,
            "distance_mad": 0.1,
        }
    ]
    hierarchy_rows = [
        {"parameter_name": "p1", "subsystem_id": "SUB1"},
        {"parameter_name": "p2", "subsystem_id": "SUB2"},
    ]

    artifacts = WindowScoreArtifacts.from_phase_rows(
        phase_window_rows,
        phase_baseline_rows,
        hierarchy_rows,
    )

    assert len(artifacts.rows) == 2
    by_win = {int(item["win_id"]): item for item in artifacts.rows}
    assert by_win[2]["global_score"] > by_win[1]["global_score"]
    assert by_win[2]["dominant_subsystem_id"] == "SUB1"
