from __future__ import annotations

from libs.scoring import build_phase_score_baselines, score_window_s_rows


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

    baselines = build_phase_score_baselines(window_s_rows, phase_assignments)
    scores = score_window_s_rows(window_s_rows, phase_assignments, baselines)

    assert len(baselines) == 1
    assert baselines[0]["stable_window_count"] == 2
    scored = {int(item["win_id"]): item for item in scores}
    assert scored[3]["global_score"] > scored[1]["global_score"]
    assert scored[3]["severity"] in {"low", "medium", "high"}
