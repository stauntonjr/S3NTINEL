from __future__ import annotations

from datetime import date

from libs.io.schemas.scoring import WINDOW_SCORES_RAW_SCHEMA
from libs.scoring import (
    WindowScoreArtifacts,
    WindowScoresCalibratedTable,
    build_phase_window_score_baselines,
    score_phase_window_rows,
)


def _score_row(
    *,
    win_id: int,
    phase_id_detected: int,
    global_score: float,
    severity: str,
    flight_id: str = "F1",
    date_utc: date = date(2025, 1, 1),
) -> dict[str, object]:
    return {
        "tail_id": "T1",
        "flight_id": flight_id,
        "win_id": int(win_id),
        "phase_state_detected": "stable",
        "phase_id_detected": int(phase_id_detected),
        "phase_confidence_detected": 0.9,
        "distance_to_centroid_detected": 0.1,
        "drift_magnitude": 0.1,
        "breadth": 0.2,
        "global_score": float(global_score),
        "p_value": 1.0,
        "severity": str(severity),
        "dominant_subsystem_id": None,
        "dominant_score_component": "structure",
        "subsystem_scores": {},
        "score_component_scores": {"structure": float(global_score), "reconstruction": 0.0},
        "date_utc": date_utc,
    }


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


def test_window_score_calibration_emits_warm_windows_by_severity_and_rarity(spark):
    rows = [
        _score_row(win_id=win_id, phase_id_detected=0, global_score=float(21 - win_id), severity="low")
        for win_id in range(1, 21)
    ]
    rows[0]["severity"] = "normal"
    rows[1]["severity"] = "low"
    rows[2]["severity"] = "high"
    rows[3]["severity"] = "low"
    rows.extend(
        [
            _score_row(
                win_id=101,
                phase_id_detected=1,
                global_score=9.0,
                severity="high",
                flight_id="F2",
            ),
            _score_row(
                win_id=102,
                phase_id_detected=1,
                global_score=8.0,
                severity="high",
                flight_id="F2",
            ),
        ]
    )

    calibrated = WindowScoresCalibratedTable.from_scores(
        spark.createDataFrame(rows, schema=WINDOW_SCORES_RAW_SCHEMA()),
        min_warm=3,
    ).to_dataframe()
    records = {
        (str(row["flight_id"]), int(row["win_id"])): row
        for row in calibrated.select("flight_id", "win_id", "p_value", "warm", "emit_ready").collect()
    }

    assert records[("F1", 1)]["warm"] is True
    assert records[("F1", 1)]["emit_ready"] is False
    assert float(records[("F1", 1)]["p_value"]) == 0.05

    assert records[("F1", 2)]["warm"] is True
    assert records[("F1", 2)]["emit_ready"] is True
    assert float(records[("F1", 2)]["p_value"]) == 0.10

    assert records[("F1", 3)]["warm"] is True
    assert records[("F1", 3)]["emit_ready"] is True
    assert float(records[("F1", 3)]["p_value"]) == 0.15

    assert records[("F1", 4)]["warm"] is True
    assert records[("F1", 4)]["emit_ready"] is False
    assert float(records[("F1", 4)]["p_value"]) == 0.20

    assert records[("F2", 101)]["warm"] is False
    assert records[("F2", 101)]["emit_ready"] is False
    assert records[("F2", 101)]["p_value"] is None
