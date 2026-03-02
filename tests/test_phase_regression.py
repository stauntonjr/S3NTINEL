from datetime import date

import pytest

from libs.phase.drift import build_phase_centroids, build_phase_windows


def test_phase_persistence_resets_on_drift_reversal(spark):
    rows = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "win_id": 1,
            "drift_mag": 1.0,
            "breadth": 1.0,
            "drift_dir": [1.0],
            "cur_block": [0.0, 0.0, 1000.0],
            "date_utc": date(2026, 1, 1),
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "win_id": 2,
            "drift_mag": 2.0,
            "breadth": 1.0,
            "drift_dir": [1.0],
            "cur_block": [0.0, 0.0, 1000.0],
            "date_utc": date(2026, 1, 1),
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "win_id": 3,
            "drift_mag": 1.0,
            "breadth": 1.0,
            "drift_dir": [-1.0],
            "cur_block": [0.0, 0.0, 1000.0],
            "date_utc": date(2026, 1, 1),
        },
    ]

    signatures_df = spark.createDataFrame(rows)
    phase_windows_df = build_phase_windows(
        signatures_df=signatures_df,
        tau_near_q=0.25,
        tau_far_q=0.9,
        persistence_q=0.95,
    )

    persistence_by_win = {
        int(row["win_id"]): float(row["persistence"])
        for row in phase_windows_df.select("win_id", "persistence").collect()
    }

    assert persistence_by_win[2] > persistence_by_win[1]
    assert persistence_by_win[3] < persistence_by_win[2]


def test_phase_centroids_use_stable_windows_only(spark):
    rows = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "win_id": 1,
            "phase_id": 1,
            "phase_state": "stable",
            "phase_confidence": 0.9,
            "distance_to_centroid": 10.0,
            "drift_magnitude": 10.0,
            "breadth": 0.2,
            "persistence": 5.0,
            "is_stable": True,
            "phase_persistent": True,
            "date_utc": date(2026, 1, 1),
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "win_id": 2,
            "phase_id": 1,
            "phase_state": "transition_region",
            "phase_confidence": 0.1,
            "distance_to_centroid": 100.0,
            "drift_magnitude": 100.0,
            "breadth": 0.95,
            "persistence": 20.0,
            "is_stable": False,
            "phase_persistent": True,
            "date_utc": date(2026, 1, 1),
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "win_id": 3,
            "phase_id": 1,
            "phase_state": "stable",
            "phase_confidence": 0.7,
            "distance_to_centroid": 20.0,
            "drift_magnitude": 20.0,
            "breadth": 0.3,
            "persistence": 7.0,
            "is_stable": True,
            "phase_persistent": True,
            "date_utc": date(2026, 1, 1),
        },
    ]

    phase_windows_df = spark.createDataFrame(rows)
    phases_df = build_phase_centroids(phase_windows_df, version=1)

    row = phases_df.where("tail_id = 'T1' and phase_id = 1").collect()[0]

    assert float(row["centroid"][0]) == pytest.approx(15.0, rel=1e-6)
    assert float(row["centroid"][1]) == pytest.approx(0.25, rel=1e-6)
    assert float(row["centroid"][2]) == pytest.approx(0.8, rel=1e-6)
