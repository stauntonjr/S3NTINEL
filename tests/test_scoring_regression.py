from datetime import date

from libs.scoring.build import build_scores_df


def test_scores_use_phase_robust_normalization_for_block_dominance(spark):
    signatures_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "pivot_block": [100.0],
                "cur_block": [0.0],
                "event_block": [1.0],
                "cat_block": [0.0],
                "breadth": 0.1,
                "drift_mag": 0.2,
                "date_utc": date(2026, 3, 2),
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 2,
                "pivot_block": [101.0],
                "cur_block": [0.0],
                "event_block": [1.0],
                "cat_block": [0.0],
                "breadth": 0.1,
                "drift_mag": 0.2,
                "date_utc": date(2026, 3, 2),
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 3,
                "pivot_block": [102.0],
                "cur_block": [0.0],
                "event_block": [10.0],
                "cat_block": [0.0],
                "breadth": 0.1,
                "drift_mag": 0.2,
                "date_utc": date(2026, 3, 2),
            },
        ]
    )

    phase_windows_df = spark.createDataFrame(
        [
            {"tail_id": "T1", "flight_id": "F1", "win_id": 1, "phase_state": "stable", "phase_id": 1, "phase_confidence": 0.9, "distance_to_centroid": 0.1, "drift_magnitude": 0.2, "breadth": 0.1, "date_utc": date(2026, 3, 2)},
            {"tail_id": "T1", "flight_id": "F1", "win_id": 2, "phase_state": "stable", "phase_id": 1, "phase_confidence": 0.9, "distance_to_centroid": 0.1, "drift_magnitude": 0.2, "breadth": 0.1, "date_utc": date(2026, 3, 2)},
            {"tail_id": "T1", "flight_id": "F1", "win_id": 3, "phase_state": "stable", "phase_id": 1, "phase_confidence": 0.9, "distance_to_centroid": 0.1, "drift_magnitude": 0.2, "breadth": 0.1, "date_utc": date(2026, 3, 2)},
        ]
    )

    scores_df = build_scores_df(signatures_df=signatures_df, phase_windows_df=phase_windows_df)
    rows = {int(row["win_id"]): row for row in scores_df.select("win_id", "dominant_block", "block_scores", "global_score").collect()}

    assert rows[3]["dominant_block"] == "event_block"
    assert float(rows[3]["block_scores"]["events"]) > float(rows[3]["block_scores"]["pivot"])
    assert float(rows[3]["global_score"]) >= 0.0
    assert float(rows[3]["global_score"]) < 10.0


def test_scores_respect_env_severity_threshold_overrides(spark, monkeypatch):
    monkeypatch.setenv("S3NTINEL_SEVERITY_LOW_THRESHOLD", "0.0001")
    monkeypatch.setenv("S3NTINEL_SEVERITY_MEDIUM_THRESHOLD", "0.0005")
    monkeypatch.setenv("S3NTINEL_SEVERITY_HIGH_THRESHOLD", "0.001")

    signatures_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "pivot_block": [100.0],
                "cur_block": [0.0],
                "event_block": [1.0],
                "cat_block": [0.0],
                "breadth": 0.1,
                "drift_mag": 0.2,
                "date_utc": date(2026, 3, 2),
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 2,
                "pivot_block": [101.0],
                "cur_block": [0.0],
                "event_block": [1.0],
                "cat_block": [0.0],
                "breadth": 0.1,
                "drift_mag": 0.2,
                "date_utc": date(2026, 3, 2),
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 3,
                "pivot_block": [102.0],
                "cur_block": [0.0],
                "event_block": [10.0],
                "cat_block": [0.0],
                "breadth": 0.1,
                "drift_mag": 0.2,
                "date_utc": date(2026, 3, 2),
            },
        ]
    )

    phase_windows_df = spark.createDataFrame(
        [
            {"tail_id": "T1", "flight_id": "F1", "win_id": 1, "phase_state": "stable", "phase_id": 1, "phase_confidence": 0.9, "distance_to_centroid": 0.1, "drift_magnitude": 0.2, "breadth": 0.1, "date_utc": date(2026, 3, 2)},
            {"tail_id": "T1", "flight_id": "F1", "win_id": 2, "phase_state": "stable", "phase_id": 1, "phase_confidence": 0.9, "distance_to_centroid": 0.1, "drift_magnitude": 0.2, "breadth": 0.1, "date_utc": date(2026, 3, 2)},
            {"tail_id": "T1", "flight_id": "F1", "win_id": 3, "phase_state": "stable", "phase_id": 1, "phase_confidence": 0.9, "distance_to_centroid": 0.1, "drift_magnitude": 0.2, "breadth": 0.1, "date_utc": date(2026, 3, 2)},
        ]
    )

    severities = [row["severity"] for row in build_scores_df(signatures_df=signatures_df, phase_windows_df=phase_windows_df).select("severity").collect()]

    assert "high" in severities
