from datetime import date, datetime

import pytest

from libs.anomaly.build import build_anomalies_df
from libs.testing.sample_data import (
    create_sample_calibrated_df,
    create_sample_events_df,
    create_sample_signatures_df,
    create_sample_windows_df,
)


def test_anomaly_object_includes_panel_context_block_contrib_and_sensor_scores(spark):
    calibrated_df = create_sample_calibrated_df(spark)
    phase_windows_df = spark.createDataFrame(
        [
            {"tail_id": "T001", "flight_id": "F001", "win_id": 1, "phase_id": 0, "phase_state": "stable", "phase_confidence": 0.91, "distance_to_centroid": 0.12, "drift_magnitude": 1.8, "breadth": 0.35, "date_utc": date(2026, 2, 28)},
            {"tail_id": "T001", "flight_id": "F001", "win_id": 2, "phase_id": 3, "phase_state": "transition_region", "phase_confidence": 0.44, "distance_to_centroid": 0.88, "drift_magnitude": 4.5, "breadth": 0.72, "date_utc": date(2026, 2, 28)},
        ]
    )
    signatures_df = create_sample_signatures_df(spark)
    windows_df = create_sample_windows_df(spark)
    events_df = create_sample_events_df(spark)

    subsystem_map_df = spark.createDataFrame(
        [
            {"sensor": "ENG_TEMP_1", "subsystem_id": "SUBSYS_0001"},
            {"sensor": "PUMP_STATE", "subsystem_id": "SUBSYS_0002"},
        ]
    )

    raw_df = spark.createDataFrame(
        [
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "timestamp_utc": datetime(2026, 2, 28, 0, 0, 0, 250000),
                "parameter_name": "LCD_MSG",
                "parameter_value": "HYD_PRESS_LOW",
                "date_utc": date(2026, 2, 28),
            },
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "timestamp_utc": datetime(2026, 2, 28, 0, 0, 0, 850000),
                "parameter_name": "PANEL_TEXT",
                "parameter_value": "ELEC_WARN_12",
                "date_utc": date(2026, 2, 28),
            },
        ]
    )

    anomalies_df = build_anomalies_df(
        calibrated_df=calibrated_df,
        phase_windows_df=phase_windows_df,
        signatures_df=signatures_df,
        windows_df=windows_df,
        events_df=events_df,
        subsystem_map_df=subsystem_map_df,
        raw_df=raw_df,
        top_k_per_subsystem=3,
    )

    first = anomalies_df.where("win_id = 1").collect()[0]

    assert first["panel_context"] is not None
    assert "HYD_PRESS_LOW" in list(first["panel_context"]["text"])
    assert "HYD_PRESS_LOW" in list(first["panel_context"]["message_codes"])
    assert "LCD_MSG" in list(first["panel_context"]["source"])

    assert first["subsystems"]
    first_subsystem = first["subsystems"][0]
    for key in ["pivot", "cur", "events", "categorical", "cooccurrence"]:
        assert key in first_subsystem["block_contrib"]

    assert first["raw"] is not None
    assert first["raw"]["sensor_scores"]
