from __future__ import annotations

from datetime import datetime, timezone

from libs.events.categorical import build_categorical_events


def test_build_categorical_events_emits_dropped_when_value_missing(spark):
    rows = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "timestamp_utc": datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc),
            "sensor": "CAT_SENSOR",
            "parameter_datatype": "categorical",
            "parameter_value": "ON",
            "date_utc": "2026-03-01",
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "timestamp_utc": datetime(2026, 3, 1, 0, 0, 1, tzinfo=timezone.utc),
            "sensor": "CAT_SENSOR",
            "parameter_datatype": "categorical",
            "parameter_value": None,
            "date_utc": "2026-03-01",
        },
    ]

    raw_df = spark.createDataFrame(rows)
    events_df = build_categorical_events(raw_df)
    event_types = [row["event_type_detected"] for row in events_df.select("event_type_detected").collect()]

    assert "state_enter" in event_types
    assert "dropped" in event_types
