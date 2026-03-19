from __future__ import annotations

from datetime import datetime, timezone

from libs.events.categorical import (
    CategoricalDetectorConfig,
    CategoricalEventDetector,
    build_categorical_events,
)
from libs.spark_sequence import SegmentedSequencePlan, SequenceOrderingPolicy, SequenceSegmentPolicy


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


def test_build_categorical_events_emits_transition_and_related_dwell_events(spark):
    rows = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "timestamp_utc": datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc),
            "sensor": "CAT_SENSOR",
            "parameter_datatype_profiled": "categorical",
            "parameter_value": "OFF",
            "date_utc": "2026-03-01",
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "timestamp_utc": datetime(2026, 3, 1, 0, 0, 1, tzinfo=timezone.utc),
            "sensor": "CAT_SENSOR",
            "parameter_datatype_profiled": "categorical",
            "parameter_value": "ON",
            "date_utc": "2026-03-01",
        },
    ]

    raw_df = spark.createDataFrame(rows)
    events_df = build_categorical_events(raw_df)
    event_types = {row["event_type_detected"] for row in events_df.select("event_type_detected").collect()}

    assert {"state_enter", "state_exit", "transition", "dwell_bucket"}.issubset(event_types)


def test_build_categorical_events_segmented_preserves_dwell_guard_across_segments(spark):
    rows = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "timestamp_utc": datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc),
            "sensor": "CAT_SENSOR",
            "parameter_datatype_profiled": "categorical",
            "parameter_value": "ON",
            "date_utc": "2026-03-01",
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "timestamp_utc": datetime(2026, 3, 1, 0, 0, 2, tzinfo=timezone.utc),
            "sensor": "CAT_SENSOR",
            "parameter_datatype_profiled": "categorical",
            "parameter_value": "ON",
            "date_utc": "2026-03-01",
        },
    ]

    raw_df = spark.createDataFrame(rows)
    detector = CategoricalEventDetector(
        config=CategoricalDetectorConfig(max_dwell_seconds=1.0),
        sequence_plan=SegmentedSequencePlan(
            ordering=SequenceOrderingPolicy(
                key_columns=("tail_id", "flight_id", "parameter_name"),
                order_columns=("sample_seq_id",),
                timestamp_column="timestamp_utc",
                row_number_column="sample_seq_id",
            ),
            policy=SequenceSegmentPolicy(max_rows_per_segment=1, max_span_ms=0),
        ),
    )
    event_types = {row["event_type_detected"] for row in detector.build(raw_df).select("event_type_detected").collect()}

    assert {"state_enter", "dwell_guard"}.issubset(event_types)
