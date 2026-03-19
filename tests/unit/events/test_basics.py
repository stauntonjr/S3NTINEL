from collections import Counter
from datetime import datetime, timedelta, timezone

from libs.events.continuous import ContinuousDetectorConfig, ContinuousEventDetector, build_continuous_events
from libs.spark_sequence import SegmentedSequencePlan, SequenceOrderingPolicy, SequenceSegmentPolicy


def _build_numeric_rows(values: list[float]) -> list[dict[str, object]]:
    t0 = datetime(2026, 3, 1, tzinfo=timezone.utc)
    return [
        {
            "tail_id": "T001",
            "flight_id": "F001",
            "parameter_name": "sensor_numeric",
            "timestamp_utc": t0 + timedelta(seconds=idx),
            "val": value,
            "parameter_value": str(value),
            "date_utc": (t0 + timedelta(seconds=idx)).date(),
            "parameter_datatype_normalized": "numeric",
        }
        for idx, value in enumerate(values)
    ]


def test_build_continuous_events_slope_source_raw_emits_noisy_slopes(spark):
    raw_df = spark.createDataFrame(_build_numeric_rows([0.0, 10.0, 0.0, 10.0, 0.0]))
    cfg = ContinuousDetectorConfig(
        slope_source="raw",
        slope_abs_threshold=5.0,
        residual_z_threshold=1e9,
        switch_z_threshold=1e9,
        switch_delta_z_threshold=1e9,
        switch_min_abs_delta=1e9,
        warmup_points=1,
    )
    events_df = build_continuous_events(raw_df, config=cfg)
    slope_event_count = events_df.where("event_type_detected in ('slope_pos', 'slope_neg')").count()
    assert slope_event_count > 0


def test_build_continuous_events_slope_source_ema_suppresses_noisy_slopes(spark):
    raw_df = spark.createDataFrame(_build_numeric_rows([0.0, 10.0, 0.0, 10.0, 0.0]))
    cfg = ContinuousDetectorConfig(
        slope_source="ema",
        ema_alpha=0.2,
        slope_abs_threshold=5.0,
        residual_z_threshold=1e9,
        switch_z_threshold=1e9,
        switch_delta_z_threshold=1e9,
        switch_min_abs_delta=1e9,
        warmup_points=1,
    )
    events_df = build_continuous_events(raw_df, config=cfg)
    slope_event_count = events_df.where("event_type_detected in ('slope_pos', 'slope_neg')").count()
    assert slope_event_count == 0


def test_build_continuous_events_emits_switch_from_segmented_source(spark):
    t0 = datetime(2026, 3, 1, tzinfo=timezone.utc)
    rows = [
        {
            "tail_id": "T001",
            "flight_id": "F001",
            "parameter_name": "sensor_numeric",
            "timestamp_utc": t0 + timedelta(seconds=idx),
            "val": value,
            "date_utc": (t0 + timedelta(seconds=idx)).date(),
            "parameter_datatype_normalized": "numeric",
            "sample_seq_id": idx + 1,
            "flight_segment_id": 0,
        }
        for idx, value in enumerate([0.0, 0.0, 20.0, 20.0])
    ]
    raw_df = spark.createDataFrame(rows)
    events_df = build_continuous_events(
        raw_df,
        config=ContinuousDetectorConfig(
            delta_threshold=0.0,
            slope_source="raw",
            slope_abs_threshold=1e9,
            residual_z_threshold=1e9,
            switch_z_threshold=1e9,
            switch_delta_z_threshold=0.0,
            switch_min_abs_delta=5.0,
            switch_delta_scale=1.0,
            switch_residual_z_min=0.0,
            switch_refractory_samples=2,
            drift_guard_abs_change=0.0,
            drift_guard_max_gap_samples=0,
            warmup_points=1,
        ),
    )

    event_types = {row["event_type_detected"] for row in events_df.select("event_type_detected").collect()}
    assert "switch" in event_types


def test_build_continuous_events_segmented_matches_stream_for_extrema_and_oscillation(spark):
    values = [0.0, 20.0, 0.0, 20.0, 0.0, 20.0, 0.0, 20.0, 0.0, 20.0, 0.0, 20.0, 0.0]
    cfg = ContinuousDetectorConfig(
        slope_source="raw",
        slope_abs_threshold=1e9,
        residual_z_threshold=1e9,
        switch_z_threshold=1e9,
        switch_delta_z_threshold=1e9,
        switch_min_abs_delta=1e9,
        warmup_points=1,
        emit_extrema_events=True,
        oscillation_window=4,
        oscillation_amplitude_window=20,
        oscillation_sign_changes=2,
        oscillation_min_amplitude=1.0,
        oscillation_min_extrema=2,
        oscillation_min_period_samples=1,
        oscillation_min_alternation_ratio=0.3,
        oscillation_period_cv_max=10.0,
        oscillation_refractory_samples=3,
    )
    raw_df = spark.createDataFrame(_build_numeric_rows(values))
    segmented_detector = ContinuousEventDetector(
        config=cfg,
        sequence_plan=SegmentedSequencePlan(
            ordering=SequenceOrderingPolicy(
                key_columns=("tail_id", "flight_id", "parameter_name"),
                order_columns=("timestamp_utc",),
                timestamp_column="timestamp_utc",
                row_number_column="sample_seq_id",
            ),
            policy=SequenceSegmentPolicy(max_rows_per_segment=3, max_span_ms=0),
        ),
    )
    unsegmented_detector = ContinuousEventDetector(
        config=cfg,
        sequence_plan=SegmentedSequencePlan(
            ordering=SequenceOrderingPolicy(
                key_columns=("tail_id", "flight_id", "parameter_name"),
                order_columns=("timestamp_utc",),
                timestamp_column="timestamp_utc",
                row_number_column="sample_seq_id",
            ),
            policy=SequenceSegmentPolicy(max_rows_per_segment=1_000, max_span_ms=0),
        ),
    )
    segmented_events = segmented_detector.build(raw_df).select("event_type_detected").collect()
    unsegmented_events = unsegmented_detector.build(raw_df).select("event_type_detected").collect()
    segmented_counts = Counter(str(row["event_type_detected"]) for row in segmented_events)
    unsegmented_counts = Counter(str(row["event_type_detected"]) for row in unsegmented_events)

    assert segmented_counts["extrema"] == unsegmented_counts["extrema"]
    assert segmented_counts["oscillation"] == unsegmented_counts["oscillation"]
    assert segmented_counts["oscillation"] > 0
