from datetime import datetime, timedelta, timezone

from libs.windows import Window, WindowPolicy, WindowSensorBuffer, build_windows_table


def test_window_policy_closes_by_duration_or_event_count():
    policy = WindowPolicy(max_ms=200, event_threshold=20, min_ms=50, inactivity_timeout_ms=0)

    assert policy.should_close(duration_ms=250, event_count=1)
    assert policy.should_close(duration_ms=10, event_count=20)
    assert policy.close_reason(duration_ms=250, event_count=30) == "event_threshold+max_ms"
    assert policy.close_reason(duration_ms=100, event_count=20) == "event_threshold"
    assert policy.close_reason(duration_ms=250, event_count=5) == "max_ms"


def test_default_window_max_ms_uses_10_samples_over_min_sampling_rate():
    assert WindowPolicy.max_ms_from_min_sampling_rate(1.0) == 10000
    assert WindowPolicy.default().max_ms == 10000


def test_window_ingest_event_updates_local_state():
    window = Window.open(datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc))
    event = {
        "tail_id": "T1",
        "flight_id": "F1",
        "parameter_name": "pump_state",
        "timestamp_utc": datetime(2026, 1, 1, 0, 0, 5, tzinfo=timezone.utc),
        "event_type_detected": "transition",
        "payload": {"to": "ON"},
    }

    window.ingest_event(event)

    assert window.event_count == 1
    assert window.duration_ms == 5000
    assert window.sensor_buffer.snapshot() == {"pump_state": "ON"}
    assert window.event_type_counts == {"transition": 1}


def test_window_sensor_buffer_updates_and_snapshots():
    buffer = WindowSensorBuffer()
    event = {
        "parameter_name": "pump_state",
        "timestamp_utc": datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        "event_type_detected": "transition",
        "payload": {"to": "ON"},
    }

    buffer.ingest_event(event)

    assert buffer.snapshot() == {"pump_state": "ON"}


def test_build_windows_table_emits_expected_sparse_max_ms_windows(spark):
    events = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "event_seq_id": 1,
            "parameter_name": "p1",
            "timestamp_utc": datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            "event_type_detected": "slope_pos",
            "payload": {"value": "1.0"},
            "date_utc": datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "event_seq_id": 2,
            "parameter_name": "p1",
            "timestamp_utc": datetime(2026, 1, 1, 0, 0, 20, tzinfo=timezone.utc),
            "event_type_detected": "slope_pos",
            "payload": {"value": "2.0"},
            "date_utc": datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
        },
    ]
    events_df = spark.createDataFrame(events)
    observed = [
        row.asDict(recursive=True)
        for row in build_windows_table(
            events_df,
            max_ms=10000,
            event_threshold=20,
            min_ms=50,
            inactivity_timeout_ms=0,
        )
        .orderBy("win_id")
        .collect()
    ]

    assert len(observed) == 2
    assert observed[0]["close_reason"] == "max_ms"
    assert observed[0]["duration_ms"] == 10000
    assert observed[0]["t_end"] == observed[0]["t_start"] + timedelta(seconds=10)
    assert observed[0]["event_count"] == 1
    assert observed[0]["sensor_count"] == 1
    assert observed[0]["event_type_counts"] == {"slope_pos": 1}
    assert observed[0]["zoh_snapshot"] == {"p1": "1.0"}
    assert observed[1]["close_reason"] == "end_of_stream"
    assert observed[1]["duration_ms"] == 50
    assert observed[1]["event_count"] == 1
    assert observed[1]["sensor_count"] == 1
    assert observed[1]["event_type_counts"] == {"slope_pos": 1}
    assert observed[1]["zoh_snapshot"] == {"p1": "2.0"}
