from datetime import datetime, timezone

from libs.windows.adaptive import build_adaptive_windows_stream_parity
from libs.windows.adaptive import (
    close_reason_for_thresholds,
    max_window_ms_from_min_sampling_rate,
    should_close_window,
)
from libs.windows.stream import StreamWindowConfig, build_adaptive_windows_stream


def test_should_close_window_by_duration():
    assert should_close_window(duration_ms=250, event_count=1, max_ms=200, event_threshold=20)


def test_should_close_window_by_event_count():
    assert should_close_window(duration_ms=10, event_count=20, max_ms=200, event_threshold=20)


def test_close_reason_combinations():
    assert close_reason_for_thresholds(250, 30, 200, 20) == "event_threshold+max_ms"
    assert close_reason_for_thresholds(100, 20, 200, 20) == "event_threshold"
    assert close_reason_for_thresholds(250, 5, 200, 20) == "max_ms"


def test_default_window_max_ms_uses_10_samples_over_min_sampling_rate():
    # 10 samples at 1Hz is 10s => 10000ms.
    assert max_window_ms_from_min_sampling_rate(1.0) == 10000
    assert StreamWindowConfig().max_ms == 10000


def test_stream_windows_enforce_strict_max_ms_with_sparse_events():
    events = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "parameter_name": "p1",
            "timestamp_utc": datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            "event_type_detected": "slope_pos",
            "payload": {"value": 1.0},
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "parameter_name": "p1",
            "timestamp_utc": datetime(2026, 1, 1, 0, 0, 20, tzinfo=timezone.utc),
            "event_type_detected": "slope_pos",
            "payload": {"value": 2.0},
        },
    ]
    windows = list(
        build_adaptive_windows_stream(
            events,
            StreamWindowConfig(max_ms=10000, min_ms=50, event_threshold=20, inactivity_timeout_ms=0),
        )
    )
    assert len(windows) == 2
    assert windows[0]["close_reason"] == "max_ms"
    assert windows[0]["duration_ms"] == 10000
    assert windows[0]["t_end"] == datetime(2026, 1, 1, 0, 0, 10, tzinfo=timezone.utc)
    assert windows[1]["close_reason"] == "end_of_stream"
    assert windows[1]["duration_ms"] == 50


def test_stream_parity_windows_enforce_strict_max_ms_with_sparse_events(spark):
    rows = [
        ("T1", "F1", datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc), "2026-01-01"),
        ("T1", "F1", datetime(2026, 1, 1, 0, 0, 20, tzinfo=timezone.utc), "2026-01-01"),
    ]
    events_df = spark.createDataFrame(rows, ["tail_id", "flight_id", "timestamp_utc", "date_utc"])
    windows_df = build_adaptive_windows_stream_parity(
        events_df,
        max_ms=10000,
        min_ms=50,
        event_threshold=20,
        inactivity_timeout_ms=0,
    )
    windows = windows_df.orderBy("win_id").collect()
    assert len(windows) == 2
    assert windows[0]["close_reason"] == "max_ms"
    assert windows[0]["duration_ms"] == 10000
    assert windows[1]["close_reason"] == "end_of_stream"
