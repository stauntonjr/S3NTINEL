from libs.windows.adaptive import close_reason_for_thresholds, should_close_window


def test_should_close_window_by_duration():
    assert should_close_window(duration_ms=250, event_count=1, max_ms=200, event_threshold=20)


def test_should_close_window_by_event_count():
    assert should_close_window(duration_ms=10, event_count=20, max_ms=200, event_threshold=20)


def test_close_reason_combinations():
    assert close_reason_for_thresholds(250, 30, 200, 20) == "event_threshold+max_ms"
    assert close_reason_for_thresholds(100, 20, 200, 20) == "event_threshold"
    assert close_reason_for_thresholds(250, 5, 200, 20) == "max_ms"
