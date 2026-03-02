from libs.events.categorical import detect_transitions
from libs.events.extrema import classify_continuous_delta_event


def test_detect_transitions_empty():
    assert detect_transitions([]) == []


def test_detect_transitions_changes_only():
    states = ["OFF", "OFF", "ON", "ON", "STBY", "STBY", "ON"]
    assert detect_transitions(states) == ["OFF->ON", "ON->STBY", "STBY->ON"]


def test_classify_continuous_delta_event_threshold_disabled():
    assert classify_continuous_delta_event(10.0, 0.5, 0.0) == "slope_pos"
    assert classify_continuous_delta_event(10.0, -0.5, 0.0) == "slope_neg"


def test_classify_continuous_delta_event_threshold_enabled():
    assert classify_continuous_delta_event(10.0, 2.5, 2.0) == "threshold"
    assert classify_continuous_delta_event(10.0, -1.0, 2.0) == "slope_neg"
