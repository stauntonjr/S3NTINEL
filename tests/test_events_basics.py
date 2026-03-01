from libs.events.categorical import detect_transitions


def test_detect_transitions_empty():
    assert detect_transitions([]) == []


def test_detect_transitions_changes_only():
    states = ["OFF", "OFF", "ON", "ON", "STBY", "STBY", "ON"]
    assert detect_transitions(states) == ["OFF->ON", "ON->STBY", "STBY->ON"]
