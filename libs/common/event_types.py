"""Shared event taxonomy constants across stream detectors and adapters."""

from __future__ import annotations


class EventType:
    NONE = "none"

    # Continuous channel events
    THRESHOLD = "threshold"
    SLOPE_POS = "slope_pos"
    SLOPE_NEG = "slope_neg"
    SWITCH = "switch"
    EXTREMA = "extrema"
    OSCILLATION = "oscillation"
    DRIFT_GUARD = "drift_guard"

    # Categorical channel events
    STATE_ENTER = "state_enter"
    STATE_EXIT = "state_exit"
    DROPPED = "dropped"
    DWELL_BUCKET = "dwell_bucket"
    TRANSITION = "transition"
    DWELL_VIOLATION = "dwell_violation"
    ILLEGAL_TRANSITION = "illegal_transition"
    DWELL_GUARD = "dwell_guard"

class TruthAnomalyType:
    NONE = "none"
    BURST_NUMERIC_SHIFT = "burst_numeric_shift"
    PRESSURIZATION_MODE_DEVIATION = "pressurization_mode_deviation"
    DIAGNOSTIC_CODE_EMIT = "diagnostic_code_emit"


CONTINUOUS_EVENT_TYPES = {
    EventType.THRESHOLD,
    EventType.SLOPE_POS,
    EventType.SLOPE_NEG,
    EventType.SWITCH,
    EventType.EXTREMA,
    EventType.OSCILLATION,
    EventType.DRIFT_GUARD,
}

CATEGORICAL_EVENT_TYPES = {
    EventType.STATE_ENTER,
    EventType.STATE_EXIT,
    EventType.DROPPED,
    EventType.DWELL_BUCKET,
    EventType.TRANSITION,
    EventType.DWELL_VIOLATION,
    EventType.ILLEGAL_TRANSITION,
    EventType.DWELL_GUARD,
}
