# File: libs/events/__init__.py
"""Event extraction package."""

from libs.events.calibration import ContinuousEventCalibrationSpec, build_continuous_event_calibration_report_spark
from libs.events.pipeline import (
    CategoricalEventDetector,
    ContinuousEventDetector,
    EventArtifactSet,
    EventDetectionPlan,
    EventOrderingPolicy,
    EventSourceFrame,
)
from libs.events.profiling import EventProfileConfig, ParameterEventProfile
from libs.events.tables import EventsTable
from libs.events.types import (
    CategoricalEvent,
    CategoricalDwellGuardEvent,
    ContinuousEvent,
    DwellBucketEvent,
    DwellViolationEvent,
    DroppedEvent,
    DriftGuardEvent,
    Event,
    ExtremaEvent,
    IllegalTransitionEvent,
    OscillationEvent,
    SlopeNegativeEvent,
    SlopePositiveEvent,
    StateEnterEvent,
    StateExitEvent,
    SwitchEvent,
    ThresholdEvent,
    TransitionEvent,
)
from libs.events.validator import build_event_validation_summary, iter_event_validation_snapshots, simulator_label_events

__all__ = [
    "Event",
    "ContinuousEvent",
    "CategoricalEvent",
    "CategoricalDwellGuardEvent",
    "ThresholdEvent",
    "SlopePositiveEvent",
    "SlopeNegativeEvent",
    "SwitchEvent",
    "ExtremaEvent",
    "OscillationEvent",
    "DriftGuardEvent",
    "StateEnterEvent",
    "StateExitEvent",
    "DroppedEvent",
    "DwellBucketEvent",
    "TransitionEvent",
    "DwellViolationEvent",
    "IllegalTransitionEvent",
    "ContinuousEventDetector",
    "CategoricalEventDetector",
    "EventOrderingPolicy",
    "EventSourceFrame",
    "EventDetectionPlan",
    "EventArtifactSet",
    "EventsTable",
    "EventProfileConfig",
    "ParameterEventProfile",
    "ContinuousEventCalibrationSpec",
    "build_continuous_event_calibration_report_spark",
    "build_event_validation_summary",
    "simulator_label_events",
    "iter_event_validation_snapshots",
]
