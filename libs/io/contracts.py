"""Typed row contracts for active pipeline and simulation I/O boundaries."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, NotRequired, TypeAlias, TypedDict


ParameterValueByName: TypeAlias = dict[str, float]
ParameterStateByName: TypeAlias = dict[str, str]
EventTypeCountByName: TypeAlias = dict[str, int]
ResidualByParameter: TypeAlias = dict[str, float]
SubsystemScoreById: TypeAlias = dict[str, float]
ScoreComponentByName: TypeAlias = dict[str, float]
EventPayload: TypeAlias = dict[str, Any]


class TelemetryRow(TypedDict, total=False):
    tail_id: str
    flight_id: str
    parameter_name: str
    timestamp_utc: datetime
    parameter_value: str | None
    parameter_value_clean: str | float | int | None
    unit: str | None
    rate_hz: float | None
    behavior_family_label: str | None
    parameter_datatype_label: str | None
    parameter_datatype_profiled: str | None
    misbehavior_active: bool
    misbehavior_applied: bool
    misbehavior_family_label: str | None
    misbehavior_detail_label: str | None
    misbehavior_window_id: str | None
    coupling_id_label: str | None
    event_type_label: str | None
    event_misbehavior_label: str | None
    anomaly_type_label: str | None
    anomaly_score_label: float | None
    fault_active: bool
    fault_applied: bool
    fault_family_label: str | None
    fault_type: str | None
    fault_window_id: str | None
    date_utc: date


class DetectedEventRow(TypedDict, total=False):
    tail_id: str
    flight_id: str
    event_seq_id: int
    parameter_name: str
    timestamp_utc: datetime
    event_type_detected: str
    anomaly_type_detected: str | None
    anomaly_score_detected: float | None
    payload: EventPayload
    date_utc: date


class EventLabelRow(TypedDict, total=False):
    tail_id: str
    flight_id: str
    parameter_name: str
    timestamp_utc: datetime
    event_type_label: str


class AdaptiveWindowRow(TypedDict, total=False):
    tail_id: str
    flight_id: str
    win_id: int
    t_start: datetime
    t_end: datetime
    duration_ms: int
    event_count: int
    zoh_version: int
    date_utc: date
    sensor_count: int
    event_type_counts: EventTypeCountByName
    zoh_snapshot: dict[str, str]
    close_reason: str
    window_events: list[DetectedEventRow]


class WindowFeatureRow(TypedDict, total=False):
    tail_id: str
    flight_id: str
    win_id: int
    t_start: datetime
    t_end: datetime
    duration_ms: int
    event_count: int
    date_utc: date
    event_type_counts: EventTypeCountByName
    continuous_vector_t_end: ParameterValueByName
    continuous_vector_t_end_scaled: ParameterValueByName
    categorical_state_t_end: ParameterStateByName
    drift_magnitude_profiled: float
    phase_label: str | None
    backbone_reconstruction_error: float
    backbone_x_c: list[float]
    backbone_residual_by_parameter: ResidualByParameter


class PhaseAssignmentRow(TypedDict, total=False):
    tail_id: str
    flight_id: str
    win_id: int
    phase_id_detected: int
    phase_state_detected: str
    phase_confidence_detected: float
    distance_to_centroid_detected: float | None
    phase_label: str | None


class PhaseBaselineRow(TypedDict, total=False):
    tail_id: str
    phase_id_detected: int
    phase_name_detected: str
    s_w_centroid: list[float]
    reconstruction_median: float
    reconstruction_mad: float
    distance_median: float
    distance_mad: float
    baseline_source_mode: str
    baseline_window_count: int
    stable_window_count: int
    feature_names: list[str]
    selected_sensors_c: list[str]
    selected_event_types: list[str]
    selected_categorical_state_pairs: list[str]
    selected_window_cooccurrence_pairs: list[str]
    backbone_all_sensors: list[str]
    backbone_weights_b: list[list[float]]
    version: int


class PhaseWindowRow(TypedDict, total=False):
    tail_id: str
    flight_id: str
    win_id: int
    t_start: datetime
    t_end: datetime
    duration_ms: int
    event_count: int
    phase_id_detected: int
    phase_state_detected: str
    phase_confidence_detected: float
    distance_to_centroid_detected: float
    drift_magnitude: float
    breadth: float
    backbone_reconstruction_error: float
    backbone_residual_by_parameter: ResidualByParameter
    x_c: list[float]
    s_w: list[float]
    date_utc: date
    feature_names: list[str]
    selected_sensors_c: list[str]
    selected_event_types: list[str]
    selected_categorical_state_pairs: list[str]
    selected_window_cooccurrence_pairs: list[str]
    backbone_all_sensors: list[str]


class WindowScoreRow(TypedDict, total=False):
    tail_id: str
    flight_id: str
    win_id: int
    phase_state_detected: str
    phase_id_detected: int
    phase_confidence_detected: float
    distance_to_centroid_detected: float
    drift_magnitude: float
    breadth: float
    global_score: float
    p_value: float | None
    severity: str
    dominant_subsystem_id: str | None
    dominant_score_component: str
    subsystem_scores: SubsystemScoreById
    score_component_scores: ScoreComponentByName
    date_utc: date
    reconstruction_score: NotRequired[float]
    structure_score: NotRequired[float | None]


class DatatypeLabelRow(TypedDict):
    tail_id: str
    flight_id: str
    parameter_name: str
    timestamp_utc: datetime
    parameter_datatype_label: str


class DatatypeProfiledRow(TypedDict):
    tail_id: str
    flight_id: str
    parameter_name: str
    timestamp_utc: datetime
    parameter_datatype_profiled: str


class EventValidatorSnapshot(TypedDict):
    tail_id: str
    flight_id: str
    parameter_name: str
    timestamp_utc: datetime
    tp: int
    fp: int
    fn: int
    tn: int


class ProfilerValidatorSnapshot(TypedDict):
    tail_id: str
    flight_id: str
    parameter_name: str
    timestamp_utc: datetime
    parameter_datatype_label: str
    parameter_datatype_profiled: str
    tp: int
    fp: int
    fn: int
    tn: int
