# File: libs/io/schemas.py
"""Schema contracts for active V2 artifacts."""

from __future__ import annotations


RAW_TELEMETRY_COLUMNS = [
    "tail_id",
    "flight_id",
    "timestamp_utc",
    "parameter_name",
    "parameter_value",
    "date_utc",
]

EVENTS_COLUMNS = [
    "tail_id",
    "flight_id",
    "win_id",
    "timestamp_utc",
    "parameter_name",
    "event_type_detected",
    "anomaly_type_detected",
    "anomaly_score_detected",
    "payload",
    "date_utc",
]

WINDOWS_COLUMNS = [
    "tail_id",
    "flight_id",
    "win_id",
    "t_start",
    "t_end",
    "duration_ms",
    "event_count",
    "zoh_version",
    "date_utc",
]

V2_PHASE_WINDOWS_COLUMNS = [
    "tail_id",
    "flight_id",
    "win_id",
    "t_start",
    "t_end",
    "duration_ms",
    "event_count",
    "phase_id_detected",
    "phase_state_detected",
    "phase_confidence_detected",
    "distance_to_centroid_detected",
    "drift_magnitude",
    "breadth",
    "backbone_reconstruction_error",
    "backbone_residual_by_parameter",
    "x_c",
    "s_w",
    "date_utc",
]

V2_PHASE_BASELINES_COLUMNS = [
    "tail_id",
    "phase_id_detected",
    "phase_name_detected",
    "s_w_centroid",
    "reconstruction_median",
    "reconstruction_mad",
    "distance_median",
    "distance_mad",
    "stable_window_count",
    "version",
]

V2_BACKBONE_COLUMNS = [
    "backbone_version",
    "selected_sensors_c",
    "all_sensors",
    "weights_b",
    "lambda_ridge",
    "training_window_count",
]

V2_BACKBONE_SENSOR_ENERGY_COLUMNS = [
    "parameter_name",
    "energy",
    "support_count",
    "selected_backbone",
    "backbone_version",
]

V2_PRECISION_GRAPH_COLUMNS = [
    "sensor_u",
    "sensor_v",
    "partial_corr",
    "precision_weight",
    "edge_family",
]

V2_EVENT_GRAPH_COLUMNS = [
    "sensor_u",
    "sensor_v",
    "cooccur_count",
    "event_weight",
    "edge_family",
]

V2_LAG_GRAPH_COLUMNS = [
    "sensor_u",
    "sensor_v",
    "lag_count",
    "lag_weight",
    "mean_lag_seconds",
    "edge_family",
]

V2_TRANSITION_GRAPH_COLUMNS = [
    "sensor_u",
    "sensor_v",
    "precedence_count",
    "precedence_weight",
    "edge_family",
]

V2_FUSED_GRAPH_COLUMNS = [
    "sensor_u",
    "sensor_v",
    "precision_weight",
    "event_weight",
    "lag_weight",
    "fused_weight",
    "edge_family",
]

V2_HIERARCHY_SENSOR_MAP_COLUMNS = [
    "parameter_name",
    "system_id",
    "subsystem_id",
    "module_id",
    "hierarchy_source",
    "hierarchy_profile_id",
]

WINDOW_SCORES_RAW_COLUMNS = [
    "tail_id",
    "flight_id",
    "win_id",
    "phase_state_detected",
    "phase_id_detected",
    "phase_confidence_detected",
    "distance_to_centroid_detected",
    "drift_magnitude",
    "breadth",
    "global_score",
    "p_value",
    "severity",
    "dominant_subsystem_id",
    "dominant_score_component",
    "subsystem_scores",
    "score_component_scores",
    "date_utc",
]

WINDOW_SCORES_CALIBRATED_COLUMNS = WINDOW_SCORES_RAW_COLUMNS + [
    "warm",
    "emit_ready",
    "min_warm",
]

V2_ANOMALY_WINDOW_ATTRIBUTION_COLUMNS = [
    "tail_id",
    "flight_id",
    "win_id",
    "timestamp_utc",
    "phase_state_detected",
    "phase_id_detected",
    "phase_confidence_detected",
    "distance_to_centroid_detected",
    "drift_magnitude",
    "breadth",
    "global_score",
    "p_value",
    "severity",
    "dominant_subsystem_id",
    "dominant_score_component",
    "panel_context",
    "subsystems",
    "attribution_context",
    "artifact_versions",
    "date_utc",
]

V2_ANOMALY_TELEMETRY_ATTRIBUTION_COLUMNS = [
    "tail_id",
    "flight_id",
    "win_id",
    "timestamp_utc",
    "parameter_name",
    "parameter_value",
    "parameter_datatype_label",
    "system_id",
    "subsystem_id",
    "module_id",
    "window_global_score",
    "severity",
    "date_utc",
]

V2_ANOMALY_EVENT_ATTRIBUTION_COLUMNS = [
    "tail_id",
    "flight_id",
    "win_id",
    "timestamp_utc",
    "parameter_name",
    "event_type_detected",
    "anomaly_type_detected",
    "anomaly_score_detected",
    "system_id",
    "subsystem_id",
    "module_id",
    "window_global_score",
    "severity",
    "date_utc",
]

ACTIVE_V2_TABLES = {
    "events": EVENTS_COLUMNS,
    "windows": WINDOWS_COLUMNS,
    "backbone": V2_BACKBONE_COLUMNS,
    "backbone_sensor_energy": V2_BACKBONE_SENSOR_ENERGY_COLUMNS,
    "precision_graph": V2_PRECISION_GRAPH_COLUMNS,
    "event_graph": V2_EVENT_GRAPH_COLUMNS,
    "lag_graph": V2_LAG_GRAPH_COLUMNS,
    "transition_graph": V2_TRANSITION_GRAPH_COLUMNS,
    "fused_graph": V2_FUSED_GRAPH_COLUMNS,
    "hierarchy_sensor_map": V2_HIERARCHY_SENSOR_MAP_COLUMNS,
    "phase_windows": V2_PHASE_WINDOWS_COLUMNS,
    "phase_baselines": V2_PHASE_BASELINES_COLUMNS,
    "window_scores_raw": WINDOW_SCORES_RAW_COLUMNS,
    "window_scores_calibrated": WINDOW_SCORES_CALIBRATED_COLUMNS,
    "anomaly_window_attribution": V2_ANOMALY_WINDOW_ATTRIBUTION_COLUMNS,
    "anomaly_telemetry_attribution": V2_ANOMALY_TELEMETRY_ATTRIBUTION_COLUMNS,
    "anomaly_event_attribution": V2_ANOMALY_EVENT_ATTRIBUTION_COLUMNS,
}
