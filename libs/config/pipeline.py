"""Typed pipeline configuration derived from defaults.yaml plus env overrides."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any


def _env_str(name: str, default: str) -> str:
    return str(os.getenv(name, default))


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def _env_optional_float(name: str, default: float | None) -> float | None:
    raw = os.getenv(name)
    if raw is None:
        return None if default is None else float(default)
    value = str(raw).strip().lower()
    if value in {"", "none", "null"}:
        return None
    return float(raw)


def _config_int(config: dict[str, Any], path: list[str], default: int) -> int:
    current: Any = config
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return int(default)
        current = current[key]
    return int(current)


def _config_float(config: dict[str, Any], path: list[str], default: float) -> float:
    current: Any = config
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return float(default)
        current = current[key]
    return float(current)


def _config_optional_float(config: dict[str, Any], path: list[str], default: float | None) -> float | None:
    current: Any = config
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return None if default is None else float(default)
        current = current[key]
    if current is None:
        return None
    return float(current)


def _config_str(config: dict[str, Any], path: list[str], default: str) -> str:
    current: Any = config
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return str(default)
        current = current[key]
    return str(current)


def _config_list(config: dict[str, Any], path: list[str], default: list[Any]) -> list[Any]:
    current: Any = config
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return list(default)
        current = current[key]
    if not isinstance(current, list):
        return list(default)
    return list(current)


@dataclass(frozen=True)
class PipelineExecutionSettings:
    table_format: str
    raw_output_format: str
    write_mode: str
    fit_write_mode: str


@dataclass(frozen=True)
class PipelineArtifactPaths:
    raw_input: str
    raw_table: str
    parameter_datatype_profile: str
    continuous_scaling_profile: str
    parameter_behavior_primitive_profile: str
    parameter_behavior_profile: str
    parameter_event_profile: str
    events: str
    window_policy_profile: str
    windows: str
    phase_labels: str
    window_features: str
    backbone: str
    backbone_sensor_energy: str
    precision_graph: str
    event_graph: str
    lag_profile: str
    lag_graph: str
    transition_graph: str
    fused_graph: str
    graph_parameter_universe: str
    hierarchy_edge_evidence: str
    hierarchy_sensor_map: str
    phase_windows: str
    phase_baselines: str
    phase_reference_model: str
    phase_label_centroids: str
    window_scores_raw: str
    window_scores_calibrated: str
    anomaly_window_attribution: str
    anomaly_telemetry_attribution: str
    anomaly_event_attribution: str
    anomaly_parameter_candidate_evidence: str
    explorer_bundle: str


@dataclass(frozen=True)
class EventSettings:
    delta_threshold: float
    slope_source: str
    ema_alpha: float
    slope_threshold_mode: str
    slope_threshold_quantile: float
    slope_threshold_scale: float
    slope_threshold_min: float
    slope_abs_threshold: float
    slope_min_persistence_samples: int
    slope_reemit_ratio: float
    warmup_points: int
    low_scale_responsiveness: float = 1.0
    repeatability_aggressiveness: float = 1.0
    drift_conservatism: float = 1.0
    chatter_suppression: float = 1.0


@dataclass(frozen=True)
class ProfilingSettings:
    numeric_ratio_threshold: float
    categorical_cardinality_max: int
    behavior_significant_diff_threshold: float
    behavior_center_band_width: float
    behavior_soft_bound_width: float
    behavior_hard_bound_width: float
    behavior_mixed_unknown_low_score_threshold: float
    behavior_mixed_unknown_ambiguous_score_threshold: float
    behavior_mixed_unknown_ambiguous_margin_threshold: float


@dataclass(frozen=True)
class WindowingSettings:
    min_sampling_rate_hz: float
    max_ms: int
    min_ms: int
    event_threshold: int
    inactivity_timeout_ms: int
    strategy: str


@dataclass(frozen=True)
class BackboneSettings:
    sensor_count: int
    ridge_lambda: float
    max_sensor_universe: int
    event_prior_alpha: float


@dataclass(frozen=True)
class EventGraphSettings:
    min_count: int
    min_npmi: float
    top_k_per_parameter_name: int


@dataclass(frozen=True)
class LagBandSettings:
    name: str
    lower_seconds: float
    upper_seconds: float
    combine_weight: float


@dataclass(frozen=True)
class LagGraphSettings:
    tau_max_seconds: float
    min_count: int
    max_mean_lag_seconds: float | None
    top_k_outgoing: int
    bands: tuple[LagBandSettings, ...]


@dataclass(frozen=True)
class TransitionGraphSettings:
    min_count: int


@dataclass(frozen=True)
class GraphFusionSettings:
    alpha: float
    beta: float
    gamma: float
    min_fused_edge_weight: float


@dataclass(frozen=True)
class GraphSettings:
    precision_ridge_lambda: float
    min_abs_partial_corr: float
    max_sensor_universe: int
    event: EventGraphSettings
    lag: LagGraphSettings
    transition: TransitionGraphSettings
    fusion: GraphFusionSettings


@dataclass(frozen=True)
class HierarchySettings:
    top_k_per_parameter_name: int
    subsystem_min_edge_weight: float | None
    system_min_edge_weight: float | None


@dataclass(frozen=True)
class PhaseSettings:
    phase_count: int
    detect_sensor_count: int
    detect_event_type_count: int
    detect_categorical_state_count: int
    stable_drift_quantile: float
    transition_penalty: float
    min_dwell_windows: int


@dataclass(frozen=True)
class ScoringSettings:
    max_bridge_reference_rows: int
    min_warm: int


@dataclass(frozen=True)
class AnomalySettings:
    subsystem_top_sensors_k: int


@dataclass(frozen=True)
class PipelineContextSettings:
    profiling: ProfilingSettings
    events: EventSettings
    windowing: WindowingSettings
    backbone: BackboneSettings
    graph: GraphSettings
    hierarchy: HierarchySettings
    phase: PhaseSettings
    scoring: ScoringSettings
    anomaly: AnomalySettings


def load_pipeline_execution_settings() -> PipelineExecutionSettings:
    table_format = _env_str("S3NTINEL_TABLE_FORMAT", "delta")
    return PipelineExecutionSettings(
        table_format=table_format,
        raw_output_format=_env_str("S3NTINEL_RAW_OUTPUT_FORMAT", table_format),
        write_mode=_env_str("S3NTINEL_WRITE_MODE", "append"),
        fit_write_mode=_env_str("S3NTINEL_FIT_WRITE_MODE", "overwrite"),
    )


def load_pipeline_artifact_paths() -> PipelineArtifactPaths:
    return PipelineArtifactPaths(
        raw_input=_env_str("S3NTINEL_RAW_INPUT_PATH", "data/input/raw_telemetry"),
        raw_table=_env_str("S3NTINEL_RAW_TABLE_PATH", "data/delta/raw_telemetry"),
        parameter_datatype_profile=_env_str(
            "S3NTINEL_PARAMETER_DATATYPE_PROFILE_TABLE_PATH",
            "data/delta/parameter_datatype_profile",
        ),
        continuous_scaling_profile=_env_str(
            "S3NTINEL_CONTINUOUS_SCALING_PROFILE_TABLE_PATH",
            "data/delta/continuous_scaling_profile",
        ),
        parameter_behavior_primitive_profile=_env_str(
            "S3NTINEL_PARAMETER_BEHAVIOR_PRIMITIVE_PROFILE_TABLE_PATH",
            "data/delta/parameter_behavior_primitive_profile",
        ),
        parameter_behavior_profile=_env_str(
            "S3NTINEL_PARAMETER_BEHAVIOR_PROFILE_TABLE_PATH",
            "data/delta/parameter_behavior_profile",
        ),
        parameter_event_profile=_env_str(
            "S3NTINEL_PARAMETER_EVENT_PROFILE_TABLE_PATH",
            "data/delta/parameter_event_profile",
        ),
        events=_env_str("S3NTINEL_EVENTS_TABLE_PATH", "data/delta/events"),
        window_policy_profile=_env_str(
            "S3NTINEL_WINDOW_POLICY_PROFILE_TABLE_PATH",
            "data/delta/window_policy_profile",
        ),
        windows=_env_str("S3NTINEL_WINDOWS_TABLE_PATH", "data/delta/windows"),
        phase_labels=_env_str("S3NTINEL_PHASE_LABELS_TABLE_PATH", "data/delta/phase_labels"),
        window_features=_env_str("S3NTINEL_WINDOW_FEATURES_TABLE_PATH", ""),
        backbone=_env_str("S3NTINEL_BACKBONE_TABLE_PATH", "data/delta/backbone"),
        backbone_sensor_energy=_env_str(
            "S3NTINEL_BACKBONE_SENSOR_ENERGY_TABLE_PATH",
            "data/delta/backbone_sensor_energy",
        ),
        precision_graph=_env_str("S3NTINEL_PRECISION_GRAPH_TABLE_PATH", "data/delta/precision_graph"),
        event_graph=_env_str("S3NTINEL_EVENT_GRAPH_TABLE_PATH", "data/delta/event_graph"),
        lag_profile=_env_str("S3NTINEL_LAG_PROFILE_TABLE_PATH", "data/delta/lag_profile"),
        lag_graph=_env_str("S3NTINEL_LAG_GRAPH_TABLE_PATH", "data/delta/lag_graph"),
        transition_graph=_env_str("S3NTINEL_TRANSITION_GRAPH_TABLE_PATH", "data/delta/transition_graph"),
        fused_graph=_env_str("S3NTINEL_FUSED_GRAPH_TABLE_PATH", "data/delta/fused_graph"),
        graph_parameter_universe=_env_str(
            "S3NTINEL_GRAPH_PARAMETER_UNIVERSE_TABLE_PATH",
            "data/delta/graph_parameter_universe",
        ),
        hierarchy_edge_evidence=_env_str(
            "S3NTINEL_HIERARCHY_EDGE_EVIDENCE_TABLE_PATH",
            "data/delta/hierarchy_edge_evidence",
        ),
        hierarchy_sensor_map=_env_str(
            "S3NTINEL_HIERARCHY_SENSOR_MAP_TABLE_PATH",
            "data/delta/hierarchy_sensor_map",
        ),
        phase_windows=_env_str("S3NTINEL_PHASE_WINDOWS_TABLE_PATH", "data/delta/phase_windows"),
        phase_baselines=_env_str("S3NTINEL_PHASE_BASELINES_TABLE_PATH", "data/delta/phase_baselines"),
        phase_reference_model=_env_str(
            "S3NTINEL_PHASE_REFERENCE_MODEL_TABLE_PATH",
            "data/delta/phase_reference_model",
        ),
        phase_label_centroids=_env_str(
            "S3NTINEL_PHASE_LABEL_CENTROIDS_TABLE_PATH",
            "data/delta/phase_label_centroids",
        ),
        window_scores_raw=_env_str("S3NTINEL_WINDOW_SCORES_RAW_TABLE_PATH", "data/delta/window_scores_raw"),
        window_scores_calibrated=_env_str(
            "S3NTINEL_WINDOW_SCORES_CALIBRATED_TABLE_PATH",
            "data/delta/window_scores_calibrated",
        ),
        anomaly_window_attribution=_env_str(
            "S3NTINEL_ANOMALY_WINDOW_ATTRIBUTION_TABLE_PATH",
            "data/delta/anomaly_window_attribution",
        ),
        anomaly_telemetry_attribution=_env_str(
            "S3NTINEL_ANOMALY_TELEMETRY_ATTRIBUTION_TABLE_PATH",
            "data/delta/anomaly_telemetry_attribution",
        ),
        anomaly_event_attribution=_env_str(
            "S3NTINEL_ANOMALY_EVENT_ATTRIBUTION_TABLE_PATH",
            "data/delta/anomaly_event_attribution",
        ),
        anomaly_parameter_candidate_evidence=_env_str(
            "S3NTINEL_ANOMALY_PARAMETER_CANDIDATE_EVIDENCE_TABLE_PATH",
            "data/delta/anomaly_parameter_candidate_evidence",
        ),
        explorer_bundle=_env_str(
            "S3NTINEL_EXPLORER_BUNDLE_PATH",
            "data/delta/explorer_bundle",
        ),
    )


def load_pipeline_context_settings(config: dict[str, Any]) -> PipelineContextSettings:
    return PipelineContextSettings(
        profiling=ProfilingSettings(
            numeric_ratio_threshold=_env_float(
                "S3NTINEL_PROFILE_NUMERIC_RATIO_THRESHOLD",
                _config_float(config, ["profiling", "numeric_ratio_threshold"], 0.8),
            ),
            categorical_cardinality_max=_env_int(
                "S3NTINEL_PROFILE_CATEGORICAL_CARDINALITY_MAX",
                _config_int(config, ["profiling", "categorical_cardinality_max"], 200),
            ),
            behavior_significant_diff_threshold=_env_float(
                "S3NTINEL_PROFILE_BEHAVIOR_SIGNIFICANT_DIFF_THRESHOLD",
                _config_float(config, ["profiling", "behavior_significant_diff_threshold"], 0.05),
            ),
            behavior_center_band_width=_env_float(
                "S3NTINEL_PROFILE_BEHAVIOR_CENTER_BAND_WIDTH",
                _config_float(config, ["profiling", "behavior_center_band_width"], 1.0),
            ),
            behavior_soft_bound_width=_env_float(
                "S3NTINEL_PROFILE_BEHAVIOR_SOFT_BOUND_WIDTH",
                _config_float(config, ["profiling", "behavior_soft_bound_width"], 2.5),
            ),
            behavior_hard_bound_width=_env_float(
                "S3NTINEL_PROFILE_BEHAVIOR_HARD_BOUND_WIDTH",
                _config_float(config, ["profiling", "behavior_hard_bound_width"], 2.0),
            ),
            behavior_mixed_unknown_low_score_threshold=_env_float(
                "S3NTINEL_PROFILE_BEHAVIOR_MIXED_UNKNOWN_LOW_SCORE_THRESHOLD",
                _config_float(config, ["profiling", "behavior_mixed_unknown_low_score_threshold"], 0.38),
            ),
            behavior_mixed_unknown_ambiguous_score_threshold=_env_float(
                "S3NTINEL_PROFILE_BEHAVIOR_MIXED_UNKNOWN_AMBIGUOUS_SCORE_THRESHOLD",
                _config_float(config, ["profiling", "behavior_mixed_unknown_ambiguous_score_threshold"], 0.55),
            ),
            behavior_mixed_unknown_ambiguous_margin_threshold=_env_float(
                "S3NTINEL_PROFILE_BEHAVIOR_MIXED_UNKNOWN_AMBIGUOUS_MARGIN_THRESHOLD",
                _config_float(config, ["profiling", "behavior_mixed_unknown_ambiguous_margin_threshold"], 0.03),
            ),
        ),
        events=EventSettings(
            delta_threshold=_env_float(
                "S3NTINEL_EVENT_DELTA_THRESHOLD",
                _config_float(config, ["events", "delta_threshold"], 0.0),
            ),
            slope_source=_env_str(
                "S3NTINEL_EVENT_SLOPE_SOURCE",
                _config_str(config, ["events", "slope_source"], "ema"),
            ),
            ema_alpha=_env_float(
                "S3NTINEL_EVENT_EMA_ALPHA",
                _config_float(config, ["events", "ema_alpha"], 0.35),
            ),
            slope_threshold_mode=_env_str(
                "S3NTINEL_EVENT_SLOPE_THRESHOLD_MODE",
                _config_str(config, ["events", "slope_threshold_mode"], "fixed"),
            ),
            slope_threshold_quantile=_env_float(
                "S3NTINEL_EVENT_SLOPE_THRESHOLD_QUANTILE",
                _config_float(config, ["events", "slope_threshold_quantile"], 0.75),
            ),
            slope_threshold_scale=_env_float(
                "S3NTINEL_EVENT_SLOPE_THRESHOLD_SCALE",
                _config_float(config, ["events", "slope_threshold_scale"], 0.35),
            ),
            slope_threshold_min=_env_float(
                "S3NTINEL_EVENT_SLOPE_THRESHOLD_MIN",
                _config_float(config, ["events", "slope_threshold_min"], 1e-6),
            ),
            slope_abs_threshold=_env_float(
                "S3NTINEL_EVENT_SLOPE_ABS_THRESHOLD",
                _config_float(config, ["events", "slope_abs_threshold"], 2.0),
            ),
            slope_min_persistence_samples=_env_int(
                "S3NTINEL_EVENT_SLOPE_MIN_PERSISTENCE_SAMPLES",
                _config_int(config, ["events", "slope_min_persistence_samples"], 2),
            ),
            slope_reemit_ratio=_env_float(
                "S3NTINEL_EVENT_SLOPE_REEMIT_RATIO",
                _config_float(config, ["events", "slope_reemit_ratio"], 1.5),
            ),
            warmup_points=_env_int(
                "S3NTINEL_EVENT_WARMUP_POINTS",
                _config_int(config, ["events", "warmup_points"], 4),
            ),
            low_scale_responsiveness=_env_float(
                "S3NTINEL_EVENT_LOW_SCALE_RESPONSIVENESS",
                _config_float(config, ["events", "low_scale_responsiveness"], 1.0),
            ),
            repeatability_aggressiveness=_env_float(
                "S3NTINEL_EVENT_REPEATABILITY_AGGRESSIVENESS",
                _config_float(config, ["events", "repeatability_aggressiveness"], 1.0),
            ),
            drift_conservatism=_env_float(
                "S3NTINEL_EVENT_DRIFT_CONSERVATISM",
                _config_float(config, ["events", "drift_conservatism"], 1.0),
            ),
            chatter_suppression=_env_float(
                "S3NTINEL_EVENT_CHATTER_SUPPRESSION",
                _config_float(config, ["events", "chatter_suppression"], 1.0),
            ),
        ),
        windowing=WindowingSettings(
            min_sampling_rate_hz=_config_float(config, ["windowing", "min_sampling_rate_hz"], 1.0),
            max_ms=_env_int("S3NTINEL_WINDOW_MAX_MS", _config_int(config, ["windowing", "max_ms"], 5000)),
            min_ms=_env_int("S3NTINEL_WINDOW_MIN_MS", _config_int(config, ["windowing", "min_ms"], 25)),
            event_threshold=_env_int(
                "S3NTINEL_WINDOW_EVENT_THRESHOLD",
                _config_int(config, ["windowing", "event_threshold"], 10),
            ),
            inactivity_timeout_ms=_env_int(
                "S3NTINEL_WINDOW_INACTIVITY_TIMEOUT_MS",
                _config_int(config, ["windowing", "inactivity_timeout_ms"], 0),
            ),
            strategy=_env_str(
                "S3NTINEL_WINDOW_STRATEGY",
                _config_str(config, ["windowing", "strategy"], "segmented"),
            ).strip().lower(),
        ),
        backbone=BackboneSettings(
            sensor_count=_env_int(
                "S3NTINEL_BACKBONE_SENSOR_COUNT",
                _config_int(config, ["backbone", "sensor_count"], 8),
            ),
            ridge_lambda=_env_float(
                "S3NTINEL_BACKBONE_RIDGE_LAMBDA",
                _config_float(config, ["backbone", "ridge_lambda"], 1.0),
            ),
            max_sensor_universe=_env_int(
                "S3NTINEL_MAX_BACKBONE_SENSOR_UNIVERSE",
                _config_int(config, ["backbone", "max_sensor_universe"], 50000),
            ),
            event_prior_alpha=_env_float(
                "S3NTINEL_BACKBONE_EVENT_PRIOR_ALPHA",
                _config_float(config, ["backbone", "event_prior_alpha"], 0.35),
            ),
        ),
        graph=GraphSettings(
            precision_ridge_lambda=_env_float(
                "S3NTINEL_PRECISION_GRAPH_RIDGE_LAMBDA",
                _config_float(config, ["graph", "precision_ridge_lambda"], 1.0),
            ),
            min_abs_partial_corr=_env_float(
                "S3NTINEL_V2_MIN_ABS_PARTIAL_CORR",
                _config_float(config, ["graph", "min_abs_partial_corr"], 0.05),
            ),
            max_sensor_universe=_env_int(
                "S3NTINEL_MAX_GRAPH_SENSOR_UNIVERSE",
                _config_int(config, ["graph", "max_sensor_universe"], 50000),
            ),
            event=EventGraphSettings(
                min_count=_env_int(
                    "S3NTINEL_V2_EVENT_GRAPH_MIN_COUNT",
                    _config_int(config, ["graph", "event", "min_count"], 1),
                ),
                min_npmi=_env_float(
                    "S3NTINEL_V2_EVENT_GRAPH_MIN_NPMI",
                    _env_float(
                        "S3NTINEL_V2_EVENT_GRAPH_MIN_JACCARD",
                        _config_float(config, ["graph", "event", "min_npmi"], 0.0),
                    ),
                ),
                top_k_per_parameter_name=_env_int(
                    "S3NTINEL_V2_EVENT_GRAPH_TOP_K_PER_SENSOR",
                    _config_int(config, ["graph", "event", "top_k_per_parameter_name"], 8),
                ),
            ),
            lag=LagGraphSettings(
                tau_max_seconds=_env_float(
                    "S3NTINEL_V2_LAG_TAU_MAX_SECONDS",
                    _config_float(config, ["graph", "lag", "tau_max_seconds"], 30.0),
                ),
                min_count=_env_int(
                    "S3NTINEL_V2_LAG_GRAPH_MIN_COUNT",
                    _config_int(config, ["graph", "lag", "min_count"], 1),
                ),
                max_mean_lag_seconds=_env_optional_float(
                    "S3NTINEL_V2_LAG_GRAPH_MAX_MEAN_LAG_SECONDS",
                    _config_optional_float(config, ["graph", "lag", "max_mean_lag_seconds"], None),
                ),
                top_k_outgoing=_env_int(
                    "S3NTINEL_V2_LAG_GRAPH_TOP_K_OUTGOING",
                    _config_int(config, ["graph", "lag", "top_k_outgoing"], 4),
                ),
                bands=tuple(
                    LagBandSettings(
                        name=str(item.get("name", "")).strip(),
                        lower_seconds=float(item.get("lower_seconds", 0.0)),
                        upper_seconds=float(item.get("upper_seconds", 0.0)),
                        combine_weight=float(item.get("combine_weight", 1.0)),
                    )
                    for item in _config_list(config, ["graph", "lag", "bands"], [])
                    if isinstance(item, dict) and str(item.get("name", "")).strip()
                ),
            ),
            transition=TransitionGraphSettings(
                min_count=_env_int(
                    "S3NTINEL_V2_TRANSITION_GRAPH_MIN_COUNT",
                    _config_int(config, ["graph", "transition", "min_count"], 1),
                ),
            ),
            fusion=GraphFusionSettings(
                alpha=_env_float("S3NTINEL_V2_GRAPH_ALPHA", _config_float(config, ["graph", "fusion", "alpha"], 1.0)),
                beta=_env_float("S3NTINEL_V2_GRAPH_BETA", _config_float(config, ["graph", "fusion", "beta"], 1.0)),
                gamma=_env_float("S3NTINEL_V2_GRAPH_GAMMA", _config_float(config, ["graph", "fusion", "gamma"], 1.0)),
                min_fused_edge_weight=_env_float(
                    "S3NTINEL_V2_GRAPH_MIN_FUSED_EDGE_WEIGHT",
                    _config_float(config, ["graph", "fusion", "min_fused_edge_weight"], 0.05),
                ),
            ),
        ),
        hierarchy=HierarchySettings(
            top_k_per_parameter_name=_env_int(
                "S3NTINEL_V2_HIERARCHY_TOP_K_PER_SENSOR",
                _config_int(config, ["hierarchy", "top_k_per_parameter_name"], 3),
            ),
            subsystem_min_edge_weight=_env_optional_float(
                "S3NTINEL_V2_HIERARCHY_SUBSYSTEM_MIN_EDGE_WEIGHT",
                _config_optional_float(config, ["hierarchy", "subsystem_min_edge_weight"], None),
            ),
            system_min_edge_weight=_env_optional_float(
                "S3NTINEL_V2_HIERARCHY_SYSTEM_MIN_EDGE_WEIGHT",
                _config_optional_float(config, ["hierarchy", "system_min_edge_weight"], None),
            ),
        ),
        phase=PhaseSettings(
            phase_count=_env_int("S3NTINEL_PHASE_COUNT", _config_int(config, ["phase", "phase_count"], 4)),
            detect_sensor_count=_env_int(
                "S3NTINEL_PHASE_DETECT_SENSOR_COUNT",
                _config_int(config, ["phase", "detect_sensor_count"], 8),
            ),
            detect_event_type_count=_env_int(
                "S3NTINEL_PHASE_DETECT_EVENT_TYPE_COUNT",
                _config_int(config, ["phase", "detect_event_type_count"], 6),
            ),
            detect_categorical_state_count=_env_int(
                "S3NTINEL_PHASE_DETECT_CATEGORICAL_STATE_COUNT",
                _config_int(config, ["phase", "detect_categorical_state_count"], 6),
            ),
            stable_drift_quantile=_env_float(
                "S3NTINEL_PHASE_STABLE_DRIFT_QUANTILE",
                _config_float(config, ["phase", "stable_drift_quantile"], 0.35),
            ),
            transition_penalty=_env_float(
                "S3NTINEL_PHASE_TRANSITION_PENALTY",
                _config_float(config, ["phase", "transition_penalty"], 1.5),
            ),
            min_dwell_windows=_env_int(
                "S3NTINEL_PHASE_MIN_DWELL_WINDOWS",
                _config_int(config, ["phase", "min_dwell_windows"], 8),
            ),
        ),
        scoring=ScoringSettings(
            max_bridge_reference_rows=_env_int(
                "S3NTINEL_MAX_BRIDGE_REFERENCE_ROWS",
                _config_int(config, ["scoring", "max_bridge_reference_rows"], 10000),
            ),
            min_warm=_env_int(
                "S3NTINEL_MIN_WARM",
                _config_int(config, ["conformal", "min_warm"], 100),
            ),
        ),
        anomaly=AnomalySettings(
            subsystem_top_sensors_k=_env_int(
                "S3NTINEL_SUBSYSTEM_TOP_SENSORS_K",
                _config_int(config, ["anomaly", "subsystem_top_sensors_k"], 5),
            ),
        ),
    )
