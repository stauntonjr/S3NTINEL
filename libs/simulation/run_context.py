"""Runtime context objects and manifest helpers for simulation pipeline runs."""

from __future__ import annotations

import contextlib
import json
import os
import sys
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from libs.io.delta import describe_spark_runtime_config
from pipelines.plans import StageRunPlan
from libs.simulation import Flight, FlightSpec
from libs.simulation.cli import DEFAULT_START_TIMESTAMP_UTC

DEFAULT_SPARK_PROFILE = "laptop_large_sim"

ARTIFACT_ENV_BY_NAME = {
    "raw_input": "S3NTINEL_RAW_INPUT_PATH",
    "raw_telemetry": "S3NTINEL_RAW_TABLE_PATH",
    "parameter_datatype_profile": "S3NTINEL_PARAMETER_DATATYPE_PROFILE_TABLE_PATH",
    "continuous_scaling_profile": "S3NTINEL_CONTINUOUS_SCALING_PROFILE_TABLE_PATH",
    "parameter_behavior_primitive_profile": "S3NTINEL_PARAMETER_BEHAVIOR_PRIMITIVE_PROFILE_TABLE_PATH",
    "parameter_behavior_profile": "S3NTINEL_PARAMETER_BEHAVIOR_PROFILE_TABLE_PATH",
    "parameter_event_profile": "S3NTINEL_PARAMETER_EVENT_PROFILE_TABLE_PATH",
    "events": "S3NTINEL_EVENTS_TABLE_PATH",
    "window_policy_profile": "S3NTINEL_WINDOW_POLICY_PROFILE_TABLE_PATH",
    "windows": "S3NTINEL_WINDOWS_TABLE_PATH",
    "phase_labels": "S3NTINEL_PHASE_LABELS_TABLE_PATH",
    "hierarchy_sensor_map_label": "S3NTINEL_HIERARCHY_SENSOR_MAP_LABEL_TABLE_PATH",
    "coupling_misbehavior_windows": "S3NTINEL_COUPLING_MISBEHAVIOR_WINDOWS_TABLE_PATH",
    "backbone": "S3NTINEL_BACKBONE_TABLE_PATH",
    "backbone_sensor_energy": "S3NTINEL_BACKBONE_SENSOR_ENERGY_TABLE_PATH",
    "precision_graph": "S3NTINEL_PRECISION_GRAPH_TABLE_PATH",
    "event_graph": "S3NTINEL_EVENT_GRAPH_TABLE_PATH",
    "lag_profile": "S3NTINEL_LAG_PROFILE_TABLE_PATH",
    "lag_graph": "S3NTINEL_LAG_GRAPH_TABLE_PATH",
    "transition_graph": "S3NTINEL_TRANSITION_GRAPH_TABLE_PATH",
    "fused_graph": "S3NTINEL_FUSED_GRAPH_TABLE_PATH",
    "graph_parameter_universe": "S3NTINEL_GRAPH_PARAMETER_UNIVERSE_TABLE_PATH",
    "hierarchy_edge_evidence": "S3NTINEL_HIERARCHY_EDGE_EVIDENCE_TABLE_PATH",
    "phase_windows": "S3NTINEL_PHASE_WINDOWS_TABLE_PATH",
    "phase_baselines": "S3NTINEL_PHASE_BASELINES_TABLE_PATH",
    "phase_label_centroids": "S3NTINEL_PHASE_LABEL_CENTROIDS_TABLE_PATH",
    "window_features": "S3NTINEL_WINDOW_FEATURES_TABLE_PATH",
    "hierarchy_sensor_map": "S3NTINEL_HIERARCHY_SENSOR_MAP_TABLE_PATH",
    "window_scores_raw": "S3NTINEL_WINDOW_SCORES_RAW_TABLE_PATH",
    "window_scores_calibrated": "S3NTINEL_WINDOW_SCORES_CALIBRATED_TABLE_PATH",
    "anomaly_window_attribution": "S3NTINEL_ANOMALY_WINDOW_ATTRIBUTION_TABLE_PATH",
    "anomaly_telemetry_attribution": "S3NTINEL_ANOMALY_TELEMETRY_ATTRIBUTION_TABLE_PATH",
    "anomaly_event_attribution": "S3NTINEL_ANOMALY_EVENT_ATTRIBUTION_TABLE_PATH",
    "explorer_bundle": "S3NTINEL_EXPLORER_BUNDLE_PATH",
}

ARTIFACT_RELATIVE_PATHS = {
    "raw_input": Path("input") / "raw_telemetry",
    "raw_telemetry": Path("delta") / "raw_telemetry",
    "parameter_datatype_profile": Path("delta") / "parameter_datatype_profile",
    "continuous_scaling_profile": Path("delta") / "continuous_scaling_profile",
    "parameter_behavior_primitive_profile": Path("delta") / "parameter_behavior_primitive_profile",
    "parameter_behavior_profile": Path("delta") / "parameter_behavior_profile",
    "parameter_event_profile": Path("delta") / "parameter_event_profile",
    "events": Path("delta") / "events",
    "window_policy_profile": Path("delta") / "window_policy_profile",
    "windows": Path("delta") / "windows",
    "phase_labels": Path("delta") / "phase_labels",
    "hierarchy_sensor_map_label": Path("delta") / "hierarchy_sensor_map_label",
    "coupling_misbehavior_windows": Path("delta") / "coupling_misbehavior_windows",
    "backbone": Path("delta") / "backbone",
    "backbone_sensor_energy": Path("delta") / "backbone_sensor_energy",
    "precision_graph": Path("delta") / "precision_graph",
    "event_graph": Path("delta") / "event_graph",
    "lag_profile": Path("delta") / "lag_profile",
    "lag_graph": Path("delta") / "lag_graph",
    "transition_graph": Path("delta") / "transition_graph",
    "fused_graph": Path("delta") / "fused_graph",
    "graph_parameter_universe": Path("delta") / "graph_parameter_universe",
    "hierarchy_edge_evidence": Path("delta") / "hierarchy_edge_evidence",
    "phase_windows": Path("delta") / "phase_windows",
    "phase_baselines": Path("delta") / "phase_baselines",
    "phase_label_centroids": Path("delta") / "phase_label_centroids",
    "window_features": Path("delta") / "window_features",
    "hierarchy_sensor_map": Path("delta") / "hierarchy_sensor_map",
    "window_scores_raw": Path("delta") / "window_scores_raw",
    "window_scores_calibrated": Path("delta") / "window_scores_calibrated",
    "anomaly_window_attribution": Path("delta") / "anomaly_window_attribution",
    "anomaly_telemetry_attribution": Path("delta") / "anomaly_telemetry_attribution",
    "anomaly_event_attribution": Path("delta") / "anomaly_event_attribution",
    "explorer_bundle": Path("delta") / "explorer_bundle",
}

RUN_SETTING_ENVS = (
    "S3NTINEL_SPARK_PROFILE",
    "S3NTINEL_TABLE_FORMAT",
    "S3NTINEL_RAW_OUTPUT_FORMAT",
    "S3NTINEL_WRITE_MODE",
    "S3NTINEL_MIN_WARM",
    "S3NTINEL_PROFILE_NUMERIC_RATIO_THRESHOLD",
    "S3NTINEL_PROFILE_CATEGORICAL_CARDINALITY_MAX",
    "S3NTINEL_PROFILE_BEHAVIOR_SIGNIFICANT_DIFF_THRESHOLD",
    "S3NTINEL_PROFILE_BEHAVIOR_CENTER_BAND_WIDTH",
    "S3NTINEL_PROFILE_BEHAVIOR_SOFT_BOUND_WIDTH",
    "S3NTINEL_PROFILE_BEHAVIOR_HARD_BOUND_WIDTH",
    "S3NTINEL_PROFILE_BEHAVIOR_MIXED_UNKNOWN_LOW_SCORE_THRESHOLD",
    "S3NTINEL_PROFILE_BEHAVIOR_MIXED_UNKNOWN_AMBIGUOUS_SCORE_THRESHOLD",
    "S3NTINEL_PROFILE_BEHAVIOR_MIXED_UNKNOWN_AMBIGUOUS_MARGIN_THRESHOLD",
    "S3NTINEL_WINDOW_MAX_MS",
    "S3NTINEL_WINDOW_EVENT_THRESHOLD",
    "S3NTINEL_WINDOW_MIN_MS",
    "S3NTINEL_WINDOW_INACTIVITY_TIMEOUT_MS",
    "S3NTINEL_WINDOW_STRATEGY",
    "S3NTINEL_EVENT_DELTA_THRESHOLD",
    "S3NTINEL_EVENT_SLOPE_SOURCE",
    "S3NTINEL_EVENT_EMA_ALPHA",
    "S3NTINEL_EVENT_SLOPE_ABS_THRESHOLD",
    "S3NTINEL_EVENT_SLOPE_MIN_PERSISTENCE_SAMPLES",
    "S3NTINEL_EVENT_SLOPE_REEMIT_RATIO",
    "S3NTINEL_PHASE_COUNT",
    "S3NTINEL_BACKBONE_SENSOR_COUNT",
    "S3NTINEL_BACKBONE_RIDGE_LAMBDA",
    "S3NTINEL_BACKBONE_EVENT_PRIOR_ALPHA",
    "S3NTINEL_LOCAL_ARTIFACT_BASE_DIR",
)

MODE_PLAN_BY_NAME = {
    "profile": StageRunPlan(
        run_name="s3ntinel.sim_profile",
        pipeline_mode="sim_profile:v2",
        stage_scripts=(
            "00_ingest_raw.py",
            "10_parameter_profiles_fit.py",
            "12_behavior_profiles_fit.py",
            "15_event_profiles_fit.py",
        ),
        summary_artifact_path="reports/profile_pipeline_run_summary.json",
    ),
    "event": StageRunPlan(
        run_name="s3ntinel.sim_event",
        pipeline_mode="sim_event:v2",
        stage_scripts=(
            "00_ingest_raw.py",
            "10_parameter_profiles_fit.py",
            "12_behavior_profiles_fit.py",
            "15_event_profiles_fit.py",
            "20_events_extract.py",
        ),
        summary_artifact_path="reports/event_pipeline_run_summary.json",
    ),
    "structural": StageRunPlan(
        run_name="s3ntinel.sim_structural",
        pipeline_mode="sim_structural:v2",
        stage_scripts=(
            "00_ingest_raw.py",
            "10_parameter_profiles_fit.py",
            "12_behavior_profiles_fit.py",
            "15_event_profiles_fit.py",
            "20_events_extract.py",
            "25_window_policy_profile.py",
            "30_windows_adaptive.py",
            "40_backbone_fit.py",
            "50_build_graph.py",
            "60_fit_hierarchy.py",
        ),
        summary_artifact_path="reports/structural_pipeline_run_summary.json",
    ),
    "full": StageRunPlan(
        run_name="s3ntinel.sim_full",
        pipeline_mode="sim_full:v2",
        stage_scripts=(
            "00_ingest_raw.py",
            "10_parameter_profiles_fit.py",
            "12_behavior_profiles_fit.py",
            "15_event_profiles_fit.py",
            "20_events_extract.py",
            "25_window_policy_profile.py",
            "30_windows_adaptive.py",
            "40_backbone_fit.py",
            "50_build_graph.py",
            "60_fit_hierarchy.py",
            "70_phase_fit.py",
            "72_phase_label_centroids.py",
            "80_window_scores_raw.py",
            "85_window_scores_calibrate.py",
            "90_anomaly_attribution.py",
            "95_emit_explorer_bundle.py",
        ),
        summary_artifact_path="reports/pipeline_run_summary.json",
    ),
}


def resolve_flight_stochasticity(
    *,
    flight: FlightSpec,
    sim_seed: int | None = None,
) -> dict[str, Any]:
    payload = dict(flight.metadata.get("stochasticity") or {})
    resolved = dict(payload)
    resolved["seed"] = int(payload.get("seed", sim_seed if sim_seed is not None else 0))
    resolved["profile_name"] = str(payload.get("profile_name") or "deterministic")
    resolved["profile_version"] = str(payload.get("profile_version") or "v1")
    resolved["enabled_channels"] = [str(item) for item in (payload.get("enabled_channels") or ())]
    return resolved


@dataclass(frozen=True)
class PipelineRunConfig:
    flight_name: str
    tail_id: str
    flight_id: str
    n_steps: int
    dt_seconds: float
    base_dir: str
    mode: str
    table_format: str
    write_mode: str
    min_warm: int
    delta_threshold: float
    slope_source: str
    ema_alpha: float
    slope_threshold_mode: str
    slope_threshold_quantile: float
    slope_threshold_scale: float
    slope_threshold_min: float
    window_max_ms: int
    window_event_threshold: int
    window_min_ms: int
    window_inactivity_timeout_ms: int
    window_strategy: str
    phase_count: int
    backbone_parameter_count: int
    backbone_ridge_lambda: float
    backbone_event_prior_alpha: float = 0.35
    slope_abs_threshold: float = 2.0
    slope_min_persistence_samples: int = 2
    slope_reemit_ratio: float = 1.5
    event_warmup_points: int = 4
    event_low_scale_responsiveness: float = 1.0
    event_repeatability_aggressiveness: float = 1.0
    event_drift_conservatism: float = 1.0
    event_chatter_suppression: float = 1.0
    profile_numeric_ratio_threshold: float = 0.8
    profile_categorical_cardinality_max: int = 200
    profile_behavior_significant_diff_threshold: float = 0.05
    profile_behavior_center_band_width: float = 1.0
    profile_behavior_soft_bound_width: float = 2.5
    profile_behavior_hard_bound_width: float = 2.0
    profile_behavior_mixed_unknown_low_score_threshold: float = 0.38
    profile_behavior_mixed_unknown_ambiguous_score_threshold: float = 0.55
    profile_behavior_mixed_unknown_ambiguous_margin_threshold: float = 0.03
    sim_seed: int | None = None
    start_stage: str | None = None
    end_stage: str | None = None
    replay_run_dir: str | None = None

    @classmethod
    def from_args(cls, args: Any) -> "PipelineRunConfig":
        return cls(
            flight_name=str(args.flight_name),
            tail_id=str(args.tail_id),
            flight_id=str(args.flight_id),
            n_steps=(0 if args.n_steps is None else int(args.n_steps)),
            dt_seconds=(0.0 if args.dt_seconds is None else float(args.dt_seconds)),
            base_dir=str(args.base_dir),
            mode=str(args.mode),
            table_format=str(args.format),
            write_mode=str(args.write_mode),
            min_warm=int(args.min_warm),
            profile_numeric_ratio_threshold=float(args.profile_numeric_ratio_threshold),
            profile_categorical_cardinality_max=int(args.profile_categorical_cardinality_max),
            profile_behavior_significant_diff_threshold=float(args.profile_behavior_significant_diff_threshold),
            profile_behavior_center_band_width=float(args.profile_behavior_center_band_width),
            profile_behavior_soft_bound_width=float(args.profile_behavior_soft_bound_width),
            profile_behavior_hard_bound_width=float(args.profile_behavior_hard_bound_width),
            profile_behavior_mixed_unknown_low_score_threshold=float(
                args.profile_behavior_mixed_unknown_low_score_threshold
            ),
            profile_behavior_mixed_unknown_ambiguous_score_threshold=float(
                args.profile_behavior_mixed_unknown_ambiguous_score_threshold
            ),
            profile_behavior_mixed_unknown_ambiguous_margin_threshold=float(
                args.profile_behavior_mixed_unknown_ambiguous_margin_threshold
            ),
            delta_threshold=float(args.delta_threshold),
            slope_source=str(args.slope_source),
            ema_alpha=float(args.ema_alpha),
            slope_threshold_mode=str(args.slope_threshold_mode),
            slope_threshold_quantile=float(args.slope_threshold_quantile),
            slope_threshold_scale=float(args.slope_threshold_scale),
            slope_threshold_min=float(args.slope_threshold_min),
            slope_abs_threshold=float(args.slope_abs_threshold),
            slope_min_persistence_samples=int(args.slope_min_persistence_samples),
            slope_reemit_ratio=float(args.slope_reemit_ratio),
            event_warmup_points=int(args.event_warmup_points),
            event_low_scale_responsiveness=float(args.event_low_scale_responsiveness),
            event_repeatability_aggressiveness=float(args.event_repeatability_aggressiveness),
            event_drift_conservatism=float(args.event_drift_conservatism),
            event_chatter_suppression=float(args.event_chatter_suppression),
            window_max_ms=int(args.window_max_ms),
            window_event_threshold=int(args.window_event_threshold),
            window_min_ms=int(args.window_min_ms),
            window_inactivity_timeout_ms=int(args.window_inactivity_timeout_ms),
            window_strategy=str(args.window_strategy),
            phase_count=int(args.phase_count),
            backbone_parameter_count=int(args.backbone_parameter_count),
            backbone_ridge_lambda=float(args.backbone_ridge_lambda),
            backbone_event_prior_alpha=float(args.backbone_event_prior_alpha),
            sim_seed=(None if args.sim_seed is None else int(args.sim_seed)),
            start_stage=(None if getattr(args, "start_stage", None) is None else str(args.start_stage)),
            end_stage=(None if getattr(args, "end_stage", None) is None else str(args.end_stage)),
            replay_run_dir=(None if getattr(args, "replay_run_dir", None) is None else str(args.replay_run_dir)),
        )

    def with_flight_defaults(self, *, flight: FlightSpec) -> "PipelineRunConfig":
        simulation_defaults = dict(flight.metadata.get("simulation_defaults", {}) or {})
        input_metadata = dict(flight.input_program_spec.metadata or {})
        stochasticity = resolve_flight_stochasticity(flight=flight, sim_seed=self.sim_seed)
        default_n_steps = int(
            simulation_defaults.get(
                "n_steps",
                input_metadata.get("recommended_n_steps", len(tuple(flight.input_program_spec.steps))),
            )
        )
        default_dt_seconds = float(
            simulation_defaults.get(
                "dt_seconds",
                input_metadata.get("default_dt_seconds", 1.0),
            )
        )
        return replace(
            self,
            n_steps=int(self.n_steps if int(self.n_steps) > 0 else default_n_steps),
            dt_seconds=float(self.dt_seconds if float(self.dt_seconds) > 0.0 else default_dt_seconds),
            sim_seed=int(self.sim_seed if self.sim_seed is not None else stochasticity["seed"]),
        )

    def build_run_dir(self) -> Path:
        if self.replay_run_dir is not None and str(self.replay_run_dir).strip():
            return Path(str(self.replay_run_dir))
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        safe_flight_name = self.flight_name.replace("/", "_")
        return Path(self.base_dir) / f"{timestamp}_{safe_flight_name}"

    def build_flight(self, *, flight: FlightSpec) -> Flight:
        return Flight.from_spec(
            flight,
            tail_id=self.tail_id,
            flight_id=self.flight_id,
            start_timestamp_utc=DEFAULT_START_TIMESTAMP_UTC,
        )


@dataclass(frozen=True)
class RunPaths:
    run_dir: Path

    def artifact_path(self, name: str) -> Path:
        return self.run_dir / ARTIFACT_RELATIVE_PATHS[name]

    def artifact_paths(self) -> dict[str, Path]:
        return {name: self.artifact_path(name) for name in ARTIFACT_RELATIVE_PATHS}

    @property
    def log_path(self) -> Path:
        return self.run_dir / "logs" / "run.log"

    @property
    def manifest_path(self) -> Path:
        return self.run_dir / "reports" / "run_manifest.json"


@dataclass(frozen=True)
class PipelineRunResult:
    paths: RunPaths
    status: str
    seed_counts: dict[str, int]


class _TeeStream:
    def __init__(self, *streams: Any) -> None:
        self._streams = streams

    def write(self, data: str) -> int:
        for stream in self._streams:
            try:
                stream.write(data)
                stream.flush()
            except ValueError:
                continue
        return len(data)

    def flush(self) -> None:
        for stream in self._streams:
            try:
                stream.flush()
            except ValueError:
                continue


@contextlib.contextmanager
def tee_console(log_path: Path):
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as log_file:
        stdout = _TeeStream(sys.stdout, log_file)
        stderr = _TeeStream(sys.stderr, log_file)
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            yield


def set_run_env(paths: RunPaths, config: PipelineRunConfig) -> dict[str, str | None]:
    previous = {
        key: os.environ.get(key)
        for key in (*ARTIFACT_ENV_BY_NAME.values(), *RUN_SETTING_ENVS)
    }
    for name, env_key in ARTIFACT_ENV_BY_NAME.items():
        os.environ[env_key] = str(paths.artifact_path(name))
    os.environ.setdefault("S3NTINEL_SPARK_PROFILE", DEFAULT_SPARK_PROFILE)
    os.environ["S3NTINEL_TABLE_FORMAT"] = config.table_format
    os.environ["S3NTINEL_RAW_OUTPUT_FORMAT"] = config.table_format
    os.environ["S3NTINEL_WRITE_MODE"] = "overwrite" if config.write_mode == "merge" else config.write_mode
    os.environ["S3NTINEL_MIN_WARM"] = str(config.min_warm)
    os.environ["S3NTINEL_PROFILE_NUMERIC_RATIO_THRESHOLD"] = str(config.profile_numeric_ratio_threshold)
    os.environ["S3NTINEL_PROFILE_CATEGORICAL_CARDINALITY_MAX"] = str(config.profile_categorical_cardinality_max)
    os.environ["S3NTINEL_PROFILE_BEHAVIOR_SIGNIFICANT_DIFF_THRESHOLD"] = str(
        config.profile_behavior_significant_diff_threshold
    )
    os.environ["S3NTINEL_PROFILE_BEHAVIOR_CENTER_BAND_WIDTH"] = str(config.profile_behavior_center_band_width)
    os.environ["S3NTINEL_PROFILE_BEHAVIOR_SOFT_BOUND_WIDTH"] = str(config.profile_behavior_soft_bound_width)
    os.environ["S3NTINEL_PROFILE_BEHAVIOR_HARD_BOUND_WIDTH"] = str(config.profile_behavior_hard_bound_width)
    os.environ["S3NTINEL_PROFILE_BEHAVIOR_MIXED_UNKNOWN_LOW_SCORE_THRESHOLD"] = str(
        config.profile_behavior_mixed_unknown_low_score_threshold
    )
    os.environ["S3NTINEL_PROFILE_BEHAVIOR_MIXED_UNKNOWN_AMBIGUOUS_SCORE_THRESHOLD"] = str(
        config.profile_behavior_mixed_unknown_ambiguous_score_threshold
    )
    os.environ["S3NTINEL_PROFILE_BEHAVIOR_MIXED_UNKNOWN_AMBIGUOUS_MARGIN_THRESHOLD"] = str(
        config.profile_behavior_mixed_unknown_ambiguous_margin_threshold
    )
    os.environ["S3NTINEL_WINDOW_MAX_MS"] = str(config.window_max_ms)
    os.environ["S3NTINEL_WINDOW_EVENT_THRESHOLD"] = str(config.window_event_threshold)
    os.environ["S3NTINEL_WINDOW_MIN_MS"] = str(config.window_min_ms)
    os.environ["S3NTINEL_WINDOW_INACTIVITY_TIMEOUT_MS"] = str(config.window_inactivity_timeout_ms)
    os.environ["S3NTINEL_WINDOW_STRATEGY"] = config.window_strategy
    os.environ["S3NTINEL_EVENT_DELTA_THRESHOLD"] = str(config.delta_threshold)
    os.environ["S3NTINEL_EVENT_SLOPE_SOURCE"] = config.slope_source
    os.environ["S3NTINEL_EVENT_EMA_ALPHA"] = str(config.ema_alpha)
    os.environ["S3NTINEL_EVENT_SLOPE_THRESHOLD_MODE"] = config.slope_threshold_mode
    os.environ["S3NTINEL_EVENT_SLOPE_THRESHOLD_QUANTILE"] = str(config.slope_threshold_quantile)
    os.environ["S3NTINEL_EVENT_SLOPE_THRESHOLD_SCALE"] = str(config.slope_threshold_scale)
    os.environ["S3NTINEL_EVENT_SLOPE_THRESHOLD_MIN"] = str(config.slope_threshold_min)
    os.environ["S3NTINEL_EVENT_SLOPE_ABS_THRESHOLD"] = str(config.slope_abs_threshold)
    os.environ["S3NTINEL_EVENT_SLOPE_MIN_PERSISTENCE_SAMPLES"] = str(config.slope_min_persistence_samples)
    os.environ["S3NTINEL_EVENT_SLOPE_REEMIT_RATIO"] = str(config.slope_reemit_ratio)
    os.environ["S3NTINEL_EVENT_WARMUP_POINTS"] = str(config.event_warmup_points)
    os.environ["S3NTINEL_EVENT_LOW_SCALE_RESPONSIVENESS"] = str(config.event_low_scale_responsiveness)
    os.environ["S3NTINEL_EVENT_REPEATABILITY_AGGRESSIVENESS"] = str(config.event_repeatability_aggressiveness)
    os.environ["S3NTINEL_EVENT_DRIFT_CONSERVATISM"] = str(config.event_drift_conservatism)
    os.environ["S3NTINEL_EVENT_CHATTER_SUPPRESSION"] = str(config.event_chatter_suppression)
    os.environ["S3NTINEL_PHASE_COUNT"] = str(config.phase_count)
    os.environ["S3NTINEL_BACKBONE_SENSOR_COUNT"] = str(config.backbone_parameter_count)
    os.environ["S3NTINEL_BACKBONE_RIDGE_LAMBDA"] = str(config.backbone_ridge_lambda)
    os.environ["S3NTINEL_BACKBONE_EVENT_PRIOR_ALPHA"] = str(config.backbone_event_prior_alpha)
    os.environ["S3NTINEL_LOCAL_ARTIFACT_BASE_DIR"] = str(paths.run_dir)
    return previous


def restore_env(previous: dict[str, str | None]) -> None:
    for key, value in previous.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def run_mode(config: PipelineRunConfig) -> tuple[str, str, list[str], str]:
    plan = MODE_PLAN_BY_NAME[config.mode]
    selected_stage_scripts = plan.selected_stage_scripts(
        start_stage=config.start_stage,
        end_stage=config.end_stage,
    )
    if selected_stage_scripts and selected_stage_scripts[0] != plan.stage_scripts[0] and not config.replay_run_dir:
        raise RuntimeError(
            f"stage replay starting at {selected_stage_scripts[0]!r} requires --replay-run-dir with existing replayable artifacts"
        )
    return (
        str(plan.run_name),
        str(plan.pipeline_mode),
        selected_stage_scripts,
        str(plan.summary_artifact_path),
    )


def write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")


def _summarize_artifacts(paths: RunPaths) -> dict[str, dict[str, Any]]:
    return {
        name: {"path": str(path), "exists": path.exists()}
        for name, path in paths.artifact_paths().items()
    }


def build_manifest(
    *,
    paths: RunPaths,
    config: PipelineRunConfig,
    flight: FlightSpec,
    status: str,
    error_message: str | None,
    start_utc: datetime,
    end_utc: datetime,
    elapsed_ms: float,
    seed_counts: dict[str, int],
) -> dict[str, Any]:
    spark_runtime = describe_spark_runtime_config()
    stochasticity = resolve_flight_stochasticity(flight=flight, sim_seed=config.sim_seed)
    return {
        "status": status,
        "error": error_message,
        "run_dir": str(paths.run_dir),
        "log_path": str(paths.log_path),
        "artifact_base_dir": str(paths.run_dir),
        "source": {
            "source_selector": "flight_name",
            "flight_name": config.flight_name,
            "tail_id": config.tail_id,
            "flight_id": config.flight_id,
        },
        "simulation": {
            "n_steps": config.n_steps,
            "dt_seconds": config.dt_seconds,
            "start_timestamp_utc": DEFAULT_START_TIMESTAMP_UTC.isoformat(),
        },
        "stochasticity": stochasticity,
        "pipeline": asdict(config),
        "replay": {
            "replay_run_dir": config.replay_run_dir,
            "start_stage": config.start_stage,
            "end_stage": config.end_stage,
        },
        "timing": {
            "started_at_utc": start_utc.isoformat(),
            "ended_at_utc": end_utc.isoformat(),
            "elapsed_ms": elapsed_ms,
        },
        "environment": {
            "python_executable": sys.executable,
            "conda_default_env": os.getenv("CONDA_DEFAULT_ENV"),
            "spark_profile": os.getenv("S3NTINEL_SPARK_PROFILE"),
            "spark_master": spark_runtime.get("spark.master"),
            "spark_driver_memory": spark_runtime.get("spark.driver.memory"),
            "spark_driver_max_result_size": spark_runtime.get("spark.driver.maxResultSize"),
            "spark_executor_memory": spark_runtime.get("spark.executor.memory"),
            "spark_shuffle_partitions": spark_runtime.get("spark.sql.shuffle.partitions"),
            "spark_default_parallelism": spark_runtime.get("spark.default.parallelism"),
            "spark_local_dir": spark_runtime.get("spark.local.dir"),
            "spark_sql_adaptive_enabled": spark_runtime.get("spark.sql.adaptive.enabled"),
        },
        "seed_counts": seed_counts,
        "artifacts": _summarize_artifacts(paths),
        "validation_reports": {
            "profile_validation": str(paths.run_dir / "reports" / "profile_validation_summary.json"),
            "event_validation": str(paths.run_dir / "reports" / "event_validation_summary.json"),
            "label_contract": str(paths.run_dir / "reports" / "label_contract_summary.json"),
            "phase_validation": str(paths.run_dir / "reports" / "phase_validation_summary.json"),
            "hierarchy_validation": str(paths.run_dir / "reports" / "hierarchy_validation_summary.json"),
            "coupling_validation": str(paths.run_dir / "reports" / "coupling_validation_summary.json"),
            "score_validation": str(paths.run_dir / "reports" / "score_validation_summary.json"),
            "misbehavior_window_validation": str(paths.run_dir / "reports" / "misbehavior_window_validation_summary.json"),
            "misbehavior_attribution_validation": str(paths.run_dir / "reports" / "misbehavior_attribution_validation_summary.json"),
            "attribution_validation": str(paths.run_dir / "reports" / "attribution_validation_summary.json"),
            "fault_window_validation": str(paths.run_dir / "reports" / "fault_window_validation_summary.json"),
            "simulation_benchmark_audit": str(paths.run_dir / "reports" / "simulation_benchmark_audit_summary.json"),
            "benchmark_scope_validation": str(paths.run_dir / "reports" / "benchmark_scope_validation_summary.json"),
            "benchmark_tier_validation": str(paths.run_dir / "reports" / "benchmark_tier_validation_summary.json"),
        },
    }
