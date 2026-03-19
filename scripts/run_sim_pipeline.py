"""Run the canonical simulation pipeline into a persisted artifact bundle."""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
import time
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

import pandas as pd

from libs.anomaly.validator import (
    validate_attribution_against_fault_truth,
    validate_attribution_against_misbehavior_truth,
)
from libs.events.validator import build_event_validation_summary
from libs.graph import build_coupling_validation_summary, build_graph_validation_summary
from libs.io.delta import describe_spark_runtime_config, get_spark, read_table, write_table
from libs.io.pandas_spark import pandas_records_for_spark
from libs.io.schemas import SIMULATION_RAW_INPUT_SCHEMA
from libs.perf import get_logger
from libs.phase import validate_detected_phases_from_tables
from libs.profiling.validator import build_profile_validation_summary
from libs.scoring.validator import (
    summarize_misbehavior_window_detection,
    validate_scores_against_fault_windows,
    validate_scores_against_misbehavior_windows,
)
from libs.simulation import Flight, FlightSpec
from libs.testing.assertions import (
    REQUIRED_DETECTED_COLUMNS,
    REQUIRED_LABEL_COLUMNS,
    REQUIRED_PROFILER_VALIDATOR_LABEL_COLUMNS,
    assert_no_banned_columns,
    assert_no_bare_detector_event_type,
    assert_required_columns,
)
from pipelines._pipeline_runner import run_stage_group
from scripts.sim_common import (
    DEFAULT_START_TIMESTAMP_UTC,
    add_event_args,
    add_source_args,
    add_window_args,
    resolve_flight,
)


LOGGER_NAME = "s3ntinel.run_sim_pipeline"

ARTIFACT_ENV_BY_NAME = {
    "raw_input": "S3NTINEL_RAW_INPUT_PATH",
    "raw_telemetry": "S3NTINEL_RAW_TABLE_PATH",
    "parameter_datatype_profile": "S3NTINEL_PARAMETER_DATATYPE_PROFILE_TABLE_PATH",
    "continuous_scaling_profile": "S3NTINEL_CONTINUOUS_SCALING_PROFILE_TABLE_PATH",
    "parameter_behavior_profile": "S3NTINEL_PARAMETER_BEHAVIOR_PROFILE_TABLE_PATH",
    "events": "S3NTINEL_EVENTS_TABLE_PATH",
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
    "phase_windows": "S3NTINEL_PHASE_WINDOWS_TABLE_PATH",
    "phase_baselines": "S3NTINEL_PHASE_BASELINES_TABLE_PATH",
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
    "parameter_behavior_profile": Path("delta") / "parameter_behavior_profile",
    "events": Path("delta") / "events",
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
    "phase_windows": Path("delta") / "phase_windows",
    "phase_baselines": Path("delta") / "phase_baselines",
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
    "S3NTINEL_TABLE_FORMAT",
    "S3NTINEL_RAW_OUTPUT_FORMAT",
    "S3NTINEL_WRITE_MODE",
    "S3NTINEL_MIN_WARM",
    "S3NTINEL_WINDOW_MAX_MS",
    "S3NTINEL_WINDOW_EVENT_THRESHOLD",
    "S3NTINEL_WINDOW_MIN_MS",
    "S3NTINEL_WINDOW_INACTIVITY_TIMEOUT_MS",
    "S3NTINEL_WINDOW_STRATEGY",
    "S3NTINEL_EVENT_DELTA_THRESHOLD",
    "S3NTINEL_EVENT_SLOPE_SOURCE",
    "S3NTINEL_EVENT_EMA_ALPHA",
    "S3NTINEL_PHASE_COUNT",
    "S3NTINEL_BACKBONE_SENSOR_COUNT",
    "S3NTINEL_BACKBONE_RIDGE_LAMBDA",
    "S3NTINEL_LOCAL_ARTIFACT_BASE_DIR",
)

MODE_PLAN_BY_NAME = {
    "profile": {
        "run_name": "s3ntinel.sim_profile",
        "pipeline_mode": "sim_profile:v2",
        "stage_scripts": [
            "00_ingest_raw.py",
            "10_parameter_profiles_fit.py",
        ],
        "summary_artifact_path": "reports/profile_pipeline_run_summary.json",
    },
    "structural": {
        "run_name": "s3ntinel.sim_structural",
        "pipeline_mode": "sim_structural:v2",
        "stage_scripts": [
            "00_ingest_raw.py",
            "10_parameter_profiles_fit.py",
            "20_events_extract.py",
            "30_windows_adaptive.py",
            "40_backbone_fit.py",
            "50_build_graph.py",
            "60_fit_hierarchy.py",
        ],
        "summary_artifact_path": "reports/structural_pipeline_run_summary.json",
    },
    "full": {
        "run_name": "s3ntinel.sim_full",
        "pipeline_mode": "sim_full:v2",
        "stage_scripts": [
            "00_ingest_raw.py",
            "10_parameter_profiles_fit.py",
            "20_events_extract.py",
            "30_windows_adaptive.py",
            "40_backbone_fit.py",
            "50_build_graph.py",
            "60_fit_hierarchy.py",
            "70_phase_fit.py",
            "80_window_scores_raw.py",
            "85_window_scores_calibrate.py",
            "90_anomaly_attribution.py",
            "95_emit_explorer_bundle.py",
        ],
        "summary_artifact_path": "reports/pipeline_run_summary.json",
    },
}


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
    window_max_ms: int
    window_event_threshold: int
    window_min_ms: int
    window_inactivity_timeout_ms: int
    window_strategy: str
    phase_count: int
    backbone_parameter_count: int
    backbone_ridge_lambda: float

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "PipelineRunConfig":
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
            delta_threshold=float(args.delta_threshold),
            slope_source=str(args.slope_source),
            ema_alpha=float(args.ema_alpha),
            window_max_ms=int(args.window_max_ms),
            window_event_threshold=int(args.window_event_threshold),
            window_min_ms=int(args.window_min_ms),
            window_inactivity_timeout_ms=int(args.window_inactivity_timeout_ms),
            window_strategy=str(args.window_strategy),
            phase_count=int(args.phase_count),
            backbone_parameter_count=int(args.backbone_parameter_count),
            backbone_ridge_lambda=float(args.backbone_ridge_lambda),
        )

    def with_flight_defaults(self, *, flight: FlightSpec) -> "PipelineRunConfig":
        simulation_defaults = dict(flight.metadata.get("simulation_defaults", {}) or {})
        input_metadata = dict(flight.input_program_spec.metadata or {})
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
        )

    def build_run_dir(self) -> Path:
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
def _tee_console(log_path: Path):
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as log_file:
        stdout = _TeeStream(sys.stdout, log_file)
        stderr = _TeeStream(sys.stderr, log_file)
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            yield


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the simulation pipeline into a persisted artifact bundle")
    add_source_args(parser)
    add_event_args(parser)
    add_window_args(parser)
    parser.add_argument("--base-dir", default="data/simulation_runs", help="Base directory for simulation pipeline runs")
    parser.add_argument("--mode", default="full", choices=("profile", "structural", "full"))
    parser.add_argument("--format", default="parquet", choices=("parquet", "delta"), help="Persisted table format")
    parser.add_argument("--write-mode", default="overwrite", choices=("overwrite", "append", "merge"))
    parser.add_argument("--min-warm", default=1, type=int, help="Conformal minimum warm size")
    parser.add_argument("--phase-count", type=int, default=3, help="Detected phase count")
    parser.add_argument("--backbone-parameter-count", type=int, default=8, help="Backbone parameter count")
    parser.add_argument("--backbone-ridge-lambda", type=float, default=1.0, help="Backbone ridge lambda")
    return parser.parse_args()


def _set_run_env(paths: RunPaths, config: PipelineRunConfig) -> dict[str, str | None]:
    previous = {
        key: os.environ.get(key)
        for key in (*ARTIFACT_ENV_BY_NAME.values(), *RUN_SETTING_ENVS)
    }

    for name, env_key in ARTIFACT_ENV_BY_NAME.items():
        os.environ[env_key] = str(paths.artifact_path(name))
    os.environ["S3NTINEL_TABLE_FORMAT"] = config.table_format
    os.environ["S3NTINEL_RAW_OUTPUT_FORMAT"] = config.table_format
    os.environ["S3NTINEL_WRITE_MODE"] = "overwrite" if config.write_mode == "merge" else config.write_mode
    os.environ["S3NTINEL_MIN_WARM"] = str(config.min_warm)
    os.environ["S3NTINEL_WINDOW_MAX_MS"] = str(config.window_max_ms)
    os.environ["S3NTINEL_WINDOW_EVENT_THRESHOLD"] = str(config.window_event_threshold)
    os.environ["S3NTINEL_WINDOW_MIN_MS"] = str(config.window_min_ms)
    os.environ["S3NTINEL_WINDOW_INACTIVITY_TIMEOUT_MS"] = str(config.window_inactivity_timeout_ms)
    os.environ["S3NTINEL_WINDOW_STRATEGY"] = config.window_strategy
    os.environ["S3NTINEL_EVENT_DELTA_THRESHOLD"] = str(config.delta_threshold)
    os.environ["S3NTINEL_EVENT_SLOPE_SOURCE"] = config.slope_source
    os.environ["S3NTINEL_EVENT_EMA_ALPHA"] = str(config.ema_alpha)
    os.environ["S3NTINEL_PHASE_COUNT"] = str(config.phase_count)
    os.environ["S3NTINEL_BACKBONE_SENSOR_COUNT"] = str(config.backbone_parameter_count)
    os.environ["S3NTINEL_BACKBONE_RIDGE_LAMBDA"] = str(config.backbone_ridge_lambda)
    os.environ["S3NTINEL_LOCAL_ARTIFACT_BASE_DIR"] = str(paths.run_dir)
    return previous


def _restore_env(previous: dict[str, str | None]) -> None:
    for key, value in previous.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def _build_hierarchy_label_df(aircraft_spec: Any) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for system_spec in aircraft_spec.systems:
        for subsystem_spec in system_spec.subsystems:
            for module_spec in subsystem_spec.modules:
                for parameter_spec in module_spec.parameters:
                    rows.append(
                        {
                            "parameter_name": str(parameter_spec.parameter_name),
                            "system_id": str(parameter_spec.system_id),
                            "subsystem_id": str(parameter_spec.subsystem_id),
                            "module_id": str(parameter_spec.module_id),
                        }
                    )
    return pd.DataFrame.from_records(rows).drop_duplicates()


def _build_coupling_misbehavior_windows_df(
    *,
    flight: FlightSpec,
    dt_seconds: float,
) -> pd.DataFrame:
    coupling_by_id = {coupling.coupling_id: coupling for coupling in flight.aircraft_spec.couplings}
    rows: list[dict[str, Any]] = []
    for window in tuple(getattr(flight.misbehavior_program_spec, "windows", ()) or ()):
        if str(getattr(window, "subject_kind", "parameter")) != "coupling":
            continue
        coupling = coupling_by_id.get(str(window.coupling_id))
        if coupling is None:
            continue
        context = dict(window.context)
        metadata = dict(window.metadata)
        detail_label = str(
            metadata.get("misbehavior_detail_label")
            or context.get("misbehavior_detail_label")
            or context.get("violation_type")
            or metadata.get("fault_type")
            or metadata.get("misbehavior_family_label")
            or context.get("misbehavior_family_label")
            or ""
        )
        family_label = str(
            metadata.get("misbehavior_family_label")
            or context.get("misbehavior_family_label")
            or detail_label
        )
        misbehavior_window_id = str(
            metadata.get("misbehavior_window_id")
            or metadata.get("fault_window_id")
            or f"{window.coupling_id}:{window.start_step}:{window.end_step_exclusive}"
        )
        start_step = int(window.start_step)
        end_step_exclusive = int(window.end_step_exclusive)
        rows.append(
            {
                "coupling_id": str(window.coupling_id),
                "source_module_id": str(coupling.source_module_id),
                "source_port_name": str(coupling.source_port_name),
                "target_module_id": str(coupling.target_module_id),
                "target_port_name": str(coupling.target_port_name),
                "relation_type": str(coupling.relation_type),
                "start_step": start_step,
                "end_step_exclusive": end_step_exclusive,
                "start_timestamp_utc": DEFAULT_START_TIMESTAMP_UTC + pd.to_timedelta(start_step * float(dt_seconds), unit="s"),
                "end_timestamp_utc_exclusive": DEFAULT_START_TIMESTAMP_UTC + pd.to_timedelta(end_step_exclusive * float(dt_seconds), unit="s"),
                "misbehavior_window_id": misbehavior_window_id,
                "misbehavior_family_label": family_label,
                "misbehavior_detail_label": detail_label,
                "fault_window_id": str(metadata.get("fault_window_id", misbehavior_window_id)),
                "fault_family_label": str(metadata.get("fault_family_label", "coupling")),
            }
        )
    return pd.DataFrame.from_records(rows).drop_duplicates()


def _simulate_seed_frames(
    *,
    config: PipelineRunConfig,
    flight: FlightSpec,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    telemetry_rows, phase_rows = config.build_flight(flight=flight).simulate_rows(
        n_steps=config.n_steps,
        dt_seconds=config.dt_seconds,
        apply_faults=True,
    )
    raw_df = pd.DataFrame.from_records(telemetry_rows)
    phase_df = pd.DataFrame.from_records(phase_rows)
    required_columns = {"tail_id", "flight_id", "timestamp_utc", "parameter_name", "parameter_value", "date_utc"}
    missing_columns = required_columns.difference(raw_df.columns)
    if missing_columns:
        missing_list = ", ".join(sorted(missing_columns))
        raise ValueError(f"simulation output is missing required canonical raw columns: {missing_list}")
    phase_columns = {"tail_id", "flight_id", "step_index", "timestamp_utc", "phase_label", "date_utc"}
    missing_phase_columns = phase_columns.difference(phase_df.columns)
    if missing_phase_columns:
        missing_list = ", ".join(sorted(missing_phase_columns))
        raise ValueError(f"simulation output is missing required phase label columns: {missing_list}")
    return raw_df, phase_df


def _write_seed_tables(
    *,
    spark: Any,
    paths: RunPaths,
    config: PipelineRunConfig,
    flight: FlightSpec,
) -> dict[str, int]:
    from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType, TimestampType

    raw_df, phase_df = _simulate_seed_frames(config=config, flight=flight)
    raw_input_sdf = spark.createDataFrame(
        pandas_records_for_spark(raw_df),
        schema=SIMULATION_RAW_INPUT_SCHEMA(),
    )
    raw_input_path = paths.artifact_path("raw_input")
    raw_input_path.parent.mkdir(parents=True, exist_ok=True)
    raw_input_sdf.write.mode("overwrite").parquet(str(raw_input_path))

    phase_label_records = pandas_records_for_spark(phase_df)
    phase_labels_schema = StructType(
        [
            StructField("tail_id", StringType(), False),
            StructField("flight_id", StringType(), False),
            StructField("step_index", IntegerType(), True),
            StructField("timestamp_utc", TimestampType(), False),
            StructField("phase_label", StringType(), True),
            StructField("date_utc", DateType(), False),
        ]
    )
    phase_labels_sdf = spark.createDataFrame(phase_label_records, schema=phase_labels_schema)
    write_table(
        phase_labels_sdf,
        path=str(paths.artifact_path("phase_labels")),
        mode="overwrite",
        fmt=os.environ["S3NTINEL_TABLE_FORMAT"],
        partition_by=["tail_id"],
    )

    hierarchy_label_df = _build_hierarchy_label_df(flight.aircraft_spec)
    hierarchy_label_schema = StructType(
        [
            StructField("parameter_name", StringType(), False),
            StructField("system_id", StringType(), False),
            StructField("subsystem_id", StringType(), False),
            StructField("module_id", StringType(), False),
        ]
    )
    hierarchy_label_sdf = spark.createDataFrame(
        pandas_records_for_spark(hierarchy_label_df),
        schema=hierarchy_label_schema,
    )
    write_table(
        hierarchy_label_sdf,
        path=str(paths.artifact_path("hierarchy_sensor_map_label")),
        mode="overwrite",
        fmt=os.environ["S3NTINEL_TABLE_FORMAT"],
    )
    coupling_misbehavior_df = _build_coupling_misbehavior_windows_df(
        flight=flight,
        dt_seconds=config.dt_seconds,
    )
    if not coupling_misbehavior_df.empty:
        coupling_misbehavior_schema = StructType(
            [
                StructField("coupling_id", StringType(), False),
                StructField("source_module_id", StringType(), False),
                StructField("source_port_name", StringType(), False),
                StructField("target_module_id", StringType(), False),
                StructField("target_port_name", StringType(), False),
                StructField("relation_type", StringType(), False),
                StructField("start_step", IntegerType(), False),
                StructField("end_step_exclusive", IntegerType(), False),
                StructField("start_timestamp_utc", TimestampType(), False),
                StructField("end_timestamp_utc_exclusive", TimestampType(), False),
                StructField("misbehavior_window_id", StringType(), False),
                StructField("misbehavior_family_label", StringType(), True),
                StructField("misbehavior_detail_label", StringType(), True),
                StructField("fault_window_id", StringType(), True),
                StructField("fault_family_label", StringType(), True),
            ]
        )
        coupling_misbehavior_sdf = spark.createDataFrame(
            pandas_records_for_spark(coupling_misbehavior_df),
            schema=coupling_misbehavior_schema,
        )
        write_table(
            coupling_misbehavior_sdf,
            path=str(paths.artifact_path("coupling_misbehavior_windows")),
            mode="overwrite",
            fmt=os.environ["S3NTINEL_TABLE_FORMAT"],
        )
    return {
        "raw_input_rows": int(len(raw_df)),
        "phase_label_rows": int(len(phase_df)),
        "hierarchy_label_rows": int(len(hierarchy_label_df)),
        "coupling_misbehavior_window_rows": int(len(coupling_misbehavior_df)),
    }


def _summarize_artifacts(paths: RunPaths) -> dict[str, dict[str, Any]]:
    return {
        name: {"path": str(path), "exists": path.exists()}
        for name, path in paths.artifact_paths().items()
    }


def _run_mode(config: PipelineRunConfig) -> tuple[str, str, list[str], str]:
    plan = MODE_PLAN_BY_NAME[config.mode]
    return (
        str(plan["run_name"]),
        str(plan["pipeline_mode"]),
        list(plan["stage_scripts"]),
        str(plan["summary_artifact_path"]),
    )


def _write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")


def _build_manifest(
    *,
    paths: RunPaths,
    config: PipelineRunConfig,
    status: str,
    error_message: str | None,
    start_utc: datetime,
    end_utc: datetime,
    elapsed_ms: float,
    seed_counts: dict[str, int],
) -> dict[str, Any]:
    spark_runtime = describe_spark_runtime_config()
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
        "pipeline": asdict(config),
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
        },
    }


def _read_optional_table_sdf(
    *,
    spark: Any,
    path: Path,
    fmt: str,
    columns: tuple[str, ...] | list[str] | None = None,
) -> Any | None:
    if not path.exists():
        return None
    df = read_table(spark, str(path), fmt=fmt)
    if columns:
        selected = [str(column) for column in columns if str(column) in df.columns]
        if selected:
            df = df.select(*selected)
    return df


def _collect_records(df: Any | None, *, order_by: tuple[str, ...] | list[str] = ()) -> list[dict[str, Any]]:
    if df is None:
        return []
    ordered_columns = [str(column) for column in order_by if str(column) in df.columns]
    if ordered_columns:
        df = df.orderBy(*ordered_columns)
    return [row.asDict(recursive=True) for row in df.collect()]


def _collect_dataframe(
    df: Any | None,
    *,
    columns: tuple[str, ...] | list[str] | None = None,
    order_by: tuple[str, ...] | list[str] = (),
) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame(columns=list(columns or ()))
    if columns:
        selected = [str(column) for column in columns if str(column) in df.columns]
        if selected:
            df = df.select(*selected)
    records = _collect_records(df, order_by=order_by)
    if records:
        return pd.DataFrame.from_records(records)
    return pd.DataFrame(columns=list(columns or ()))


def _load_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _path_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return int(path.stat().st_size)
    total = 0
    for child in path.rglob("*"):
        if child.is_file():
            total += int(child.stat().st_size)
    return total


def _text_expr(df: Any, primary: str, fallback: str | None = None) -> Any:
    from pyspark.sql import functions as F

    if primary in df.columns:
        return F.trim(F.coalesce(F.col(primary).cast("string"), F.lit("")))
    if fallback and fallback in df.columns:
        return F.trim(F.coalesce(F.col(fallback).cast("string"), F.lit("")))
    return F.lit("")


def _bool_expr(df: Any, primary: str, fallback: str | None = None) -> Any:
    from pyspark.sql import functions as F

    if primary in df.columns:
        return F.coalesce(F.col(primary).cast("boolean"), F.lit(False))
    if fallback and fallback in df.columns:
        return F.coalesce(F.col(fallback).cast("boolean"), F.lit(False))
    return F.lit(False)


def _first_non_empty_agg(column_name: str, alias: str) -> Any:
    from pyspark.sql import functions as F

    value = F.trim(F.coalesce(F.col(column_name).cast("string"), F.lit("")))
    return F.first(F.when(value != "", value), ignorenulls=True).alias(alias)


def _build_label_contract_summary_spark(
    *,
    raw_telemetry_sdf: Any | None,
    events_sdf: Any | None,
) -> dict[str, Any]:
    from pyspark.sql import functions as F

    raw_columns = list(raw_telemetry_sdf.columns) if raw_telemetry_sdf is not None else []
    event_columns = list(events_sdf.columns) if events_sdf is not None else []
    failures: list[str] = []

    try:
        assert_no_banned_columns(raw_columns)
        assert_required_columns(raw_columns, REQUIRED_LABEL_COLUMNS | REQUIRED_PROFILER_VALIDATOR_LABEL_COLUMNS)
    except AssertionError as exc:
        failures.append(str(exc))

    if event_columns:
        try:
            assert_no_bare_detector_event_type(event_columns)
            assert_required_columns(event_columns, REQUIRED_DETECTED_COLUMNS)
        except AssertionError as exc:
            failures.append(str(exc))

    def _non_empty_count(df: Any | None, column: str) -> int:
        if df is None or column not in df.columns:
            return 0
        if column == "anomaly_score_label":
            row = df.agg(F.sum(F.when(F.col(column).isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias("count")).first()
        else:
            text_value = F.trim(F.coalesce(F.col(column).cast("string"), F.lit("")))
            row = df.agg(F.sum(F.when(text_value != "", F.lit(1)).otherwise(F.lit(0))).alias("count")).first()
        return int(row["count"] or 0)

    return {
        "status": "ok" if not failures else "failed",
        "failures": failures,
        "raw_telemetry_columns": raw_columns,
        "events_columns": event_columns,
        "raw_label_non_null_counts": {
            "event_type_label": _non_empty_count(raw_telemetry_sdf, "event_type_label"),
            "anomaly_type_label": _non_empty_count(raw_telemetry_sdf, "anomaly_type_label"),
            "anomaly_score_label": _non_empty_count(raw_telemetry_sdf, "anomaly_score_label"),
            "misbehavior_family_label": _non_empty_count(raw_telemetry_sdf, "misbehavior_family_label"),
            "coupling_id_label": _non_empty_count(raw_telemetry_sdf, "coupling_id_label"),
            "unit": _non_empty_count(raw_telemetry_sdf, "unit"),
            "rate_hz": _non_empty_count(raw_telemetry_sdf, "rate_hz"),
        },
    }


def _build_profile_validation_summary_spark(
    *,
    raw_telemetry_sdf: Any | None,
    parameter_datatype_profile_sdf: Any | None,
    parameter_behavior_profile_sdf: Any | None,
) -> dict[str, Any]:
    return build_profile_validation_summary(
        raw_telemetry_df=_collect_dataframe(
            raw_telemetry_sdf,
            columns=("parameter_name", "parameter_datatype_label", "behavior_family_label"),
        ),
        parameter_datatype_profile_df=_collect_dataframe(
            parameter_datatype_profile_sdf,
            columns=("parameter_name", "parameter_datatype_profiled"),
        ),
        parameter_behavior_profile_df=_collect_dataframe(
            parameter_behavior_profile_sdf,
            columns=("parameter_name", "behavior_family_profiled"),
        ),
    )


def _build_event_validation_summary_spark(
    *,
    raw_telemetry_sdf: Any | None,
    events_sdf: Any | None,
    tolerance_seconds: float = 0.5,
) -> dict[str, Any]:
    return build_event_validation_summary(
        simulator_rows=_collect_records(
            raw_telemetry_sdf.select("tail_id", "flight_id", "parameter_name", "timestamp_utc", "event_type_label")
            if raw_telemetry_sdf is not None
            and {"tail_id", "flight_id", "parameter_name", "timestamp_utc", "event_type_label"}.issubset(set(raw_telemetry_sdf.columns))
            else raw_telemetry_sdf,
            order_by=("tail_id", "flight_id", "parameter_name", "timestamp_utc"),
        ),
        detected_events=_collect_records(
            events_sdf.select("tail_id", "flight_id", "parameter_name", "timestamp_utc", "event_type_detected")
            if events_sdf is not None
            and {"tail_id", "flight_id", "parameter_name", "timestamp_utc", "event_type_detected"}.issubset(set(events_sdf.columns))
            else events_sdf,
            order_by=("tail_id", "flight_id", "parameter_name", "timestamp_utc"),
        ),
        tolerance_seconds=tolerance_seconds,
    )


def _build_phase_validation_summary_spark(
    *,
    phase_windows_sdf: Any | None,
    phase_labels_sdf: Any | None,
    windows_sdf: Any | None = None,
) -> dict[str, Any]:
    return validate_detected_phases_from_tables(
        phase_windows_df=_collect_dataframe(
            phase_windows_sdf,
            columns=(
                "tail_id",
                "flight_id",
                "win_id",
                "t_start",
                "t_end",
                "phase_id_detected",
                "phase_state_detected",
                "phase_confidence_detected",
                "distance_to_centroid_detected",
            ),
            order_by=("tail_id", "flight_id", "win_id"),
        ),
        phase_labels_df=_collect_dataframe(
            phase_labels_sdf,
            columns=("tail_id", "flight_id", "timestamp_utc", "phase_label"),
            order_by=("tail_id", "flight_id", "timestamp_utc"),
        ),
        windows_df=_collect_dataframe(
            windows_sdf,
            columns=("tail_id", "flight_id", "win_id", "t_start", "t_end"),
            order_by=("tail_id", "flight_id", "win_id"),
        ),
    )

def _build_graph_validation_summary_spark(
    *,
    hierarchy_sensor_map_sdf: Any | None,
    hierarchy_label_sdf: Any | None,
    lag_graph_sdf: Any | None = None,
    fused_graph_sdf: Any | None = None,
    expected_lag_edges: tuple[dict[str, str], ...] = (),
    expected_fused_edges: tuple[dict[str, str], ...] = (),
) -> dict[str, Any]:
    return build_graph_validation_summary(
        hierarchy_sensor_map_df=_collect_dataframe(
            hierarchy_sensor_map_sdf,
            columns=("parameter_name", "system_id", "subsystem_id", "module_id"),
            order_by=("parameter_name",),
        ),
        hierarchy_label_df=_collect_dataframe(
            hierarchy_label_sdf,
            columns=("parameter_name", "system_id", "subsystem_id", "module_id"),
            order_by=("parameter_name",),
        ),
        lag_graph_df=_collect_dataframe(
            lag_graph_sdf,
            columns=("parameter_name_u", "parameter_name_v", "lag_weight"),
            order_by=("parameter_name_u", "parameter_name_v"),
        ),
        fused_graph_df=_collect_dataframe(
            fused_graph_sdf,
            columns=("parameter_name_u", "parameter_name_v", "fused_weight"),
            order_by=("parameter_name_u", "parameter_name_v"),
        ),
        expected_lag_edges=expected_lag_edges,
        expected_fused_edges=expected_fused_edges,
    )


def _build_coupling_validation_summary_spark(
    *,
    coupling_misbehavior_windows_sdf: Any | None,
    lag_graph_sdf: Any | None = None,
    precision_graph_sdf: Any | None = None,
    fused_graph_sdf: Any | None = None,
    expected_coupling_signatures: tuple[dict[str, Any], ...] = (),
) -> dict[str, Any]:
    coupling_truth_pdf = pd.DataFrame.from_records(
        _collect_records(
            coupling_misbehavior_windows_sdf,
            order_by=("coupling_id", "start_step", "misbehavior_window_id"),
        )
    )
    lag_pdf = pd.DataFrame.from_records(
        _collect_records(
            lag_graph_sdf.select("parameter_name_u", "parameter_name_v", "lag_weight", "mean_lag_seconds")
            if lag_graph_sdf is not None and {"parameter_name_u", "parameter_name_v", "lag_weight", "mean_lag_seconds"}.issubset(set(lag_graph_sdf.columns))
            else None,
            order_by=("parameter_name_u", "parameter_name_v"),
        )
    )
    precision_pdf = pd.DataFrame.from_records(
        _collect_records(
            precision_graph_sdf.select("parameter_name_u", "parameter_name_v", "partial_corr", "precision_weight")
            if precision_graph_sdf is not None and {"parameter_name_u", "parameter_name_v", "partial_corr", "precision_weight"}.issubset(set(precision_graph_sdf.columns))
            else None,
            order_by=("parameter_name_u", "parameter_name_v"),
        )
    )
    fused_pdf = pd.DataFrame.from_records(
        _collect_records(
            fused_graph_sdf.select("parameter_name_u", "parameter_name_v", "fused_weight")
            if fused_graph_sdf is not None and {"parameter_name_u", "parameter_name_v", "fused_weight"}.issubset(set(fused_graph_sdf.columns))
            else None,
            order_by=("parameter_name_u", "parameter_name_v"),
        )
    )
    return build_coupling_validation_summary(
        coupling_truth_df=coupling_truth_pdf,
        lag_graph_df=lag_pdf,
        precision_graph_df=precision_pdf,
        fused_graph_df=fused_pdf,
        expected_coupling_signatures=expected_coupling_signatures,
    )


def _build_misbehavior_score_summary_spark(
    *,
    raw_telemetry_sdf: Any | None,
    windows_sdf: Any | None,
    calibrated_scores_sdf: Any | None,
) -> dict[str, Any]:
    return validate_scores_against_misbehavior_windows(
        raw_telemetry_df=_collect_dataframe(raw_telemetry_sdf, order_by=("tail_id", "flight_id", "timestamp_utc")),
        windows_df=_collect_dataframe(
            windows_sdf,
            columns=("tail_id", "flight_id", "win_id", "t_start", "t_end", "date_utc"),
            order_by=("tail_id", "flight_id", "win_id"),
        ),
        calibrated_scores_df=_collect_dataframe(
            calibrated_scores_sdf,
            columns=("tail_id", "flight_id", "win_id", "date_utc", "global_score", "severity", "emit_ready"),
            order_by=("tail_id", "flight_id", "win_id"),
        ),
    )


def _build_misbehavior_window_summary_from_score_summary(summary: dict[str, Any]) -> dict[str, Any]:
    if summary.get("status") != "ok":
        return summary
    return {
        "status": "ok",
        "misbehavior_window_count": int(summary.get("misbehavior_window_count", 0)),
        "detected_misbehavior_window_count": int(summary.get("detected_misbehavior_window_count", 0)),
        "emit_ready_misbehavior_window_count": int(summary.get("emit_ready_misbehavior_window_count", 0)),
        "misbehavior_windows": summary.get("misbehavior_windows", []),
    }


def _build_fault_score_summary_from_misbehavior(summary: dict[str, Any]) -> dict[str, Any]:
    if summary.get("status") != "ok":
        return summary
    return {
        "status": "ok",
        "fault_window_count": int(summary.get("misbehavior_window_count", 0)),
        "detected_fault_window_count": int(summary.get("detected_misbehavior_window_count", 0)),
        "emit_ready_fault_window_count": int(summary.get("emit_ready_misbehavior_window_count", 0)),
        "detected_fault_window_rate": summary.get("detected_misbehavior_window_rate"),
        "emit_ready_fault_window_rate": summary.get("emit_ready_misbehavior_window_rate"),
        "median_fault_window_score": summary.get("median_misbehavior_window_score"),
        "median_detection_latency_seconds": summary.get("median_detection_latency_seconds"),
        "median_emit_ready_latency_seconds": summary.get("median_emit_ready_latency_seconds"),
        "fault_windows": [
            {
                **row,
                "fault_window_id": row.get("fault_window_id", row.get("misbehavior_window_id", "")),
                "fault_family_label": row.get("fault_family_label", ""),
                "fault_type": row.get("fault_type", ""),
            }
            for row in summary.get("misbehavior_windows", [])
        ],
    }


def _build_fault_window_summary_from_misbehavior(summary: dict[str, Any]) -> dict[str, Any]:
    if summary.get("status") != "ok":
        return summary
    return {
        "status": "ok",
        "fault_window_count": int(summary.get("misbehavior_window_count", 0)),
        "detected_fault_window_count": int(summary.get("detected_misbehavior_window_count", 0)),
        "emit_ready_fault_window_count": int(summary.get("emit_ready_misbehavior_window_count", 0)),
        "fault_windows": [
            {
                **row,
                "fault_window_id": row.get("fault_window_id", row.get("misbehavior_window_id", "")),
                "fault_family_label": row.get("fault_family_label", ""),
                "fault_type": row.get("fault_type", ""),
            }
            for row in summary.get("misbehavior_windows", [])
        ],
    }


def _build_detected_subsystem_truth_map(
    *,
    hierarchy_sensor_map_rows: list[dict[str, Any]],
    hierarchy_label_rows: list[dict[str, Any]],
) -> tuple[dict[str, str], set[str]]:
    truth_subsystem_by_parameter = {
        str(row.get("parameter_name", "")): str(row.get("subsystem_id", ""))
        for row in hierarchy_label_rows
        if str(row.get("parameter_name", "")) and str(row.get("subsystem_id", ""))
    }
    detected_to_truth: dict[str, str] = {}
    ambiguous: set[str] = set()
    grouped_truths: dict[str, list[str]] = {}
    for row in hierarchy_sensor_map_rows:
        detected_subsystem = str(row.get("subsystem_id", ""))
        truth_subsystem = truth_subsystem_by_parameter.get(str(row.get("parameter_name", "")))
        if detected_subsystem and truth_subsystem:
            grouped_truths.setdefault(detected_subsystem, []).append(truth_subsystem)
    for detected_subsystem, truths in grouped_truths.items():
        counts = Counter(truths)
        ranked = counts.most_common()
        if not ranked:
            continue
        top_truth_subsystem = str(ranked[0][0])
        top_count = int(ranked[0][1])
        second_count = int(ranked[1][1]) if len(ranked) > 1 else -1
        if top_count > second_count:
            detected_to_truth[detected_subsystem] = top_truth_subsystem
        else:
            ambiguous.add(detected_subsystem)
    return detected_to_truth, ambiguous


def _resolve_detected_subsystem(
    detected_subsystem_id: str,
    *,
    detected_to_truth: dict[str, str],
    ambiguous_detected_subsystems: set[str],
) -> tuple[str | None, bool]:
    detected = str(detected_subsystem_id or "")
    if not detected or detected in ambiguous_detected_subsystems:
        return None, False
    return detected_to_truth.get(detected, detected), True


def _build_misbehavior_attribution_summary_spark(
    *,
    raw_telemetry_sdf: Any | None,
    windows_sdf: Any | None,
    anomaly_window_sdf: Any | None,
    anomaly_telemetry_sdf: Any | None,
    anomaly_event_sdf: Any | None,
    hierarchy_sensor_map_sdf: Any | None = None,
    hierarchy_label_sdf: Any | None = None,
) -> dict[str, Any]:
    return validate_attribution_against_misbehavior_truth(
        raw_telemetry_df=_collect_dataframe(raw_telemetry_sdf, order_by=("tail_id", "flight_id", "timestamp_utc")),
        windows_df=_collect_dataframe(
            windows_sdf,
            columns=("tail_id", "flight_id", "win_id", "t_start", "t_end"),
            order_by=("tail_id", "flight_id", "win_id"),
        ),
        anomaly_window_attribution_df=_collect_dataframe(
            anomaly_window_sdf,
            columns=("tail_id", "flight_id", "win_id", "dominant_subsystem_id"),
            order_by=("tail_id", "flight_id", "win_id"),
        ),
        anomaly_telemetry_attribution_df=_collect_dataframe(
            anomaly_telemetry_sdf,
            columns=("tail_id", "flight_id", "win_id", "parameter_name"),
            order_by=("tail_id", "flight_id", "win_id"),
        ),
        anomaly_event_attribution_df=_collect_dataframe(
            anomaly_event_sdf,
            columns=("tail_id", "flight_id", "win_id", "parameter_name"),
            order_by=("tail_id", "flight_id", "win_id"),
        ),
        hierarchy_sensor_map_df=_collect_dataframe(
            hierarchy_sensor_map_sdf,
            columns=("parameter_name", "subsystem_id"),
            order_by=("parameter_name",),
        ),
        hierarchy_label_df=_collect_dataframe(
            hierarchy_label_sdf,
            columns=("parameter_name", "subsystem_id"),
            order_by=("parameter_name",),
        ),
    )


def _build_fault_attribution_summary_from_misbehavior(summary: dict[str, Any]) -> dict[str, Any]:
    if summary.get("status") != "ok":
        return summary
    return {
        "status": "ok",
        "fault_window_count": int(summary.get("misbehavior_window_count", 0)),
        "dominant_subsystem_match_count": int(summary.get("dominant_subsystem_match_count", 0)),
        "dominant_subsystem_mappable_count": int(summary.get("dominant_subsystem_mappable_count", 0)),
        "dominant_subsystem_match_rate": summary.get("dominant_subsystem_match_rate"),
        "dominant_subsystem_mappable_rate": summary.get("dominant_subsystem_mappable_rate"),
        "telemetry_parameter_match_count": int(summary.get("telemetry_parameter_match_count", 0)),
        "event_parameter_match_count": int(summary.get("event_parameter_match_count", 0)),
        "telemetry_parameter_match_rate": summary.get("telemetry_parameter_match_rate"),
        "event_parameter_match_rate": summary.get("event_parameter_match_rate"),
        "telemetry_truth_subsystem_present_rate": summary.get("telemetry_truth_subsystem_present_rate"),
        "event_truth_subsystem_present_rate": summary.get("event_truth_subsystem_present_rate"),
        "fault_windows": [
            {
                **row,
                "fault_window_id": row.get("fault_window_id", row.get("misbehavior_window_id", "")),
                "fault_family_label": row.get("fault_family_label", ""),
                "fault_type": row.get("fault_type", ""),
            }
            for row in summary.get("misbehavior_windows", [])
        ],
    }


def _write_validation_reports(
    *,
    spark: Any,
    paths: RunPaths,
    flight: FlightSpec,
    table_format: str,
) -> dict[str, Any]:
    raw_telemetry_sdf = _read_optional_table_sdf(
        spark=spark,
        path=paths.artifact_path("raw_telemetry"),
        fmt=table_format,
        columns=(
            "tail_id",
            "flight_id",
            "timestamp_utc",
            "parameter_name",
            "system_id",
            "subsystem_id",
            "module_id",
            "behavior_family_label",
            "parameter_datatype_label",
            "misbehavior_active",
            "misbehavior_applied",
            "misbehavior_family_label",
            "misbehavior_detail_label",
            "misbehavior_window_id",
            "event_type_label",
            "anomaly_type_label",
            "anomaly_score_label",
            "fault_active",
            "fault_applied",
            "fault_family_label",
            "fault_type",
            "fault_window_id",
        ),
    )
    parameter_datatype_profile_sdf = _read_optional_table_sdf(
        spark=spark,
        path=paths.artifact_path("parameter_datatype_profile"),
        fmt=table_format,
        columns=("parameter_name", "parameter_datatype_profiled"),
    )
    parameter_behavior_profile_sdf = _read_optional_table_sdf(
        spark=spark,
        path=paths.artifact_path("parameter_behavior_profile"),
        fmt=table_format,
        columns=("parameter_name", "behavior_family_profiled"),
    )
    events_sdf = _read_optional_table_sdf(
        spark=spark,
        path=paths.artifact_path("events"),
        fmt=table_format,
        columns=(
            "tail_id",
            "flight_id",
            "parameter_name",
            "timestamp_utc",
            "event_type_detected",
            "anomaly_type_detected",
            "anomaly_score_detected",
            "payload",
            "date_utc",
        ),
    )
    phase_labels_sdf = _read_optional_table_sdf(
        spark=spark,
        path=paths.artifact_path("phase_labels"),
        fmt=table_format,
        columns=("tail_id", "flight_id", "timestamp_utc", "phase_label"),
    )
    phase_windows_sdf = _read_optional_table_sdf(
        spark=spark,
        path=paths.artifact_path("phase_windows"),
        fmt=table_format,
        columns=(
            "tail_id",
            "flight_id",
            "win_id",
            "t_start",
            "t_end",
            "phase_id_detected",
            "phase_state_detected",
            "phase_confidence_detected",
            "distance_to_centroid_detected",
        ),
    )
    hierarchy_sensor_map_sdf = _read_optional_table_sdf(
        spark=spark,
        path=paths.artifact_path("hierarchy_sensor_map"),
        fmt=table_format,
        columns=("parameter_name", "system_id", "subsystem_id", "module_id"),
    )
    hierarchy_label_sdf = _read_optional_table_sdf(
        spark=spark,
        path=paths.artifact_path("hierarchy_sensor_map_label"),
        fmt=table_format,
        columns=("parameter_name", "system_id", "subsystem_id", "module_id"),
    )
    coupling_misbehavior_windows_sdf = _read_optional_table_sdf(
        spark=spark,
        path=paths.artifact_path("coupling_misbehavior_windows"),
        fmt=table_format,
        columns=(
            "coupling_id",
            "start_step",
            "end_step_exclusive",
            "misbehavior_window_id",
            "misbehavior_family_label",
            "misbehavior_detail_label",
            "fault_window_id",
            "fault_family_label",
        ),
    )
    lag_graph_sdf = _read_optional_table_sdf(
        spark=spark,
        path=paths.artifact_path("lag_graph"),
        fmt=table_format,
        columns=("parameter_name_u", "parameter_name_v", "lag_weight", "mean_lag_seconds"),
    )
    precision_graph_sdf = _read_optional_table_sdf(
        spark=spark,
        path=paths.artifact_path("precision_graph"),
        fmt=table_format,
        columns=("parameter_name_u", "parameter_name_v", "partial_corr", "precision_weight"),
    )
    fused_graph_sdf = _read_optional_table_sdf(
        spark=spark,
        path=paths.artifact_path("fused_graph"),
        fmt=table_format,
        columns=("parameter_name_u", "parameter_name_v", "fused_weight"),
    )
    windows_sdf = _read_optional_table_sdf(
        spark=spark,
        path=paths.artifact_path("windows"),
        fmt=table_format,
        columns=("tail_id", "flight_id", "win_id", "t_start", "t_end", "date_utc"),
    )
    calibrated_scores_sdf = _read_optional_table_sdf(
        spark=spark,
        path=paths.artifact_path("window_scores_calibrated"),
        fmt=table_format,
        columns=("tail_id", "flight_id", "win_id", "date_utc", "global_score", "severity", "emit_ready"),
    )
    anomaly_window_sdf = _read_optional_table_sdf(
        spark=spark,
        path=paths.artifact_path("anomaly_window_attribution"),
        fmt=table_format,
        columns=("tail_id", "flight_id", "win_id", "dominant_subsystem_id"),
    )
    anomaly_telemetry_sdf = _read_optional_table_sdf(
        spark=spark,
        path=paths.artifact_path("anomaly_telemetry_attribution"),
        fmt=table_format,
        columns=("tail_id", "flight_id", "win_id", "parameter_name"),
    )
    anomaly_event_sdf = _read_optional_table_sdf(
        spark=spark,
        path=paths.artifact_path("anomaly_event_attribution"),
        fmt=table_format,
        columns=("tail_id", "flight_id", "win_id", "parameter_name"),
    )

    validation_expectations = dict(flight.metadata.get("validation", {}) or {})
    profile_summary = _build_profile_validation_summary_spark(
        raw_telemetry_sdf=raw_telemetry_sdf,
        parameter_datatype_profile_sdf=parameter_datatype_profile_sdf,
        parameter_behavior_profile_sdf=parameter_behavior_profile_sdf,
    )
    event_summary = _build_event_validation_summary_spark(
        raw_telemetry_sdf=raw_telemetry_sdf,
        events_sdf=events_sdf,
    )
    label_contract_summary = _build_label_contract_summary_spark(
        raw_telemetry_sdf=raw_telemetry_sdf,
        events_sdf=events_sdf,
    )
    phase_summary = _build_phase_validation_summary_spark(
        phase_windows_sdf=phase_windows_sdf,
        phase_labels_sdf=phase_labels_sdf,
        windows_sdf=windows_sdf,
    )
    hierarchy_summary = _build_graph_validation_summary_spark(
        hierarchy_sensor_map_sdf=hierarchy_sensor_map_sdf,
        hierarchy_label_sdf=hierarchy_label_sdf,
        lag_graph_sdf=lag_graph_sdf,
        fused_graph_sdf=fused_graph_sdf,
        expected_lag_edges=tuple(validation_expectations.get("expected_lag_edges", ()) or ()),
        expected_fused_edges=tuple(validation_expectations.get("expected_fused_edges", ()) or ()),
    )
    coupling_summary = _build_coupling_validation_summary_spark(
        coupling_misbehavior_windows_sdf=coupling_misbehavior_windows_sdf,
        lag_graph_sdf=lag_graph_sdf,
        precision_graph_sdf=precision_graph_sdf,
        fused_graph_sdf=fused_graph_sdf,
        expected_coupling_signatures=tuple(validation_expectations.get("expected_coupling_signatures", ()) or ()),
    )
    misbehavior_score_summary = _build_misbehavior_score_summary_spark(
        raw_telemetry_sdf=raw_telemetry_sdf,
        windows_sdf=windows_sdf,
        calibrated_scores_sdf=calibrated_scores_sdf,
    )
    score_summary = _build_fault_score_summary_from_misbehavior(misbehavior_score_summary)
    misbehavior_window_summary = _build_misbehavior_window_summary_from_score_summary(misbehavior_score_summary)
    fault_window_summary = _build_fault_window_summary_from_misbehavior(misbehavior_window_summary)
    misbehavior_attribution_summary = _build_misbehavior_attribution_summary_spark(
        raw_telemetry_sdf=raw_telemetry_sdf,
        windows_sdf=windows_sdf,
        anomaly_window_sdf=anomaly_window_sdf,
        anomaly_telemetry_sdf=anomaly_telemetry_sdf,
        anomaly_event_sdf=anomaly_event_sdf,
        hierarchy_sensor_map_sdf=hierarchy_sensor_map_sdf,
        hierarchy_label_sdf=hierarchy_label_sdf,
    )
    attribution_summary = _build_fault_attribution_summary_from_misbehavior(misbehavior_attribution_summary)

    payloads = {
        "profile_validation_summary.json": profile_summary,
        "event_validation_summary.json": event_summary,
        "label_contract_summary.json": label_contract_summary,
        "phase_validation_summary.json": phase_summary,
        "hierarchy_validation_summary.json": hierarchy_summary,
        "coupling_validation_summary.json": coupling_summary,
        "score_validation_summary.json": score_summary,
        "misbehavior_score_validation_summary.json": misbehavior_score_summary,
        "misbehavior_window_validation_summary.json": misbehavior_window_summary,
        "misbehavior_attribution_validation_summary.json": misbehavior_attribution_summary,
        "fault_window_validation_summary.json": fault_window_summary,
        "attribution_validation_summary.json": attribution_summary,
    }
    for filename, payload in payloads.items():
        _write_manifest(paths.run_dir / "reports" / filename, payload)
    return payloads


def _modeling_sections_by_stage(validation_payloads: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    payloads = validation_payloads or {}
    return {
        "10_parameter_profiles_fit.py": {
            "profile_validation": payloads.get("profile_validation_summary.json"),
        },
        "20_events_extract.py": {
            "event_validation": payloads.get("event_validation_summary.json"),
            "label_contract": payloads.get("label_contract_summary.json"),
        },
        "60_fit_hierarchy.py": {
            "hierarchy_validation": payloads.get("hierarchy_validation_summary.json"),
            "coupling_validation": payloads.get("coupling_validation_summary.json"),
        },
        "70_phase_fit.py": {
            "phase_validation": payloads.get("phase_validation_summary.json"),
        },
        "85_window_scores_calibrate.py": {
            "score_validation": payloads.get("score_validation_summary.json"),
            "misbehavior_score_validation": payloads.get("misbehavior_score_validation_summary.json"),
            "fault_window_validation": payloads.get("fault_window_validation_summary.json"),
            "misbehavior_window_validation": payloads.get("misbehavior_window_validation_summary.json"),
        },
        "90_anomaly_attribution.py": {
            "attribution_validation": payloads.get("attribution_validation_summary.json"),
            "misbehavior_attribution_validation": payloads.get("misbehavior_attribution_validation_summary.json"),
        },
    }


def _build_modeling_performance_summary(validation_payloads: dict[str, Any] | None) -> dict[str, Any]:
    payloads = validation_payloads or {}
    hierarchy = (payloads.get("hierarchy_validation_summary.json") or {}).get("hierarchy", {})
    graph_signatures = (payloads.get("hierarchy_validation_summary.json") or {}).get("graph_signatures", {})
    return {
        "profile_validation": payloads.get("profile_validation_summary.json"),
        "event_validation": payloads.get("event_validation_summary.json"),
        "label_contract": payloads.get("label_contract_summary.json"),
        "phase_validation": payloads.get("phase_validation_summary.json"),
        "hierarchy_validation": hierarchy,
        "graph_signatures": graph_signatures,
        "coupling_validation": payloads.get("coupling_validation_summary.json"),
        "score_validation": payloads.get("score_validation_summary.json"),
        "misbehavior_score_validation": payloads.get("misbehavior_score_validation_summary.json"),
        "fault_window_validation": payloads.get("fault_window_validation_summary.json"),
        "misbehavior_window_validation": payloads.get("misbehavior_window_validation_summary.json"),
        "attribution_validation": payloads.get("attribution_validation_summary.json"),
        "misbehavior_attribution_validation": payloads.get("misbehavior_attribution_validation_summary.json"),
    }


def _build_stage_engineering_sections(
    *,
    paths: RunPaths,
    summary_artifact_path: str | None,
    validation_payloads: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    pipeline_summary = (
        _load_json_if_exists(paths.run_dir / summary_artifact_path)
        if summary_artifact_path is not None
        else None
    ) or {}
    total_elapsed_ms = float(pipeline_summary.get("total_elapsed_ms") or 0.0)
    modeling_by_stage = _modeling_sections_by_stage(validation_payloads)
    stage_sections: list[dict[str, Any]] = []
    for stage in pipeline_summary.get("stages", []) or []:
        stage_script = str(stage.get("stage_script", ""))
        stage_id = stage_script.removesuffix(".py")
        stage_summary_path = paths.run_dir / "reports" / "stages" / f"{stage_id}_summary.json"
        stage_manifest_path = paths.run_dir / "reports" / "stages" / f"{stage_id}_manifest.json"
        stage_summary = _load_json_if_exists(stage_summary_path) or {}
        output_paths = {
            str(value)
            for key, value in stage_summary.items()
            if key.endswith("_path") and isinstance(value, str)
        }
        output_artifact_size_bytes = sum(_path_size_bytes(Path(path)) for path in output_paths)
        elapsed_ms = float(stage.get("elapsed_ms") or 0.0)
        stage_sections.append(
            {
                "stage_script": stage_script,
                "status": stage.get("status"),
                "engineering_performance": {
                    "elapsed_ms": elapsed_ms,
                    "elapsed_seconds": (elapsed_ms / 1000.0),
                    "share_of_total_elapsed": (
                        float(elapsed_ms / total_elapsed_ms)
                        if total_elapsed_ms > 0.0
                        else None
                    ),
                    "summary_path": str(stage_summary_path),
                    "manifest_path": str(stage_manifest_path),
                    "stage_summary": stage_summary,
                    "output_artifact_size_bytes": output_artifact_size_bytes,
                },
                "modeling_performance": modeling_by_stage.get(stage_script, {}),
            }
        )
    return stage_sections


def _build_scale_signature(
    *,
    manifest: dict[str, Any],
    validation_payloads: dict[str, Any] | None,
    stage_sections: list[dict[str, Any]],
) -> dict[str, Any]:
    payloads = validation_payloads or {}
    stage_by_script = {section["stage_script"]: section for section in stage_sections}
    graph_stage = stage_by_script.get("50_build_graph.py", {})
    graph_stage_summary = ((graph_stage.get("engineering_performance") or {}).get("stage_summary") or {})
    phase_summary = payloads.get("phase_validation_summary.json") or {}
    score_summary = payloads.get("score_validation_summary.json") or {}
    event_summary = payloads.get("event_validation_summary.json") or {}
    profile_summary = payloads.get("profile_validation_summary.json") or {}
    return {
        "seed_counts": dict(manifest.get("seed_counts", {}) or {}),
        "validation_counts": {
            "labeled_event_count": event_summary.get("label_event_count"),
            "detected_event_count": event_summary.get("detected_event_count"),
            "parameter_count": profile_summary.get("parameter_count"),
            "phase_assignment_count": phase_summary.get("assignment_count"),
            "fault_window_count": score_summary.get("fault_window_count"),
        },
        "graph_counts": {
            "lag_edge_count": graph_stage_summary.get("lag_edge_count"),
            "event_edge_count": graph_stage_summary.get("event_edge_count"),
            "transition_edge_count": graph_stage_summary.get("transition_edge_count"),
            "fused_edge_count": graph_stage_summary.get("fused_edge_count"),
            "graph_parameter_universe_count": graph_stage_summary.get("graph_parameter_universe_count"),
        },
        "current_scale_visibility": {
            "size_proxies_present_in_run": True,
            "variant_benchmarking_script": "scripts/profile_pipeline_performance.py",
            "dataset_size_sweep_available": False,
            "recommendation": "set up an explicit scale-sweep experiment; current tooling compares tuning variants on a fixed workload and only exposes size proxies within single runs",
        },
    }


def _build_engineering_performance_summary(
    *,
    paths: RunPaths,
    manifest: dict[str, Any],
    summary_artifact_path: str | None,
    validation_payloads: dict[str, Any] | None,
) -> dict[str, Any]:
    pipeline_summary = (
        _load_json_if_exists(paths.run_dir / summary_artifact_path)
        if summary_artifact_path is not None
        else None
    ) or {}
    stage_sections = _build_stage_engineering_sections(
        paths=paths,
        summary_artifact_path=summary_artifact_path,
        validation_payloads=validation_payloads,
    )
    artifact_sizes = {
        name: _path_size_bytes(Path(payload.get("path", "")))
        for name, payload in (manifest.get("artifacts", {}) or {}).items()
        if isinstance(payload, dict) and payload.get("exists")
    }
    return {
        "overall": {
            "pipeline_summary": pipeline_summary,
            "manifest_timing": manifest.get("timing"),
            "environment": manifest.get("environment"),
            "memory_snapshot_end": pipeline_summary.get("memory_snapshot_end"),
            "artifact_disk_bytes_total": int(sum(artifact_sizes.values())),
            "artifact_disk_bytes_by_name": artifact_sizes,
        },
        "stages": stage_sections,
        "scale_signature": _build_scale_signature(
            manifest=manifest,
            validation_payloads=validation_payloads,
            stage_sections=stage_sections,
        ),
    }


def _render_full_run_report_markdown(report: dict[str, Any]) -> str:
    engineering = report.get("engineering_performance", {})
    overall = engineering.get("overall", {})
    pipeline_summary = overall.get("pipeline_summary", {})
    lines = [
        "# Full Run Report",
        "",
        "## Modeling Performance",
    ]
    modeling = report.get("modeling_performance", {})
    for key in (
        "profile_validation",
        "event_validation",
        "phase_validation",
        "hierarchy_validation",
        "coupling_validation",
        "score_validation",
        "attribution_validation",
    ):
        payload = modeling.get(key)
        if payload is None:
            continue
        lines.append(f"### {key}")
        lines.append("```json")
        lines.append(json.dumps(payload, indent=2, sort_keys=True, default=str))
        lines.append("```")
    lines.extend(
        [
            "",
            "## Engineering Performance",
            "",
            "### Overall",
            "```json",
            json.dumps(
                {
                    "status": report.get("status"),
                    "total_elapsed_ms": pipeline_summary.get("total_elapsed_ms"),
                    "stage_count": pipeline_summary.get("stage_count"),
                    "artifact_disk_bytes_total": overall.get("artifact_disk_bytes_total"),
                    "memory_snapshot_end": overall.get("memory_snapshot_end"),
                    "scale_signature": engineering.get("scale_signature"),
                },
                indent=2,
                sort_keys=True,
                default=str,
            ),
            "```",
            "",
            "### Stages",
        ]
    )
    for stage in engineering.get("stages", []) or []:
        lines.append(f"#### {stage.get('stage_script')}")
        lines.append("```json")
        lines.append(json.dumps(stage, indent=2, sort_keys=True, default=str))
        lines.append("```")
    lines.append("")
    return "\n".join(lines)


def _write_full_run_report(
    *,
    paths: RunPaths,
    manifest: dict[str, Any],
    summary_artifact_path: str | None,
    validation_payloads: dict[str, Any] | None,
) -> dict[str, Any]:
    report = {
        "report_version": "v1",
        "status": manifest.get("status"),
        "run_dir": str(paths.run_dir),
        "modeling_performance": _build_modeling_performance_summary(validation_payloads),
        "engineering_performance": _build_engineering_performance_summary(
            paths=paths,
            manifest=manifest,
            summary_artifact_path=summary_artifact_path,
            validation_payloads=validation_payloads,
        ),
    }
    _write_manifest(paths.run_dir / "reports" / "full_run_report.json", report)
    (paths.run_dir / "reports" / "full_run_report.md").write_text(
        _render_full_run_report_markdown(report),
        encoding="utf-8",
    )
    return report


def run_pipeline(config: PipelineRunConfig) -> PipelineRunResult:
    flight = resolve_flight(config.flight_name)
    config = config.with_flight_defaults(flight=flight)
    paths = RunPaths(run_dir=config.build_run_dir())
    paths.run_dir.mkdir(parents=True, exist_ok=True)
    previous_env = _set_run_env(paths, config)

    logger = None
    run_start = time.perf_counter()
    start_utc = datetime.now(timezone.utc)
    status = "success"
    error_message: str | None = None
    seed_counts: dict[str, int] = {}
    summary_artifact_path: str | None = None
    validation_payloads: dict[str, Any] | None = None

    try:
        with _tee_console(paths.log_path):
            logger = get_logger(LOGGER_NAME)
            spark = get_spark(LOGGER_NAME)
            seed_counts = _write_seed_tables(spark=spark, paths=paths, config=config, flight=flight)
            run_name, pipeline_mode, stage_scripts, summary_artifact_path = _run_mode(config)
            logger.info(
                "sim_run_start flight=%s mode=%s run_dir=%s format=%s",
                config.flight_name,
                config.mode,
                paths.run_dir,
                config.table_format,
            )
            run_stage_group(
                run_name=run_name,
                pipeline_mode=pipeline_mode,
                stage_scripts=stage_scripts,
                summary_artifact_path=summary_artifact_path,
                logger_name=LOGGER_NAME,
            )
            validation_payloads = _write_validation_reports(
                spark=spark,
                paths=paths,
                flight=flight,
                table_format=config.table_format,
            )
            logger.info("sim_run_complete flight=%s mode=%s run_dir=%s", config.flight_name, config.mode, paths.run_dir)
    except Exception as exc:
        status = "failed"
        error_message = f"{exc.__class__.__name__}: {exc}"
        if logger is not None:
            logger.exception("sim_run_failed flight=%s mode=%s", config.flight_name, config.mode)
        raise
    finally:
        end_utc = datetime.now(timezone.utc)
        elapsed_ms = (time.perf_counter() - run_start) * 1000.0
        manifest = _build_manifest(
            paths=paths,
            config=config,
            status=status,
            error_message=error_message,
            start_utc=start_utc,
            end_utc=end_utc,
            elapsed_ms=elapsed_ms,
            seed_counts=seed_counts,
        )
        _write_manifest(paths.manifest_path, manifest)
        _write_full_run_report(
            paths=paths,
            manifest=manifest,
            summary_artifact_path=summary_artifact_path,
            validation_payloads=validation_payloads,
        )
        _restore_env(previous_env)
    return PipelineRunResult(paths=paths, status=status, seed_counts=seed_counts)


def main() -> None:
    run_pipeline(PipelineRunConfig.from_args(parse_args()))


if __name__ == "__main__":
    main()
