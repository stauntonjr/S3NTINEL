"""Run the canonical simulation pipeline into a persisted artifact bundle."""

from __future__ import annotations

import argparse
from collections import Counter
import contextlib
import json
import os
import sys
import time
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from math import comb
from pathlib import Path
from statistics import median
from typing import Any

import pandas as pd

from libs.io.delta import describe_spark_runtime_config, get_spark, read_table, write_table
from libs.io.pandas_spark import pandas_records_for_spark
from libs.io.schemas import SIMULATION_RAW_INPUT_SCHEMA
from libs.perf import get_logger
from libs.graph import build_coupling_validation_summary
from libs.phase import evaluate_detected_phases
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
            "05_parameter_profiles_fit.py",
        ],
        "summary_artifact_path": "reports/profile_pipeline_run_summary.json",
    },
    "structural": {
        "run_name": "s3ntinel.sim_structural",
        "pipeline_mode": "sim_structural:v2",
        "stage_scripts": [
            "00_ingest_raw.py",
            "05_parameter_profiles_fit.py",
            "20_events_extract.py",
            "30_windows_adaptive.py",
            "10_backbone_fit.py",
            "11_build_graph.py",
            "12_fit_hierarchy.py",
        ],
        "summary_artifact_path": "reports/structural_pipeline_run_summary.json",
    },
    "full": {
        "run_name": "s3ntinel.sim_full",
        "pipeline_mode": "sim_full:v2",
        "stage_scripts": [
            "00_ingest_raw.py",
            "05_parameter_profiles_fit.py",
            "20_events_extract.py",
            "30_windows_adaptive.py",
            "10_backbone_fit.py",
            "11_build_graph.py",
            "12_fit_hierarchy.py",
            "50_phase_fit.py",
            "60_window_scores_raw.py",
            "70_window_scores_calibrate.py",
            "80_anomaly_attribution.py",
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
    from pyspark.sql import functions as F

    if raw_telemetry_sdf is None or "parameter_name" not in raw_telemetry_sdf.columns:
        return {
            "status": "ok",
            "parameter_count": 0,
            "datatype_labeled_parameter_count": 0,
            "datatype_profiled_parameter_count": 0,
            "behavior_labeled_parameter_count": 0,
            "behavior_profiled_parameter_count": 0,
        }

    raw_labels = raw_telemetry_sdf.select(
        F.trim(F.coalesce(F.col("parameter_name").cast("string"), F.lit(""))).alias("parameter_name"),
        _text_expr(raw_telemetry_sdf, "parameter_datatype_label").alias("parameter_datatype_label"),
        _text_expr(raw_telemetry_sdf, "behavior_family_label").alias("behavior_family_label"),
    )
    label_df = raw_labels.groupBy("parameter_name").agg(
        _first_non_empty_agg("parameter_datatype_label", "parameter_datatype_label"),
        _first_non_empty_agg("behavior_family_label", "behavior_family_label"),
    )

    merged = label_df
    if parameter_datatype_profile_sdf is not None and {
        "parameter_name",
        "parameter_datatype_profiled",
    }.issubset(set(parameter_datatype_profile_sdf.columns)):
        datatype_profile_df = parameter_datatype_profile_sdf.select(
            F.trim(F.coalesce(F.col("parameter_name").cast("string"), F.lit(""))).alias("parameter_name"),
            _text_expr(parameter_datatype_profile_sdf, "parameter_datatype_profiled").alias("parameter_datatype_profiled"),
        ).dropDuplicates(["parameter_name"])
        merged = merged.join(datatype_profile_df, on="parameter_name", how="left")
    else:
        merged = merged.withColumn("parameter_datatype_profiled", F.lit(""))

    if parameter_behavior_profile_sdf is not None and {
        "parameter_name",
        "behavior_family_profiled",
    }.issubset(set(parameter_behavior_profile_sdf.columns)):
        behavior_profile_df = parameter_behavior_profile_sdf.select(
            F.trim(F.coalesce(F.col("parameter_name").cast("string"), F.lit(""))).alias("parameter_name"),
            _text_expr(parameter_behavior_profile_sdf, "behavior_family_profiled").alias("behavior_family_profiled"),
        ).dropDuplicates(["parameter_name"])
        merged = merged.join(behavior_profile_df, on="parameter_name", how="left")
    else:
        merged = merged.withColumn("behavior_family_profiled", F.lit(""))

    summary_row = merged.agg(
        F.count(F.lit(1)).alias("parameter_count"),
        F.sum(F.when(F.col("parameter_datatype_label") != "", F.lit(1)).otherwise(F.lit(0))).alias(
            "datatype_labeled_parameter_count"
        ),
        F.sum(F.when(F.col("parameter_datatype_profiled") != "", F.lit(1)).otherwise(F.lit(0))).alias(
            "datatype_profiled_parameter_count"
        ),
        F.sum(
            F.when(
                (F.col("parameter_datatype_label") != "")
                & (F.col("parameter_datatype_profiled") != "")
                & (F.col("parameter_datatype_label") == F.col("parameter_datatype_profiled")),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("datatype_exact_match_count"),
        F.sum(F.when(F.col("behavior_family_label") != "", F.lit(1)).otherwise(F.lit(0))).alias(
            "behavior_labeled_parameter_count"
        ),
        F.sum(F.when(F.col("behavior_family_profiled") != "", F.lit(1)).otherwise(F.lit(0))).alias(
            "behavior_profiled_parameter_count"
        ),
        F.sum(
            F.when(
                (F.col("behavior_family_label") != "")
                & (F.col("behavior_family_profiled") != "")
                & (F.col("behavior_family_label") == F.col("behavior_family_profiled")),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("behavior_exact_match_count"),
    ).first()

    datatype_labeled_parameter_count = int(summary_row["datatype_labeled_parameter_count"] or 0)
    behavior_labeled_parameter_count = int(summary_row["behavior_labeled_parameter_count"] or 0)
    datatype_exact_match_count = int(summary_row["datatype_exact_match_count"] or 0)
    behavior_exact_match_count = int(summary_row["behavior_exact_match_count"] or 0)
    return {
        "status": "ok",
        "parameter_count": int(summary_row["parameter_count"] or 0),
        "datatype_labeled_parameter_count": datatype_labeled_parameter_count,
        "datatype_profiled_parameter_count": int(summary_row["datatype_profiled_parameter_count"] or 0),
        "datatype_exact_match_count": datatype_exact_match_count,
        "datatype_accuracy": (
            float(datatype_exact_match_count / datatype_labeled_parameter_count)
            if datatype_labeled_parameter_count > 0
            else None
        ),
        "behavior_labeled_parameter_count": behavior_labeled_parameter_count,
        "behavior_profiled_parameter_count": int(summary_row["behavior_profiled_parameter_count"] or 0),
        "behavior_exact_match_count": behavior_exact_match_count,
        "behavior_accuracy": (
            float(behavior_exact_match_count / behavior_labeled_parameter_count)
            if behavior_labeled_parameter_count > 0
            else None
        ),
    }


def _build_event_validation_summary_spark(
    *,
    raw_telemetry_sdf: Any | None,
    events_sdf: Any | None,
    tolerance_seconds: float = 0.5,
) -> dict[str, Any]:
    from pyspark.sql import functions as F

    if raw_telemetry_sdf is None:
        return {
            "status": "ok",
            "label_event_count": 0,
            "detected_event_count": 0,
            "tp": 0,
            "fp": 0,
            "fn": 0,
            "tn": 0,
            "precision": None,
            "recall": None,
            "tolerance_seconds": float(abs(tolerance_seconds)),
        }

    raw_base = raw_telemetry_sdf.select(
        F.trim(F.coalesce(F.col("tail_id").cast("string"), F.lit(""))).alias("tail_id"),
        F.trim(F.coalesce(F.col("flight_id").cast("string"), F.lit(""))).alias("flight_id"),
        F.trim(F.coalesce(F.col("parameter_name").cast("string"), F.lit(""))).alias("parameter_name"),
        F.col("timestamp_utc"),
        _text_expr(raw_telemetry_sdf, "event_type_label").alias("event_type_label"),
    )
    raw_counts = raw_base.agg(
        F.count(F.lit(1)).alias("raw_row_count"),
        F.sum(F.when(F.col("event_type_label") != "", F.lit(1)).otherwise(F.lit(0))).alias("label_event_count"),
    ).first()

    labels = raw_base.filter(F.col("event_type_label") != "").groupBy(
        "tail_id",
        "flight_id",
        "parameter_name",
        "timestamp_utc",
        "event_type_label",
    ).agg(F.count(F.lit(1)).alias("label_count"))

    if events_sdf is not None and {
        "tail_id",
        "flight_id",
        "parameter_name",
        "timestamp_utc",
        "event_type_detected",
    }.issubset(set(events_sdf.columns)):
        detected = events_sdf.select(
            F.trim(F.coalesce(F.col("tail_id").cast("string"), F.lit(""))).alias("tail_id"),
            F.trim(F.coalesce(F.col("flight_id").cast("string"), F.lit(""))).alias("flight_id"),
            F.trim(F.coalesce(F.col("parameter_name").cast("string"), F.lit(""))).alias("parameter_name"),
            F.col("timestamp_utc"),
            _text_expr(events_sdf, "event_type_detected").alias("event_type_detected"),
        ).filter(F.col("event_type_detected") != "")
        detected_grouped = detected.groupBy(
            "tail_id",
            "flight_id",
            "parameter_name",
            "timestamp_utc",
            "event_type_detected",
        ).agg(F.count(F.lit(1)).alias("detected_count"))
        detected_event_count_row = detected_grouped.agg(F.sum(F.col("detected_count")).alias("detected_event_count")).first()
    else:
        detected_grouped = None
        detected_event_count_row = {"detected_event_count": 0}

    if detected_grouped is None:
        tp = 0
        fp = 0
        fn = int(raw_counts["label_event_count"] or 0)
    else:
        matched = labels.alias("labels").join(
            detected_grouped.alias("detected"),
            on=[
                F.col("labels.tail_id") == F.col("detected.tail_id"),
                F.col("labels.flight_id") == F.col("detected.flight_id"),
                F.col("labels.parameter_name") == F.col("detected.parameter_name"),
                F.col("labels.timestamp_utc") == F.col("detected.timestamp_utc"),
                F.col("labels.event_type_label") == F.col("detected.event_type_detected"),
            ],
            how="full_outer",
        )
        counts_row = matched.agg(
            F.sum(F.least(F.coalesce(F.col("label_count"), F.lit(0)), F.coalesce(F.col("detected_count"), F.lit(0)))).alias(
                "tp"
            ),
            F.sum(
                F.greatest(
                    F.coalesce(F.col("detected_count"), F.lit(0)) - F.coalesce(F.col("label_count"), F.lit(0)),
                    F.lit(0),
                )
            ).alias("fp"),
            F.sum(
                F.greatest(
                    F.coalesce(F.col("label_count"), F.lit(0)) - F.coalesce(F.col("detected_count"), F.lit(0)),
                    F.lit(0),
                )
            ).alias("fn"),
        ).first()
        tp = int(counts_row["tp"] or 0)
        fp = int(counts_row["fp"] or 0)
        fn = int(counts_row["fn"] or 0)

    label_event_count = int(raw_counts["label_event_count"] or 0)
    detected_event_count = int(detected_event_count_row["detected_event_count"] or 0)
    raw_row_count = int(raw_counts["raw_row_count"] or 0)
    tn = max(raw_row_count - label_event_count, 0)
    precision = float(tp / (tp + fp)) if (tp + fp) > 0 else None
    recall = float(tp / (tp + fn)) if (tp + fn) > 0 else None
    return {
        "status": "ok",
        "label_event_count": label_event_count,
        "detected_event_count": detected_event_count,
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "precision": precision,
        "recall": recall,
        "tolerance_seconds": float(abs(tolerance_seconds)),
    }


def _build_phase_validation_summary_spark(
    *,
    phase_windows_sdf: Any | None,
    phase_labels_sdf: Any | None,
    windows_sdf: Any | None = None,
) -> dict[str, Any]:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if phase_windows_sdf is None or phase_labels_sdf is None:
        return {
            "status": "skipped",
            "reason": "no overlapping phase windows and phase labels",
            "assignment_count": 0,
        }

    windows = phase_windows_sdf
    if windows_sdf is not None and {"tail_id", "flight_id", "win_id", "t_start", "t_end"}.issubset(set(windows_sdf.columns)):
        window_times = windows_sdf.select("tail_id", "flight_id", "win_id", "t_start", "t_end")
        windows = windows.drop("t_start", "t_end").join(
            window_times,
            on=["tail_id", "flight_id", "win_id"],
            how="left",
        )

    label_rows = phase_labels_sdf.select(
        F.trim(F.coalesce(F.col("tail_id").cast("string"), F.lit(""))).alias("tail_id"),
        F.trim(F.coalesce(F.col("flight_id").cast("string"), F.lit(""))).alias("flight_id"),
        F.col("timestamp_utc"),
        _text_expr(phase_labels_sdf, "phase_label").alias("phase_label"),
    ).filter(F.col("phase_label") != "")

    joined = windows.alias("w").join(
        label_rows.alias("l"),
        on=(
            (F.col("w.tail_id") == F.col("l.tail_id"))
            & (F.col("w.flight_id") == F.col("l.flight_id"))
            & (F.col("l.timestamp_utc") >= F.col("w.t_start"))
            & (F.col("l.timestamp_utc") <= F.col("w.t_end"))
        ),
        how="inner",
    )
    counts = joined.groupBy(
        F.col("w.tail_id").alias("tail_id"),
        F.col("w.flight_id").alias("flight_id"),
        F.col("w.win_id").alias("win_id"),
        F.col("w.phase_id_detected").alias("phase_id_detected"),
        F.col("w.phase_state_detected").alias("phase_state_detected"),
        F.col("w.phase_confidence_detected").alias("phase_confidence_detected"),
        F.col("w.distance_to_centroid_detected").alias("distance_to_centroid_detected"),
        F.col("l.phase_label").alias("phase_label"),
    ).agg(F.count(F.lit(1)).alias("label_count"))

    ranking = Window.partitionBy("tail_id", "flight_id", "win_id").orderBy(
        F.desc("label_count"),
        F.asc("phase_label"),
    )
    assignments = _collect_records(
        counts.withColumn("rank", F.row_number().over(ranking))
        .filter(F.col("rank") == 1)
        .drop("rank", "label_count"),
        order_by=("tail_id", "flight_id", "win_id"),
    )
    if not assignments:
        return {
            "status": "skipped",
            "reason": "no overlapping phase windows and phase labels",
            "assignment_count": 0,
        }
    summary = evaluate_detected_phases(assignments)
    summary["status"] = "ok"
    summary["assignment_count"] = len(assignments)
    return summary


def _pairwise_partition_metrics_records(
    records: list[dict[str, Any]],
    *,
    detected_key: str,
    truth_key: str,
) -> dict[str, Any]:
    if not records:
        return {
            "same_cluster_pair_precision": None,
            "same_cluster_pair_recall": None,
            "same_cluster_pair_f1": None,
            "true_positive_pair_count": 0,
            "same_detected_pair_count": 0,
            "same_truth_pair_count": 0,
        }

    detected_counts = Counter(str(row.get(detected_key, "")) for row in records)
    truth_counts = Counter(str(row.get(truth_key, "")) for row in records)
    pair_counts = Counter((str(row.get(detected_key, "")), str(row.get(truth_key, ""))) for row in records)
    same_detected = int(sum(comb(count, 2) for count in detected_counts.values() if count >= 2))
    same_truth = int(sum(comb(count, 2) for count in truth_counts.values() if count >= 2))
    tp = int(sum(comb(count, 2) for count in pair_counts.values() if count >= 2))
    precision = float(tp / same_detected) if same_detected else None
    recall = float(tp / same_truth) if same_truth else None
    if precision is None or recall is None or (precision + recall) <= 0.0:
        f1 = None
    else:
        f1 = float((2.0 * precision * recall) / (precision + recall))
    return {
        "same_cluster_pair_precision": precision,
        "same_cluster_pair_recall": recall,
        "same_cluster_pair_f1": f1,
        "true_positive_pair_count": tp,
        "same_detected_pair_count": same_detected,
        "same_truth_pair_count": same_truth,
    }


def _validate_hierarchy_recovery_records(
    *,
    hierarchy_sensor_map_rows: list[dict[str, Any]],
    hierarchy_label_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    if not hierarchy_sensor_map_rows or not hierarchy_label_rows:
        return {
            "status": "skipped",
            "reason": "missing hierarchy_sensor_map or hierarchy_label rows",
            "sensor_count": 0,
        }

    label_by_parameter = {
        str(row.get("parameter_name", "")): row
        for row in hierarchy_label_rows
        if str(row.get("parameter_name", ""))
    }
    joined: list[dict[str, Any]] = []
    for detected in hierarchy_sensor_map_rows:
        parameter_name = str(detected.get("parameter_name", ""))
        truth = label_by_parameter.get(parameter_name)
        if truth is None:
            continue
        joined.append(
            {
                "parameter_name": parameter_name,
                "system_id_detected": str(detected.get("system_id", "")),
                "subsystem_id_detected": str(detected.get("subsystem_id", "")),
                "module_id_detected": str(detected.get("module_id", "")),
                "system_id_truth": str(truth.get("system_id", "")),
                "subsystem_id_truth": str(truth.get("subsystem_id", "")),
                "module_id_truth": str(truth.get("module_id", "")),
            }
        )

    sensor_count = len(joined)
    if sensor_count == 0:
        return {
            "status": "skipped",
            "reason": "no overlapping parameter_name rows between detected hierarchy and labels",
            "sensor_count": 0,
        }

    detected_systems = {row["system_id_detected"] for row in joined}
    detected_subsystems = {row["subsystem_id_detected"] for row in joined}
    detected_modules = {row["module_id_detected"] for row in joined}
    truth_systems = {str(row.get("system_id", "")) for row in hierarchy_label_rows}
    truth_subsystems = {str(row.get("subsystem_id", "")) for row in hierarchy_label_rows}
    truth_modules = {str(row.get("module_id", "")) for row in hierarchy_label_rows}
    return {
        "status": "ok",
        "sensor_count": sensor_count,
        "system_exact_match": float(
            sum(1 for row in joined if row["system_id_detected"] == row["system_id_truth"]) / sensor_count
        ),
        "subsystem_exact_match": float(
            sum(1 for row in joined if row["subsystem_id_detected"] == row["subsystem_id_truth"]) / sensor_count
        ),
        "module_exact_match": float(
            sum(1 for row in joined if row["module_id_detected"] == row["module_id_truth"]) / sensor_count
        ),
        "system_partition": _pairwise_partition_metrics_records(
            joined,
            detected_key="system_id_detected",
            truth_key="system_id_truth",
        ),
        "subsystem_partition": _pairwise_partition_metrics_records(
            joined,
            detected_key="subsystem_id_detected",
            truth_key="subsystem_id_truth",
        ),
        "module_partition": _pairwise_partition_metrics_records(
            joined,
            detected_key="module_id_detected",
            truth_key="module_id_truth",
        ),
        "truth_system_count": len(truth_systems),
        "truth_subsystem_count": len(truth_subsystems),
        "truth_module_count": len(truth_modules),
        "detected_system_count": len(detected_systems),
        "detected_subsystem_count": len(detected_subsystems),
        "detected_module_count": len(detected_modules),
        "detected_nontrivial_system_partition": len(detected_systems) > 1,
        "detected_nontrivial_subsystem_partition": len(detected_subsystems) > 1,
        "detected_nontrivial_module_partition": len(detected_modules) > 1,
    }


def _validate_expected_graph_signatures_records(
    *,
    lag_rows: list[dict[str, Any]],
    fused_rows: list[dict[str, Any]],
    expected_lag_edges: tuple[dict[str, str], ...] = (),
    expected_fused_edges: tuple[dict[str, str], ...] = (),
) -> dict[str, Any]:
    lag_index = {
        (str(row.get("parameter_name_u", "")), str(row.get("parameter_name_v", ""))): float(row.get("lag_weight") or 0.0)
        for row in lag_rows
    }
    fused_index = {
        tuple(sorted((str(row.get("parameter_name_u", "")), str(row.get("parameter_name_v", ""))))): float(
            row.get("fused_weight") or 0.0
        )
        for row in fused_rows
    }

    lag_edge_rows: list[dict[str, Any]] = []
    for edge in expected_lag_edges:
        key = (str(edge["parameter_name_u"]), str(edge["parameter_name_v"]))
        reverse_key = (key[1], key[0])
        lag_edge_rows.append(
            {
                "parameter_name_u": key[0],
                "parameter_name_v": key[1],
                "present_forward": key in lag_index,
                "present_reverse": reverse_key in lag_index,
                "present_any_direction": (key in lag_index) or (reverse_key in lag_index),
                "lag_weight": lag_index.get(key),
                "reverse_lag_weight": lag_index.get(reverse_key),
            }
        )

    fused_edge_rows: list[dict[str, Any]] = []
    for edge in expected_fused_edges:
        key = tuple(sorted((str(edge["parameter_name_u"]), str(edge["parameter_name_v"]))))
        fused_edge_rows.append(
            {
                "parameter_name_u": key[0],
                "parameter_name_v": key[1],
                "present": key in fused_index,
                "fused_weight": fused_index.get(key),
            }
        )

    return {
        "status": "ok",
        "lag_expected_edge_count": len(lag_edge_rows),
        "lag_expected_edge_hit_rate": (
            float(sum(1 for row in lag_edge_rows if row["present_any_direction"]) / len(lag_edge_rows))
            if lag_edge_rows
            else None
        ),
        "lag_expected_edge_hit_rate_forward": (
            float(sum(1 for row in lag_edge_rows if row["present_forward"]) / len(lag_edge_rows))
            if lag_edge_rows
            else None
        ),
        "lag_expected_edge_hit_rate_any_direction": (
            float(sum(1 for row in lag_edge_rows if row["present_any_direction"]) / len(lag_edge_rows))
            if lag_edge_rows
            else None
        ),
        "lag_edges": lag_edge_rows,
        "fused_expected_edge_count": len(fused_edge_rows),
        "fused_expected_edge_hit_rate": (
            float(sum(1 for row in fused_edge_rows if row["present"]) / len(fused_edge_rows))
            if fused_edge_rows
            else None
        ),
        "fused_edges": fused_edge_rows,
    }


def _build_graph_validation_summary_spark(
    *,
    hierarchy_sensor_map_sdf: Any | None,
    hierarchy_label_sdf: Any | None,
    lag_graph_sdf: Any | None = None,
    fused_graph_sdf: Any | None = None,
    expected_lag_edges: tuple[dict[str, str], ...] = (),
    expected_fused_edges: tuple[dict[str, str], ...] = (),
) -> dict[str, Any]:
    from pyspark.sql import functions as F

    hierarchy_sensor_map_rows = _collect_records(
        hierarchy_sensor_map_sdf.select("parameter_name", "system_id", "subsystem_id", "module_id").dropDuplicates()
        if hierarchy_sensor_map_sdf is not None and {"parameter_name", "system_id", "subsystem_id", "module_id"}.issubset(set(hierarchy_sensor_map_sdf.columns))
        else None,
        order_by=("parameter_name",),
    )
    hierarchy_label_rows = _collect_records(
        hierarchy_label_sdf.select("parameter_name", "system_id", "subsystem_id", "module_id").dropDuplicates()
        if hierarchy_label_sdf is not None and {"parameter_name", "system_id", "subsystem_id", "module_id"}.issubset(set(hierarchy_label_sdf.columns))
        else None,
        order_by=("parameter_name",),
    )

    lag_rows: list[dict[str, Any]] = []
    if expected_lag_edges and lag_graph_sdf is not None and {"parameter_name_u", "parameter_name_v", "lag_weight"}.issubset(set(lag_graph_sdf.columns)):
        relevant_parameters = sorted(
            {
                str(edge["parameter_name_u"])
                for edge in expected_lag_edges
            }
            | {
                str(edge["parameter_name_v"])
                for edge in expected_lag_edges
            }
        )
        lag_rows = _collect_records(
            lag_graph_sdf.filter(
                F.col("parameter_name_u").isin(relevant_parameters)
                & F.col("parameter_name_v").isin(relevant_parameters)
            ).select("parameter_name_u", "parameter_name_v", "lag_weight"),
            order_by=("parameter_name_u", "parameter_name_v"),
        )

    fused_rows: list[dict[str, Any]] = []
    if expected_fused_edges and fused_graph_sdf is not None and {"parameter_name_u", "parameter_name_v", "fused_weight"}.issubset(set(fused_graph_sdf.columns)):
        relevant_parameters = sorted(
            {
                str(edge["parameter_name_u"])
                for edge in expected_fused_edges
            }
            | {
                str(edge["parameter_name_v"])
                for edge in expected_fused_edges
            }
        )
        fused_rows = _collect_records(
            fused_graph_sdf.filter(
                F.col("parameter_name_u").isin(relevant_parameters)
                & F.col("parameter_name_v").isin(relevant_parameters)
            ).select("parameter_name_u", "parameter_name_v", "fused_weight"),
            order_by=("parameter_name_u", "parameter_name_v"),
        )

    return {
        "hierarchy": _validate_hierarchy_recovery_records(
            hierarchy_sensor_map_rows=hierarchy_sensor_map_rows,
            hierarchy_label_rows=hierarchy_label_rows,
        ),
        "graph_signatures": _validate_expected_graph_signatures_records(
            lag_rows=lag_rows,
            fused_rows=fused_rows,
            expected_lag_edges=expected_lag_edges,
            expected_fused_edges=expected_fused_edges,
        ),
    }


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


def _build_truth_windows_spark(raw_telemetry_sdf: Any | None) -> Any | None:
    from pyspark.sql import functions as F

    if raw_telemetry_sdf is None:
        return None
    normalized = raw_telemetry_sdf.select(
        F.trim(F.coalesce(F.col("tail_id").cast("string"), F.lit(""))).alias("tail_id"),
        F.trim(F.coalesce(F.col("flight_id").cast("string"), F.lit(""))).alias("flight_id"),
        F.col("timestamp_utc"),
        F.trim(F.coalesce(F.col("system_id").cast("string"), F.lit(""))).alias("system_id"),
        F.trim(F.coalesce(F.col("subsystem_id").cast("string"), F.lit(""))).alias("subsystem_id"),
        F.trim(F.coalesce(F.col("module_id").cast("string"), F.lit(""))).alias("module_id"),
        F.trim(F.coalesce(F.col("parameter_name").cast("string"), F.lit(""))).alias("parameter_name"),
        _bool_expr(raw_telemetry_sdf, "misbehavior_active", "fault_active").alias("misbehavior_active"),
        _text_expr(raw_telemetry_sdf, "misbehavior_window_id", "fault_window_id").alias("misbehavior_window_id"),
        _text_expr(raw_telemetry_sdf, "misbehavior_family_label").alias("misbehavior_family_label"),
        _text_expr(raw_telemetry_sdf, "misbehavior_detail_label", "fault_type").alias("misbehavior_detail_label"),
        _text_expr(raw_telemetry_sdf, "fault_window_id", "misbehavior_window_id").alias("fault_window_id"),
        _text_expr(raw_telemetry_sdf, "fault_family_label", "behavior_family_label").alias("fault_family_label"),
        _text_expr(raw_telemetry_sdf, "fault_type", "misbehavior_detail_label").alias("fault_type"),
    ).filter(F.col("misbehavior_active") & (F.col("misbehavior_window_id") != ""))

    return normalized.groupBy("tail_id", "flight_id", "misbehavior_window_id").agg(
        F.min("timestamp_utc").alias("misbehavior_start_timestamp_utc"),
        F.max("timestamp_utc").alias("misbehavior_end_timestamp_utc"),
        _first_non_empty_agg("misbehavior_family_label", "misbehavior_family_label"),
        _first_non_empty_agg("misbehavior_detail_label", "misbehavior_detail_label"),
        _first_non_empty_agg("fault_window_id", "fault_window_id"),
        _first_non_empty_agg("fault_family_label", "fault_family_label"),
        _first_non_empty_agg("fault_type", "fault_type"),
        _first_non_empty_agg("system_id", "system_id"),
        _first_non_empty_agg("subsystem_id", "subsystem_id"),
        _first_non_empty_agg("module_id", "module_id"),
        _first_non_empty_agg("parameter_name", "parameter_name"),
    )


def _build_misbehavior_score_summary_spark(
    *,
    raw_telemetry_sdf: Any | None,
    windows_sdf: Any | None,
    calibrated_scores_sdf: Any | None,
) -> dict[str, Any]:
    from pyspark.sql import functions as F

    truth_windows_sdf = _build_truth_windows_spark(raw_telemetry_sdf)
    truth_rows = _collect_records(truth_windows_sdf, order_by=("tail_id", "flight_id", "misbehavior_window_id"))
    if not truth_rows:
        return {
            "status": "ok",
            "misbehavior_window_count": 0,
            "detected_misbehavior_window_count": 0,
            "emit_ready_misbehavior_window_count": 0,
        }

    if windows_sdf is None or not {"tail_id", "flight_id", "win_id", "t_start", "t_end", "date_utc"}.issubset(set(windows_sdf.columns)):
        return {
            "status": "ok",
            "misbehavior_window_count": len(truth_rows),
            "detected_misbehavior_window_count": 0,
            "emit_ready_misbehavior_window_count": 0,
            "reason": "misbehavior windows did not overlap any calibrated windows",
        }

    merged_windows = windows_sdf.select("tail_id", "flight_id", "win_id", "t_start", "t_end", "date_utc")
    if calibrated_scores_sdf is not None and {"tail_id", "flight_id", "win_id", "date_utc", "global_score", "severity", "emit_ready"}.issubset(set(calibrated_scores_sdf.columns)):
        merged_windows = merged_windows.join(
            calibrated_scores_sdf.select(
                "tail_id",
                "flight_id",
                "win_id",
                "date_utc",
                "global_score",
                "severity",
                "emit_ready",
            ),
            on=["tail_id", "flight_id", "win_id", "date_utc"],
            how="left",
        )
    else:
        merged_windows = (
            merged_windows.withColumn("global_score", F.lit(None).cast("double"))
            .withColumn("severity", F.lit(None).cast("string"))
            .withColumn("emit_ready", F.lit(None).cast("boolean"))
        )

    overlaps = truth_windows_sdf.alias("truth").join(
        merged_windows.alias("windows"),
        on=(
            (F.col("truth.tail_id") == F.col("windows.tail_id"))
            & (F.col("truth.flight_id") == F.col("windows.flight_id"))
            & (F.col("windows.t_end") >= F.col("truth.misbehavior_start_timestamp_utc"))
            & (F.col("windows.t_start") <= F.col("truth.misbehavior_end_timestamp_utc"))
        ),
        how="inner",
    )
    if not overlaps.take(1):
        return {
            "status": "ok",
            "misbehavior_window_count": len(truth_rows),
            "detected_misbehavior_window_count": 0,
            "emit_ready_misbehavior_window_count": 0,
            "reason": "misbehavior windows did not overlap any calibrated windows",
        }

    severity_text = F.coalesce(F.col("windows.severity").cast("string"), F.lit("normal"))
    emit_ready_flag = F.coalesce(F.col("windows.emit_ready").cast("boolean"), F.lit(False))
    score_value = F.coalesce(F.col("windows.global_score").cast("double"), F.lit(0.0))
    aggregated = overlaps.groupBy(
        F.col("truth.tail_id").alias("tail_id"),
        F.col("truth.flight_id").alias("flight_id"),
        F.col("truth.misbehavior_window_id").alias("misbehavior_window_id"),
        F.col("truth.misbehavior_family_label").alias("misbehavior_family_label"),
        F.col("truth.misbehavior_detail_label").alias("misbehavior_detail_label"),
        F.col("truth.fault_window_id").alias("fault_window_id"),
        F.col("truth.fault_family_label").alias("fault_family_label"),
        F.col("truth.fault_type").alias("fault_type"),
        F.col("truth.subsystem_id").alias("subsystem_id"),
        F.col("truth.parameter_name").alias("parameter_name"),
        F.col("truth.misbehavior_start_timestamp_utc").alias("misbehavior_start_timestamp_utc"),
    ).agg(
        F.count(F.lit(1)).alias("overlapping_window_count"),
        F.sum(F.when(severity_text != "normal", F.lit(1)).otherwise(F.lit(0))).alias("detected_window_count"),
        F.sum(F.when(emit_ready_flag, F.lit(1)).otherwise(F.lit(0))).alias("emit_ready_window_count"),
        F.max(score_value).alias("max_global_score"),
        F.percentile_approx(score_value, 0.5, 100).alias("median_global_score"),
        F.min(F.when(severity_text != "normal", F.col("windows.t_start"))).alias("first_detected_start"),
        F.min(F.when(emit_ready_flag, F.col("windows.t_start"))).alias("first_emit_ready_start"),
    )

    per_window: list[dict[str, Any]] = []
    for row in _collect_records(aggregated, order_by=("tail_id", "flight_id", "misbehavior_window_id")):
        misbehavior_start = row.get("misbehavior_start_timestamp_utc")
        first_detected = row.get("first_detected_start")
        first_emit_ready = row.get("first_emit_ready_start")
        per_window.append(
            {
                "tail_id": str(row.get("tail_id", "")),
                "flight_id": str(row.get("flight_id", "")),
                "misbehavior_window_id": str(row.get("misbehavior_window_id", "")),
                "misbehavior_family_label": str(row.get("misbehavior_family_label", "")),
                "misbehavior_detail_label": str(row.get("misbehavior_detail_label", "")),
                "fault_window_id": str(row.get("fault_window_id", "")),
                "fault_family_label": str(row.get("fault_family_label", "")),
                "fault_type": str(row.get("fault_type", "")),
                "subsystem_id": str(row.get("subsystem_id", "")),
                "parameter_name": str(row.get("parameter_name", "")),
                "overlapping_window_count": int(row.get("overlapping_window_count", 0) or 0),
                "detected_window_count": int(row.get("detected_window_count", 0) or 0),
                "emit_ready_window_count": int(row.get("emit_ready_window_count", 0) or 0),
                "max_global_score": float(row.get("max_global_score", 0.0) or 0.0),
                "median_global_score": float(row.get("median_global_score", 0.0) or 0.0),
                "detection_latency_seconds": (
                    None
                    if misbehavior_start is None or first_detected is None
                    else float((first_detected - misbehavior_start).total_seconds())
                ),
                "emit_ready_latency_seconds": (
                    None
                    if misbehavior_start is None or first_emit_ready is None
                    else float((first_emit_ready - misbehavior_start).total_seconds())
                ),
            }
        )

    detected_misbehavior_window_count = int(sum(1 for row in per_window if row["detected_window_count"] > 0))
    emit_ready_misbehavior_window_count = int(sum(1 for row in per_window if row["emit_ready_window_count"] > 0))
    median_scores = [float(row["median_global_score"]) for row in per_window]
    return {
        "status": "ok",
        "misbehavior_window_count": len(truth_rows),
        "detected_misbehavior_window_count": detected_misbehavior_window_count,
        "emit_ready_misbehavior_window_count": emit_ready_misbehavior_window_count,
        "median_misbehavior_window_score": float(median(median_scores)) if median_scores else None,
        "misbehavior_windows": per_window,
    }


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
        "median_fault_window_score": summary.get("median_misbehavior_window_score"),
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
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    truth_windows_sdf = _build_truth_windows_spark(raw_telemetry_sdf)
    truth_rows = _collect_records(truth_windows_sdf, order_by=("tail_id", "flight_id", "misbehavior_window_id"))
    if not truth_rows:
        return {
            "status": "ok",
            "misbehavior_window_count": 0,
            "dominant_subsystem_match_rate": None,
            "telemetry_parameter_match_rate": None,
            "event_parameter_match_rate": None,
        }

    hierarchy_sensor_map_rows = _collect_records(
        hierarchy_sensor_map_sdf.select("parameter_name", "subsystem_id").dropDuplicates()
        if hierarchy_sensor_map_sdf is not None and {"parameter_name", "subsystem_id"}.issubset(set(hierarchy_sensor_map_sdf.columns))
        else None
    )
    hierarchy_label_rows = _collect_records(
        hierarchy_label_sdf.select("parameter_name", "subsystem_id").dropDuplicates()
        if hierarchy_label_sdf is not None and {"parameter_name", "subsystem_id"}.issubset(set(hierarchy_label_sdf.columns))
        else None
    )
    truth_parameter_to_subsystem = {
        str(row.get("parameter_name", "")): str(row.get("subsystem_id", ""))
        for row in hierarchy_label_rows
        if str(row.get("parameter_name", ""))
    }
    detected_to_truth_subsystem, ambiguous_detected_subsystems = _build_detected_subsystem_truth_map(
        hierarchy_sensor_map_rows=hierarchy_sensor_map_rows,
        hierarchy_label_rows=hierarchy_label_rows,
    )

    overlap_map: dict[tuple[str, str, str], dict[str, Any]] = {}
    overlap_hits = None
    if windows_sdf is not None and {"tail_id", "flight_id", "win_id", "t_start", "t_end"}.issubset(set(windows_sdf.columns)):
        overlap_hits = truth_windows_sdf.alias("truth").join(
            windows_sdf.select("tail_id", "flight_id", "win_id", "t_start", "t_end").alias("windows"),
            on=(
                (F.col("truth.tail_id") == F.col("windows.tail_id"))
                & (F.col("truth.flight_id") == F.col("windows.flight_id"))
                & (F.col("windows.t_end") >= F.col("truth.misbehavior_start_timestamp_utc"))
                & (F.col("windows.t_start") <= F.col("truth.misbehavior_end_timestamp_utc"))
            ),
            how="left",
        ).select(
            F.col("truth.tail_id").alias("tail_id"),
            F.col("truth.flight_id").alias("flight_id"),
            F.col("truth.misbehavior_window_id").alias("misbehavior_window_id"),
            F.col("windows.win_id").alias("win_id"),
        )
        overlap_rows = _collect_records(
            overlap_hits.groupBy("tail_id", "flight_id", "misbehavior_window_id").agg(
                F.countDistinct(F.when(F.col("win_id").isNotNull(), F.col("win_id"))).alias("overlapping_window_count"),
                F.collect_set(F.when(F.col("win_id").isNotNull(), F.col("win_id"))).alias("overlapping_win_ids"),
            ),
            order_by=("tail_id", "flight_id", "misbehavior_window_id"),
        )
        overlap_map = {
            (str(row["tail_id"]), str(row["flight_id"]), str(row["misbehavior_window_id"])): row
            for row in overlap_rows
        }

    dominant_subsystem_by_truth: dict[tuple[str, str, str], str] = {}
    if overlap_hits is not None and anomaly_window_sdf is not None and {"tail_id", "flight_id", "win_id", "dominant_subsystem_id"}.issubset(set(anomaly_window_sdf.columns)):
        dominant_hits = overlap_hits.filter(F.col("win_id").isNotNull()).alias("overlap").join(
            anomaly_window_sdf.select("tail_id", "flight_id", "win_id", "dominant_subsystem_id").alias("window_attr"),
            on=(
                (F.col("overlap.tail_id") == F.col("window_attr.tail_id"))
                & (F.col("overlap.flight_id") == F.col("window_attr.flight_id"))
                & (F.col("overlap.win_id") == F.col("window_attr.win_id"))
            ),
            how="inner",
        ).select(
            F.col("overlap.tail_id").alias("tail_id"),
            F.col("overlap.flight_id").alias("flight_id"),
            F.col("overlap.misbehavior_window_id").alias("misbehavior_window_id"),
            F.trim(F.coalesce(F.col("window_attr.dominant_subsystem_id").cast("string"), F.lit(""))).alias(
                "dominant_subsystem_id"
            ),
        )
        dominance_counts = dominant_hits.filter(F.col("dominant_subsystem_id") != "").groupBy(
            "tail_id",
            "flight_id",
            "misbehavior_window_id",
            "dominant_subsystem_id",
        ).agg(F.count(F.lit(1)).alias("hit_count"))
        dominance_ranking = Window.partitionBy("tail_id", "flight_id", "misbehavior_window_id").orderBy(
            F.desc("hit_count"),
            F.asc("dominant_subsystem_id"),
        )
        dominant_subsystem_by_truth = {
            (str(row["tail_id"]), str(row["flight_id"]), str(row["misbehavior_window_id"])): str(
                row["dominant_subsystem_id"]
            )
            for row in _collect_records(
                dominance_counts.withColumn("rank", F.row_number().over(dominance_ranking))
                .filter(F.col("rank") == 1)
                .drop("rank", "hit_count")
            )
        }

    telemetry_parameters_by_truth: dict[tuple[str, str, str], set[str]] = {}
    if overlap_hits is not None and anomaly_telemetry_sdf is not None and {"tail_id", "flight_id", "win_id", "parameter_name"}.issubset(set(anomaly_telemetry_sdf.columns)):
        telemetry_rows = _collect_records(
            overlap_hits.filter(F.col("win_id").isNotNull()).alias("overlap").join(
                anomaly_telemetry_sdf.select("tail_id", "flight_id", "win_id", "parameter_name").alias("telemetry_attr"),
                on=(
                    (F.col("overlap.tail_id") == F.col("telemetry_attr.tail_id"))
                    & (F.col("overlap.flight_id") == F.col("telemetry_attr.flight_id"))
                    & (F.col("overlap.win_id") == F.col("telemetry_attr.win_id"))
                ),
                how="inner",
            ).groupBy(
                F.col("overlap.tail_id").alias("tail_id"),
                F.col("overlap.flight_id").alias("flight_id"),
                F.col("overlap.misbehavior_window_id").alias("misbehavior_window_id"),
            ).agg(F.collect_set(F.col("telemetry_attr.parameter_name")).alias("parameter_names"))
        )
        telemetry_parameters_by_truth = {
            (str(row["tail_id"]), str(row["flight_id"]), str(row["misbehavior_window_id"])): {
                str(parameter_name)
                for parameter_name in (row.get("parameter_names") or [])
                if str(parameter_name)
            }
            for row in telemetry_rows
        }

    event_parameters_by_truth: dict[tuple[str, str, str], set[str]] = {}
    if overlap_hits is not None and anomaly_event_sdf is not None and {"tail_id", "flight_id", "win_id", "parameter_name"}.issubset(set(anomaly_event_sdf.columns)):
        event_rows = _collect_records(
            overlap_hits.filter(F.col("win_id").isNotNull()).alias("overlap").join(
                anomaly_event_sdf.select("tail_id", "flight_id", "win_id", "parameter_name").alias("event_attr"),
                on=(
                    (F.col("overlap.tail_id") == F.col("event_attr.tail_id"))
                    & (F.col("overlap.flight_id") == F.col("event_attr.flight_id"))
                    & (F.col("overlap.win_id") == F.col("event_attr.win_id"))
                ),
                how="inner",
            ).groupBy(
                F.col("overlap.tail_id").alias("tail_id"),
                F.col("overlap.flight_id").alias("flight_id"),
                F.col("overlap.misbehavior_window_id").alias("misbehavior_window_id"),
            ).agg(F.collect_set(F.col("event_attr.parameter_name")).alias("parameter_names"))
        )
        event_parameters_by_truth = {
            (str(row["tail_id"]), str(row["flight_id"]), str(row["misbehavior_window_id"])): {
                str(parameter_name)
                for parameter_name in (row.get("parameter_names") or [])
                if str(parameter_name)
            }
            for row in event_rows
        }

    per_truth_rows: list[dict[str, Any]] = []
    for truth in truth_rows:
        key = (
            str(truth.get("tail_id", "")),
            str(truth.get("flight_id", "")),
            str(truth.get("misbehavior_window_id", "")),
        )
        truth_subsystem = str(truth.get("subsystem_id", ""))
        truth_parameter = str(truth.get("parameter_name", ""))
        overlap_info = overlap_map.get(key, {"overlapping_window_count": 0, "overlapping_win_ids": []})
        telemetry_parameters = telemetry_parameters_by_truth.get(key, set())
        event_parameters = event_parameters_by_truth.get(key, set())
        telemetry_truth_subsystems = {
            truth_parameter_to_subsystem.get(parameter_name)
            for parameter_name in telemetry_parameters
            if truth_parameter_to_subsystem.get(parameter_name)
        }
        event_truth_subsystems = {
            truth_parameter_to_subsystem.get(parameter_name)
            for parameter_name in event_parameters
            if truth_parameter_to_subsystem.get(parameter_name)
        }
        dominant_detected_subsystem = dominant_subsystem_by_truth.get(key, "")
        dominant_subsystem_truth, dominant_subsystem_mappable = _resolve_detected_subsystem(
            dominant_detected_subsystem,
            detected_to_truth=detected_to_truth_subsystem,
            ambiguous_detected_subsystems=ambiguous_detected_subsystems,
        )
        payload = {
            "tail_id": key[0],
            "flight_id": key[1],
            "misbehavior_window_id": key[2],
            "subsystem_id": truth_subsystem,
            "parameter_name": truth_parameter,
            "overlapping_window_count": int(overlap_info.get("overlapping_window_count", 0) or 0),
            "dominant_subsystem_match": bool(
                dominant_subsystem_mappable and dominant_subsystem_truth == truth_subsystem
            ),
            "dominant_subsystem_mappable": bool(dominant_subsystem_mappable),
            "dominant_subsystem_truth": dominant_subsystem_truth,
            "telemetry_parameter_match": truth_parameter in telemetry_parameters,
            "event_parameter_match": truth_parameter in event_parameters,
            "telemetry_truth_subsystem_present": truth_subsystem in telemetry_truth_subsystems,
            "event_truth_subsystem_present": truth_subsystem in event_truth_subsystems,
            "misbehavior_family_label": str(truth.get("misbehavior_family_label", "")),
            "misbehavior_detail_label": str(truth.get("misbehavior_detail_label", "")),
            "fault_window_id": str(truth.get("fault_window_id", "")),
            "fault_family_label": str(truth.get("fault_family_label", "")),
            "fault_type": str(truth.get("fault_type", "")),
        }
        per_truth_rows.append(payload)

    mappable_rows = [row for row in per_truth_rows if row["dominant_subsystem_mappable"]]
    return {
        "status": "ok",
        "misbehavior_window_count": len(per_truth_rows),
        "dominant_subsystem_match_rate": (
            float(sum(1 for row in mappable_rows if row["dominant_subsystem_match"]) / len(mappable_rows))
            if mappable_rows
            else None
        ),
        "dominant_subsystem_mappable_rate": (
            float(sum(1 for row in per_truth_rows if row["dominant_subsystem_mappable"]) / len(per_truth_rows))
            if per_truth_rows
            else None
        ),
        "telemetry_parameter_match_rate": (
            float(sum(1 for row in per_truth_rows if row["telemetry_parameter_match"]) / len(per_truth_rows))
            if per_truth_rows
            else None
        ),
        "event_parameter_match_rate": (
            float(sum(1 for row in per_truth_rows if row["event_parameter_match"]) / len(per_truth_rows))
            if per_truth_rows
            else None
        ),
        "telemetry_truth_subsystem_present_rate": (
            float(sum(1 for row in per_truth_rows if row["telemetry_truth_subsystem_present"]) / len(per_truth_rows))
            if per_truth_rows
            else None
        ),
        "event_truth_subsystem_present_rate": (
            float(sum(1 for row in per_truth_rows if row["event_truth_subsystem_present"]) / len(per_truth_rows))
            if per_truth_rows
            else None
        ),
        "misbehavior_windows": per_truth_rows,
    }


def _build_fault_attribution_summary_from_misbehavior(summary: dict[str, Any]) -> dict[str, Any]:
    if summary.get("status") != "ok":
        return summary
    return {
        "status": "ok",
        "fault_window_count": int(summary.get("misbehavior_window_count", 0)),
        "dominant_subsystem_match_rate": summary.get("dominant_subsystem_match_rate"),
        "dominant_subsystem_mappable_rate": summary.get("dominant_subsystem_mappable_rate"),
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
            _write_validation_reports(
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
        _restore_env(previous_env)
    return PipelineRunResult(paths=paths, status=status, seed_counts=seed_counts)


def main() -> None:
    run_pipeline(PipelineRunConfig.from_args(parse_args()))


if __name__ == "__main__":
    main()
