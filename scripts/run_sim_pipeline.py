"""Run the canonical simulation pipeline into a persisted artifact bundle."""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from libs.io.delta import get_spark, write_table
from libs.io.delta import read_table
from libs.io.pandas_spark import pandas_records_for_spark
from libs.perf import get_logger
from libs.anomaly import validate_attribution_against_fault_truth
from libs.graph import build_graph_validation_summary
from libs.phase import validate_detected_phases_from_tables
from libs.scoring import summarize_fault_window_detection, validate_scores_against_fault_windows
from libs.simulation import Flight, FlightSpec
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
    "backbone": "S3NTINEL_BACKBONE_TABLE_PATH",
    "backbone_sensor_energy": "S3NTINEL_BACKBONE_SENSOR_ENERGY_TABLE_PATH",
    "precision_graph": "S3NTINEL_PRECISION_GRAPH_TABLE_PATH",
    "event_graph": "S3NTINEL_EVENT_GRAPH_TABLE_PATH",
    "lag_graph": "S3NTINEL_LAG_GRAPH_TABLE_PATH",
    "transition_graph": "S3NTINEL_TRANSITION_GRAPH_TABLE_PATH",
    "fused_graph": "S3NTINEL_FUSED_GRAPH_TABLE_PATH",
    "phase_windows": "S3NTINEL_PHASE_WINDOWS_TABLE_PATH",
    "phase_baselines": "S3NTINEL_PHASE_BASELINES_TABLE_PATH",
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
    "backbone": Path("delta") / "backbone",
    "backbone_sensor_energy": Path("delta") / "backbone_sensor_energy",
    "precision_graph": Path("delta") / "precision_graph",
    "event_graph": Path("delta") / "event_graph",
    "lag_graph": Path("delta") / "lag_graph",
    "transition_graph": Path("delta") / "transition_graph",
    "fused_graph": Path("delta") / "fused_graph",
    "phase_windows": Path("delta") / "phase_windows",
    "phase_baselines": Path("delta") / "phase_baselines",
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
            "11_graph_fit.py",
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
            "11_graph_fit.py",
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
            n_steps=int(args.n_steps),
            dt_seconds=float(args.dt_seconds),
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
    parser.add_argument("--base-dir", default="data/sim_runs", help="Base directory for simulation pipeline runs")
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
    raw_input_sdf = spark.createDataFrame(pandas_records_for_spark(raw_df))
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
    return {
        "raw_input_rows": int(len(raw_df)),
        "phase_label_rows": int(len(phase_df)),
        "hierarchy_label_rows": int(len(hierarchy_label_df)),
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
            "spark_master": os.getenv("S3NTINEL_SPARK_MASTER", "local[2]"),
        },
        "seed_counts": seed_counts,
        "artifacts": _summarize_artifacts(paths),
        "validation_reports": {
            "phase_validation": str(paths.run_dir / "reports" / "phase_validation_summary.json"),
            "hierarchy_validation": str(paths.run_dir / "reports" / "hierarchy_validation_summary.json"),
            "score_validation": str(paths.run_dir / "reports" / "score_validation_summary.json"),
            "attribution_validation": str(paths.run_dir / "reports" / "attribution_validation_summary.json"),
            "fault_window_validation": str(paths.run_dir / "reports" / "fault_window_validation_summary.json"),
        },
    }


def _read_optional_table_pdf(*, spark: Any, path: Path, fmt: str) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return read_table(spark, str(path), fmt=fmt).toPandas()


def _write_validation_reports(
    *,
    spark: Any,
    paths: RunPaths,
    flight: FlightSpec,
    table_format: str,
) -> dict[str, Any]:
    raw_telemetry_df = _read_optional_table_pdf(
        spark=spark,
        path=paths.artifact_path("raw_telemetry"),
        fmt=table_format,
    )
    phase_labels_df = _read_optional_table_pdf(
        spark=spark,
        path=paths.artifact_path("phase_labels"),
        fmt=table_format,
    )
    phase_windows_df = _read_optional_table_pdf(
        spark=spark,
        path=paths.artifact_path("phase_windows"),
        fmt=table_format,
    )
    hierarchy_sensor_map_df = _read_optional_table_pdf(
        spark=spark,
        path=paths.artifact_path("hierarchy_sensor_map"),
        fmt=table_format,
    )
    hierarchy_label_df = _read_optional_table_pdf(
        spark=spark,
        path=paths.artifact_path("hierarchy_sensor_map_label"),
        fmt=table_format,
    )
    lag_graph_df = _read_optional_table_pdf(
        spark=spark,
        path=paths.artifact_path("lag_graph"),
        fmt=table_format,
    )
    fused_graph_df = _read_optional_table_pdf(
        spark=spark,
        path=paths.artifact_path("fused_graph"),
        fmt=table_format,
    )
    windows_df = _read_optional_table_pdf(
        spark=spark,
        path=paths.artifact_path("windows"),
        fmt=table_format,
    )
    calibrated_scores_df = _read_optional_table_pdf(
        spark=spark,
        path=paths.artifact_path("window_scores_calibrated"),
        fmt=table_format,
    )
    anomaly_window_df = _read_optional_table_pdf(
        spark=spark,
        path=paths.artifact_path("anomaly_window_attribution"),
        fmt=table_format,
    )
    anomaly_telemetry_df = _read_optional_table_pdf(
        spark=spark,
        path=paths.artifact_path("anomaly_telemetry_attribution"),
        fmt=table_format,
    )
    anomaly_event_df = _read_optional_table_pdf(
        spark=spark,
        path=paths.artifact_path("anomaly_event_attribution"),
        fmt=table_format,
    )

    validation_expectations = dict(flight.metadata.get("validation", {}) or {})
    phase_summary = validate_detected_phases_from_tables(
        phase_windows_df=phase_windows_df,
        phase_labels_df=phase_labels_df,
        windows_df=windows_df,
    )
    hierarchy_summary = build_graph_validation_summary(
        hierarchy_sensor_map_df=hierarchy_sensor_map_df,
        hierarchy_label_df=hierarchy_label_df,
        lag_graph_df=lag_graph_df,
        fused_graph_df=fused_graph_df,
        expected_lag_edges=tuple(validation_expectations.get("expected_lag_edges", ()) or ()),
        expected_fused_edges=tuple(validation_expectations.get("expected_fused_edges", ()) or ()),
    )
    score_summary = validate_scores_against_fault_windows(
        raw_telemetry_df=raw_telemetry_df,
        windows_df=windows_df,
        calibrated_scores_df=calibrated_scores_df,
    )
    fault_window_summary = summarize_fault_window_detection(
        raw_telemetry_df=raw_telemetry_df,
        windows_df=windows_df,
        calibrated_scores_df=calibrated_scores_df,
    )
    attribution_summary = validate_attribution_against_fault_truth(
        raw_telemetry_df=raw_telemetry_df,
        windows_df=windows_df,
        anomaly_window_attribution_df=anomaly_window_df,
        anomaly_telemetry_attribution_df=anomaly_telemetry_df,
        anomaly_event_attribution_df=anomaly_event_df,
        hierarchy_sensor_map_df=hierarchy_sensor_map_df,
        hierarchy_label_df=hierarchy_label_df,
    )

    payloads = {
        "phase_validation_summary.json": phase_summary,
        "hierarchy_validation_summary.json": hierarchy_summary,
        "score_validation_summary.json": score_summary,
        "fault_window_validation_summary.json": fault_window_summary,
        "attribution_validation_summary.json": attribution_summary,
    }
    for filename, payload in payloads.items():
        _write_manifest(paths.run_dir / "reports" / filename, payload)
    return payloads


def run_pipeline(config: PipelineRunConfig) -> PipelineRunResult:
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
            flight = resolve_flight(config.flight_name)
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
