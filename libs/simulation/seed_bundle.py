"""Seed artifact emission for simulation pipeline runs."""

from __future__ import annotations

import os
from typing import Any

import pandas as pd

from libs.io.delta import write_table
from libs.io.pandas_spark import pandas_records_for_spark
from libs.io.schemas import SIMULATION_RAW_INPUT_SCHEMA
from libs.simulation import FlightSpec
from libs.simulation.cli import DEFAULT_START_TIMESTAMP_UTC
from libs.simulation.run_context import PipelineRunConfig, RunPaths


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


def write_seed_tables(
    *,
    spark: Any,
    paths: RunPaths,
    config: PipelineRunConfig,
    flight: FlightSpec,
    write_table_fn: Any = write_table,
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

    phase_labels_sdf = spark.createDataFrame(
        pandas_records_for_spark(phase_df),
        schema=StructType(
            [
                StructField("tail_id", StringType(), False),
                StructField("flight_id", StringType(), False),
                StructField("step_index", IntegerType(), True),
                StructField("timestamp_utc", TimestampType(), False),
                StructField("phase_label", StringType(), True),
                StructField("date_utc", DateType(), False),
            ]
        ),
    )
    write_table_fn(
        phase_labels_sdf,
        path=str(paths.artifact_path("phase_labels")),
        mode="overwrite",
        fmt=os.environ["S3NTINEL_TABLE_FORMAT"],
        partition_by=["tail_id"],
    )

    hierarchy_label_df = _build_hierarchy_label_df(flight.aircraft_spec)
    hierarchy_label_sdf = spark.createDataFrame(
        pandas_records_for_spark(hierarchy_label_df),
        schema=StructType(
            [
                StructField("parameter_name", StringType(), False),
                StructField("system_id", StringType(), False),
                StructField("subsystem_id", StringType(), False),
                StructField("module_id", StringType(), False),
            ]
        ),
    )
    write_table_fn(
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
        coupling_misbehavior_sdf = spark.createDataFrame(
            pandas_records_for_spark(coupling_misbehavior_df),
            schema=StructType(
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
            ),
        )
        write_table_fn(
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
