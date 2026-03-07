# File: libs/testing/sample_data.py
"""Deterministic sample DataFrame generators for local pipeline testing."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path


def _base_time() -> datetime:
    return datetime(2026, 2, 28, 0, 0, 0)


def create_sample_raw_input_df(spark: "SparkSession") -> "DataFrame":
    base = _base_time()
    rows = []
    for idx in range(12):
        ts = base + timedelta(milliseconds=100 * idx)
        rows.append(
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "timestamp": ts,
                "parameter_name": "ENG_TEMP_1",
                "parameter_value": str(450.0 + idx * 0.8),
            }
        )
        rows.append(
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "timestamp": ts,
                "parameter_name": "HYD_PRESS_1",
                "parameter_value": str(3000.0 + ((-1) ** idx) * 5.0),
            }
        )
        rows.append(
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "timestamp": ts,
                "parameter_name": "PUMP_STATE",
                "parameter_value": "ON" if idx % 3 else "OFF",
            }
        )
    return spark.createDataFrame(rows)


def create_scaled_raw_input_df(
    spark: "SparkSession",
    tail_count: int,
    flights_per_tail: int,
    sensor_count: int,
    timestamp_count: int,
    step_ms: int,
) -> "DataFrame":
    base = _base_time()
    rows: list[dict[str, object]] = []

    numeric_sensor_count = max(1, sensor_count - 1)
    numeric_sensors = [f"NUM_SENSOR_{sensor_index + 1:03d}" for sensor_index in range(numeric_sensor_count)]
    categorical_sensor = "PUMP_STATE"

    for tail_index in range(tail_count):
        tail_id = f"T{tail_index + 1:03d}"
        for flight_index in range(flights_per_tail):
            flight_id = f"F{flight_index + 1:03d}"
            flight_base = base + timedelta(days=tail_index, minutes=flight_index * 30)
            for timestamp_index in range(timestamp_count):
                ts = flight_base + timedelta(milliseconds=step_ms * timestamp_index)
                for sensor_index, sensor_name in enumerate(numeric_sensors):
                    baseline = 100.0 + sensor_index * 10.0 + tail_index * 2.5 + flight_index * 1.5
                    oscillation = ((timestamp_index % 7) - 3) * (0.6 + sensor_index * 0.02)
                    rows.append(
                        {
                            "tail_id": tail_id,
                            "flight_id": flight_id,
                            "timestamp": ts,
                            "parameter_name": sensor_name,
                            "parameter_value": str(baseline + timestamp_index * 0.35 + oscillation),
                        }
                    )
                rows.append(
                    {
                        "tail_id": tail_id,
                        "flight_id": flight_id,
                        "timestamp": ts,
                        "parameter_name": categorical_sensor,
                        "parameter_value": "ON" if ((timestamp_index + flight_index + tail_index) % 5) else "OFF",
                    }
                )

    return spark.createDataFrame(rows)


def create_sample_raw_table_df(spark: "SparkSession") -> "DataFrame":
    from pyspark.sql import functions as F

    raw_input = create_sample_raw_input_df(spark)
    return (
        raw_input.withColumnRenamed("timestamp", "timestamp_utc")
        .withColumn("date_utc", F.to_date("timestamp_utc"))
        .select("tail_id", "flight_id", "timestamp_utc", "parameter_name", "parameter_value", "date_utc")
    )


def create_sample_events_df(spark: "SparkSession") -> "DataFrame":
    base = _base_time()
    rows = []
    event_types = [
        "threshold",
        "slope_pos",
        "transition",
        "cooccur",
        "slope_neg",
        "state_enter",
        "dropped",
        "threshold",
    ]
    for idx, event_type in enumerate(event_types, start=1):
        ts = base + timedelta(milliseconds=120 * idx)
        rows.append(
            {
                "tail_id": "T001",
                "flight_id": "F001",
                "win_id": (idx - 1) // 4 + 1,
                "timestamp_utc": ts,
                "parameter_name": "ENG_TEMP_1" if idx % 2 else "PUMP_STATE",
                "event_type_detected": event_type,
                "anomaly_type_detected": "",
                "anomaly_score_detected": 0.0,
                "payload": {"idx": str(idx)},
                "date_utc": ts.date(),
            }
        )
    return spark.createDataFrame(rows)


def create_sample_windows_df(spark: "SparkSession") -> "DataFrame":
    base = _base_time()
    rows = [
        {
            "tail_id": "T001",
            "flight_id": "F001",
            "win_id": 1,
            "t_start": base + timedelta(milliseconds=100),
            "t_end": base + timedelta(milliseconds=500),
            "duration_ms": 400,
            "event_count": 4,
            "zoh_version": 1,
            "date_utc": base.date(),
        },
        {
            "tail_id": "T001",
            "flight_id": "F001",
            "win_id": 2,
            "t_start": base + timedelta(milliseconds=600),
            "t_end": base + timedelta(milliseconds=1100),
            "duration_ms": 500,
            "event_count": 4,
            "zoh_version": 1,
            "date_utc": base.date(),
        },
    ]
    return spark.createDataFrame(rows)


def create_sample_phase_labels_df(spark: "SparkSession") -> "DataFrame":
    base = _base_time()
    rows = [
        {
            "tail_id": "T001",
            "flight_id": "F001",
            "win_id": 1,
            "phase_label": "steady",
            "timestamp_utc": base + timedelta(milliseconds=500),
            "date_utc": base.date(),
        },
        {
            "tail_id": "T001",
            "flight_id": "F001",
            "win_id": 2,
            "phase_label": "transition",
            "timestamp_utc": base + timedelta(milliseconds=1100),
            "date_utc": base.date(),
        },
    ]
    return spark.createDataFrame(rows)


def create_sample_hierarchy_sensor_map_label_df(spark: "SparkSession") -> "DataFrame":
    rows = [
        {"parameter_name": "ENG_TEMP_1", "system_id": "SYS_0001", "subsystem_id": "SUBSYS_0001", "module_id": "MOD_0001"},
        {"parameter_name": "HYD_PRESS_1", "system_id": "SYS_0001", "subsystem_id": "SUBSYS_0001", "module_id": "MOD_0002"},
        {"parameter_name": "PUMP_STATE", "system_id": "SYS_0001", "subsystem_id": "SUBSYS_0002", "module_id": "MOD_0003"},
    ]
    return spark.createDataFrame(rows)


def create_sample_phase_windows_df(spark: "SparkSession") -> "DataFrame":
    base = _base_time()
    rows = [
        {
            "tail_id": "T001",
            "flight_id": "F001",
            "win_id": 1,
            "t_start": base + timedelta(milliseconds=100),
            "t_end": base + timedelta(milliseconds=500),
            "duration_ms": 400,
            "event_count": 4,
            "phase_id_detected": 0,
            "phase_state_detected": "stable",
            "phase_confidence_detected": 0.91,
            "distance_to_centroid_detected": 0.12,
            "drift_magnitude": 1.8,
            "breadth": 0.35,
            "backbone_reconstruction_error": 0.22,
            "backbone_residual_by_parameter": {"ENG_TEMP_1": 0.15, "HYD_PRESS_1": -0.07},
            "x_c": [0.2],
            "s_w": [0.2, 0.5, 0.0, 10.0, 0.5, 0.5, 1.0],
            "date_utc": base.date(),
        },
        {
            "tail_id": "T001",
            "flight_id": "F001",
            "win_id": 2,
            "t_start": base + timedelta(milliseconds=600),
            "t_end": base + timedelta(milliseconds=1100),
            "duration_ms": 500,
            "event_count": 4,
            "phase_id_detected": 3,
            "phase_state_detected": "transition_region",
            "phase_confidence_detected": 0.44,
            "distance_to_centroid_detected": 0.88,
            "drift_magnitude": 4.5,
            "breadth": 0.72,
            "backbone_reconstruction_error": 0.91,
            "backbone_residual_by_parameter": {"ENG_TEMP_1": 0.62, "HYD_PRESS_1": 0.29},
            "x_c": [0.8],
            "s_w": [0.8, 0.3, 1.0, 8.0, 0.25, 0.75, 1.0],
            "date_utc": base.date(),
        },
    ]
    return spark.createDataFrame(rows)


def create_sample_scores_df(spark: "SparkSession") -> "DataFrame":
    base = _base_time()
    rows = [
        {
            "tail_id": "T001",
            "flight_id": "F001",
            "win_id": 1,
            "phase_state_detected": "stable",
            "phase_id_detected": 0,
            "phase_confidence_detected": 0.91,
            "distance_to_centroid_detected": 0.12,
            "drift_magnitude": 1.8,
            "breadth": 0.35,
            "global_score": 3.2,
            "p_value": 0.65,
            "severity": "low",
            "dominant_subsystem_id": "",
            "dominant_score_component": "structure",
            "subsystem_scores": {"SUBSYS_0001": 0.8, "SUBSYS_0002": 0.2},
            "score_component_scores": {"structure": 1.1, "reconstruction": 0.8},
            "date_utc": base.date(),
        },
        {
            "tail_id": "T001",
            "flight_id": "F001",
            "win_id": 2,
            "phase_state_detected": "transition_region",
            "phase_id_detected": 3,
            "phase_confidence_detected": 0.44,
            "distance_to_centroid_detected": 0.88,
            "drift_magnitude": 4.5,
            "breadth": 0.72,
            "global_score": 8.4,
            "p_value": 0.22,
            "severity": "medium",
            "dominant_subsystem_id": "",
            "dominant_score_component": "reconstruction",
            "subsystem_scores": {"SUBSYS_0002": 0.7, "SUBSYS_0001": 0.3},
            "score_component_scores": {"structure": 2.1, "reconstruction": 3.0},
            "date_utc": base.date(),
        },
    ]
    return spark.createDataFrame(rows)


def create_sample_calibrated_df(spark: "SparkSession") -> "DataFrame":
    base = _base_time()
    rows = [
        {
            "tail_id": "T001",
            "flight_id": "F001",
            "win_id": 1,
            "phase_state_detected": "stable",
            "phase_id_detected": 0,
            "phase_confidence_detected": 0.91,
            "distance_to_centroid_detected": 0.12,
            "drift_magnitude": 1.8,
            "breadth": 0.35,
            "global_score": 3.2,
            "p_value": 0.60,
            "severity": "low",
            "dominant_subsystem_id": "",
            "dominant_score_component": "structure",
            "subsystem_scores": {"SUBSYS_0001": 0.8, "SUBSYS_0002": 0.2},
            "score_component_scores": {"structure": 1.1, "reconstruction": 0.8},
            "warm": True,
            "emit_ready": True,
            "min_warm": 1,
            "date_utc": base.date(),
        },
        {
            "tail_id": "T001",
            "flight_id": "F001",
            "win_id": 2,
            "phase_state_detected": "transition_region",
            "phase_id_detected": 3,
            "phase_confidence_detected": 0.44,
            "distance_to_centroid_detected": 0.88,
            "drift_magnitude": 4.5,
            "breadth": 0.72,
            "global_score": 8.4,
            "p_value": 0.20,
            "severity": "medium",
            "dominant_subsystem_id": "",
            "dominant_score_component": "reconstruction",
            "subsystem_scores": {"SUBSYS_0002": 0.7, "SUBSYS_0001": 0.3},
            "score_component_scores": {"structure": 2.1, "reconstruction": 3.0},
            "warm": True,
            "emit_ready": True,
            "min_warm": 1,
            "date_utc": base.date(),
        },
    ]
    return spark.createDataFrame(rows)


def seed_sample_dataset(
    spark: "SparkSession",
    base_dir: str = "data",
    mode: str = "overwrite",
    table_format: str = "delta",
    tail_count: int = 1,
    flights_per_tail: int = 1,
    sensor_count: int = 3,
    timestamp_count: int = 12,
    step_ms: int = 100,
    include_intermediate_tables: bool = True,
) -> dict[str, str]:
    """Write deterministic sample inputs/intermediate tables for smoke testing.

    Returns mapping of logical table names to written paths.
    """
    base_path = Path(base_dir)
    input_path = base_path / "input" / "raw_telemetry"
    delta_path = base_path / "delta"

    paths = {
        "raw_input": str(input_path),
        "raw_telemetry": str(delta_path / "raw_telemetry"),
        "events": str(delta_path / "events"),
        "windows": str(delta_path / "windows"),
        "phase_labels": str(delta_path / "phase_labels"),
        "hierarchy_sensor_map_label": str(delta_path / "hierarchy_sensor_map_label"),
        "phase_windows": str(delta_path / "phase_windows"),
        "window_scores_raw": str(delta_path / "window_scores_raw"),
        "window_scores_calibrated": str(delta_path / "window_scores_calibrated"),
    }

    if (
        tail_count == 1
        and flights_per_tail == 1
        and sensor_count == 3
        and timestamp_count == 12
        and step_ms == 100
    ):
        raw_input_df = create_sample_raw_input_df(spark)
    else:
        raw_input_df = create_scaled_raw_input_df(
            spark=spark,
            tail_count=tail_count,
            flights_per_tail=flights_per_tail,
            sensor_count=sensor_count,
            timestamp_count=timestamp_count,
            step_ms=step_ms,
        )

    raw_input_df.write.mode(mode).parquet(paths["raw_input"])

    writer_fmt = table_format
    canonical_partitions = ["tail_id", "flight_id", "date_utc"]

    def _write_seed_table(df: "DataFrame", path: str) -> None:
        writer = df.write.format(writer_fmt).mode(mode)
        partition_cols = [col for col in canonical_partitions if col in df.columns]
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(path)

    _write_seed_table(create_sample_raw_table_df(spark), paths["raw_telemetry"])
    if include_intermediate_tables:
        _write_seed_table(create_sample_events_df(spark), paths["events"])
        _write_seed_table(create_sample_windows_df(spark), paths["windows"])
        _write_seed_table(create_sample_phase_labels_df(spark), paths["phase_labels"])
        _write_seed_table(create_sample_hierarchy_sensor_map_label_df(spark), paths["hierarchy_sensor_map_label"])
        _write_seed_table(create_sample_phase_windows_df(spark), paths["phase_windows"])
        _write_seed_table(create_sample_scores_df(spark), paths["window_scores_raw"])
        _write_seed_table(create_sample_calibrated_df(spark), paths["window_scores_calibrated"])
    return paths


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
