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


def create_sample_raw_table_df(spark: "SparkSession") -> "DataFrame":
    from pyspark.sql import functions as F

    raw_input = create_sample_raw_input_df(spark)
    return (
        raw_input.withColumnRenamed("timestamp", "timestamp_utc")
        .withColumn("sensor", F.col("parameter_name"))
        .withColumn("val", F.expr("try_cast(parameter_value as double)"))
        .withColumn("state", F.when(F.col("val").isNull(), F.col("parameter_value")))
        .withColumn("unit", F.lit(None).cast("string"))
        .withColumn("rate_hz", F.lit(None).cast("double"))
        .withColumn("meta", F.expr("cast(map() as map<string,string>)"))
        .withColumn("date_utc", F.to_date("timestamp_utc"))
        .select(
            "tail_id",
            "flight_id",
            "timestamp_utc",
            "parameter_name",
            "parameter_value",
            "sensor",
            "val",
            "state",
            "unit",
            "rate_hz",
            "meta",
            "date_utc",
        )
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
                "ts": ts,
                "sensor": "ENG_TEMP_1" if idx % 2 else "PUMP_STATE",
                "subsystem": "unknown",
                "event_type": event_type,
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


def create_sample_signatures_df(spark: "SparkSession") -> "DataFrame":
    base = _base_time()
    rows = [
        {
            "tail_id": "T001",
            "flight_id": "F001",
            "win_id": 1,
            "phase_id": 0,
            "sig_version": 1,
            "pivot_block": [451.0, 0.9, 450.0, 452.0],
            "cur_block": [6.0, 2.0, 400.0],
            "event_block": [4.0, 2.0, 1.0],
            "cat_block": [1.0, 0.0, 1.0],
            "breadth": 0.35,
            "drift_mag": 1.8,
            "drift_dir": [0.2],
            "date_utc": base.date(),
        },
        {
            "tail_id": "T001",
            "flight_id": "F001",
            "win_id": 2,
            "phase_id": 1,
            "sig_version": 1,
            "pivot_block": [453.0, 1.2, 451.0, 454.0],
            "cur_block": [6.0, 3.0, 500.0],
            "event_block": [4.0, 1.0, 2.0],
            "cat_block": [2.0, 1.0, 0.0],
            "breadth": 0.72,
            "drift_mag": 4.5,
            "drift_dir": [0.8],
            "date_utc": base.date(),
        },
    ]
    return spark.createDataFrame(rows)


def create_sample_phase_windows_df(spark: "SparkSession") -> "DataFrame":
    base = _base_time()
    rows = [
        {
            "tail_id": "T001",
            "flight_id": "F001",
            "win_id": 1,
            "phase_id": 0,
            "phase_state": "stable",
            "phase_confidence": 0.91,
            "distance_to_centroid": 0.12,
            "drift_magnitude": 1.8,
            "breadth": 0.35,
            "persistence": 0.63,
            "is_stable": True,
            "phase_persistent": False,
            "date_utc": base.date(),
        },
        {
            "tail_id": "T001",
            "flight_id": "F001",
            "win_id": 2,
            "phase_id": 3,
            "phase_state": "transition_region",
            "phase_confidence": 0.44,
            "distance_to_centroid": 0.88,
            "drift_magnitude": 4.5,
            "breadth": 0.72,
            "persistence": 3.24,
            "is_stable": False,
            "phase_persistent": True,
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
            "phase_state": "stable",
            "phase_id": 0,
            "phase_confidence": 0.91,
            "distance_to_centroid": 0.12,
            "drift_magnitude": 1.8,
            "breadth": 0.35,
            "global_score": 3.2,
            "p_value": 0.65,
            "severity": "low",
            "dominant_subsystem": "unknown",
            "dominant_block": "event_block",
            "block_scores": {"pivot": 1.1, "cur": 1.0, "events": 0.8, "categorical": 0.3},
            "date_utc": base.date(),
        },
        {
            "tail_id": "T001",
            "flight_id": "F001",
            "win_id": 2,
            "phase_state": "transition_region",
            "phase_id": 3,
            "phase_confidence": 0.44,
            "distance_to_centroid": 0.88,
            "drift_magnitude": 4.5,
            "breadth": 0.72,
            "global_score": 8.4,
            "p_value": 0.22,
            "severity": "medium",
            "dominant_subsystem": "unknown",
            "dominant_block": "cur_block",
            "block_scores": {"pivot": 2.1, "cur": 3.0, "events": 2.4, "categorical": 0.9},
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
            "phase_state": "stable",
            "phase_id": 0,
            "phase_confidence": 0.91,
            "distance_to_centroid": 0.12,
            "drift_magnitude": 1.8,
            "breadth": 0.35,
            "global_score": 3.2,
            "p_value": 0.60,
            "severity": "low",
            "dominant_subsystem": "unknown",
            "dominant_block": "event_block",
            "block_scores": {"pivot": 1.1, "cur": 1.0, "events": 0.8, "categorical": 0.3},
            "warm": True,
            "emit_ready": True,
            "min_warm": 1,
            "date_utc": base.date(),
        },
        {
            "tail_id": "T001",
            "flight_id": "F001",
            "win_id": 2,
            "phase_state": "transition_region",
            "phase_id": 3,
            "phase_confidence": 0.44,
            "distance_to_centroid": 0.88,
            "drift_magnitude": 4.5,
            "breadth": 0.72,
            "global_score": 8.4,
            "p_value": 0.20,
            "severity": "medium",
            "dominant_subsystem": "unknown",
            "dominant_block": "cur_block",
            "block_scores": {"pivot": 2.1, "cur": 3.0, "events": 2.4, "categorical": 0.9},
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
        "signatures": str(delta_path / "signatures"),
        "phase_windows": str(delta_path / "phase_windows"),
        "scores": str(delta_path / "scores"),
        "calibrated": str(delta_path / "calibrated"),
    }

    create_sample_raw_input_df(spark).write.mode(mode).parquet(paths["raw_input"])

    writer_fmt = table_format
    create_sample_raw_table_df(spark).write.format(writer_fmt).mode(mode).save(paths["raw_telemetry"])
    create_sample_events_df(spark).write.format(writer_fmt).mode(mode).save(paths["events"])
    create_sample_windows_df(spark).write.format(writer_fmt).mode(mode).save(paths["windows"])
    create_sample_signatures_df(spark).write.format(writer_fmt).mode(mode).save(paths["signatures"])
    create_sample_phase_windows_df(spark).write.format(writer_fmt).mode(mode).save(paths["phase_windows"])
    create_sample_scores_df(spark).write.format(writer_fmt).mode(mode).save(paths["scores"])
    create_sample_calibrated_df(spark).write.format(writer_fmt).mode(mode).save(paths["calibrated"])
    return paths


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
