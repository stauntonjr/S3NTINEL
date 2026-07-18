"""Dataset seeding helpers for smoke and integration workflows."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from libs.events import ParameterEventProfile
from libs.profiling import TelemetryProfilingPlan
from libs.testing.data import (
    create_sample_calibrated_df,
    create_sample_events_df,
    create_sample_hierarchy_sensor_map_label_df,
    create_sample_phase_labels_df,
    create_sample_phase_windows_df,
    create_sample_raw_input_df,
    create_sample_raw_table_df,
    create_sample_scores_df,
    create_sample_windows_df,
    create_scaled_raw_input_df,
)


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
    inject_anomaly: bool = False,
) -> dict[str, str]:
    """Write deterministic sample inputs/intermediate tables for smoke testing."""
    base_path = Path(base_dir)
    input_path = base_path / "input" / "raw_telemetry"
    delta_path = base_path / "delta"

    paths = {
        "raw_input": str(input_path),
        "raw_telemetry": str(delta_path / "raw_telemetry"),
        "continuous_scaling_profile": str(delta_path / "continuous_scaling_profile"),
        "parameter_behavior_primitive_profile": str(delta_path / "parameter_behavior_primitive_profile"),
        "parameter_behavior_profile": str(delta_path / "parameter_behavior_profile"),
        "parameter_event_profile": str(delta_path / "parameter_event_profile"),
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

    canonical_partitions = ["tail_id", "flight_id", "date_utc"]

    def _write_seed_table(df: "DataFrame", path: str) -> None:
        writer = df.write.format(table_format).mode(mode)
        partition_cols = [col for col in canonical_partitions if col in df.columns]
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(path)

    if (
        tail_count == 1
        and flights_per_tail == 1
        and sensor_count == 3
        and timestamp_count == 12
        and step_ms == 100
    ):
        raw_table_df = create_sample_raw_table_df(spark)
    else:
        from pyspark.sql import functions as F

        raw_table_df = (
            raw_input_df.withColumnRenamed("timestamp", "timestamp_utc")
            .withColumn("date_utc", F.to_date("timestamp_utc"))
            .select("tail_id", "flight_id", "timestamp_utc", "parameter_name", "parameter_value", "date_utc")
        )
    profiling_raw_table_df = raw_table_df
    if inject_anomaly:
        from pyspark.sql import functions as F

        anomaly_start = F.to_timestamp(F.lit("2026-02-28 00:00:00.800"))
        raw_table_df = raw_table_df.withColumn(
            "parameter_value",
            F.when(
                (F.col("parameter_name") == F.lit("ENG_TEMP_1"))
                & (F.col("timestamp_utc") >= anomaly_start),
                (F.col("parameter_value").cast("double") + F.lit(100.0)).cast("string"),
            ).otherwise(F.col("parameter_value")),
        )
    _write_seed_table(raw_table_df, paths["raw_telemetry"])
    if include_intermediate_tables:
        profiles = TelemetryProfilingPlan.from_raw_input(profiling_raw_table_df).build()
        _write_seed_table(profiles.scaling_profile.to_dataframe(), paths["continuous_scaling_profile"])
        _write_seed_table(profiles.primitive_profile.to_dataframe(), paths["parameter_behavior_primitive_profile"])
        _write_seed_table(profiles.behavior_profile.to_dataframe(), paths["parameter_behavior_profile"])
        _write_seed_table(
            ParameterEventProfile.from_raw_input(
                raw_table_df,
                datatype_profile_df=profiles.datatype_profile.to_dataframe(),
            ).to_dataframe(),
            paths["parameter_event_profile"],
        )
        _write_seed_table(create_sample_events_df(spark), paths["events"])
        _write_seed_table(create_sample_windows_df(spark), paths["windows"])
        _write_seed_table(create_sample_phase_labels_df(spark), paths["phase_labels"])
        _write_seed_table(create_sample_hierarchy_sensor_map_label_df(spark), paths["hierarchy_sensor_map_label"])
        _write_seed_table(create_sample_phase_windows_df(spark), paths["phase_windows"])
        _write_seed_table(create_sample_scores_df(spark), paths["window_scores_raw"])
        _write_seed_table(create_sample_calibrated_df(spark), paths["window_scores_calibrated"])
    return paths


if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
