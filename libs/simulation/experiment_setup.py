from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd

from libs.simulation.flight_simulator import iter_single_flight_row_events
from libs.simulation.delay_engine import (
    _normalize_corr_group_key as _delay_normalize_corr_group_key,
)
from libs.simulation.delay_engine import (
    _resolve_delay_map_for_groups as _delay_resolve_delay_map_for_groups,
)
from libs.simulation.event_labeling import (
    _attach_event_labels_to_telemetry_df as _event_attach_event_labels_to_telemetry_df,
)
from libs.simulation.event_labeling import (
    _attach_event_labels_to_telemetry_rows as _event_attach_event_labels_to_telemetry_rows,
)
from libs.simulation.event_labeling import (
    _detect_sensor_event_labels_df as _event_detect_sensor_event_labels_df,
)
from libs.simulation.event_labeling import _value_col as _event_value_col
from libs.simulation.setup_builders import (
    build_default_sensor_behavior as _builders_build_default_sensor_behavior,
)
from libs.simulation.setup_builders import build_fleet_manifest as _builders_build_fleet_manifest
from libs.simulation.setup_builders import build_mermaid_hierarchy as _builders_build_mermaid_hierarchy
from libs.simulation.setup_builders import build_tail_profiles as _builders_build_tail_profiles
from libs.simulation.setup_builders import default_phase_definitions as _builders_default_phase_definitions
from libs.simulation.setup_builders import flatten_hierarchy_spec as _builders_flatten_hierarchy_spec


_SIM_ROW_CHUNK_SIZE = 50000


def _normalize_corr_group_key(value: object) -> str:
    return _delay_normalize_corr_group_key(value)


def _resolve_delay_map_for_groups(delay_map_raw: dict, corr_groups: list[str]) -> tuple[dict[str, float], list[str]]:
    return _delay_resolve_delay_map_for_groups(delay_map_raw, corr_groups)


def flatten_hierarchy_spec(hierarchy_spec: dict) -> pd.DataFrame:
    return _builders_flatten_hierarchy_spec(hierarchy_spec)


def build_mermaid_hierarchy(hierarchy_df: pd.DataFrame, max_sensors: int = 200) -> str:
    return _builders_build_mermaid_hierarchy(hierarchy_df, max_sensors=max_sensors)


def build_default_sensor_behavior(hierarchy_df: pd.DataFrame) -> dict[str, dict]:
    return _builders_build_default_sensor_behavior(hierarchy_df)


def default_phase_definitions() -> list[dict]:
    return _builders_default_phase_definitions()


def build_tail_profiles(systems: list[str], m_tails: int, rng: np.random.Generator) -> list[dict]:
    return _builders_build_tail_profiles(systems, m_tails=m_tails, rng=rng)


def build_fleet_manifest(
    tail_profiles: list[dict],
    n_flights_per_tail: int,
    rng: np.random.Generator,
    start_ts: datetime | None = None,
) -> pd.DataFrame:
    return _builders_build_fleet_manifest(
        tail_profiles,
        n_flights_per_tail=n_flights_per_tail,
        rng=rng,
        start_ts=start_ts,
    )


def _value_col(df: pd.DataFrame) -> str | None:
    return _event_value_col(df)


def _detect_sensor_event_labels_df(telemetry_df: pd.DataFrame, *, value_col: str | None = None) -> pd.DataFrame:
    return _event_detect_sensor_event_labels_df(telemetry_df, value_col=value_col)


def _attach_event_labels_to_telemetry_df(telemetry_df: pd.DataFrame, *, label_value_col: str = "parameter_value_clean") -> pd.DataFrame:
    return _event_attach_event_labels_to_telemetry_df(telemetry_df, label_value_col=label_value_col)


def _attach_event_labels_to_telemetry_rows(telemetry_rows: list[dict], *, label_value_col: str = "parameter_value_clean") -> list[dict]:
    return _event_attach_event_labels_to_telemetry_rows(telemetry_rows, label_value_col=label_value_col)


def simulate_fleet_dataset(
    *,
    hierarchy_df: pd.DataFrame,
    sensor_behavior: dict[str, dict],
    phase_definitions: list[dict],
    flight_setup: dict,
    tail_profiles: list[dict],
    fleet_manifest_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    phase_map = {item["phase_name"]: item for item in phase_definitions}

    telemetry_chunks: list[pd.DataFrame] = []
    phase_chunks: list[pd.DataFrame] = []
    telemetry_buffer: list[dict] = []
    phase_buffer: list[dict] = []
    chunk_rows = int(max(int(flight_setup.get("row_chunk_size", _SIM_ROW_CHUNK_SIZE)), 1))

    tails_by_id = {item["tail_id"]: item for item in tail_profiles}
    sensors_in_order = [str(sensor) for sensor in hierarchy_df["sensor"].tolist()]
    sensors_by_group: dict[str, list[str]] = {}
    for sensor_name in sensors_in_order:
        corr_group_name = str(sensor_behavior[sensor_name]["corr_group"])
        sensors_by_group.setdefault(corr_group_name, []).append(sensor_name)
    reported_unknown_delay_keys: set[str] = set()
    for _, flight_row in fleet_manifest_df.iterrows():
        tail_profile = tails_by_id[str(flight_row["tail_id"])]
        for row_type, row_payload in iter_single_flight_row_events(
            hierarchy_df=hierarchy_df,
            sensor_behavior=sensor_behavior,
            flight_setup=flight_setup,
            phase_map=phase_map,
            tail_profile=tail_profile,
            flight_row=flight_row,
            sensors_in_order=sensors_in_order,
            sensors_by_group=sensors_by_group,
            reported_unknown_delay_keys=reported_unknown_delay_keys,
            warning_prefix="[simulate_fleet_dataset]",
            timestamp_mode="py",
        ):
            if row_type == "telemetry":
                telemetry_buffer.append(row_payload)
                if len(telemetry_buffer) >= chunk_rows:
                    telemetry_chunks.append(pd.DataFrame.from_records(telemetry_buffer))
                    telemetry_buffer = []
            else:
                phase_buffer.append(row_payload)
                if len(phase_buffer) >= chunk_rows:
                    phase_chunks.append(pd.DataFrame.from_records(phase_buffer))
                    phase_buffer = []

    if telemetry_buffer:
        telemetry_chunks.append(pd.DataFrame.from_records(telemetry_buffer))
    if phase_buffer:
        phase_chunks.append(pd.DataFrame.from_records(phase_buffer))

    telemetry_df = pd.concat(telemetry_chunks, ignore_index=True)
    phase_labels_df = pd.concat(phase_chunks, ignore_index=True)
    telemetry_df["timestamp_utc"] = pd.to_datetime(telemetry_df["timestamp_utc"], utc=True)
    telemetry_df = _attach_event_labels_to_telemetry_df(telemetry_df, label_value_col="parameter_value_clean")
    phase_labels_df["timestamp_utc"] = pd.to_datetime(phase_labels_df["timestamp_utc"], utc=True)
    return telemetry_df, phase_labels_df


def simulate_fleet_dataset_spark(
    *,
    spark: "SparkSession",
    hierarchy_df: pd.DataFrame,
    sensor_behavior: dict[str, dict],
    phase_definitions: list[dict],
    flight_setup: dict,
    tail_profiles: list[dict],
    fleet_manifest_df: pd.DataFrame,
) -> tuple["DataFrame", "DataFrame"]:
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    phase_map = {item["phase_name"]: item for item in phase_definitions}
    telemetry_schema = StructType(
        [
            StructField("tail_id", StringType(), True),
            StructField("flight_id", StringType(), True),
            StructField("timestamp_utc", TimestampType(), True),
            StructField("system_id", StringType(), True),
            StructField("subsystem_id", StringType(), True),
            StructField("module_id", StringType(), True),
            StructField("sensor", StringType(), True),
            StructField("parameter_name", StringType(), True),
            StructField("parameter_datatype", StringType(), True),
            StructField("parameter_value", StringType(), True),
            StructField("parameter_value_clean", StringType(), True),
            StructField("phase_id_detected", LongType(), True),
            StructField("phase_name", StringType(), True),
            StructField("event_type_label", StringType(), True),
            StructField("anomaly_type_label", StringType(), True),
            StructField("anomaly_score_label", DoubleType(), True),
            StructField("date_utc", StringType(), True),
        ]
    )
    phase_schema = StructType(
        [
            StructField("tail_id", StringType(), True),
            StructField("flight_id", StringType(), True),
            StructField("timestamp_utc", TimestampType(), True),
            StructField("phase_id_detected", LongType(), True),
            StructField("phase_name", StringType(), True),
        ]
    )

    telemetry_sdf: DataFrame | None = None
    phase_labels_sdf: DataFrame | None = None
    chunk_rows = int(max(int(flight_setup.get("row_chunk_size", _SIM_ROW_CHUNK_SIZE)), 1))

    telemetry_order = [field.name for field in telemetry_schema.fields]

    def _spark_value(value: object) -> object:
        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()
        return value

    def _union_chunks(
        current_sdf: DataFrame | None,
        rows: list[tuple],
        schema: "StructType",
    ) -> DataFrame | None:
        if not rows:
            return current_sdf
        sdf = spark.createDataFrame(rows, schema=schema)
        return sdf if current_sdf is None else current_sdf.unionByName(sdf)

    tails_by_id = {item["tail_id"]: item for item in tail_profiles}
    sensors_in_order = [str(sensor) for sensor in hierarchy_df["sensor"].tolist()]
    sensors_by_group: dict[str, list[str]] = {}
    for sensor_name in sensors_in_order:
        corr_group_name = str(sensor_behavior[sensor_name]["corr_group"])
        sensors_by_group.setdefault(corr_group_name, []).append(sensor_name)
    reported_unknown_delay_keys: set[str] = set()
    for _, flight_row in fleet_manifest_df.iterrows():
        tail_profile = tails_by_id[str(flight_row["tail_id"])]
        telemetry_rows: list[dict] = []
        phase_rows_tuple_chunk: list[tuple] = []
        for row_type, row_payload in iter_single_flight_row_events(
            hierarchy_df=hierarchy_df,
            sensor_behavior=sensor_behavior,
            flight_setup=flight_setup,
            phase_map=phase_map,
            tail_profile=tail_profile,
            flight_row=flight_row,
            sensors_in_order=sensors_in_order,
            sensors_by_group=sensors_by_group,
            reported_unknown_delay_keys=reported_unknown_delay_keys,
            warning_prefix="[simulate_fleet_dataset_spark]",
            timestamp_mode="py",
        ):
            if row_type == "telemetry":
                telemetry_rows.append(row_payload)
            else:
                phase_rows_tuple_chunk.append(
                    (
                        str(row_payload["tail_id"]),
                        str(row_payload["flight_id"]),
                        row_payload["timestamp_utc"],
                        int(row_payload["phase_id"]),
                        str(row_payload["phase_name"]),
                    )
                )
                if len(phase_rows_tuple_chunk) >= chunk_rows:
                    phase_labels_sdf = _union_chunks(phase_labels_sdf, phase_rows_tuple_chunk, phase_schema)
                    phase_rows_tuple_chunk = []

        phase_labels_sdf = _union_chunks(phase_labels_sdf, phase_rows_tuple_chunk, phase_schema)

        telemetry_records_chunk: list[tuple] = []
        labeled_telemetry_rows = _attach_event_labels_to_telemetry_rows(telemetry_rows, label_value_col="parameter_value_clean")
        for row in labeled_telemetry_rows:
            spark_record: list[object] = []
            for col in telemetry_order:
                value = row.get(col)
                if col == "timestamp_utc":
                    value = pd.to_datetime(value, utc=True, errors="coerce")
                    if isinstance(value, pd.Timestamp):
                        value = value.tz_localize(None)
                spark_record.append(_spark_value(value))
            telemetry_records_chunk.append(tuple(spark_record))
            if len(telemetry_records_chunk) >= chunk_rows:
                telemetry_sdf = _union_chunks(telemetry_sdf, telemetry_records_chunk, telemetry_schema)
                telemetry_records_chunk = []
        telemetry_sdf = _union_chunks(telemetry_sdf, telemetry_records_chunk, telemetry_schema)

    if telemetry_sdf is None:
        telemetry_sdf = spark.createDataFrame([], schema=telemetry_schema)
    if phase_labels_sdf is None:
        phase_labels_sdf = spark.createDataFrame([], schema=phase_schema)

    return telemetry_sdf, phase_labels_sdf


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
