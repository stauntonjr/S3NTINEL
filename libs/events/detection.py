"""Common streaming event detection generator with thin pandas/spark adapters."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Iterable, Generator

import pandas as pd

from libs.common import DetectedEventRow, SensorDataType, TelemetryRow, normalize_sensor_datatype
from libs.events.categorical import CategoricalDetectorConfig, CategoricalSample, detect_categorical_events_stream
from libs.events.cooccur import CooccurrenceDetectorConfig, detect_cooccurrence_events_stream
from libs.events.extrema import ContinuousDetectorConfig, ContinuousSample, detect_continuous_events_stream
from libs.common.event_types import EventType


def _row_timestamp_utc(row: TelemetryRow) -> datetime:
    return pd.to_datetime(row.get("timestamp_utc"), utc=True).to_pydatetime()


def _row_datatype_for_detection(row: TelemetryRow) -> str:
    # Canonical precedence: label first, then profiled fallback.
    for field in ("parameter_datatype_label", "parameter_datatype_profiled", "parameter_datatype"):
        value = row.get(field)
        text = str(value).strip().lower() if value is not None else ""
        if text and text not in {"none", "null", "nan"}:
            return normalize_sensor_datatype(value)
    return SensorDataType.UNKNOWN.value


def detect_events_from_rows(
    telemetry_rows: Iterable[TelemetryRow],
    *,
    continuous_config: ContinuousDetectorConfig | None = None,
    categorical_config: CategoricalDetectorConfig | None = None,
    cooccur_config: CooccurrenceDetectorConfig | None = None,
    include_cooccur: bool = True,
) -> Generator[DetectedEventRow, None, None]:
    rows_sorted = sorted(
        [dict(row) for row in telemetry_rows],
        key=lambda row: (
            str(row.get("tail_id", "")),
            str(row.get("flight_id", "")),
            str(row.get("parameter_name", row.get("sensor", ""))),
            _row_timestamp_utc(row),
        ),
    )

    continuous_samples: list[ContinuousSample] = []
    categorical_samples: list[CategoricalSample] = []

    for row in rows_sorted:
        dtype = _row_datatype_for_detection(row)
        tail_id = str(row.get("tail_id", ""))
        flight_id = str(row.get("flight_id", ""))
        parameter_name = str(row.get("parameter_name", row.get("sensor", "")))
        timestamp_utc = _row_timestamp_utc(row)

        if dtype == SensorDataType.NUMERIC.value:
            # Downstream detectors should consume the observed signal. The clean
            # value is retained for simulation labels/validation only.
            value = row.get("parameter_value", None)
            if value is None or pd.isna(value):
                value = row.get("parameter_value_clean", None)
            if value is None or pd.isna(value):
                value_num = None
            else:
                try:
                    value_num = float(value)
                except Exception:
                    value_num = None
            continuous_samples.append(ContinuousSample(tail_id=tail_id, flight_id=flight_id, sensor=parameter_name, ts=timestamp_utc, value=value_num))
        else:
            state = row.get("parameter_value")
            state_text = None if state is None or pd.isna(state) else str(state)
            categorical_samples.append(CategoricalSample(tail_id=tail_id, flight_id=flight_id, sensor=parameter_name, ts=timestamp_utc, state=state_text))

    # Detected events from detectors only (no label passthrough)
    detected_events: list[DetectedEventRow] = []
    if continuous_samples:
        detected_events.extend(list(detect_continuous_events_stream(continuous_samples, config=continuous_config)))
    if categorical_samples:
        detected_events.extend(list(detect_categorical_events_stream(categorical_samples, config=categorical_config)))

    for ev in detected_events:
        ev["anomaly_type_detected"] = None
        ev["anomaly_score_detected"] = None

    # Build cooccurrence from detected events only
    cooccur_events: list[DetectedEventRow] = []
    if include_cooccur:
        cooccur_events.extend(list(detect_cooccurrence_events_stream(detected_events, config=cooccur_config if cooccur_config is not None else CooccurrenceDetectorConfig())))
        for ev in cooccur_events:
            ev["anomaly_type_detected"] = None
            ev["anomaly_score_detected"] = None

    all_events = detected_events + cooccur_events

    for event in all_events:
        if "parameter_name" not in event and "sensor" in event:
            event["parameter_name"] = event["sensor"]
        if "timestamp_utc" not in event and "ts" in event:
            event["timestamp_utc"] = event["ts"]

    all_events = sorted(
        all_events,
        key=lambda event: (
            str(event.get("tail_id", "")),
            str(event.get("flight_id", "")),
            pd.to_datetime(event.get("timestamp_utc"), utc=True),
            str(event.get("parameter_name", "")),
            str(event.get("event_type_detected", "")),
        ),
    )

    for ev in all_events:
        yield ev


def detect_events_from_pandas(
    telemetry_df: pd.DataFrame,
    *,
    include_cooccur: bool = True,
) -> pd.DataFrame:
    if telemetry_df.empty:
        return pd.DataFrame(
            columns=[
                "tail_id",
                "flight_id",
                "parameter_name",
                "timestamp_utc",
                "event_type_detected",
                "anomaly_type_detected",
                "anomaly_score_detected",
                "payload",
            ]
        )

    events = detect_events_from_rows(
        telemetry_df.to_dict(orient="records"),
        include_cooccur=include_cooccur,
    )

    if not events:
        return pd.DataFrame(
            columns=[
                "tail_id",
                "flight_id",
                "parameter_name",
                "timestamp_utc",
                "event_type_detected",
                "anomaly_type_detected",
                "anomaly_score_detected",
                "payload",
            ]
        )

    out = pd.DataFrame(events)
    out["timestamp_utc"] = pd.to_datetime(out["timestamp_utc"], utc=True)
    out["event_type_detected"] = out["event_type_detected"].fillna(EventType.NONE).astype(str)
    out["anomaly_type_detected"] = out.get("anomaly_type_detected", None)
    out["anomaly_score_detected"] = out.get("anomaly_score_detected", None)
    out["payload"] = out["payload"].apply(lambda item: item if isinstance(item, dict) else {})
    return out


def build_events_spark(
    telemetry_df: "DataFrame",
    *,
    include_cooccur: bool = True,
) -> "DataFrame":
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    def _col_or_null(name: str):
        if name in telemetry_df.columns:
            return F.col(name).cast("string").alias(name)
        return F.lit(None).cast("string").alias(name)

    source = telemetry_df.select(
        F.col("tail_id").cast("string").alias("tail_id"),
        F.col("flight_id").cast("string").alias("flight_id"),
        F.col("timestamp_utc").cast("timestamp").alias("timestamp_utc"),
        F.col("parameter_name").cast("string").alias("parameter_name"),
        _col_or_null("parameter_datatype_label"),
        _col_or_null("parameter_datatype_profiled"),
        _col_or_null("parameter_datatype"),
        F.col("parameter_value").cast("string").alias("parameter_value"),
        F.col("parameter_value_clean").cast("string").alias("parameter_value_clean"),
        F.col("parameter_value").cast("string").alias("parameter_value"),
        F.col("date_utc").cast("string").alias("date_utc"),
    )

    schema = T.StructType(
        [
            T.StructField("tail_id", T.StringType(), False),
            T.StructField("flight_id", T.StringType(), False),
            T.StructField("parameter_name", T.StringType(), False),
            T.StructField("timestamp_utc", T.TimestampType(), True),
            T.StructField("event_type_detected", T.StringType(), True),
            T.StructField("anomaly_type_detected", T.StringType(), True),
            T.StructField("anomaly_score_detected", T.DoubleType(), True),
            T.StructField("payload_json", T.StringType(), True),
            T.StructField("date_utc", T.StringType(), True),
        ]
    )

    def _emit(pdf: pd.DataFrame) -> pd.DataFrame:
        if pdf.empty:
            return pd.DataFrame(
                columns=[
                    "tail_id",
                    "flight_id",
                    "parameter_name",
                    "timestamp_utc",
                    "event_type_detected",
                    "anomaly_type_detected",
                    "anomaly_score_detected",
                    "payload_json",
                    "date_utc",
                ]
            )

        events = detect_events_from_rows(pdf.to_dict(orient="records"), include_cooccur=include_cooccur)
        out_rows: list[dict[str, Any]] = []
        for event in events:
            payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
            out_rows.append(
                {
                    "tail_id": str(event.get("tail_id", "")),
                    "flight_id": str(event.get("flight_id", "")),
                    "parameter_name": str(event.get("parameter_name", event.get("sensor", ""))),
                    "timestamp_utc": pd.to_datetime(event.get("timestamp_utc", event.get("ts")), utc=True).tz_localize(None),
                    "event_type_detected": str(event.get("event_type_detected", EventType.NONE)),
                    "anomaly_type_detected": None if event.get("anomaly_type_detected") is None else str(event.get("anomaly_type_detected")),
                    "anomaly_score_detected": None if event.get("anomaly_score_detected") is None else float(event.get("anomaly_score_detected")),
                    "payload_json": json.dumps({str(k): str(v) for k, v in payload.items()}, separators=(",", ":")),
                    "date_utc": str(pd.to_datetime(event.get("timestamp_utc", event.get("ts")), utc=True).date()),
                }
            )
        return pd.DataFrame(out_rows)

    grouped = source.groupBy("tail_id", "flight_id").applyInPandas(_emit, schema=schema)
    map_schema = T.MapType(T.StringType(), T.StringType(), valueContainsNull=True)
    return grouped.withColumn("payload", F.from_json(F.col("payload_json"), map_schema)).drop("payload_json")


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
