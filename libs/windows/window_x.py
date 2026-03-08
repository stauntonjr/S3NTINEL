"""Shared `window_x` builders for active V2 Spark and pandas paths."""

from __future__ import annotations

from collections import Counter
from typing import Any

import pandas as pd

from libs.common import AdaptiveWindowRow, DetectedEventRow, WindowXRow
from libs.common.event_types import EventType
from libs.windows.representations import build_continuous_robust_scaler, build_window_x_row

WINDOW_X_SCHEMA = """
tail_id string,
flight_id string,
win_id int,
t_start timestamp,
t_end timestamp,
duration_ms int,
event_count int,
date_utc date,
event_type_counts map<string,int>,
continuous_vector_t_end map<string,double>,
continuous_vector_t_end_scaled map<string,double>,
categorical_state_t_end map<string,string>,
drift_magnitude_profiled double,
phase_label string
"""


def _prepare_raw_telemetry(raw_df: pd.DataFrame) -> pd.DataFrame:
    rows = raw_df.copy()
    if "timestamp_utc" not in rows.columns and "timestamp" in rows.columns:
        rows["timestamp_utc"] = rows["timestamp"]
    if "parameter_name" not in rows.columns and "sensor" in rows.columns:
        rows["parameter_name"] = rows["sensor"]
    if "parameter_value" not in rows.columns and "parameter_value_clean" in rows.columns:
        rows["parameter_value"] = rows["parameter_value_clean"]
    rows["tail_id"] = rows.get("tail_id", "").astype(str)
    rows["flight_id"] = rows.get("flight_id", "").astype(str)
    rows["parameter_name"] = rows.get("parameter_name", "").astype(str)
    rows["timestamp_utc"] = pd.to_datetime(rows.get("timestamp_utc"), utc=True, errors="coerce")
    rows = rows.dropna(subset=["tail_id", "flight_id", "parameter_name", "timestamp_utc"])
    return rows.sort_values(["tail_id", "flight_id", "timestamp_utc", "parameter_name"], kind="mergesort").reset_index(drop=True)


def _prepare_events(events_df: pd.DataFrame) -> pd.DataFrame:
    rows = events_df.copy()
    rows["tail_id"] = rows.get("tail_id", "").astype(str)
    rows["flight_id"] = rows.get("flight_id", "").astype(str)
    if "parameter_name" not in rows.columns and "sensor" in rows.columns:
        rows["parameter_name"] = rows["sensor"]
    if "timestamp_utc" not in rows.columns and "ts" in rows.columns:
        rows["timestamp_utc"] = rows["ts"]
    rows["parameter_name"] = rows.get("parameter_name", "").astype(str)
    rows["timestamp_utc"] = pd.to_datetime(rows.get("timestamp_utc"), utc=True, errors="coerce")
    if "event_type_detected" not in rows.columns:
        rows["event_type_detected"] = ""
    rows["event_type_detected"] = rows["event_type_detected"].astype(str)
    rows = rows.dropna(subset=["tail_id", "flight_id", "parameter_name", "timestamp_utc"])
    rows = rows[rows["event_type_detected"] != EventType.COOCCUR].copy()
    return rows.sort_values(["tail_id", "flight_id", "timestamp_utc", "parameter_name", "event_type_detected"], kind="mergesort").reset_index(drop=True)


def _prepare_windows(windows_df: pd.DataFrame) -> pd.DataFrame:
    rows = windows_df.copy()
    rows["tail_id"] = rows.get("tail_id", "").astype(str)
    rows["flight_id"] = rows.get("flight_id", "").astype(str)
    rows["win_id"] = pd.to_numeric(rows.get("win_id"), errors="coerce").fillna(0).astype(int)
    rows["t_start"] = pd.to_datetime(rows.get("t_start"), utc=True, errors="coerce")
    rows["t_end"] = pd.to_datetime(rows.get("t_end"), utc=True, errors="coerce")
    rows["duration_ms"] = pd.to_numeric(rows.get("duration_ms"), errors="coerce").fillna(0).astype(int)
    rows["event_count"] = pd.to_numeric(rows.get("event_count"), errors="coerce").fillna(0).astype(int)
    rows = rows.dropna(subset=["tail_id", "flight_id", "t_start", "t_end"])
    if "date_utc" not in rows.columns:
        rows["date_utc"] = rows["t_start"].dt.date
    return rows.sort_values(["tail_id", "flight_id", "t_start", "win_id"], kind="mergesort").reset_index(drop=True)


def _build_window_context_rows(
    raw_df: pd.DataFrame,
    events_df: pd.DataFrame,
    windows_df: pd.DataFrame,
) -> list[AdaptiveWindowRow]:
    raw_by_flight = {
        key: group.sort_values(["timestamp_utc", "parameter_name"], kind="mergesort").reset_index(drop=True)
        for key, group in raw_df.groupby(["tail_id", "flight_id"], sort=True)
    }
    events_by_flight = {
        key: group.sort_values(["timestamp_utc", "parameter_name", "event_type_detected"], kind="mergesort").reset_index(drop=True)
        for key, group in events_df.groupby(["tail_id", "flight_id"], sort=True)
    }

    out: list[AdaptiveWindowRow] = []
    for (tail_id, flight_id), group in windows_df.groupby(["tail_id", "flight_id"], sort=True):
        raw_rows = raw_by_flight.get((tail_id, flight_id), pd.DataFrame())
        event_rows = events_by_flight.get((tail_id, flight_id), pd.DataFrame())

        raw_idx = 0
        event_idx = 0
        zoh_snapshot: dict[str, Any] = {}
        raw_len = len(raw_rows)
        event_len = len(event_rows)

        windows_ordered = group.sort_values(["t_end", "win_id"], kind="mergesort")
        for window in windows_ordered.to_dict(orient="records"):
            t_end = pd.to_datetime(window["t_end"], utc=True)
            t_start = pd.to_datetime(window["t_start"], utc=True)

            while raw_idx < raw_len:
                row = raw_rows.iloc[raw_idx]
                timestamp_utc = pd.to_datetime(row["timestamp_utc"], utc=True)
                if timestamp_utc > t_end:
                    break
                zoh_snapshot[str(row["parameter_name"])] = row.get("parameter_value")
                raw_idx += 1

            window_events: list[DetectedEventRow] = []
            event_type_counts: Counter[str] = Counter()
            while event_idx < event_len:
                row = event_rows.iloc[event_idx]
                timestamp_utc = pd.to_datetime(row["timestamp_utc"], utc=True)
                if timestamp_utc > t_end:
                    break
                if timestamp_utc >= t_start:
                    payload = row.get("payload")
                    event = {
                        "tail_id": tail_id,
                        "flight_id": flight_id,
                        "parameter_name": str(row.get("parameter_name", "")),
                        "timestamp_utc": timestamp_utc.to_pydatetime(),
                        "event_type_detected": str(row.get("event_type_detected", "")),
                        "payload": payload if isinstance(payload, dict) else {},
                    }
                    window_events.append(event)
                    event_type_counts[event["event_type_detected"]] += 1
                event_idx += 1

            enriched = dict(window)
            enriched["zoh_snapshot"] = dict(zoh_snapshot)
            enriched["event_type_counts"] = dict(event_type_counts)
            enriched["window_events"] = window_events
            out.append(enriched)
    return out


def build_window_x_table(raw_df: pd.DataFrame, events_df: pd.DataFrame, windows_df: pd.DataFrame) -> pd.DataFrame:
    raw_rows = _prepare_raw_telemetry(raw_df)
    event_rows = _prepare_events(events_df)
    window_rows = _prepare_windows(windows_df)
    if raw_rows.empty or window_rows.empty:
        return pd.DataFrame()

    scaler_by_sensor = build_continuous_robust_scaler(raw_rows)
    window_contexts = _build_window_context_rows(raw_rows, event_rows, window_rows)
    previous_scaled_by_flight: dict[tuple[str, str], dict[str, float]] = {}
    window_x_rows: list[WindowXRow] = []
    for window in window_contexts:
        window_x_rows.append(
            build_window_x_row(
                window=window,
                window_events=list(window.get("window_events", [])),
                scaler_by_sensor=scaler_by_sensor,
                previous_scaled_by_flight=previous_scaled_by_flight,
                phase_label=None,
            )
        )
    return pd.DataFrame(window_x_rows)


def build_window_x_spark_table(raw_df: "DataFrame", events_df: "DataFrame", windows_df: "DataFrame") -> "DataFrame":
    """Build `window_x` in Spark with per-flight grouped pandas execution."""
    from pyspark.sql import functions as F

    raw_columns = set(raw_df.columns)
    if "parameter_value" in raw_columns:
        raw_value_expr = F.col("parameter_value").cast("string")
    elif "parameter_value_clean" in raw_columns:
        raw_value_expr = F.col("parameter_value_clean").cast("string")
    else:
        raw_value_expr = F.lit(None).cast("string")

    raw_rows = raw_df.select(
        "tail_id",
        "flight_id",
        F.lit("raw").alias("row_type"),
        F.col("timestamp_utc"),
        F.lit(None).cast("int").alias("win_id"),
        F.lit(None).cast("timestamp").alias("t_start"),
        F.lit(None).cast("timestamp").alias("t_end"),
        F.lit(None).cast("int").alias("duration_ms"),
        F.lit(None).cast("int").alias("event_count"),
        F.col("parameter_name"),
        raw_value_expr.alias("parameter_value"),
        F.lit(None).cast("string").alias("event_type_detected"),
        F.expr("cast(null as map<string,string>)").alias("payload"),
    )
    event_rows = events_df.select(
        "tail_id",
        "flight_id",
        F.lit("event").alias("row_type"),
        F.col("timestamp_utc"),
        F.lit(None).cast("int").alias("win_id"),
        F.lit(None).cast("timestamp").alias("t_start"),
        F.lit(None).cast("timestamp").alias("t_end"),
        F.lit(None).cast("int").alias("duration_ms"),
        F.lit(None).cast("int").alias("event_count"),
        F.col("parameter_name"),
        F.lit(None).cast("string").alias("parameter_value"),
        F.col("event_type_detected"),
        F.col("payload").cast("map<string,string>").alias("payload"),
    )
    window_rows = windows_df.select(
        "tail_id",
        "flight_id",
        F.lit("window").alias("row_type"),
        F.col("t_end").alias("timestamp_utc"),
        F.col("win_id"),
        F.col("t_start"),
        F.col("t_end"),
        F.col("duration_ms"),
        F.col("event_count"),
        F.lit(None).cast("string").alias("parameter_name"),
        F.lit(None).cast("string").alias("parameter_value"),
        F.lit(None).cast("string").alias("event_type_detected"),
        F.expr("cast(null as map<string,string>)").alias("payload"),
    )
    flight_context_df = raw_rows.unionByName(event_rows).unionByName(window_rows)

    def _emit_window_x(group_pdf: pd.DataFrame) -> pd.DataFrame:
        raw_pdf = group_pdf[group_pdf["row_type"] == "raw"][
            ["tail_id", "flight_id", "timestamp_utc", "parameter_name", "parameter_value"]
        ].copy()
        event_pdf = group_pdf[group_pdf["row_type"] == "event"][
            ["tail_id", "flight_id", "timestamp_utc", "parameter_name", "event_type_detected", "payload"]
        ].copy()
        window_pdf = group_pdf[group_pdf["row_type"] == "window"][
            ["tail_id", "flight_id", "win_id", "t_start", "t_end", "duration_ms", "event_count"]
        ].copy()
        return build_window_x_table(raw_pdf, event_pdf, window_pdf)

    return flight_context_df.groupBy("tail_id", "flight_id").applyInPandas(_emit_window_x, schema=WINDOW_X_SCHEMA)
