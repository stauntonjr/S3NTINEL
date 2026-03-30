"""Shared graph data preparation and internal graph utilities."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import pandas as pd

from libs.io.pandas_spark import coerce_spark_map_like


def prepare_events_df(events_df: pd.DataFrame) -> pd.DataFrame:
    rows = events_df.copy()
    default_text = pd.Series("", index=rows.index, dtype="object")
    rows["tail_id"] = rows.get("tail_id", default_text).astype(str)
    rows["flight_id"] = rows.get("flight_id", default_text).astype(str)
    if "event_seq_id" not in rows.columns:
        raise ValueError("graph builders expect canonical events with event_seq_id; missing columns: event_seq_id")
    rows["parameter_name"] = rows.get("parameter_name", default_text).astype(str)
    rows["event_seq_id"] = pd.to_numeric(rows.get("event_seq_id"), errors="coerce")
    rows["timestamp_utc"] = pd.to_datetime(rows.get("timestamp_utc"), utc=True, errors="coerce")
    rows["event_type_detected"] = rows.get("event_type_detected", default_text).astype(str)
    rows = rows.dropna(subset=["tail_id", "flight_id", "event_seq_id", "parameter_name", "timestamp_utc"])
    rows["event_seq_id"] = rows["event_seq_id"].astype("int64")
    return rows.sort_values(["tail_id", "flight_id", "event_seq_id"], kind="mergesort").reset_index(drop=True)


def prepare_windows_df(windows_df: pd.DataFrame) -> pd.DataFrame:
    rows = windows_df.copy()
    default_text = pd.Series("", index=rows.index, dtype="object")
    rows["tail_id"] = rows.get("tail_id", default_text).astype(str)
    rows["flight_id"] = rows.get("flight_id", default_text).astype(str)
    rows["win_id"] = pd.to_numeric(rows.get("win_id"), errors="coerce").fillna(0).astype(int)
    rows["t_start"] = pd.to_datetime(rows.get("t_start"), utc=True, errors="coerce")
    rows["t_end"] = pd.to_datetime(rows.get("t_end"), utc=True, errors="coerce")
    rows = rows.dropna(subset=["tail_id", "flight_id", "t_start", "t_end"])
    if "date_utc" not in rows.columns:
        rows["date_utc"] = rows["t_start"].dt.date
    return rows.sort_values(["tail_id", "flight_id", "t_start", "win_id"], kind="mergesort").reset_index(drop=True)


def selected_backbone_sensors(backbone_df: pd.DataFrame) -> list[str]:
    if backbone_df.empty:
        return []
    selected = backbone_df.iloc[0].get("selected_sensors_c", [])
    if not isinstance(selected, list):
        return []
    return [str(item) for item in selected if str(item)]


def retain_top_k_undirected(
    rows: list[dict[str, Any]],
    *,
    weight_key: str,
    top_k_per_parameter_name: int,
) -> list[dict[str, Any]]:
    if top_k_per_parameter_name <= 0 or not rows:
        return rows
    by_parameter_name: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_parameter_name[str(row["parameter_name_u"])].append(row)
        by_parameter_name[str(row["parameter_name_v"])].append(row)
    keep: set[tuple[str, str]] = set()
    for parameter_rows in by_parameter_name.values():
        ranked = sorted(
            parameter_rows,
            key=lambda item: (-float(item.get(weight_key, 0.0) or 0.0), item["parameter_name_u"], item["parameter_name_v"]),
        )[:top_k_per_parameter_name]
        for item in ranked:
            keep.add(tuple(sorted((str(item["parameter_name_u"]), str(item["parameter_name_v"])))))
    return [row for row in rows if tuple(sorted((str(row["parameter_name_u"]), str(row["parameter_name_v"])))) in keep]


def retain_top_k_directed(
    rows: list[dict[str, Any]],
    *,
    weight_key: str,
    top_k_outgoing: int,
) -> list[dict[str, Any]]:
    if top_k_outgoing <= 0 or not rows:
        return rows
    by_source: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_source[str(row["parameter_name_u"])].append(row)
    keep: set[tuple[str, str]] = set()
    for source_rows in by_source.values():
        ranked = sorted(
            source_rows,
            key=lambda item: (-float(item.get(weight_key, 0.0) or 0.0), item["parameter_name_u"], item["parameter_name_v"]),
        )[:top_k_outgoing]
        for item in ranked:
            keep.add((str(item["parameter_name_u"]), str(item["parameter_name_v"])))
    return [row for row in rows if (str(row["parameter_name_u"]), str(row["parameter_name_v"])) in keep]


def parameter_name_union_from_window_features(
    window_features_df: pd.DataFrame,
    events_df: pd.DataFrame,
    selected_sensors: list[str],
) -> list[str]:
    return sorted(
        set(
            window_features_df.get("continuous_vector_t_end_scaled", pd.Series(dtype=object))
            .apply(lambda item: list((coerce_spark_map_like(item) or {}).keys()))
            .explode()
            .dropna()
            .astype(str)
            .tolist()
        )
        | set(events_df.get("parameter_name", pd.Series(dtype=object)).dropna().astype(str).tolist())
        | set(selected_sensors)
    )


def parameter_name_union_from_component_tables(
    backbone_df: pd.DataFrame,
    event_df: pd.DataFrame,
    lag_df: pd.DataFrame,
    selected_sensors: list[str],
) -> list[str]:
    backbone_all_sensors: list[str] = []
    if not backbone_df.empty:
        all_sensors = backbone_df.iloc[0].get("all_sensors", [])
        if isinstance(all_sensors, list):
            backbone_all_sensors = [str(item) for item in all_sensors if str(item)]
    return sorted(
        set(backbone_all_sensors)
        | set(event_df.get("parameter_name_u", pd.Series(dtype=object)).dropna().astype(str).tolist())
        | set(event_df.get("parameter_name_v", pd.Series(dtype=object)).dropna().astype(str).tolist())
        | set(lag_df.get("parameter_name_u", pd.Series(dtype=object)).dropna().astype(str).tolist())
        | set(lag_df.get("parameter_name_v", pd.Series(dtype=object)).dropna().astype(str).tolist())
        | set(selected_sensors)
    )
