"""Graph artifact builders for Spark fitting stages.

Edge-weight semantics:
- precision_graph: absolute partial correlation
- event_graph: positive normalized PMI over same-window co-occurrence
- lag_graph: row-normalized lagged conditional probability, discounted by mean lag
- transition_graph: row-normalized immediate transition probability
"""

from __future__ import annotations

from collections import Counter, defaultdict, deque
from datetime import timedelta
from math import log
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

from libs.graph.hierarchy import assign_hierarchy_from_weighted_edges

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def _spark_functions():
    from pyspark.sql import functions as F

    return F


def _event_graph_schema():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("sensor_u", T.StringType(), False),
            T.StructField("sensor_v", T.StringType(), False),
            T.StructField("cooccur_count", T.IntegerType(), False),
            T.StructField("event_weight", T.DoubleType(), False),
            T.StructField("edge_family", T.StringType(), False),
        ]
    )


def _lag_graph_schema():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("sensor_u", T.StringType(), False),
            T.StructField("sensor_v", T.StringType(), False),
            T.StructField("lag_count", T.IntegerType(), False),
            T.StructField("lag_weight", T.DoubleType(), False),
            T.StructField("mean_lag_seconds", T.DoubleType(), False),
            T.StructField("edge_family", T.StringType(), False),
        ]
    )


def _transition_graph_schema():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("sensor_u", T.StringType(), False),
            T.StructField("sensor_v", T.StringType(), False),
            T.StructField("precedence_count", T.IntegerType(), False),
            T.StructField("precedence_weight", T.DoubleType(), False),
            T.StructField("edge_family", T.StringType(), False),
        ]
    )


def _retain_top_k_undirected(
    rows: list[dict[str, Any]],
    *,
    weight_key: str,
    top_k_per_sensor: int,
) -> list[dict[str, Any]]:
    if top_k_per_sensor <= 0 or not rows:
        return rows
    by_sensor: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_sensor[str(row["sensor_u"])].append(row)
        by_sensor[str(row["sensor_v"])].append(row)

    keep: set[tuple[str, str]] = set()
    for parameter_name, parameter_rows in by_sensor.items():
        ranked = sorted(
            parameter_rows,
            key=lambda item: (-float(item.get(weight_key, 0.0) or 0.0), item["sensor_u"], item["sensor_v"]),
        )[:top_k_per_sensor]
        for item in ranked:
            keep.add(tuple(sorted((str(item["sensor_u"]), str(item["sensor_v"])))))
    return [row for row in rows if tuple(sorted((str(row["sensor_u"]), str(row["sensor_v"])))) in keep]


def _retain_top_k_directed(
    rows: list[dict[str, Any]],
    *,
    weight_key: str,
    top_k_outgoing: int,
) -> list[dict[str, Any]]:
    if top_k_outgoing <= 0 or not rows:
        return rows
    by_source: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_source[str(row["sensor_u"])].append(row)
    keep: set[tuple[str, str]] = set()
    for sensor_u, sensor_rows in by_source.items():
        ranked = sorted(
            sensor_rows,
            key=lambda item: (-float(item.get(weight_key, 0.0) or 0.0), item["sensor_u"], item["sensor_v"]),
        )[:top_k_outgoing]
        for item in ranked:
            keep.add((str(item["sensor_u"]), str(item["sensor_v"])))
    return [row for row in rows if (str(row["sensor_u"]), str(row["sensor_v"])) in keep]


def retain_event_graph_top_k(event_df: pd.DataFrame, *, top_k_per_sensor: int) -> pd.DataFrame:
    """Retain the top-k undirected event edges per sensor from a precomputed event graph."""
    if event_df.empty:
        return event_df.copy()
    rows = _retain_top_k_undirected(
        event_df.to_dict(orient="records"),
        weight_key="event_weight",
        top_k_per_sensor=int(top_k_per_sensor),
    )
    if not rows:
        return pd.DataFrame(columns=event_df.columns)
    return pd.DataFrame(rows, columns=event_df.columns)


def retain_lag_graph_top_k(lag_df: pd.DataFrame, *, top_k_outgoing: int) -> pd.DataFrame:
    """Retain the top-k directed lag edges per source from a precomputed lag graph."""
    if lag_df.empty:
        return lag_df.copy()
    rows = _retain_top_k_directed(
        lag_df.to_dict(orient="records"),
        weight_key="lag_weight",
        top_k_outgoing=int(top_k_outgoing),
    )
    if not rows:
        return pd.DataFrame(columns=lag_df.columns)
    return pd.DataFrame(rows, columns=lag_df.columns)


def _invert_small_matrix(matrix: np.ndarray) -> np.ndarray:
    n = int(matrix.shape[0])
    aug = np.concatenate([matrix.astype(float).copy(), np.eye(n, dtype=float)], axis=1)
    for pivot_idx in range(n):
        best_row = pivot_idx
        best_abs = abs(float(aug[pivot_idx, pivot_idx]))
        for row_idx in range(pivot_idx + 1, n):
            cand = abs(float(aug[row_idx, pivot_idx]))
            if cand > best_abs:
                best_row = row_idx
                best_abs = cand
        if best_row != pivot_idx:
            aug[[pivot_idx, best_row], :] = aug[[best_row, pivot_idx], :]
        pivot = float(aug[pivot_idx, pivot_idx])
        if abs(pivot) <= 1e-12:
            pivot = 1e-12
            aug[pivot_idx, pivot_idx] = pivot
        aug[pivot_idx, :] = aug[pivot_idx, :] / pivot
        for row_idx in range(n):
            if row_idx == pivot_idx:
                continue
            factor = float(aug[row_idx, pivot_idx])
            if abs(factor) <= 1e-18:
                continue
            aug[row_idx, :] = aug[row_idx, :] - (factor * aug[pivot_idx, :])
    return aug[:, n:]


def _prepare_events(events_df: pd.DataFrame) -> pd.DataFrame:
    rows = events_df.copy()
    default_text = pd.Series("", index=rows.index, dtype="object")
    rows["tail_id"] = rows.get("tail_id", default_text).astype(str)
    rows["flight_id"] = rows.get("flight_id", default_text).astype(str)
    if "parameter_name" not in rows.columns and "sensor" in rows.columns:
        rows["parameter_name"] = rows["sensor"]
    if "timestamp_utc" not in rows.columns and "ts" in rows.columns:
        rows["timestamp_utc"] = rows["ts"]
    rows["parameter_name"] = rows.get("parameter_name", default_text).astype(str)
    rows["timestamp_utc"] = pd.to_datetime(rows.get("timestamp_utc"), utc=True, errors="coerce")
    rows["event_type_detected"] = rows.get("event_type_detected", default_text).astype(str)
    rows = rows.dropna(subset=["tail_id", "flight_id", "parameter_name", "timestamp_utc"])
    rows = rows[rows["event_type_detected"] != "cooccur"].copy()
    return rows.sort_values(["tail_id", "flight_id", "timestamp_utc", "parameter_name"], kind="mergesort").reset_index(drop=True)


def _prepare_windows(windows_df: pd.DataFrame) -> pd.DataFrame:
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


def _selected_backbone_sensors(backbone_df: pd.DataFrame) -> list[str]:
    if backbone_df.empty:
        return []
    selected = backbone_df.iloc[0].get("selected_sensors_c", [])
    if not isinstance(selected, list):
        return []
    return [str(item) for item in selected if str(item)]


def _build_precision_graph_from_covariance(
    selected_sensors: list[str],
    covariance: np.ndarray,
    *,
    ridge_lambda: float,
    min_abs_partial_corr: float,
) -> pd.DataFrame:
    if not selected_sensors:
        return pd.DataFrame(columns=["sensor_u", "sensor_v", "partial_corr", "precision_weight", "edge_family"])
    if covariance.size == 0 or covariance.shape[0] != len(selected_sensors):
        return pd.DataFrame(columns=["sensor_u", "sensor_v", "partial_corr", "precision_weight", "edge_family"])
    theta = _invert_small_matrix(covariance + (max(float(ridge_lambda), 1e-6) * np.eye(covariance.shape[0], dtype=float)))
    out: list[dict[str, Any]] = []
    for i, sensor_u in enumerate(selected_sensors):
        for j in range(i + 1, len(selected_sensors)):
            sensor_v = selected_sensors[j]
            denom = max(theta[i, i] * theta[j, j], 1e-12) ** 0.5
            partial_corr = float(0.0 if denom <= 0 else (-theta[i, j] / denom))
            weight = abs(partial_corr)
            if weight < float(max(min_abs_partial_corr, 0.0)):
                continue
            out.append(
                {
                    "sensor_u": sensor_u,
                    "sensor_v": sensor_v,
                    "partial_corr": partial_corr,
                    "precision_weight": weight,
                    "edge_family": "precision",
                }
            )
    return pd.DataFrame(out)


def _build_precision_graph(
    window_x_df: pd.DataFrame,
    selected_sensors: list[str],
    *,
    ridge_lambda: float,
    min_abs_partial_corr: float,
) -> pd.DataFrame:
    if not selected_sensors:
        return pd.DataFrame(columns=["sensor_u", "sensor_v", "partial_corr", "precision_weight", "edge_family"])

    if window_x_df.empty:
        return pd.DataFrame(columns=["sensor_u", "sensor_v", "partial_corr", "precision_weight", "edge_family"])

    rows: list[list[float]] = []
    for _, row in window_x_df.sort_values(["tail_id", "flight_id", "t_end", "win_id"], kind="mergesort").iterrows():
        scaled = row.get("continuous_vector_t_end_scaled")
        if not isinstance(scaled, dict):
            continue
        rows.append([float(scaled.get(parameter_name, 0.0) or 0.0) for parameter_name in selected_sensors])

    if len(rows) < 2:
        return pd.DataFrame(columns=["sensor_u", "sensor_v", "partial_corr", "precision_weight", "edge_family"])

    x = np.asarray(rows, dtype=float)
    means = [float(sum(float(row[col_idx]) for row in x.tolist())) / float(len(rows)) for col_idx in range(len(selected_sensors))]
    cov = np.zeros((len(selected_sensors), len(selected_sensors)), dtype=float)
    denom = float(max(len(rows) - 1, 1))
    for row in x.tolist():
        centered = [float(value) - means[idx] for idx, value in enumerate(row)]
        for i in range(len(selected_sensors)):
            for j in range(len(selected_sensors)):
                cov[i, j] += centered[i] * centered[j]
    cov = cov / denom
    return _build_precision_graph_from_covariance(
        selected_sensors,
        cov,
        ridge_lambda=ridge_lambda,
        min_abs_partial_corr=min_abs_partial_corr,
    )


def _build_event_graph(
    events_df: pd.DataFrame,
    windows_df: pd.DataFrame,
    *,
    min_count: int,
    min_npmi: float,
    top_k_per_sensor: int,
) -> pd.DataFrame:
    if events_df.empty or windows_df.empty:
        return pd.DataFrame(columns=["sensor_u", "sensor_v", "cooccur_count", "event_weight", "edge_family"])
    pair_counts: Counter[tuple[str, str]] = Counter()
    sensor_window_counts: Counter[str] = Counter()
    max_count = 0
    by_events = {
        key: group.sort_values(["timestamp_utc", "parameter_name"], kind="mergesort").reset_index(drop=True)
        for key, group in events_df.groupby(["tail_id", "flight_id"], sort=True)
    }
    for key, window_group in windows_df.groupby(["tail_id", "flight_id"], sort=True):
        event_rows = by_events.get(key, pd.DataFrame())
        if event_rows.empty:
            continue
        event_idx = 0
        event_len = len(event_rows)
        for window in window_group.sort_values(["t_start", "win_id"], kind="mergesort").to_dict(orient="records"):
            t_start = pd.to_datetime(window["t_start"], utc=True)
            t_end = pd.to_datetime(window["t_end"], utc=True)
            sensors: set[str] = set()
            idx = event_idx
            while idx < event_len:
                row = event_rows.iloc[idx]
                timestamp_utc = pd.to_datetime(row["timestamp_utc"], utc=True)
                if timestamp_utc < t_start:
                    idx += 1
                    event_idx = idx
                    continue
                if timestamp_utc > t_end:
                    break
                sensors.add(str(row["parameter_name"]))
                idx += 1
            distinct = sorted(sensors)
            for parameter_name in distinct:
                sensor_window_counts[parameter_name] += 1
            for i, left in enumerate(distinct):
                for right in distinct[i + 1 :]:
                    pair_counts[(left, right)] += 1
                    max_count = max(max_count, pair_counts[(left, right)])
    total_windows = int(len(windows_df))
    out: list[dict[str, Any]] = []
    for (left, right), count in sorted(pair_counts.items(), key=lambda item: (-item[1], item[0][0], item[0][1])):
        if count < max(int(min_count), 1):
            continue
        if total_windows <= 0:
            continue
        p_xy = float(count) / float(total_windows)
        p_x = float(sensor_window_counts[left]) / float(total_windows)
        p_y = float(sensor_window_counts[right]) / float(total_windows)
        if p_xy <= 0.0 or p_x <= 0.0 or p_y <= 0.0:
            continue
        pmi = log(p_xy / max(p_x * p_y, 1e-12))
        npmi = pmi / max(-log(p_xy), 1e-12)
        event_weight = max(float(npmi), 0.0)
        if event_weight < float(min_npmi):
            continue
        out.append(
            {
                "sensor_u": left,
                "sensor_v": right,
                "cooccur_count": int(count),
                "event_weight": event_weight,
                "edge_family": "event",
            }
        )
    out = _retain_top_k_undirected(out, weight_key="event_weight", top_k_per_sensor=int(top_k_per_sensor))
    return pd.DataFrame(out)


def _build_lag_graph(
    events_df: pd.DataFrame,
    *,
    tau_max_seconds: float,
    min_count: int,
    max_mean_lag_seconds: float | None,
    top_k_outgoing: int,
) -> pd.DataFrame:
    if events_df.empty:
        return pd.DataFrame(columns=["sensor_u", "sensor_v", "lag_count", "lag_weight", "mean_lag_seconds", "edge_family"])
    tau = max(float(tau_max_seconds), 0.0)
    pair_counts: Counter[tuple[str, str]] = Counter()
    lag_sums: defaultdict[tuple[str, str], float] = defaultdict(float)
    outgoing_counts: Counter[str] = Counter()
    for _, group in events_df.groupby(["tail_id", "flight_id"], sort=True):
        buffer: deque[tuple[pd.Timestamp, str]] = deque()
        for row in group.sort_values(["timestamp_utc", "parameter_name"], kind="mergesort").to_dict(orient="records"):
            timestamp_utc = pd.to_datetime(row["timestamp_utc"], utc=True)
            parameter_name = str(row["parameter_name"])
            lower = timestamp_utc - pd.Timedelta(seconds=tau)
            while buffer and buffer[0][0] < lower:
                buffer.popleft()
            seen_prev_parameters: set[str] = set()
            for prev_timestamp_utc, prev_parameter_name in reversed(buffer):
                if prev_parameter_name == parameter_name:
                    continue
                if prev_parameter_name in seen_prev_parameters:
                    continue
                pair = (prev_parameter_name, parameter_name)
                lag = max((timestamp_utc - prev_timestamp_utc).total_seconds(), 0.0)
                pair_counts[pair] += 1
                lag_sums[pair] += lag
                outgoing_counts[prev_parameter_name] += 1
                seen_prev_parameters.add(prev_parameter_name)
            buffer.append((timestamp_utc, parameter_name))
    out: list[dict[str, Any]] = []
    for (left, right), count in sorted(pair_counts.items(), key=lambda item: (-item[1], item[0][0], item[0][1])):
        if count < max(int(min_count), 1):
            continue
        mean_lag_seconds = float(lag_sums[(left, right)] / float(max(count, 1)))
        if max_mean_lag_seconds is not None and mean_lag_seconds > float(max_mean_lag_seconds):
            continue
        shortness = max(0.0, 1.0 - (mean_lag_seconds / float(max(tau, 1e-6))))
        conditional_probability = float(count) / float(max(outgoing_counts[left], 1))
        out.append(
            {
                "sensor_u": left,
                "sensor_v": right,
                "lag_count": int(count),
                "lag_weight": conditional_probability * shortness,
                "mean_lag_seconds": mean_lag_seconds,
                "edge_family": "lag_directed",
            }
        )
    out = _retain_top_k_directed(out, weight_key="lag_weight", top_k_outgoing=int(top_k_outgoing))
    return pd.DataFrame(out)


def _build_transition_graph(events_df: pd.DataFrame, *, min_count: int) -> pd.DataFrame:
    if events_df.empty:
        return pd.DataFrame(columns=["sensor_u", "sensor_v", "precedence_count", "precedence_weight", "edge_family"])
    pair_counts: Counter[tuple[str, str]] = Counter()
    outgoing_counts: Counter[str] = Counter()
    for _, group in events_df.groupby(["tail_id", "flight_id"], sort=True):
        previous_parameter_name: str | None = None
        for row in group.sort_values(["timestamp_utc", "parameter_name"], kind="mergesort").to_dict(orient="records"):
            parameter_name = str(row["parameter_name"])
            if previous_parameter_name is not None and previous_parameter_name != parameter_name:
                pair = (previous_parameter_name, parameter_name)
                pair_counts[pair] += 1
                outgoing_counts[previous_parameter_name] += 1
            previous_parameter_name = parameter_name
    out: list[dict[str, Any]] = []
    for (left, right), count in sorted(pair_counts.items(), key=lambda item: (-item[1], item[0][0], item[0][1])):
        if count < max(int(min_count), 1):
            continue
        out.append(
            {
                "sensor_u": left,
                "sensor_v": right,
                "precedence_count": int(count),
                "precedence_weight": float(count) / float(max(outgoing_counts[left], 1)),
                "edge_family": "transition",
            }
        )
    return pd.DataFrame(out)


def build_event_graph_spark_table(
    events_df: DataFrame,
    windows_df: DataFrame,
    *,
    min_count: int,
    min_npmi: float,
    top_k_per_sensor: int,
) -> DataFrame:
    """Build same-window cooccurrence edges in Spark using positive normalized PMI."""
    F = _spark_functions()
    event_columns = ["tail_id", "flight_id", "timestamp_utc", "parameter_name"]
    window_columns = ["tail_id", "flight_id", "win_id", "t_start", "t_end"]
    joined = (
        events_df.select(*event_columns)
        .join(
            windows_df.select(*window_columns),
            on=["tail_id", "flight_id"],
            how="inner",
        )
        .where((F.col("timestamp_utc") >= F.col("t_start")) & (F.col("timestamp_utc") <= F.col("t_end")))
    )
    grouped = joined.groupBy("tail_id", "flight_id", "win_id").agg(F.sort_array(F.collect_set("parameter_name")).alias("parameter_names"))

    pair_rows = grouped.select(
        F.posexplode("parameter_names").alias("left_idx", "sensor_u"),
        F.col("parameter_names"),
    ).select(
        "sensor_u",
        F.expr("slice(parameter_names, left_idx + 2, size(parameter_names))").alias("right_candidates"),
    ).select("sensor_u", F.explode_outer("right_candidates").alias("sensor_v")).where(F.col("sensor_v").isNotNull())

    pair_counts = pair_rows.groupBy("sensor_u", "sensor_v").agg(F.count(F.lit(1)).cast("int").alias("cooccur_count"))
    sensor_window_counts = grouped.select(F.explode("parameter_names").alias("parameter_name")).groupBy("parameter_name").agg(
        F.count(F.lit(1)).cast("int").alias("sensor_window_count")
    )
    total_windows_df = grouped.agg(F.count(F.lit(1)).cast("double").alias("total_windows"))
    with_counts = (
        pair_counts.join(sensor_window_counts.withColumnRenamed("parameter_name", "sensor_u"), on="sensor_u")
        .withColumnRenamed("sensor_window_count", "left_window_count")
        .join(sensor_window_counts.withColumnRenamed("parameter_name", "sensor_v"), on="sensor_v")
        .withColumnRenamed("sensor_window_count", "right_window_count")
        .crossJoin(total_windows_df)
        .withColumn("p_xy", F.col("cooccur_count") / F.col("total_windows"))
        .withColumn("p_x", F.col("left_window_count") / F.col("total_windows"))
        .withColumn("p_y", F.col("right_window_count") / F.col("total_windows"))
        .withColumn(
            "event_weight_raw",
            F.log(F.col("p_xy") / (F.col("p_x") * F.col("p_y"))) / (-F.log(F.col("p_xy"))),
        )
        .withColumn("event_weight", F.greatest(F.col("event_weight_raw"), F.lit(0.0)))
        .where((F.col("cooccur_count") >= F.lit(max(int(min_count), 1))) & (F.col("event_weight") >= F.lit(float(min_npmi))))
    )
    if int(top_k_per_sensor) > 0:
        from pyspark.sql import Window

        undirected = with_counts.select(
            F.col("sensor_u"),
            F.col("sensor_v"),
            F.col("cooccur_count"),
            F.col("event_weight"),
            F.least("sensor_u", "sensor_v").alias("sensor_min"),
            F.greatest("sensor_u", "sensor_v").alias("sensor_max"),
        )
        exploded = undirected.select(
            F.col("sensor_u").alias("parameter_name"),
            F.col("sensor_min"),
            F.col("sensor_max"),
            F.col("event_weight"),
        ).unionByName(
            undirected.select(
                F.col("sensor_v").alias("parameter_name"),
                F.col("sensor_min"),
                F.col("sensor_max"),
                F.col("event_weight"),
            )
        )
        rank_window = Window.partitionBy("parameter_name").orderBy(F.col("event_weight").desc(), F.col("sensor_min"), F.col("sensor_max"))
        keep = exploded.withColumn("rank", F.row_number().over(rank_window)).where(F.col("rank") <= int(top_k_per_sensor)).select(
            "sensor_min", "sensor_max"
        ).distinct()
        with_counts = with_counts.join(
            keep,
            on=[with_counts.sensor_u == keep.sensor_min, with_counts.sensor_v == keep.sensor_max],
            how="inner",
        ).select(with_counts["*"])
    return with_counts.select(
        "sensor_u",
        "sensor_v",
        "cooccur_count",
        "event_weight",
        F.lit("event").alias("edge_family"),
    )


def build_lag_graph_spark_table(
    events_df: DataFrame,
    *,
    tau_max_seconds: float,
    min_count: int,
    max_mean_lag_seconds: float | None,
    top_k_outgoing: int,
) -> DataFrame:
    """Build directed lag edges as conditional probabilities discounted by mean lag."""
    F = _spark_functions()
    grouped = (
        events_df.select("tail_id", "flight_id", "timestamp_utc", "parameter_name")
        .groupBy("tail_id", "flight_id")
        .applyInPandas(
            lambda pdf: _build_lag_graph(
                _prepare_events(pdf),
                tau_max_seconds=tau_max_seconds,
                min_count=min_count,
                max_mean_lag_seconds=max_mean_lag_seconds,
                top_k_outgoing=top_k_outgoing,
            ),
            schema=_lag_graph_schema(),
        )
    )
    aggregated = grouped.groupBy("sensor_u", "sensor_v", "edge_family").agg(
        F.sum("lag_count").cast("int").alias("lag_count"),
        F.sum(F.col("mean_lag_seconds") * F.col("lag_count")).alias("weighted_mean_lag"),
    ).select(
        "sensor_u",
        "sensor_v",
        "edge_family",
        "lag_count",
        (F.col("weighted_mean_lag") / F.col("lag_count")).alias("mean_lag_seconds"),
    )
    source_totals = aggregated.groupBy("sensor_u").agg(F.sum("lag_count").cast("double").alias("source_total"))
    tau = max(float(tau_max_seconds), 1e-6)
    return (
        aggregated.join(source_totals, on="sensor_u", how="inner")
        .withColumn("conditional_probability", F.col("lag_count") / F.col("source_total"))
        .withColumn("shortness", F.greatest(F.lit(0.0), F.lit(1.0) - (F.col("mean_lag_seconds") / F.lit(tau))))
        .withColumn("lag_weight", F.col("conditional_probability") * F.col("shortness"))
        .select("sensor_u", "sensor_v", "edge_family", "lag_count", "lag_weight", "mean_lag_seconds")
    )


def build_transition_graph_spark_table(events_df: DataFrame, *, min_count: int) -> DataFrame:
    """Build immediate transition edges as row-normalized transition probabilities."""
    F = _spark_functions()

    grouped = (
        events_df.select("tail_id", "flight_id", "timestamp_utc", "parameter_name")
        .groupBy("tail_id", "flight_id")
        .applyInPandas(
            lambda pdf: _build_transition_graph(_prepare_events(pdf), min_count=min_count),
            schema=_transition_graph_schema(),
        )
    )
    count_df = grouped.groupBy("sensor_u", "sensor_v", "edge_family").agg(
        F.sum("precedence_count").cast("int").alias("precedence_count")
    )
    source_totals = count_df.groupBy("sensor_u").agg(F.sum("precedence_count").cast("double").alias("source_total"))
    return count_df.join(source_totals, on="sensor_u", how="inner").withColumn(
        "precedence_weight",
        F.col("precedence_count") / F.col("source_total"),
    )


def build_precision_graph_from_window_x_spark_table(
    window_x_df: DataFrame,
    *,
    selected_sensors: list[str],
    ridge_lambda: float,
    min_abs_partial_corr: float,
) -> pd.DataFrame:
    """Build precision edges from Spark-aggregated covariance stats."""
    F = _spark_functions()
    backbone_sensors = [str(item) for item in selected_sensors if str(item)]
    if not backbone_sensors:
        return pd.DataFrame(columns=["sensor_u", "sensor_v", "partial_corr", "precision_weight", "edge_family"])

    projection_exprs = [
        F.coalesce(
            F.element_at(F.col("continuous_vector_t_end_scaled"), F.lit(parameter_name)).cast("double"),
            F.lit(0.0),
        ).alias(f"x_{idx}")
        for idx, parameter_name in enumerate(backbone_sensors)
    ]
    projected = window_x_df.select(*projection_exprs)

    agg_exprs = [F.count(F.lit(1)).cast("long").alias("n")]
    for idx in range(len(backbone_sensors)):
        agg_exprs.append(F.sum(F.col(f"x_{idx}")).cast("double").alias(f"sum_{idx}"))
    for i in range(len(backbone_sensors)):
        for j in range(i, len(backbone_sensors)):
            agg_exprs.append((F.sum(F.col(f"x_{i}") * F.col(f"x_{j}")).cast("double")).alias(f"sum_{i}_{j}"))

    stats_row = projected.agg(*agg_exprs).collect()[0]
    row_count = int(stats_row["n"] or 0)
    if row_count < 2:
        return pd.DataFrame(columns=["sensor_u", "sensor_v", "partial_corr", "precision_weight", "edge_family"])

    means = [float(stats_row[f"sum_{idx}"] or 0.0) / float(row_count) for idx in range(len(backbone_sensors))]
    cov = np.zeros((len(backbone_sensors), len(backbone_sensors)), dtype=float)
    denom = float(max(row_count - 1, 1))
    for i in range(len(backbone_sensors)):
        for j in range(i, len(backbone_sensors)):
            cross_sum = float(stats_row[f"sum_{i}_{j}"] or 0.0)
            centered_sum = cross_sum - (float(row_count) * means[i] * means[j])
            cov_ij = centered_sum / denom
            cov[i, j] = cov_ij
            cov[j, i] = cov_ij

    return _build_precision_graph_from_covariance(
        backbone_sensors,
        cov,
        ridge_lambda=ridge_lambda,
        min_abs_partial_corr=min_abs_partial_corr,
    )


def _fuse_graphs(
    precision_df: pd.DataFrame,
    event_df: pd.DataFrame,
    lag_df: pd.DataFrame,
    *,
    alpha: float,
    beta: float,
    gamma: float,
) -> pd.DataFrame:
    event_map = {
        (str(row["sensor_u"]), str(row["sensor_v"])): float(row["event_weight"])
        for row in event_df.to_dict(orient="records")
    }
    lag_weight_map: dict[tuple[str, str], float] = {}
    lag_weight_lists: defaultdict[tuple[str, str], list[float]] = defaultdict(list)
    for row in lag_df.to_dict(orient="records"):
        left = str(row["sensor_u"])
        right = str(row["sensor_v"])
        key = tuple(sorted((left, right)))
        lag_weight_lists[key].append(float(row.get("lag_weight", 0.0) or 0.0))
    for key, weights in lag_weight_lists.items():
        lag_weight_map[key] = max(weights, default=0.0)

    all_pairs = set(event_map.keys()) | set(lag_weight_map.keys()) | {
        (str(row["sensor_u"]), str(row["sensor_v"])) for row in precision_df.to_dict(orient="records")
    }
    precision_map = {
        (str(row["sensor_u"]), str(row["sensor_v"])): float(row["precision_weight"])
        for row in precision_df.to_dict(orient="records")
    }

    out: list[dict[str, Any]] = []
    for key in sorted(all_pairs):
        sensor_u, sensor_v = key
        p = float(precision_map.get(key, 0.0))
        e = float(event_map.get(key, 0.0))
        l = float(lag_weight_map.get(tuple(sorted(key)), 0.0))
        fused = (alpha * p) + (beta * e) + (gamma * l)
        if fused <= 0.0:
            continue
        out.append(
            {
                "sensor_u": sensor_u,
                "sensor_v": sensor_v,
                "precision_weight": p,
                "event_weight": e,
                "lag_weight": l,
                "fused_weight": fused,
                "edge_family": "fused",
            }
        )
    return pd.DataFrame(out)


def _assign_hierarchy(
    fused_df: pd.DataFrame,
    parameter_names: list[str],
    *,
    min_edge_weight: float,
    top_k_per_sensor: int = 3,
    subsystem_min_edge_weight: float | None = None,
    system_min_edge_weight: float | None = None,
) -> pd.DataFrame:
    ranked_neighbors: defaultdict[str, list[tuple[float, str]]] = defaultdict(list)
    filtered_rows: list[dict[str, Any]] = []
    parameter_set = {str(item) for item in parameter_names}
    for row in fused_df.to_dict(orient="records"):
        weight = float(row.get("fused_weight", 0.0) or 0.0)
        if weight < float(min_edge_weight):
            continue
        a = str(row.get("sensor_u", ""))
        b = str(row.get("sensor_v", ""))
        if not a or not b or a not in parameter_set or b not in parameter_set:
            continue
        filtered_rows.append(row)
        ranked_neighbors[a].append((weight, b))
        ranked_neighbors[b].append((weight, a))

    keep_neighbors: dict[str, set[str]] = {}
    for parameter_name, neighbors in ranked_neighbors.items():
        ranked = sorted(neighbors, key=lambda item: (-item[0], item[1]))
        keep_neighbors[parameter_name] = {neighbor for _, neighbor in ranked[: max(int(top_k_per_sensor), 1)]}

    retained_edges: list[tuple[str, str, float]] = []
    for row in filtered_rows:
        a = str(row["sensor_u"])
        b = str(row["sensor_v"])
        if b in keep_neighbors.get(a, set()) and a in keep_neighbors.get(b, set()):
            retained_edges.append((a, b, float(row["fused_weight"])))

    rollup_edges = [
        (str(row["sensor_u"]), str(row["sensor_v"]), float(row["fused_weight"]))
        for row in filtered_rows
    ]

    hierarchy_rows = assign_hierarchy_from_weighted_edges(
        list(parameter_names),
        retained_edges,
        module_min_edge_weight=float(min_edge_weight),
        subsystem_min_edge_weight=subsystem_min_edge_weight,
        system_min_edge_weight=system_min_edge_weight,
        rollup_edges=rollup_edges,
    )
    out: list[dict[str, Any]] = []
    for row in hierarchy_rows:
        out.append(
            {
                "parameter_name": str(row["parameter_name"]),
                "system_id": str(row["system_id"]),
                "subsystem_id": str(row["subsystem_id"]),
                "module_id": str(row["module_id"]),
                "hierarchy_source": "v2_fused_graph_mutual_topk_levels",
                "hierarchy_profile_id": "HIER_V2",
            }
        )
    return pd.DataFrame(out)


def build_graph_artifact_tables(
    raw_df: pd.DataFrame,
    events_df: pd.DataFrame,
    windows_df: pd.DataFrame,
    backbone_df: pd.DataFrame,
    *,
    precision_ridge_lambda: float = 1.0,
    min_abs_partial_corr: float = 0.05,
    min_event_count: int = 1,
    min_event_npmi: float = 0.0,
    event_top_k_per_sensor: int = 8,
    lag_tau_max_seconds: float = 30.0,
    min_lag_count: int = 1,
    max_mean_lag_seconds: float | None = None,
    lag_top_k_outgoing: int = 8,
    min_transition_count: int = 1,
    alpha: float = 1.0,
    beta: float = 1.0,
    gamma: float = 1.0,
    min_fused_edge_weight: float = 0.05,
    hierarchy_top_k_per_sensor: int = 3,
    hierarchy_subsystem_min_edge_weight: float | None = None,
    hierarchy_system_min_edge_weight: float | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    from libs.windows import build_window_x_table

    window_x_df = build_window_x_table(raw_df, pd.DataFrame(columns=["tail_id", "flight_id", "parameter_name", "timestamp_utc", "event_type_detected", "payload"]), windows_df)
    return build_graph_artifacts_from_window_x_table(
        window_x_df,
        events_df,
        windows_df,
        backbone_df,
        precision_ridge_lambda=precision_ridge_lambda,
        min_abs_partial_corr=min_abs_partial_corr,
        min_event_count=min_event_count,
        min_event_npmi=min_event_npmi,
        event_top_k_per_sensor=event_top_k_per_sensor,
        lag_tau_max_seconds=lag_tau_max_seconds,
        min_lag_count=min_lag_count,
        max_mean_lag_seconds=max_mean_lag_seconds,
        lag_top_k_outgoing=lag_top_k_outgoing,
        min_transition_count=min_transition_count,
        alpha=alpha,
        beta=beta,
        gamma=gamma,
        min_fused_edge_weight=min_fused_edge_weight,
        hierarchy_top_k_per_sensor=hierarchy_top_k_per_sensor,
        hierarchy_subsystem_min_edge_weight=hierarchy_subsystem_min_edge_weight,
        hierarchy_system_min_edge_weight=hierarchy_system_min_edge_weight,
    )


def build_graph_artifacts_from_window_x_table(
    window_x_df: pd.DataFrame,
    events_df: pd.DataFrame,
    windows_df: pd.DataFrame,
    backbone_df: pd.DataFrame,
    *,
    precision_ridge_lambda: float = 1.0,
    min_abs_partial_corr: float = 0.05,
    min_event_count: int = 1,
    min_event_npmi: float = 0.0,
    event_top_k_per_sensor: int = 8,
    lag_tau_max_seconds: float = 30.0,
    min_lag_count: int = 1,
    max_mean_lag_seconds: float | None = None,
    lag_top_k_outgoing: int = 8,
    min_transition_count: int = 1,
    alpha: float = 1.0,
    beta: float = 1.0,
    gamma: float = 1.0,
    min_fused_edge_weight: float = 0.05,
    hierarchy_top_k_per_sensor: int = 3,
    hierarchy_subsystem_min_edge_weight: float | None = None,
    hierarchy_system_min_edge_weight: float | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    event_rows = _prepare_events(events_df)
    window_rows = _prepare_windows(windows_df)
    selected_sensors = _selected_backbone_sensors(backbone_df)

    precision_df = _build_precision_graph(
        window_x_df,
        selected_sensors,
        ridge_lambda=precision_ridge_lambda,
        min_abs_partial_corr=min_abs_partial_corr,
    )
    event_df = _build_event_graph(
        event_rows,
        window_rows,
        min_count=min_event_count,
        min_npmi=min_event_npmi,
        top_k_per_sensor=event_top_k_per_sensor,
    )
    lag_df = _build_lag_graph(
        event_rows,
        tau_max_seconds=lag_tau_max_seconds,
        min_count=min_lag_count,
        max_mean_lag_seconds=max_mean_lag_seconds,
        top_k_outgoing=lag_top_k_outgoing,
    )
    transition_df = _build_transition_graph(
        event_rows,
        min_count=min_transition_count,
    )
    fused_df = _fuse_graphs(
        precision_df,
        event_df,
        lag_df,
        alpha=float(alpha),
        beta=float(beta),
        gamma=float(gamma),
    )
    parameter_name_union = sorted(
        set(window_x_df.get("continuous_vector_t_end_scaled", pd.Series(dtype=object)).apply(lambda item: list(item.keys()) if isinstance(item, dict) else []).explode().dropna().astype(str).tolist())
        | set(event_rows["parameter_name"].dropna().astype(str).tolist())
        | set(selected_sensors)
    )
    hierarchy_df = _assign_hierarchy(
        fused_df,
        parameter_name_union,
        min_edge_weight=min_fused_edge_weight,
        top_k_per_sensor=hierarchy_top_k_per_sensor,
        subsystem_min_edge_weight=hierarchy_subsystem_min_edge_weight,
        system_min_edge_weight=hierarchy_system_min_edge_weight,
    )
    return precision_df, event_df, lag_df, transition_df, fused_df, hierarchy_df


def build_graph_component_tables_from_window_x_table(
    window_x_df: pd.DataFrame,
    events_df: pd.DataFrame,
    windows_df: pd.DataFrame,
    backbone_df: pd.DataFrame,
    *,
    precision_ridge_lambda: float = 1.0,
    min_abs_partial_corr: float = 0.05,
    min_event_count: int = 1,
    min_event_npmi: float = 0.0,
    lag_tau_max_seconds: float = 30.0,
    min_lag_count: int = 1,
    max_mean_lag_seconds: float | None = None,
    min_transition_count: int = 1,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build graph components without event/lag top-k pruning for cacheable graph sweeps."""
    event_rows = _prepare_events(events_df)
    window_rows = _prepare_windows(windows_df)
    selected_sensors = _selected_backbone_sensors(backbone_df)

    precision_df = _build_precision_graph(
        window_x_df,
        selected_sensors,
        ridge_lambda=precision_ridge_lambda,
        min_abs_partial_corr=min_abs_partial_corr,
    )
    event_df = _build_event_graph(
        event_rows,
        window_rows,
        min_count=min_event_count,
        min_npmi=min_event_npmi,
        top_k_per_sensor=0,
    )
    lag_df = _build_lag_graph(
        event_rows,
        tau_max_seconds=lag_tau_max_seconds,
        min_count=min_lag_count,
        max_mean_lag_seconds=max_mean_lag_seconds,
        top_k_outgoing=0,
    )
    transition_df = _build_transition_graph(
        event_rows,
        min_count=min_transition_count,
    )
    return precision_df, event_df, lag_df, transition_df


def build_graph_fusion_from_tables(
    window_x_df: pd.DataFrame,
    event_df: pd.DataFrame,
    lag_df: pd.DataFrame,
    backbone_df: pd.DataFrame,
    *,
    precision_ridge_lambda: float = 1.0,
    min_abs_partial_corr: float = 0.05,
    alpha: float = 1.0,
    beta: float = 1.0,
    gamma: float = 1.0,
    min_fused_edge_weight: float = 0.05,
    hierarchy_top_k_per_sensor: int = 3,
    hierarchy_subsystem_min_edge_weight: float | None = None,
    hierarchy_system_min_edge_weight: float | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build precision, fused graph, and hierarchy from pre-aggregated graph tables."""
    selected_sensors = _selected_backbone_sensors(backbone_df)
    precision_df = _build_precision_graph(
        window_x_df,
        selected_sensors,
        ridge_lambda=precision_ridge_lambda,
        min_abs_partial_corr=min_abs_partial_corr,
    )
    fused_df, hierarchy_df = build_graph_fusion_from_component_tables(
        precision_df,
        event_df,
        lag_df,
        backbone_df,
        alpha=alpha,
        beta=beta,
        gamma=gamma,
        min_fused_edge_weight=min_fused_edge_weight,
        hierarchy_top_k_per_sensor=hierarchy_top_k_per_sensor,
        hierarchy_subsystem_min_edge_weight=hierarchy_subsystem_min_edge_weight,
        hierarchy_system_min_edge_weight=hierarchy_system_min_edge_weight,
    )
    return precision_df, fused_df, hierarchy_df


def build_graph_fusion_from_component_tables(
    precision_df: pd.DataFrame,
    event_df: pd.DataFrame,
    lag_df: pd.DataFrame,
    backbone_df: pd.DataFrame,
    *,
    alpha: float = 1.0,
    beta: float = 1.0,
    gamma: float = 1.0,
    min_fused_edge_weight: float = 0.05,
    hierarchy_top_k_per_sensor: int = 3,
    hierarchy_subsystem_min_edge_weight: float | None = None,
    hierarchy_system_min_edge_weight: float | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build fused graph and hierarchy from already-computed component tables."""
    selected_sensors = _selected_backbone_sensors(backbone_df)
    fused_df = _fuse_graphs(
        precision_df,
        event_df,
        lag_df,
        alpha=float(alpha),
        beta=float(beta),
        gamma=float(gamma),
    )
    backbone_all_sensors = []
    if not backbone_df.empty:
        all_sensors = backbone_df.iloc[0].get("all_sensors", [])
        if isinstance(all_sensors, list):
            backbone_all_sensors = [str(item) for item in all_sensors if str(item)]
    parameter_name_union = sorted(
        set(backbone_all_sensors)
        | set(event_df.get("sensor_u", pd.Series(dtype=object)).dropna().astype(str).tolist())
        | set(event_df.get("sensor_v", pd.Series(dtype=object)).dropna().astype(str).tolist())
        | set(lag_df.get("sensor_u", pd.Series(dtype=object)).dropna().astype(str).tolist())
        | set(lag_df.get("sensor_v", pd.Series(dtype=object)).dropna().astype(str).tolist())
        | set(selected_sensors)
    )
    hierarchy_df = _assign_hierarchy(
        fused_df,
        parameter_name_union,
        min_edge_weight=min_fused_edge_weight,
        top_k_per_sensor=hierarchy_top_k_per_sensor,
        subsystem_min_edge_weight=hierarchy_subsystem_min_edge_weight,
        system_min_edge_weight=hierarchy_system_min_edge_weight,
    )
    return fused_df, hierarchy_df
