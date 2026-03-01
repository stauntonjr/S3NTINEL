"""Scalable CUR fitting helpers over normalized sensor telemetry."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from libs.cur.sample import select_topk_deterministic, select_topk_weighted_without_replacement
from pyspark.sql import functions as F


@dataclass(frozen=True)
class CurFitConfig:
    pivots_k: int
    row_samples_k: int
    downsample_hz: float
    normalize_mode: str
    normalize_clip_sigma: float
    sampling_mode: str
    sampling_seed: int


def _normalize_numeric_values(
    numeric_df: "DataFrame",
    normalization_profile_df: "DataFrame",
    *,
    normalize_mode: str,
    normalize_clip_sigma: float,
) -> "DataFrame":
    mode = str(normalize_mode or "none").strip().lower()
    if mode == "none":
        return numeric_df

    joined = numeric_df.join(
        normalization_profile_df.select("sensor", "mean", "q50", "scale_std", "scale_iqr"),
        on="sensor",
        how="left",
    )
    if mode == "robust":
        robust_expr = (F.col("val") - F.col("q50")) / F.greatest(F.coalesce(F.col("scale_iqr"), F.lit(1.0)), F.lit(1e-6))
        centered = F.when(F.col("q50").isNull(), F.col("val")).otherwise(robust_expr)
    else:
        zscore_expr = (F.col("val") - F.col("mean")) / F.greatest(F.coalesce(F.col("scale_std"), F.lit(1.0)), F.lit(1e-6))
        centered = F.when(F.col("mean").isNull(), F.col("val")).otherwise(zscore_expr)

    clip_bound = max(float(normalize_clip_sigma), 0.0)
    clipped = (
        F.when(centered > clip_bound, F.lit(clip_bound))
        .when(centered < -clip_bound, F.lit(-clip_bound))
        .otherwise(centered)
    )
    return joined.withColumn("val", clipped).select("tail_id", "flight_id", "timestamp_utc", "sensor", "val")


def build_normalized_sampled_points(
    raw_df: "DataFrame",
    normalization_profile_df: "DataFrame",
    cfg: CurFitConfig,
) -> "DataFrame":
    numeric = (
        raw_df.where(F.col("val").isNotNull())
        .select("tail_id", "flight_id", "timestamp_utc", "sensor", F.col("val").cast("double").alias("val"))
    )
    numeric = _normalize_numeric_values(
        numeric,
        normalization_profile_df,
        normalize_mode=cfg.normalize_mode,
        normalize_clip_sigma=cfg.normalize_clip_sigma,
    )

    sampled = (
        numeric.withColumn("bucket_index", F.floor(F.col("timestamp_utc").cast("double") * F.lit(max(cfg.downsample_hz, 0.1))))
        .groupBy("tail_id", "flight_id", "bucket_index", "sensor")
        .agg(F.avg("val").alias("x"))
    )

    return sampled


def build_column_sketch(sampled_points_df: "DataFrame") -> "DataFrame":
    return (
        sampled_points_df.groupBy("sensor")
        .agg(
            F.count("*").alias("points"),
            F.sum(F.col("x")).alias("sum_x"),
            F.sum(F.abs(F.col("x"))).alias("sum_abs_x"),
            F.sum(F.col("x") * F.col("x")).alias("sum_x2"),
        )
        .withColumn("energy", F.coalesce(F.col("sum_x2"), F.lit(0.0)))
    )


def build_column_leverage(column_sketch_df: "DataFrame") -> "DataFrame":
    total_energy_row = column_sketch_df.agg(F.sum("energy").alias("total_energy")).collect()
    total_energy = float(total_energy_row[0]["total_energy"]) if total_energy_row and total_energy_row[0]["total_energy"] is not None else 0.0
    if total_energy <= 1e-12:
        return column_sketch_df.withColumn("leverage_score", F.lit(0.0))

    return column_sketch_df.withColumn("leverage_score", F.col("energy") / F.lit(total_energy))


def select_sampled_sensors(
    column_leverage_df: "DataFrame",
    *,
    pivots_k: int,
    sampling_mode: str,
    sampling_seed: int,
) -> "DataFrame":
    mode = str(sampling_mode or "deterministic").strip().lower()
    if mode == "weighted":
        selected = select_topk_weighted_without_replacement(
            column_leverage_df,
            k=pivots_k,
            weight_column="leverage_score",
            seed=sampling_seed,
            tie_break_columns=[F.col("sensor").asc()],
        )
    else:
        selected = select_topk_deterministic(
            column_leverage_df,
            k=pivots_k,
            order_columns=[F.col("leverage_score").desc(), F.col("energy").desc(), F.col("sensor").asc()],
        )

    return selected.select("sensor", "points", "sum_x", "sum_abs_x", "sum_x2", "energy", "leverage_score")


def build_row_sketch(sampled_points_df: "DataFrame") -> "DataFrame":
    return (
        sampled_points_df.groupBy("tail_id", "flight_id", "bucket_index")
        .agg(F.sum(F.col("x") * F.col("x")).alias("row_energy"), F.countDistinct("sensor").alias("sensor_count"))
        .withColumn("row_id", F.sha2(F.concat_ws("|", F.col("tail_id"), F.col("flight_id"), F.col("bucket_index").cast("string")), 256))
    )


def select_sampled_rows(
    row_sketch_df: "DataFrame",
    *,
    row_samples_k: int,
    sampling_mode: str,
    sampling_seed: int,
) -> "DataFrame":
    mode = str(sampling_mode or "deterministic").strip().lower()
    if mode == "weighted":
        return select_topk_weighted_without_replacement(
            row_sketch_df,
            k=row_samples_k,
            weight_column="row_energy",
            seed=sampling_seed + 17,
            tie_break_columns=[F.col("row_id").asc()],
        )
    return select_topk_deterministic(
        row_sketch_df,
        k=row_samples_k,
        order_columns=[F.col("row_energy").desc(), F.col("sensor_count").desc(), F.col("row_id").asc()],
    )


def build_training_matrix_samples(
    raw_df: "DataFrame",
    normalization_profile_df: "DataFrame",
    cfg: CurFitConfig,
) -> tuple["DataFrame", "DataFrame", "DataFrame", "DataFrame", "DataFrame", "DataFrame", "DataFrame", "DataFrame"]:
    sampled = build_normalized_sampled_points(raw_df, normalization_profile_df, cfg)
    column_sketch = build_column_sketch(sampled)
    column_leverage = build_column_leverage(column_sketch)
    sensor_leverage = select_sampled_sensors(
        column_leverage,
        pivots_k=cfg.pivots_k,
        sampling_mode=cfg.sampling_mode,
        sampling_seed=cfg.sampling_seed,
    )
    row_sketch = build_row_sketch(sampled)
    row_energy = select_sampled_rows(
        row_sketch,
        row_samples_k=cfg.row_samples_k,
        sampling_mode=cfg.sampling_mode,
        sampling_seed=cfg.sampling_seed,
    )

    sampled_with_row_id = sampled.withColumn(
        "row_id",
        F.sha2(F.concat_ws("|", F.col("tail_id"), F.col("flight_id"), F.col("bucket_index").cast("string")), 256),
    )

    c_matrix = (
        sampled_with_row_id.join(sensor_leverage.select("sensor"), on="sensor", how="inner")
        .select("row_id", "sensor", F.col("x").alias("value"))
    )

    r_matrix = (
        sampled_with_row_id.join(row_energy.select("row_id"), on="row_id", how="inner")
        .select("row_id", "sensor", F.col("x").alias("value"))
    )

    w_matrix = (
        sampled_with_row_id.join(row_energy.select("row_id"), on="row_id", how="inner")
        .join(sensor_leverage.select("sensor"), on="sensor", how="inner")
        .select("row_id", "sensor", F.col("x").alias("value"))
    )

    return sensor_leverage, row_energy, c_matrix, r_matrix, w_matrix, column_sketch, column_leverage, row_sketch


def build_u_core_from_w(
    spark: "SparkSession",
    w_matrix_df: "DataFrame",
    sampled_rows_df: "DataFrame",
    sampled_sensors_df: "DataFrame",
    *,
    max_core_cells: int,
    min_core_rows: int,
    min_core_cols: int,
) -> tuple["DataFrame", dict[str, int | bool]]:
    max_cells = max(int(max_core_cells), 1)
    min_rows = max(int(min_core_rows), 1)
    min_cols = max(int(min_core_cols), 1)

    row_order = [
        row.asDict(recursive=True)
        for row in sampled_rows_df.orderBy(F.col("row_energy").desc(), F.col("row_id").asc())
        .select("row_id", "row_energy")
        .collect()
    ]
    col_order = [
        row.asDict(recursive=True)
        for row in sampled_sensors_df.orderBy(F.col("leverage_score").desc(), F.col("energy").desc(), F.col("sensor").asc())
        .select("sensor", "leverage_score", "energy")
        .collect()
    ]

    requested_row_count = len(row_order)
    requested_col_count = len(col_order)
    requested_core_cells = requested_row_count * requested_col_count

    if not row_order or not col_order:
        schema = "row_id string, sensor string, u_value double"
        return spark.createDataFrame([], schema=schema), {
            "requested_row_count": requested_row_count,
            "requested_col_count": requested_col_count,
            "requested_core_cells": requested_core_cells,
            "effective_row_count": 0,
            "effective_col_count": 0,
            "effective_core_cells": 0,
            "guardrail_applied": False,
            "max_core_cells": max_cells,
        }

    effective_row_count = requested_row_count
    effective_col_count = requested_col_count
    guardrail_applied = False
    if requested_core_cells > max_cells:
        guardrail_applied = True
        row_to_col_ratio = float(requested_row_count) / float(max(requested_col_count, 1))
        target_rows = int((float(max_cells) * row_to_col_ratio) ** 0.5)
        target_rows = max(min_rows, min(target_rows, requested_row_count))
        target_cols = max(min_cols, min(requested_col_count, max_cells // max(target_rows, 1)))

        if target_rows * target_cols > max_cells:
            target_rows = max(min_rows, min(target_rows, max_cells // max(target_cols, 1)))
        if target_rows * target_cols > max_cells:
            target_cols = max(min_cols, min(target_cols, max_cells // max(target_rows, 1)))

        if target_rows * target_cols <= 0:
            target_rows = 1
            target_cols = 1

        target_rows = min(target_rows, requested_row_count)
        target_cols = min(target_cols, requested_col_count)
        effective_row_count = target_rows
        effective_col_count = target_cols

        row_order = row_order[:effective_row_count]
        col_order = col_order[:effective_col_count]

    row_ids = [str(row["row_id"]) for row in row_order]
    sensors = [str(row["sensor"]) for row in col_order]
    effective_core_cells = len(row_ids) * len(sensors)

    pivot_rows = (
        w_matrix_df.groupBy("row_id")
        .pivot("sensor", sensors)
        .agg(F.first("value"))
        .where(F.col("row_id").isin(row_ids))
        .collect()
    )

    matrix_rows: dict[str, dict[str, float]] = {}
    for row in pivot_rows:
        values = row.asDict(recursive=True)
        rid = str(values.get("row_id"))
        matrix_rows[rid] = {sensor: float(values.get(sensor) or 0.0) for sensor in sensors}

    import numpy as np

    w = np.array([[matrix_rows.get(rid, {}).get(sensor, 0.0) for sensor in sensors] for rid in row_ids], dtype=float)
    if w.size == 0:
        schema = "row_id string, sensor string, u_value double"
        return spark.createDataFrame([], schema=schema), {
            "requested_row_count": requested_row_count,
            "requested_col_count": requested_col_count,
            "requested_core_cells": requested_core_cells,
            "effective_row_count": len(row_ids),
            "effective_col_count": len(sensors),
            "effective_core_cells": effective_core_cells,
            "guardrail_applied": guardrail_applied,
            "max_core_cells": max_cells,
        }

    u = np.linalg.pinv(w)

    u_rows: list[tuple[str, str, float]] = []
    for sensor_index, sensor in enumerate(sensors):
        for row_index, row_id in enumerate(row_ids):
            value = float(u[sensor_index, row_index])
            if abs(value) <= 1e-12:
                continue
            u_rows.append((row_id, sensor, value))

    schema = "row_id string, sensor string, u_value double"
    return spark.createDataFrame(u_rows, schema=schema), {
        "requested_row_count": requested_row_count,
        "requested_col_count": requested_col_count,
        "requested_core_cells": requested_core_cells,
        "effective_row_count": len(row_ids),
        "effective_col_count": len(sensors),
        "effective_core_cells": effective_core_cells,
        "guardrail_applied": guardrail_applied,
        "max_core_cells": max_cells,
    }


if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
