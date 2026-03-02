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
    column_sketch_dim: int = 256
    row_sketch_dim: int = 256
    sketch_seed: int = 42


def resolve_u_contraction_mode(contraction_mode: str | None, *, has_a_matrix: bool) -> str:
    mode = str(contraction_mode or "pivot_restricted_a").strip().lower()
    if mode not in {"pivot_restricted_a", "core_w", "full_a"}:
        mode = "pivot_restricted_a"
    if mode == "full_a" and not bool(has_a_matrix):
        mode = "pivot_restricted_a"
    return mode


def _countsketch_projection(
    values_df: "DataFrame",
    *,
    entity_col: str,
    key_col: str,
    value_col: str,
    sketch_dim: int,
    seed: int,
) -> "DataFrame":
    dim = max(int(sketch_dim), 1)
    bucket = F.pmod(F.xxhash64(F.col(key_col).cast("string"), F.lit(int(seed))), F.lit(dim)).cast("int")
    sign = F.when(
        F.pmod(F.xxhash64(F.col(key_col).cast("string"), F.lit(int(seed) + 1)), F.lit(2)) == F.lit(0),
        F.lit(1.0),
    ).otherwise(F.lit(-1.0))
    return (
        values_df.withColumn("sketch_bucket", bucket)
        .withColumn("signed_value", F.col(value_col).cast("double") * sign)
        .groupBy(entity_col, "sketch_bucket")
        .agg(F.sum("signed_value").alias("bucket_sum"))
    )


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


def build_column_sketch(sampled_points_df: "DataFrame", cfg: CurFitConfig) -> "DataFrame":
    row_key = F.concat_ws("|", F.col("tail_id"), F.col("flight_id"), F.col("bucket_index").cast("string"))
    base = sampled_points_df.select("sensor", row_key.alias("row_key"), F.col("x").cast("double").alias("x"))
    projected = _countsketch_projection(
        base,
        entity_col="sensor",
        key_col="row_key",
        value_col="x",
        sketch_dim=cfg.column_sketch_dim,
        seed=cfg.sketch_seed,
    )

    sketch_stats = projected.groupBy("sensor").agg(
        F.sum(F.col("bucket_sum") * F.col("bucket_sum")).alias("sketch_energy"),
        F.sum(F.abs(F.col("bucket_sum"))).alias("sketch_l1"),
        F.count("*").alias("sketch_buckets_nonzero"),
    )
    exact_stats = sampled_points_df.groupBy("sensor").agg(
        F.count("*").alias("points"),
        F.sum(F.col("x")).alias("sum_x"),
        F.sum(F.abs(F.col("x"))).alias("sum_abs_x"),
        F.sum(F.col("x") * F.col("x")).alias("sum_x2"),
    )

    return (
        exact_stats.join(sketch_stats, on="sensor", how="left")
        .withColumn("sum_x2", F.coalesce(F.col("sum_x2"), F.lit(0.0)))
        .withColumn("raw_energy", F.col("sum_x2"))
        .withColumn("energy", F.coalesce(F.col("sketch_energy"), F.col("sum_x2"), F.lit(0.0)))
        .withColumn("sketch_dim", F.lit(max(int(cfg.column_sketch_dim), 1)).cast("int"))
        .withColumn("sketch_seed", F.lit(int(cfg.sketch_seed)).cast("int"))
        .withColumn("sketch_type", F.lit("countsketch"))
        .drop("sketch_energy")
    )


def build_column_leverage(column_sketch_df: "DataFrame") -> "DataFrame":
    total_energy_df = column_sketch_df.agg(F.sum("energy").alias("total_energy"))
    return (
        column_sketch_df.crossJoin(total_energy_df)
        .withColumn(
            "leverage_score",
            F.when(F.col("total_energy") > F.lit(1e-12), F.col("energy") / F.col("total_energy")).otherwise(F.lit(0.0)),
        )
        .drop("total_energy")
    )


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


def build_row_sketch(sampled_points_df: "DataFrame", cfg: CurFitConfig) -> "DataFrame":
    with_row_id = sampled_points_df.withColumn(
        "row_id",
        F.sha2(F.concat_ws("|", F.col("tail_id"), F.col("flight_id"), F.col("bucket_index").cast("string")), 256),
    )

    projected = _countsketch_projection(
        with_row_id.select("row_id", "sensor", F.col("x").cast("double").alias("x")),
        entity_col="row_id",
        key_col="sensor",
        value_col="x",
        sketch_dim=cfg.row_sketch_dim,
        seed=cfg.sketch_seed + 7919,
    )
    sketch_stats = projected.groupBy("row_id").agg(
        F.sum(F.col("bucket_sum") * F.col("bucket_sum")).alias("row_sketch_energy"),
        F.sum(F.abs(F.col("bucket_sum"))).alias("row_sketch_l1"),
        F.count("*").alias("row_sketch_buckets_nonzero"),
    )
    base_rows = with_row_id.select("tail_id", "flight_id", "bucket_index", "row_id").distinct()
    sensor_counts = with_row_id.groupBy("row_id").agg(F.countDistinct("sensor").alias("sensor_count"))

    return (
        base_rows.join(sensor_counts, on="row_id", how="left")
        .join(sketch_stats, on="row_id", how="left")
        .withColumn("row_energy", F.coalesce(F.col("row_sketch_energy"), F.lit(0.0)))
        .withColumn("sketch_dim", F.lit(max(int(cfg.row_sketch_dim), 1)).cast("int"))
        .withColumn("sketch_seed", F.lit(int(cfg.sketch_seed + 7919)).cast("int"))
        .withColumn("sketch_type", F.lit("countsketch"))
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
) -> tuple["DataFrame", "DataFrame", "DataFrame", "DataFrame", "DataFrame", "DataFrame", "DataFrame", "DataFrame", "DataFrame"]:
    sampled = build_normalized_sampled_points(raw_df, normalization_profile_df, cfg)
    column_sketch = build_column_sketch(sampled, cfg)
    column_leverage = build_column_leverage(column_sketch)
    sensor_leverage = select_sampled_sensors(
        column_leverage,
        pivots_k=cfg.pivots_k,
        sampling_mode=cfg.sampling_mode,
        sampling_seed=cfg.sampling_seed,
    )
    row_sketch = build_row_sketch(sampled, cfg)
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

    a_matrix = sampled_with_row_id.select("row_id", "sensor", F.col("x").alias("value"))

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

    return sensor_leverage, row_energy, a_matrix, c_matrix, r_matrix, w_matrix, column_sketch, column_leverage, row_sketch


def build_u_core_from_w(
    spark: "SparkSession",
    w_matrix_df: "DataFrame",
    a_matrix_df: "DataFrame | None",
    c_matrix_df: "DataFrame",
    r_matrix_df: "DataFrame",
    sampled_rows_df: "DataFrame",
    sampled_sensors_df: "DataFrame",
    *,
    max_core_cells: int,
    min_core_rows: int,
    min_core_cols: int,
    contraction_mode: str = "pivot_restricted_a",
) -> tuple["DataFrame", dict[str, int | bool]]:
    from pyspark.mllib.linalg import Vectors as MLLibVectors
    from pyspark.mllib.linalg.distributed import RowMatrix

    max_cells = max(int(max_core_cells), 1)
    min_rows = max(int(min_core_rows), 1)
    min_cols = max(int(min_core_cols), 1)

    empty_u_df = spark.createDataFrame([], schema="row_id string, sensor string, u_value double")

    ordered_rows_df = sampled_rows_df.orderBy(F.col("row_energy").desc(), F.col("row_id").asc()).select("row_id")
    ordered_cols_df = sampled_sensors_df.orderBy(F.col("leverage_score").desc(), F.col("energy").desc(), F.col("sensor").asc()).select(
        "sensor"
    )

    requested_row_count = int(ordered_rows_df.count())
    requested_col_count = int(ordered_cols_df.count())
    requested_core_cells = requested_row_count * requested_col_count

    if requested_row_count <= 0 or requested_col_count <= 0:
        return empty_u_df, {
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

        effective_row_count = min(target_rows, requested_row_count)
        effective_col_count = min(target_cols, requested_col_count)

    effective_core_cells = effective_row_count * effective_col_count
    if effective_core_cells <= 0:
        return empty_u_df, {
            "requested_row_count": requested_row_count,
            "requested_col_count": requested_col_count,
            "requested_core_cells": requested_core_cells,
            "effective_row_count": effective_row_count,
            "effective_col_count": effective_col_count,
            "effective_core_cells": effective_core_cells,
            "guardrail_applied": guardrail_applied,
            "max_core_cells": max_cells,
        }

    selected_rows_indexed = spark.createDataFrame(
        ordered_rows_df.limit(effective_row_count).rdd.map(lambda row: str(row["row_id"])).zipWithIndex().map(
            lambda pair: (int(pair[1]) + 1, pair[0])
        ),
        schema="rn long, row_id string",
    )
    selected_cols_indexed = spark.createDataFrame(
        ordered_cols_df.limit(effective_col_count).rdd.map(lambda row: str(row["sensor"])).zipWithIndex().map(
            lambda pair: (int(pair[1]) + 1, pair[0])
        ),
        schema="cn long, sensor string",
    )

    selected_w = (
        w_matrix_df.join(selected_rows_indexed, on="row_id", how="inner")
        .join(selected_cols_indexed, on="sensor", how="inner")
        .select("rn", "row_id", "cn", F.col("value").cast("double").alias("value"))
    )

    c_rows_indexed = spark.createDataFrame(
        c_matrix_df.select("row_id").distinct().orderBy(F.col("row_id").asc()).rdd.map(lambda row: str(row["row_id"])).zipWithIndex().map(
            lambda indexed: (indexed[0], int(indexed[1]) + 1)
        ),
        schema="row_id string, c_row_idx long",
    )
    c_row_count = int(c_rows_indexed.count())
    if c_row_count <= 0:
        return empty_u_df, {
            "requested_row_count": requested_row_count,
            "requested_col_count": requested_col_count,
            "requested_core_cells": requested_core_cells,
            "effective_row_count": effective_row_count,
            "effective_col_count": effective_col_count,
            "effective_core_cells": effective_core_cells,
            "guardrail_applied": guardrail_applied,
            "max_core_cells": max_cells,
        }

    c_selected = (
        c_matrix_df.join(selected_cols_indexed.select("sensor", "cn"), on="sensor", how="inner")
        .join(c_rows_indexed, on="row_id", how="inner")
        .select("c_row_idx", "cn", F.col("value").cast("double").alias("value"))
    )
    c_selected_agg = c_selected.groupBy("c_row_idx").agg(
        F.map_from_entries(F.collect_list(F.struct(F.col("cn"), F.coalesce(F.col("value"), F.lit(0.0))))).alias("value_by_cn")
    )
    c_row_vectors = (
        c_rows_indexed.join(c_selected_agg, on="c_row_idx", how="left")
        .withColumn(
            "values",
            F.expr(f"transform(sequence(1, {effective_col_count}), i -> coalesce(element_at(value_by_cn, i), 0D))"),
        )
        .orderBy("c_row_idx")
        .select("c_row_idx", "row_id", "values")
    )
    c_rows_rdd = c_row_vectors.select("values").rdd.map(lambda row: MLLibVectors.dense([float(v) for v in row["values"]]))
    c_rank = int(min(c_row_count, effective_col_count))
    if c_rank <= 0:
        return empty_u_df, {
            "requested_row_count": requested_row_count,
            "requested_col_count": requested_col_count,
            "requested_core_cells": requested_core_cells,
            "effective_row_count": effective_row_count,
            "effective_col_count": effective_col_count,
            "effective_core_cells": effective_core_cells,
            "guardrail_applied": guardrail_applied,
            "max_core_cells": max_cells,
        }

    c_svd = RowMatrix(c_rows_rdd).computeSVD(c_rank, computeU=True)
    c_singular_values = [float(v) for v in c_svd.s]
    c_inv_s = [1.0 / s if s > 1e-12 else 0.0 for s in c_singular_values]
    c_nonzero_rank = len(c_inv_s)
    if c_nonzero_rank <= 0:
        return empty_u_df, {
            "requested_row_count": requested_row_count,
            "requested_col_count": requested_col_count,
            "requested_core_cells": requested_core_cells,
            "effective_row_count": effective_row_count,
            "effective_col_count": effective_col_count,
            "effective_core_cells": effective_core_cells,
            "guardrail_applied": guardrail_applied,
            "max_core_cells": max_cells,
        }

    c_v_values_bc = spark.sparkContext.broadcast([float(v) for v in c_svd.V.values])
    c_inv_s_bc = spark.sparkContext.broadcast(c_inv_s)
    c_rank_bc = spark.sparkContext.broadcast(c_nonzero_rank)
    c_cols_bc = spark.sparkContext.broadcast(effective_col_count)

    def _emit_cplus_entries(record: tuple[object, int]):
        u_row, row_idx = record
        c_row_pos = int(row_idx) + 1

        rank_count = c_rank_bc.value
        col_count = c_cols_bc.value
        v_values = c_v_values_bc.value
        inv_s_local = c_inv_s_bc.value
        out: list[tuple[int, int, float]] = []
        for cn in range(1, col_count + 1):
            total = 0.0
            for j in range(rank_count):
                v_cj = v_values[(cn - 1) + (j * col_count)]
                total += v_cj * inv_s_local[j] * float(u_row[j])
            if abs(total) > 1e-12:
                out.append((cn, c_row_pos, float(total)))
        return out

    cplus_idx_df_raw = spark.createDataFrame(
        c_svd.U.rows.zipWithIndex().flatMap(_emit_cplus_entries),
        schema="cn long, c_row_idx long, cplus_value double",
    )
    cplus_idx_df = (
        cplus_idx_df_raw.join(c_rows_indexed.select("c_row_idx", "row_id"), on="c_row_idx", how="inner")
        .select("cn", "row_id", "cplus_value")
    )

    c_v_values_bc.unpersist(blocking=False)
    c_inv_s_bc.unpersist(blocking=False)
    c_rank_bc.unpersist(blocking=False)
    c_cols_bc.unpersist(blocking=False)

    r_sensors_indexed = spark.createDataFrame(
        r_matrix_df.select("sensor").distinct().orderBy(F.col("sensor").asc()).rdd.map(lambda row: str(row["sensor"])).zipWithIndex().map(
            lambda indexed: (indexed[0], int(indexed[1]) + 1)
        ),
        schema="sensor string, r_sensor_idx long",
    )
    r_sensor_count = int(r_sensors_indexed.count())
    if r_sensor_count <= 0:
        return empty_u_df, {
            "requested_row_count": requested_row_count,
            "requested_col_count": requested_col_count,
            "requested_core_cells": requested_core_cells,
            "effective_row_count": effective_row_count,
            "effective_col_count": effective_col_count,
            "effective_core_cells": effective_core_cells,
            "guardrail_applied": guardrail_applied,
            "max_core_cells": max_cells,
        }

    r_selected = (
        r_matrix_df.join(selected_rows_indexed.select("row_id", "rn"), on="row_id", how="inner")
        .join(r_sensors_indexed, on="sensor", how="inner")
        .select("rn", "r_sensor_idx", F.col("value").cast("double").alias("value"))
    )
    r_selected_agg = r_selected.groupBy("rn").agg(
        F.map_from_entries(F.collect_list(F.struct(F.col("r_sensor_idx"), F.coalesce(F.col("value"), F.lit(0.0))))).alias("value_by_sensor_idx")
    )
    r_row_vectors = (
        selected_rows_indexed.join(r_selected_agg, on="rn", how="left")
        .withColumn(
            "values",
            F.expr(f"transform(sequence(1, {r_sensor_count}), i -> coalesce(element_at(value_by_sensor_idx, i), 0D))"),
        )
        .orderBy("rn")
        .select("rn", "row_id", "values")
    )

    r_rows_rdd = r_row_vectors.select("values").rdd.map(lambda row: MLLibVectors.dense([float(v) for v in row["values"]]))
    r_rank = int(min(effective_row_count, r_sensor_count))
    if r_rank <= 0:
        return empty_u_df, {
            "requested_row_count": requested_row_count,
            "requested_col_count": requested_col_count,
            "requested_core_cells": requested_core_cells,
            "effective_row_count": effective_row_count,
            "effective_col_count": effective_col_count,
            "effective_core_cells": effective_core_cells,
            "guardrail_applied": guardrail_applied,
            "max_core_cells": max_cells,
        }

    r_svd = RowMatrix(r_rows_rdd).computeSVD(r_rank, computeU=True)
    r_singular_values = [float(v) for v in r_svd.s]
    r_inv_s = [1.0 / s if s > 1e-12 else 0.0 for s in r_singular_values]
    r_nonzero_rank = len(r_inv_s)
    if r_nonzero_rank <= 0:
        return empty_u_df, {
            "requested_row_count": requested_row_count,
            "requested_col_count": requested_col_count,
            "requested_core_cells": requested_core_cells,
            "effective_row_count": effective_row_count,
            "effective_col_count": effective_col_count,
            "effective_core_cells": effective_core_cells,
            "guardrail_applied": guardrail_applied,
            "max_core_cells": max_cells,
        }

    selected_sensor_positions = [
        (int(row["cn"]), int(row["r_sensor_idx"]))
        for row in selected_cols_indexed.join(r_sensors_indexed, on="sensor", how="inner")
        .select("cn", "r_sensor_idx")
        .collect()
    ]
    r_u_rows = {
        int(idx) + 1: [float(u_row[j]) for j in range(r_nonzero_rank)]
        for u_row, idx in r_svd.U.rows.zipWithIndex().collect()
    }
    r_v_values = [float(v) for v in r_svd.V.values]

    mode = resolve_u_contraction_mode(contraction_mode, has_a_matrix=(a_matrix_df is not None))

    rplus_sensor_positions = (
        [sensor_pos for _, sensor_pos in selected_sensor_positions]
        if mode in {"pivot_restricted_a", "core_w"}
        else list(range(1, r_sensor_count + 1))
    )

    rplus_rows: list[tuple[int, int, float]] = []
    for sensor_pos in rplus_sensor_positions:
        for rn in range(1, effective_row_count + 1):
            u_vec = r_u_rows.get(rn)
            if u_vec is None:
                continue
            total = 0.0
            for j in range(r_nonzero_rank):
                v_nj = r_v_values[(sensor_pos - 1) + (j * r_sensor_count)]
                total += v_nj * r_inv_s[j] * u_vec[j]
            if abs(total) > 1e-12:
                rplus_rows.append((sensor_pos, rn, float(total)))

    rplus_idx_df = spark.createDataFrame(rplus_rows, schema="r_sensor_idx long, rn_out long, rplus_value double")

    cn_to_rsensor_df = selected_cols_indexed.join(r_sensors_indexed, on="sensor", how="inner").select(
        "cn", "r_sensor_idx"
    )

    if mode == "core_w":
        cw_df = (
            cplus_idx_df.alias("c")
            .join(selected_w.alias("w"), on=F.col("c.row_id") == F.col("w.row_id"), how="inner")
            .groupBy(F.col("c.cn").alias("cn_left"), F.col("w.cn").alias("cn_right"))
            .agg(F.sum(F.col("c.cplus_value") * F.col("w.value")).alias("cw_value"))
        ).join(cn_to_rsensor_df.alias("m"), on=F.col("cn_right") == F.col("m.cn"), how="inner").select(
            F.col("cn_left"), F.col("m.r_sensor_idx"), F.col("cw_value")
        )
    else:
        if mode == "pivot_restricted_a":
            a_pivot_df = (
                c_matrix_df.join(selected_cols_indexed.select("sensor", "cn"), on="sensor", how="inner")
                .select("row_id", F.col("cn").alias("cn_right"), F.col("value").cast("double").alias("a_value"))
            )
            cw_df = (
                cplus_idx_df.alias("c")
                .join(a_pivot_df.alias("a"), on=F.col("c.row_id") == F.col("a.row_id"), how="inner")
                .groupBy(F.col("c.cn").alias("cn_left"), F.col("a.cn_right").alias("cn_right"))
                .agg(F.sum(F.col("c.cplus_value") * F.col("a.a_value")).alias("cw_value"))
            ).join(cn_to_rsensor_df.alias("m"), on=F.col("cn_right") == F.col("m.cn"), how="inner").select(
                F.col("cn_left"), F.col("m.r_sensor_idx"), F.col("cw_value")
            )
        else:
            a_full_df = (
                a_matrix_df.join(r_sensors_indexed, on="sensor", how="inner")
                .select("row_id", "r_sensor_idx", F.col("value").cast("double").alias("a_value"))
            )
            cw_df = (
                cplus_idx_df.alias("c")
                .join(a_full_df.alias("a"), on=F.col("c.row_id") == F.col("a.row_id"), how="inner")
                .groupBy(F.col("c.cn").alias("cn_left"), F.col("a.r_sensor_idx").alias("r_sensor_idx"))
                .agg(F.sum(F.col("c.cplus_value") * F.col("a.a_value")).alias("cw_value"))
            )

    u_idx_df = (
        cw_df.alias("cw")
        .join(rplus_idx_df.alias("r"), on=F.col("cw.r_sensor_idx") == F.col("r.r_sensor_idx"), how="inner")
        .groupBy(F.col("cw.cn_left").alias("cn"), F.col("r.rn_out").alias("rn"))
        .agg(F.sum(F.col("cw.cw_value") * F.col("r.rplus_value")).alias("u_value"))
        .where(F.abs(F.col("u_value")) > F.lit(1e-12))
    )

    u_df = (
        u_idx_df.join(selected_cols_indexed.select("cn", "sensor"), on="cn", how="inner")
        .join(selected_rows_indexed.select("rn", "row_id"), on="rn", how="inner")
        .select("row_id", "sensor", "u_value")
    )

    return u_df, {
        "requested_row_count": requested_row_count,
        "requested_col_count": requested_col_count,
        "requested_core_cells": requested_core_cells,
        "effective_row_count": effective_row_count,
        "effective_col_count": effective_col_count,
        "effective_core_cells": effective_core_cells,
        "guardrail_applied": guardrail_applied,
        "max_core_cells": max_cells,
        "cplus_rank": c_nonzero_rank,
        "rplus_rank": r_nonzero_rank,
        "contraction_mode": mode,
    }


if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
