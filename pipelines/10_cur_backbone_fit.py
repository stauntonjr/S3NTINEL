# File: pipelines/10_cur_backbone_fit.py
"""Fit fleet-level CUR backbone and publish a new version."""

import json
import os
from pathlib import Path

from libs.cur.fit import CurFitConfig, build_training_matrix_samples, build_u_core_from_w
from libs.graph.fusion import (
    build_sensor_normalization_profile,
    build_cur_proxy_sensor_graph,
    build_event_cooccurrence_sensor_graph,
    fuse_sensor_graphs,
)
from libs.io.delta import get_spark, read_table, write_table
from libs.perf import get_logger, log_params_if_active, log_wall_time, track_mlflow_run
from pipelines.common import build_context
from pyspark.sql import functions as F


LOGGER = get_logger(__name__)


@track_mlflow_run(stage_name="10_cur_backbone_fit", logger=LOGGER)
@log_wall_time(logger=LOGGER)
def run() -> None:
    context = build_context()
    input_path = os.getenv("S3NTINEL_RAW_TABLE_PATH", "data/delta/raw_telemetry")
    table_format = os.getenv("S3NTINEL_TABLE_FORMAT", "delta")
    write_mode = os.getenv("S3NTINEL_FIT_WRITE_MODE", "overwrite")

    cur_graph_path = os.getenv("S3NTINEL_CUR_GRAPH_TABLE_PATH", "data/delta/cur_sensor_graph")
    event_graph_path = os.getenv("S3NTINEL_EVENT_GRAPH_TABLE_PATH", "data/delta/event_cooccurrence_graph")
    fused_graph_path = os.getenv("S3NTINEL_FUSED_GRAPH_TABLE_PATH", "data/delta/fused_sensor_graph")
    normalization_path = os.getenv("S3NTINEL_CUR_NORMALIZATION_TABLE_PATH", "data/delta/cur_normalization_profile")
    graph_report_path = os.getenv("S3NTINEL_FIT_GRAPH_REPORT_PATH", "reports/fitting_graph_report.json")
    cur_sensor_sample_path = os.getenv("S3NTINEL_CUR_SENSOR_SAMPLE_TABLE_PATH", "data/delta/cur_sensor_sample")
    cur_row_sample_path = os.getenv("S3NTINEL_CUR_ROW_SAMPLE_TABLE_PATH", "data/delta/cur_row_sample")
    cur_column_sketch_path = os.getenv("S3NTINEL_CUR_COLUMN_SKETCH_TABLE_PATH", "data/delta/cur_column_sketch")
    cur_column_leverage_path = os.getenv("S3NTINEL_CUR_COLUMN_LEVERAGE_TABLE_PATH", "data/delta/cur_column_leverage")
    cur_row_sketch_path = os.getenv("S3NTINEL_CUR_ROW_SKETCH_TABLE_PATH", "data/delta/cur_row_sketch")
    cur_c_matrix_path = os.getenv("S3NTINEL_CUR_C_MATRIX_TABLE_PATH", "data/delta/cur_c_matrix")
    cur_r_matrix_path = os.getenv("S3NTINEL_CUR_R_MATRIX_TABLE_PATH", "data/delta/cur_r_matrix")
    cur_w_matrix_path = os.getenv("S3NTINEL_CUR_W_MATRIX_TABLE_PATH", "data/delta/cur_w_matrix")
    cur_u_matrix_path = os.getenv("S3NTINEL_CUR_U_MATRIX_TABLE_PATH", "data/delta/cur_u_matrix")
    effective_pivots_k = int(os.getenv("S3NTINEL_CUR_PIVOTS_K", str(context.config["cur"]["pivots_k"])))
    effective_row_samples_k = int(os.getenv("S3NTINEL_CUR_ROW_SAMPLES_K", str(context.config["cur"]["row_samples_k"])))
    cur_max_core_cells = int(os.getenv("S3NTINEL_CUR_MAX_CORE_CELLS", str(context.config["cur"].get("max_core_cells", 1000000))))
    cur_min_core_rows = int(os.getenv("S3NTINEL_CUR_MIN_CORE_ROWS", str(context.config["cur"].get("min_core_rows", 1))))
    cur_min_core_cols = int(os.getenv("S3NTINEL_CUR_MIN_CORE_COLS", str(context.config["cur"].get("min_core_cols", 1))))

    max_sensors = int(os.getenv("S3NTINEL_CUR_GRAPH_MAX_SENSORS", str(effective_pivots_k)))
    downsample_hz = float(os.getenv("S3NTINEL_CUR_DOWNSAMPLE_HZ", str(context.config["cur"]["downsample_hz"])))
    min_overlap = int(os.getenv("S3NTINEL_CUR_GRAPH_MIN_OVERLAP", str(context.config["graph"]["min_overlap"])))
    min_abs_corr = float(os.getenv("S3NTINEL_CUR_GRAPH_MIN_ABS_CORR", str(context.config["graph"]["min_abs_corr"])))
    min_sensor_points = int(os.getenv("S3NTINEL_CUR_NORMALIZATION_MIN_POINTS", str(context.config["graph"]["normalization"]["min_sensor_points"])))
    normalize_mode = str(os.getenv("S3NTINEL_CUR_NORMALIZATION_MODE", str(context.config["graph"]["normalization"]["mode"])))
    normalize_clip_sigma = float(os.getenv("S3NTINEL_CUR_NORMALIZATION_CLIP_SIGMA", str(context.config["graph"]["normalization"]["clip_sigma"])))
    min_cooccur_count = int(os.getenv("S3NTINEL_EVENT_GRAPH_MIN_COUNT", str(context.config["graph"]["min_cooccur_count"])))
    cur_weight_alpha = float(os.getenv("S3NTINEL_GRAPH_FUSE_ALPHA", str(context.config["graph"]["cur_weight_alpha"])))
    cur_sampling_mode = str(os.getenv("S3NTINEL_CUR_SAMPLING_MODE", str(context.config["cur"].get("sampling_mode", "deterministic"))))
    cur_sampling_seed = int(os.getenv("S3NTINEL_CUR_SAMPLING_SEED", str(context.config["cur"].get("sampling_seed", 42))))

    spark = get_spark("s3ntinel.cur_backbone_fit")
    raw_df = read_table(spark, input_path, fmt=table_format)
    normalization_df = build_sensor_normalization_profile(raw_df, min_sensor_points=min_sensor_points)
    normalization_fallback_applied = False
    normalization_sensors = normalization_df.count()
    effective_min_sensor_points = int(min_sensor_points)
    if normalization_sensors <= 0 and int(min_sensor_points) > 1:
        effective_min_sensor_points = 1
        normalization_df = build_sensor_normalization_profile(raw_df, min_sensor_points=effective_min_sensor_points)
        normalization_sensors = normalization_df.count()
        normalization_fallback_applied = True

    cur_cfg = CurFitConfig(
        pivots_k=max(int(effective_pivots_k), 1),
        row_samples_k=max(int(effective_row_samples_k), 1),
        downsample_hz=max(float(downsample_hz), 0.1),
        normalize_mode=normalize_mode,
        normalize_clip_sigma=normalize_clip_sigma,
        sampling_mode=cur_sampling_mode,
        sampling_seed=cur_sampling_seed,
    )
    (
        sampled_sensors_df,
        sampled_rows_df,
        c_matrix_df,
        r_matrix_df,
        w_matrix_df,
        column_sketch_df,
        column_leverage_df,
        row_sketch_df,
    ) = build_training_matrix_samples(
        raw_df,
        normalization_df,
        cfg=cur_cfg,
    )
    u_matrix_df, u_core_meta = build_u_core_from_w(
        spark,
        w_matrix_df=w_matrix_df,
        sampled_rows_df=sampled_rows_df,
        sampled_sensors_df=sampled_sensors_df,
        max_core_cells=cur_max_core_cells,
        min_core_rows=cur_min_core_rows,
        min_core_cols=cur_min_core_cols,
    )

    cur_graph_df = build_cur_proxy_sensor_graph(
        raw_df,
        normalization_df,
        max_sensors=max_sensors,
        min_overlap=min_overlap,
        min_abs_corr=min_abs_corr,
        downsample_hz=downsample_hz,
        normalize_mode=normalize_mode,
        normalize_clip_sigma=normalize_clip_sigma,
    )
    event_graph_df = build_event_cooccurrence_sensor_graph(
        raw_df,
        min_cooccur_count=min_cooccur_count,
    )
    fused_graph_df = fuse_sensor_graphs(
        cur_graph_df,
        event_graph_df,
        cur_weight_alpha=cur_weight_alpha,
    )

    write_table(normalization_df, path=normalization_path, mode=write_mode, fmt=table_format)
    write_table(column_sketch_df, path=cur_column_sketch_path, mode=write_mode, fmt=table_format)
    write_table(column_leverage_df, path=cur_column_leverage_path, mode=write_mode, fmt=table_format)
    write_table(row_sketch_df, path=cur_row_sketch_path, mode=write_mode, fmt=table_format)
    write_table(sampled_sensors_df, path=cur_sensor_sample_path, mode=write_mode, fmt=table_format)
    write_table(sampled_rows_df, path=cur_row_sample_path, mode=write_mode, fmt=table_format)
    write_table(c_matrix_df, path=cur_c_matrix_path, mode=write_mode, fmt=table_format)
    write_table(r_matrix_df, path=cur_r_matrix_path, mode=write_mode, fmt=table_format)
    write_table(w_matrix_df, path=cur_w_matrix_path, mode=write_mode, fmt=table_format)
    write_table(u_matrix_df, path=cur_u_matrix_path, mode=write_mode, fmt=table_format)
    write_table(cur_graph_df, path=cur_graph_path, mode=write_mode, fmt=table_format)
    write_table(event_graph_df, path=event_graph_path, mode=write_mode, fmt=table_format)
    write_table(fused_graph_df, path=fused_graph_path, mode=write_mode, fmt=table_format)

    column_sketch_count = column_sketch_df.count()
    column_leverage_count = column_leverage_df.count()
    row_sketch_count = row_sketch_df.count()
    sampled_sensor_count = sampled_sensors_df.count()
    sampled_row_count = sampled_rows_df.count()
    c_nnz = c_matrix_df.count()
    r_nnz = r_matrix_df.count()
    w_nnz = w_matrix_df.count()
    u_nnz = u_matrix_df.count()
    cur_edges = cur_graph_df.count()
    event_edges = event_graph_df.count()
    fused_edges = fused_graph_df.count()

    source_mix_rows = [
        row.asDict(recursive=True)
        for row in fused_graph_df.groupBy("edge_source").agg(F.count("*").alias("edge_count")).collect()
    ]
    top_edges_rows = [
        row.asDict(recursive=True)
        for row in fused_graph_df.orderBy(F.col("fused_weight").desc(), F.col("sensor_u").asc(), F.col("sensor_v").asc()).limit(20).collect()
    ]
    fused_weight_stats_row = fused_graph_df.agg(
        F.min("fused_weight").alias("min_fused_weight"),
        F.max("fused_weight").alias("max_fused_weight"),
        F.avg("fused_weight").alias("avg_fused_weight"),
    ).collect()
    fused_weight_stats = fused_weight_stats_row[0].asDict(recursive=True) if fused_weight_stats_row else {}

    report_payload = {
        "input_path": input_path,
        "table_format": table_format,
        "normalization": {
            "path": normalization_path,
            "mode": normalize_mode,
            "clip_sigma": normalize_clip_sigma,
            "min_sensor_points": effective_min_sensor_points,
            "min_sensor_points_requested": min_sensor_points,
            "fallback_applied": normalization_fallback_applied,
            "sensor_count": normalization_sensors,
        },
        "cur_graph": {
            "path": cur_graph_path,
            "max_sensors": max_sensors,
            "min_overlap": min_overlap,
            "min_abs_corr": min_abs_corr,
            "downsample_hz": downsample_hz,
            "edge_count": cur_edges,
        },
        "cur_matrices": {
            "column_sketch_path": cur_column_sketch_path,
            "column_leverage_path": cur_column_leverage_path,
            "row_sketch_path": cur_row_sketch_path,
            "sensor_sample_path": cur_sensor_sample_path,
            "row_sample_path": cur_row_sample_path,
            "c_matrix_path": cur_c_matrix_path,
            "r_matrix_path": cur_r_matrix_path,
            "w_matrix_path": cur_w_matrix_path,
            "u_matrix_path": cur_u_matrix_path,
            "column_sketch_count": column_sketch_count,
            "column_leverage_count": column_leverage_count,
            "row_sketch_count": row_sketch_count,
            "sampling_mode": cur_sampling_mode,
            "sampling_seed": cur_sampling_seed,
            "sampled_sensor_count": sampled_sensor_count,
            "sampled_row_count": sampled_row_count,
            "c_nnz": c_nnz,
            "r_nnz": r_nnz,
            "w_nnz": w_nnz,
            "u_nnz": u_nnz,
            "u_core": u_core_meta,
            "pivots_k": max(int(effective_pivots_k), 1),
            "row_samples_k": max(int(effective_row_samples_k), 1),
        },
        "event_graph": {
            "path": event_graph_path,
            "min_cooccur_count": min_cooccur_count,
            "edge_count": event_edges,
        },
        "fused_graph": {
            "path": fused_graph_path,
            "alpha_cur": cur_weight_alpha,
            "alpha_event": 1.0 - cur_weight_alpha,
            "edge_count": fused_edges,
            "edge_source_mix": source_mix_rows,
            "weight_stats": fused_weight_stats,
            "top_edges": top_edges_rows,
        },
    }
    report_file = Path(graph_report_path)
    report_file.parent.mkdir(parents=True, exist_ok=True)
    report_file.write_text(json.dumps(report_payload, indent=2, default=str), encoding="utf-8")

    log_params_if_active(
        {
            "cur_pivots_k": max(int(effective_pivots_k), 1),
            "cur_row_samples_k": max(int(effective_row_samples_k), 1),
            "cur_graph_max_sensors": max_sensors,
            "cur_graph_min_overlap": min_overlap,
            "cur_graph_min_abs_corr": min_abs_corr,
            "cur_normalization_mode": normalize_mode,
            "cur_normalization_min_sensor_points": effective_min_sensor_points,
            "cur_normalization_fallback_applied": int(normalization_fallback_applied),
            "cur_normalization_sensor_count": normalization_sensors,
            "cur_sampling_mode": cur_sampling_mode,
            "cur_sampling_seed": cur_sampling_seed,
            "cur_column_sketch_count": column_sketch_count,
            "cur_column_leverage_count": column_leverage_count,
            "cur_row_sketch_count": row_sketch_count,
            "cur_sampled_sensor_count": sampled_sensor_count,
            "cur_sampled_row_count": sampled_row_count,
            "cur_u_requested_row_count": int(u_core_meta.get("requested_row_count", 0)),
            "cur_u_requested_col_count": int(u_core_meta.get("requested_col_count", 0)),
            "cur_u_requested_core_cells": int(u_core_meta.get("requested_core_cells", 0)),
            "cur_u_effective_row_count": int(u_core_meta.get("effective_row_count", 0)),
            "cur_u_effective_col_count": int(u_core_meta.get("effective_col_count", 0)),
            "cur_u_effective_core_cells": int(u_core_meta.get("effective_core_cells", 0)),
            "cur_u_guardrail_applied": int(bool(u_core_meta.get("guardrail_applied", False))),
            "cur_u_max_core_cells": int(u_core_meta.get("max_core_cells", cur_max_core_cells)),
            "cur_c_nnz": c_nnz,
            "cur_r_nnz": r_nnz,
            "cur_w_nnz": w_nnz,
            "cur_u_nnz": u_nnz,
            "event_graph_min_count": min_cooccur_count,
            "graph_fuse_alpha": cur_weight_alpha,
            "cur_graph_edges": cur_edges,
            "event_graph_edges": event_edges,
            "fused_graph_edges": fused_edges,
        }
    )
    LOGGER.info(
        "pipeline=cur_backbone_fit input=%s norm_sensors=%s sampled_sensors=%s sampled_rows=%s cur_edges=%s event_edges=%s fused_edges=%s norm=%s c=%s r=%s u=%s cur_graph=%s event_graph=%s fused_graph=%s report=%s",
        input_path,
        normalization_sensors,
        sampled_sensor_count,
        sampled_row_count,
        cur_edges,
        event_edges,
        fused_edges,
        normalization_path,
        cur_c_matrix_path,
        cur_r_matrix_path,
        cur_u_matrix_path,
        cur_graph_path,
        event_graph_path,
        fused_graph_path,
        graph_report_path,
    )


if __name__ == "__main__":
    run()
