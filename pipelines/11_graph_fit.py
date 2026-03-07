"""Fit graph and hierarchy artifacts from backbone, events, and windows."""

import os

from libs.graph import (
    build_event_graph_spark_table,
    build_graph_fusion_from_component_tables,
    build_lag_graph_spark_table,
    build_precision_graph_from_window_x_spark_table,
    build_transition_graph_spark_table,
)
from libs.io.delta import get_spark, read_table, write_table
from libs.perf import (
    build_artifact_manifest,
    build_stage_manifest,
    get_logger,
    log_dict_artifact_if_active,
    log_params_if_active,
    log_stage_manifest_if_active,
    log_wall_time,
    track_mlflow_run,
)
from libs.windows import build_window_x_spark_table


LOGGER = get_logger(__name__)


def _bounded_count(df: "DataFrame", *, limit: int) -> int:
    return int(df.limit(max(int(limit), 0) + 1).count())


@track_mlflow_run(stage_name="11_graph_fit", logger=LOGGER)
@log_wall_time(logger=LOGGER)
def run() -> None:
    raw_path = os.getenv("S3NTINEL_RAW_TABLE_PATH", "data/delta/raw_telemetry")
    events_path = os.getenv("S3NTINEL_EVENTS_TABLE_PATH", "data/delta/events")
    windows_path = os.getenv("S3NTINEL_WINDOWS_TABLE_PATH", "data/delta/windows")
    backbone_path = os.getenv("S3NTINEL_BACKBONE_TABLE_PATH", "data/delta/backbone")
    precision_graph_path = os.getenv("S3NTINEL_PRECISION_GRAPH_TABLE_PATH", "data/delta/precision_graph")
    event_graph_path = os.getenv("S3NTINEL_EVENT_GRAPH_TABLE_PATH", "data/delta/event_graph")
    lag_graph_path = os.getenv("S3NTINEL_LAG_GRAPH_TABLE_PATH", "data/delta/lag_graph")
    transition_graph_path = os.getenv("S3NTINEL_TRANSITION_GRAPH_TABLE_PATH", "data/delta/transition_graph")
    fused_graph_path = os.getenv("S3NTINEL_FUSED_GRAPH_TABLE_PATH", "data/delta/fused_graph")
    hierarchy_map_path = os.getenv("S3NTINEL_HIERARCHY_SENSOR_MAP_TABLE_PATH", "data/delta/hierarchy_sensor_map")
    table_format = os.getenv("S3NTINEL_TABLE_FORMAT", "delta")
    write_mode = os.getenv("S3NTINEL_FIT_WRITE_MODE", "overwrite")

    precision_ridge_lambda = float(os.getenv("S3NTINEL_PRECISION_GRAPH_RIDGE_LAMBDA", "1.0"))
    min_abs_partial_corr = float(os.getenv("S3NTINEL_V2_MIN_ABS_PARTIAL_CORR", "0.05"))
    min_event_count = int(os.getenv("S3NTINEL_V2_EVENT_GRAPH_MIN_COUNT", "1"))
    min_event_npmi = float(
        os.getenv(
            "S3NTINEL_V2_EVENT_GRAPH_MIN_NPMI",
            os.getenv("S3NTINEL_V2_EVENT_GRAPH_MIN_JACCARD", "0.0"),
        )
    )
    lag_tau_max_seconds = float(os.getenv("S3NTINEL_V2_LAG_TAU_MAX_SECONDS", "30.0"))
    min_lag_count = int(os.getenv("S3NTINEL_V2_LAG_GRAPH_MIN_COUNT", "1"))
    min_transition_count = int(os.getenv("S3NTINEL_V2_TRANSITION_GRAPH_MIN_COUNT", "1"))
    alpha = float(os.getenv("S3NTINEL_V2_GRAPH_ALPHA", "1.0"))
    beta = float(os.getenv("S3NTINEL_V2_GRAPH_BETA", "1.0"))
    gamma = float(os.getenv("S3NTINEL_V2_GRAPH_GAMMA", "1.0"))
    min_fused_edge_weight = float(os.getenv("S3NTINEL_V2_GRAPH_MIN_FUSED_EDGE_WEIGHT", "0.05"))
    hierarchy_top_k_per_sensor = int(os.getenv("S3NTINEL_V2_HIERARCHY_TOP_K_PER_SENSOR", "3"))
    hierarchy_subsystem_min_edge_weight_raw = os.getenv("S3NTINEL_V2_HIERARCHY_SUBSYSTEM_MIN_EDGE_WEIGHT")
    hierarchy_system_min_edge_weight_raw = os.getenv("S3NTINEL_V2_HIERARCHY_SYSTEM_MIN_EDGE_WEIGHT")
    hierarchy_subsystem_min_edge_weight = (
        float(hierarchy_subsystem_min_edge_weight_raw)
        if hierarchy_subsystem_min_edge_weight_raw not in (None, "")
        else None
    )
    hierarchy_system_min_edge_weight = (
        float(hierarchy_system_min_edge_weight_raw)
        if hierarchy_system_min_edge_weight_raw not in (None, "")
        else None
    )
    max_bridge_rows = int(os.getenv("S3NTINEL_MAX_BRIDGE_GRAPH_INPUT_ROWS", "250000"))

    spark = get_spark("s3ntinel.graph_fit")
    raw_df = read_table(spark, raw_path, fmt=table_format)
    events_df = read_table(spark, events_path, fmt=table_format)
    windows_df = read_table(spark, windows_path, fmt=table_format)
    backbone_df = read_table(spark, backbone_path, fmt=table_format)
    raw_count = _bounded_count(raw_df, limit=max_bridge_rows)
    events_count = _bounded_count(events_df, limit=max_bridge_rows)
    windows_count = _bounded_count(windows_df, limit=max_bridge_rows)
    backbone_count = _bounded_count(backbone_df, limit=max_bridge_rows)
    if raw_count > max_bridge_rows or events_count > max_bridge_rows or windows_count > max_bridge_rows or backbone_count > max_bridge_rows:
        raise RuntimeError(
            "11_graph_fit still uses a bounded pandas bridge; input exceeds "
            f"S3NTINEL_MAX_BRIDGE_GRAPH_INPUT_ROWS={max_bridge_rows}. "
            "Reduce input size or replace this stage with a distributed implementation."
        )
    empty_events_df = spark.createDataFrame(
        [],
        schema="tail_id string, flight_id string, parameter_name string, timestamp_utc timestamp, event_type_detected string, payload map<string,string>",
    )
    window_x_df = build_window_x_spark_table(raw_df, empty_events_df, windows_df)
    window_x_count = _bounded_count(window_x_df, limit=max_bridge_rows)
    if window_x_count > max_bridge_rows:
        raise RuntimeError(
            "11_graph_fit still bridges `window_x` / events to pandas; "
            f"window_x exceeds S3NTINEL_MAX_BRIDGE_GRAPH_INPUT_ROWS={max_bridge_rows}. "
            "Replace the remaining graph fit with a distributed implementation."
        )

    event_sdf = build_event_graph_spark_table(
        events_df,
        windows_df,
        min_count=min_event_count,
        min_npmi=min_event_npmi,
        top_k_per_sensor=int(os.getenv("S3NTINEL_V2_EVENT_GRAPH_TOP_K_PER_SENSOR", "8")),
    )
    lag_sdf = build_lag_graph_spark_table(
        events_df,
        tau_max_seconds=lag_tau_max_seconds,
        min_count=min_lag_count,
        max_mean_lag_seconds=float(os.getenv("S3NTINEL_V2_LAG_GRAPH_MAX_MEAN_LAG_SECONDS", "5.0")),
        top_k_outgoing=int(os.getenv("S3NTINEL_V2_LAG_GRAPH_TOP_K_OUTGOING", "4")),
    )
    transition_sdf = build_transition_graph_spark_table(
        events_df,
        min_count=min_transition_count,
    )

    backbone_pdf = backbone_df.toPandas()
    event_pdf = event_sdf.toPandas()
    lag_pdf = lag_sdf.toPandas()
    transition_pdf = transition_sdf.toPandas()
    precision_pdf = build_precision_graph_from_window_x_spark_table(
        window_x_df,
        selected_sensors=(backbone_pdf.iloc[0]["selected_sensors_c"] if not backbone_pdf.empty else []),
        ridge_lambda=precision_ridge_lambda,
        min_abs_partial_corr=min_abs_partial_corr,
    )
    fused_pdf, hierarchy_pdf = build_graph_fusion_from_component_tables(
        precision_pdf,
        event_pdf,
        lag_pdf,
        backbone_pdf,
        alpha=alpha,
        beta=beta,
        gamma=gamma,
        min_fused_edge_weight=min_fused_edge_weight,
        hierarchy_top_k_per_sensor=hierarchy_top_k_per_sensor,
        hierarchy_subsystem_min_edge_weight=hierarchy_subsystem_min_edge_weight,
        hierarchy_system_min_edge_weight=hierarchy_system_min_edge_weight,
    )

    precision_df = spark.createDataFrame(precision_pdf) if not precision_pdf.empty else spark.createDataFrame([], schema="sensor_u string, sensor_v string, partial_corr double, precision_weight double, edge_family string")
    event_df = spark.createDataFrame(event_pdf) if not event_pdf.empty else spark.createDataFrame([], schema="sensor_u string, sensor_v string, cooccur_count int, event_weight double, edge_family string")
    lag_df = spark.createDataFrame(lag_pdf) if not lag_pdf.empty else spark.createDataFrame([], schema="sensor_u string, sensor_v string, lag_count int, lag_weight double, mean_lag_seconds double, edge_family string")
    transition_df = spark.createDataFrame(transition_pdf) if not transition_pdf.empty else spark.createDataFrame([], schema="sensor_u string, sensor_v string, precedence_count int, precedence_weight double, edge_family string")
    fused_df = spark.createDataFrame(fused_pdf) if not fused_pdf.empty else spark.createDataFrame([], schema="sensor_u string, sensor_v string, precision_weight double, event_weight double, lag_weight double, fused_weight double, edge_family string")
    hierarchy_df = spark.createDataFrame(hierarchy_pdf) if not hierarchy_pdf.empty else spark.createDataFrame([], schema="parameter_name string, system_id string, subsystem_id string, module_id string, hierarchy_source string, hierarchy_profile_id string")

    write_table(precision_df, path=precision_graph_path, mode=write_mode, fmt=table_format)
    write_table(event_df, path=event_graph_path, mode=write_mode, fmt=table_format)
    write_table(lag_df, path=lag_graph_path, mode=write_mode, fmt=table_format)
    write_table(transition_df, path=transition_graph_path, mode=write_mode, fmt=table_format)
    write_table(fused_df, path=fused_graph_path, mode=write_mode, fmt=table_format)
    write_table(hierarchy_df, path=hierarchy_map_path, mode=write_mode, fmt=table_format)

    log_params_if_active(
        {
            "precision_ridge_lambda": precision_ridge_lambda,
            "min_abs_partial_corr": min_abs_partial_corr,
            "min_event_count": min_event_count,
            "min_event_npmi": min_event_npmi,
            "lag_tau_max_seconds": lag_tau_max_seconds,
            "min_lag_count": min_lag_count,
            "min_transition_count": min_transition_count,
            "alpha": alpha,
            "beta": beta,
            "gamma": gamma,
            "min_fused_edge_weight": min_fused_edge_weight,
            "hierarchy_top_k_per_sensor": hierarchy_top_k_per_sensor,
            "hierarchy_subsystem_min_edge_weight": hierarchy_subsystem_min_edge_weight,
            "hierarchy_system_min_edge_weight": hierarchy_system_min_edge_weight,
        }
    )
    log_dict_artifact_if_active(
        {
            "stage": "11_graph_fit",
            "raw_path": raw_path,
            "events_path": events_path,
            "windows_path": windows_path,
            "backbone_path": backbone_path,
            "precision_graph_path": precision_graph_path,
            "event_graph_path": event_graph_path,
            "lag_graph_path": lag_graph_path,
            "transition_graph_path": transition_graph_path,
            "fused_graph_path": fused_graph_path,
            "hierarchy_map_path": hierarchy_map_path,
            "precision_edge_count": int(len(precision_pdf)),
            "event_edge_count": int(len(event_pdf)),
            "lag_edge_count": int(len(lag_pdf)),
            "transition_edge_count": int(len(transition_pdf)),
            "fused_edge_count": int(len(fused_pdf)),
            "hierarchy_sensor_count": int(len(hierarchy_pdf)),
            "min_event_npmi": min_event_npmi,
            "hierarchy_top_k_per_sensor": hierarchy_top_k_per_sensor,
            "hierarchy_subsystem_min_edge_weight": hierarchy_subsystem_min_edge_weight,
            "hierarchy_system_min_edge_weight": hierarchy_system_min_edge_weight,
            "max_bridge_graph_input_rows": max_bridge_rows,
            "raw_count_bounded": raw_count,
            "events_count_bounded": events_count,
            "windows_count_bounded": windows_count,
            "backbone_count_bounded": backbone_count,
            "window_x_count_bounded": window_x_count,
            "table_format": table_format,
            "write_mode": write_mode,
        },
        "reports/stages/11_graph_fit_summary.json",
    )
    stage_manifest = build_stage_manifest(
        stage_name="11_graph_fit",
        config={
            "table_format": table_format,
            "write_mode": write_mode,
            "precision_ridge_lambda": precision_ridge_lambda,
            "min_abs_partial_corr": min_abs_partial_corr,
            "min_event_count": min_event_count,
            "min_event_npmi": min_event_npmi,
            "lag_tau_max_seconds": lag_tau_max_seconds,
            "min_lag_count": min_lag_count,
            "min_transition_count": min_transition_count,
            "alpha": alpha,
            "beta": beta,
            "gamma": gamma,
            "min_fused_edge_weight": min_fused_edge_weight,
            "hierarchy_top_k_per_sensor": hierarchy_top_k_per_sensor,
            "hierarchy_subsystem_min_edge_weight": hierarchy_subsystem_min_edge_weight,
            "hierarchy_system_min_edge_weight": hierarchy_system_min_edge_weight,
            "max_bridge_graph_input_rows": max_bridge_rows,
        },
        input_artifacts={
            "raw_telemetry": build_artifact_manifest(path=raw_path, dataframe=raw_df, row_count=raw_count),
            "events": build_artifact_manifest(path=events_path, dataframe=events_df, row_count=events_count),
            "windows": build_artifact_manifest(path=windows_path, dataframe=windows_df, row_count=windows_count),
            "backbone": build_artifact_manifest(path=backbone_path, dataframe=backbone_df, row_count=backbone_count),
            "window_x": build_artifact_manifest(path="window_x::ephemeral", dataframe=window_x_df, row_count=window_x_count),
        },
        output_artifacts={
            "precision_graph": build_artifact_manifest(path=precision_graph_path, dataframe=precision_df, row_count=len(precision_pdf)),
            "event_graph": build_artifact_manifest(path=event_graph_path, dataframe=event_df, row_count=len(event_pdf)),
            "lag_graph": build_artifact_manifest(path=lag_graph_path, dataframe=lag_df, row_count=len(lag_pdf)),
            "transition_graph": build_artifact_manifest(path=transition_graph_path, dataframe=transition_df, row_count=len(transition_pdf)),
            "fused_graph": build_artifact_manifest(path=fused_graph_path, dataframe=fused_df, row_count=len(fused_pdf)),
            "hierarchy_sensor_map": build_artifact_manifest(path=hierarchy_map_path, dataframe=hierarchy_df, row_count=len(hierarchy_pdf)),
        },
        replayable_from=["window_x", "events", "windows", "backbone"],
        cache_artifacts={
            "graph_component_cache": {
                "precision_graph_path": precision_graph_path,
                "event_graph_path": event_graph_path,
                "lag_graph_path": lag_graph_path,
                "transition_graph_path": transition_graph_path,
            }
        },
    )
    log_stage_manifest_if_active(stage_manifest, "reports/stages/11_graph_fit_manifest.json")
    LOGGER.info(
        "pipeline=graph_fit format=%s write_mode=%s precision_edges=%s event_edges=%s lag_edges=%s transition_edges=%s fused_edges=%s hierarchy_sensors=%s",
        table_format,
        write_mode,
        len(precision_pdf),
        len(event_pdf),
        len(lag_pdf),
        len(transition_pdf),
        len(fused_pdf),
        len(hierarchy_pdf),
    )


if __name__ == "__main__":
    run()
