"""Build graph component artifacts from backbone, events, and windows."""

import os
import time

from libs.graph import (
    build_event_graph_spark_table,
    build_fused_graph_spark_table,
    build_graph_parameter_universe_spark_table,
    build_lag_graph_spark_table,
    build_precision_graph_from_window_features_spark_table,
    build_transition_graph_spark_table,
)
from libs.io.schemas import GRAPH_PARAMETER_UNIVERSE_SCHEMA, PRECISION_GRAPH_SCHEMA
from libs.io.delta import get_spark, read_table, write_table
from libs.perf import (
    build_artifact_manifest,
    build_stage_manifest,
    get_logger,
    log_memory_usage,
    log_dict_artifact_if_active,
    log_params_if_active,
    log_stage_manifest_if_active,
    log_wall_time,
    track_mlflow_run,
)
from pipelines.common import require_artifact_path


LOGGER = get_logger(__name__)


def _elapsed_ms(start_time: float) -> float:
    return (time.perf_counter() - start_time) * 1000.0


def _materialize_df(df: "DataFrame") -> None:
    """Force persistence so step timings reflect the owning builder."""
    df.count()


def _prepare_graph_events(events_df: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return (
        events_df.select("tail_id", "flight_id", "event_seq_id", "timestamp_utc", "parameter_name")
        .where(
            F.col("tail_id").isNotNull()
            & F.col("flight_id").isNotNull()
            & F.col("event_seq_id").isNotNull()
            & F.col("timestamp_utc").isNotNull()
            & F.col("parameter_name").isNotNull()
        )
        .repartition("tail_id", "flight_id")
    )


def _prepare_graph_windows(windows_df: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return (
        windows_df.select("tail_id", "flight_id", "win_id", "t_start", "t_end")
        .where(
            F.col("tail_id").isNotNull()
            & F.col("flight_id").isNotNull()
            & F.col("win_id").isNotNull()
            & F.col("t_start").isNotNull()
            & F.col("t_end").isNotNull()
        )
        .repartition("tail_id", "flight_id")
    )


@track_mlflow_run(stage_name="11_build_graph", logger=LOGGER)
@log_memory_usage(logger=LOGGER, label="11_build_graph")
@log_wall_time(logger=LOGGER)
def run() -> None:
    from pyspark import StorageLevel

    raw_path = os.getenv("S3NTINEL_RAW_TABLE_PATH", "data/delta/raw_telemetry")
    events_path = os.getenv("S3NTINEL_EVENTS_TABLE_PATH", "data/delta/events")
    windows_path = os.getenv("S3NTINEL_WINDOWS_TABLE_PATH", "data/delta/windows")
    window_features_path = os.getenv("S3NTINEL_WINDOW_FEATURES_TABLE_PATH", "")
    backbone_path = os.getenv("S3NTINEL_BACKBONE_TABLE_PATH", "data/delta/backbone")
    precision_graph_path = os.getenv("S3NTINEL_PRECISION_GRAPH_TABLE_PATH", "data/delta/precision_graph")
    event_graph_path = os.getenv("S3NTINEL_EVENT_GRAPH_TABLE_PATH", "data/delta/event_graph")
    lag_graph_path = os.getenv("S3NTINEL_LAG_GRAPH_TABLE_PATH", "data/delta/lag_graph")
    transition_graph_path = os.getenv("S3NTINEL_TRANSITION_GRAPH_TABLE_PATH", "data/delta/transition_graph")
    fused_graph_path = os.getenv("S3NTINEL_FUSED_GRAPH_TABLE_PATH", "data/delta/fused_graph")
    graph_parameter_universe_path = os.getenv(
        "S3NTINEL_GRAPH_PARAMETER_UNIVERSE_TABLE_PATH",
        "data/delta/graph_parameter_universe",
    )
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
    max_graph_sensor_universe = int(os.getenv("S3NTINEL_MAX_GRAPH_SENSOR_UNIVERSE", "50000"))

    spark = get_spark("s3ntinel.build_graph")
    events_df = read_table(spark, events_path, fmt=table_format)
    windows_df = read_table(spark, windows_path, fmt=table_format)
    backbone_df = read_table(spark, backbone_path, fmt=table_format)
    graph_events_df = _prepare_graph_events(events_df).persist(StorageLevel.MEMORY_AND_DISK)
    graph_windows_df = _prepare_graph_windows(windows_df)
    timing_ms: dict[str, float] = {}
    resolved_window_features_path = require_artifact_path(
        window_features_path,
        env_name="S3NTINEL_WINDOW_FEATURES_TABLE_PATH",
        artifact_name="window_features",
    )
    window_features_df = read_table(spark, str(resolved_window_features_path), fmt=table_format).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    try:
        started = time.perf_counter()
        window_features_count = int(window_features_df.count())
        timing_ms["window_features_count"] = _elapsed_ms(started)

        started = time.perf_counter()
        event_sdf = build_event_graph_spark_table(
            graph_events_df,
            graph_windows_df,
            min_count=min_event_count,
            min_npmi=min_event_npmi,
            top_k_per_parameter_name=int(os.getenv("S3NTINEL_V2_EVENT_GRAPH_TOP_K_PER_SENSOR", "8")),
        ).persist(StorageLevel.MEMORY_AND_DISK)
        _materialize_df(event_sdf)
        timing_ms["event_graph_build"] = _elapsed_ms(started)
        started = time.perf_counter()
        lag_sdf = build_lag_graph_spark_table(
            graph_events_df,
            tau_max_seconds=lag_tau_max_seconds,
            min_count=min_lag_count,
            max_mean_lag_seconds=float(os.getenv("S3NTINEL_V2_LAG_GRAPH_MAX_MEAN_LAG_SECONDS", "5.0")),
            top_k_outgoing=int(os.getenv("S3NTINEL_V2_LAG_GRAPH_TOP_K_OUTGOING", "4")),
        ).persist(StorageLevel.MEMORY_AND_DISK)
        _materialize_df(lag_sdf)
        timing_ms["lag_graph_build"] = _elapsed_ms(started)
        started = time.perf_counter()
        transition_sdf = build_transition_graph_spark_table(
            graph_events_df,
            min_count=min_transition_count,
        ).persist(StorageLevel.MEMORY_AND_DISK)
        _materialize_df(transition_sdf)
        timing_ms["transition_graph_build"] = _elapsed_ms(started)
        try:
            backbone_row = backbone_df.first()
            backbone_row = backbone_row.asDict() if backbone_row is not None else {}
            selected_sensors = list(backbone_row.get("selected_sensors_c") or [])
            started = time.perf_counter()
            precision_pdf = build_precision_graph_from_window_features_spark_table(
                window_features_df,
                selected_sensors=selected_sensors,
                ridge_lambda=precision_ridge_lambda,
                min_abs_partial_corr=min_abs_partial_corr,
            )
            timing_ms["precision_graph_build"] = _elapsed_ms(started)

            precision_df = (
                spark.createDataFrame(precision_pdf)
                if not precision_pdf.empty
                else spark.createDataFrame([], schema=PRECISION_GRAPH_SCHEMA())
            )
            started = time.perf_counter()
            fused_df = build_fused_graph_spark_table(
                precision_df,
                event_sdf,
                lag_sdf,
                alpha=alpha,
                beta=beta,
                gamma=gamma,
            ).persist(StorageLevel.MEMORY_AND_DISK)
            _materialize_df(fused_df)
            timing_ms["fused_graph_build"] = _elapsed_ms(started)

            started = time.perf_counter()
            backbone_all_sensors = backbone_row.get("all_sensors") or []
            parameter_universe_df, _ = build_graph_parameter_universe_spark_table(
                event_sdf,
                lag_sdf,
                transition_sdf,
                backbone_all_sensors=backbone_all_sensors,
                max_graph_sensor_universe=max_graph_sensor_universe,
            )
            parameter_universe_df = parameter_universe_df.persist(StorageLevel.MEMORY_AND_DISK)
            _materialize_df(parameter_universe_df)
            timing_ms["parameter_universe_build"] = _elapsed_ms(started)
            try:
                started = time.perf_counter()
                event_count_out = int(event_sdf.count())
                lag_count_out = int(lag_sdf.count())
                transition_count_out = int(transition_sdf.count())
                fused_count_out = int(fused_df.count())
                parameter_universe_count_out = int(parameter_universe_df.count())
                timing_ms["output_counts"] = _elapsed_ms(started)

                started = time.perf_counter()
                write_table(precision_df, path=precision_graph_path, mode=write_mode, fmt=table_format)
                write_table(event_sdf, path=event_graph_path, mode=write_mode, fmt=table_format)
                write_table(lag_sdf, path=lag_graph_path, mode=write_mode, fmt=table_format)
                write_table(transition_sdf, path=transition_graph_path, mode=write_mode, fmt=table_format)
                write_table(fused_df, path=fused_graph_path, mode=write_mode, fmt=table_format)
                write_table(
                    parameter_universe_df,
                    path=graph_parameter_universe_path,
                    mode=write_mode,
                    fmt=table_format,
                )
                timing_ms["output_writes"] = _elapsed_ms(started)
            finally:
                parameter_universe_df.unpersist()
                fused_df.unpersist()
        finally:
            event_sdf.unpersist()
            lag_sdf.unpersist()
            transition_sdf.unpersist()
    finally:
        window_features_df.unpersist()
        graph_events_df.unpersist()

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
        }
    )
    log_dict_artifact_if_active(
        {
            "stage": "11_build_graph",
            "raw_path": raw_path,
            "events_path": events_path,
            "windows_path": windows_path,
            "window_features_path": window_features_path,
            "backbone_path": backbone_path,
            "precision_graph_path": precision_graph_path,
            "event_graph_path": event_graph_path,
            "lag_graph_path": lag_graph_path,
            "transition_graph_path": transition_graph_path,
            "fused_graph_path": fused_graph_path,
            "graph_parameter_universe_path": graph_parameter_universe_path,
            "precision_edge_count": int(len(precision_pdf)),
            "event_edge_count": event_count_out,
            "lag_edge_count": lag_count_out,
            "transition_edge_count": transition_count_out,
            "fused_edge_count": fused_count_out,
            "min_event_npmi": min_event_npmi,
            "max_graph_sensor_universe": max_graph_sensor_universe,
            "graph_parameter_universe_count": parameter_universe_count_out,
            "window_features_count": window_features_count,
            "timing_ms": {key: round(value, 3) for key, value in timing_ms.items()},
            "table_format": table_format,
            "write_mode": write_mode,
        },
        "reports/stages/11_build_graph_summary.json",
    )
    stage_manifest = build_stage_manifest(
        stage_name="11_build_graph",
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
            "max_graph_sensor_universe": max_graph_sensor_universe,
        },
        input_artifacts={
            "events": build_artifact_manifest(path=events_path, dataframe=events_df),
            "windows": build_artifact_manifest(path=windows_path, dataframe=windows_df),
            "backbone": build_artifact_manifest(path=backbone_path, dataframe=backbone_df),
            "window_features": build_artifact_manifest(
                path=(window_features_path or "window_features::ephemeral"),
                dataframe=window_features_df,
                row_count=window_features_count,
            ),
        },
        output_artifacts={
            "precision_graph": build_artifact_manifest(path=precision_graph_path, dataframe=precision_df, row_count=len(precision_pdf)),
            "event_graph": build_artifact_manifest(path=event_graph_path, dataframe=event_sdf, row_count=event_count_out),
            "lag_graph": build_artifact_manifest(path=lag_graph_path, dataframe=lag_sdf, row_count=lag_count_out),
            "transition_graph": build_artifact_manifest(path=transition_graph_path, dataframe=transition_sdf, row_count=transition_count_out),
            "fused_graph": build_artifact_manifest(path=fused_graph_path, dataframe=fused_df, row_count=fused_count_out),
            "graph_parameter_universe": build_artifact_manifest(
                path=graph_parameter_universe_path,
                dataframe=parameter_universe_df,
                row_count=parameter_universe_count_out,
            ),
        },
        replayable_from=["window_features", "events", "windows", "backbone"],
        cache_artifacts={
            "graph_component_cache": {
                "precision_graph_path": precision_graph_path,
                "event_graph_path": event_graph_path,
                "lag_graph_path": lag_graph_path,
                "transition_graph_path": transition_graph_path,
                "fused_graph_path": fused_graph_path,
                "graph_parameter_universe_path": graph_parameter_universe_path,
            }
        },
    )
    log_stage_manifest_if_active(stage_manifest, "reports/stages/11_build_graph_manifest.json")
    LOGGER.info(
        "pipeline=build_graph format=%s write_mode=%s precision_edges=%s event_edges=%s lag_edges=%s transition_edges=%s fused_edges=%s parameter_universe=%s timing_ms=%s",
        table_format,
        write_mode,
        len(precision_pdf),
        event_count_out,
        lag_count_out,
        transition_count_out,
        fused_count_out,
        parameter_universe_count_out,
        {key: round(value, 1) for key, value in timing_ms.items()},
    )


if __name__ == "__main__":
    run()


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
