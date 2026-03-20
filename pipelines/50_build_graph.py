"""Build graph component artifacts from backbone, events, and windows."""
import time

from libs.graph import (
    build_event_graph_spark_table,
    build_fused_graph_spark_table,
    build_graph_parameter_universe_spark_table,
    build_lag_candidate_pairs_spark_table,
    build_lag_profile_spark_table,
    build_precision_graph_from_window_features_spark_table,
    build_transition_graph_spark_table,
    collapse_lag_profile_spark_table,
    LagBandSpec,
)
from libs.graph.evaluation import build_graph_stage_evaluation_report_spark
from libs.graph.hierarchy_artifacts import HierarchySpec
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
from pipelines.common import build_context, context_artifacts, context_execution, context_settings, require_artifact_path


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


@track_mlflow_run(stage_name="50_build_graph", logger=LOGGER)
@log_memory_usage(logger=LOGGER, label="50_build_graph")
@log_wall_time(logger=LOGGER)
def run() -> None:
    from pyspark import StorageLevel

    context = build_context()
    artifacts = context_artifacts(context)
    execution = context_execution(context)
    settings = context_settings(context)
    raw_path = artifacts.raw_table
    events_path = artifacts.events
    windows_path = artifacts.windows
    window_features_path = artifacts.window_features
    backbone_path = artifacts.backbone
    precision_graph_path = artifacts.precision_graph
    event_graph_path = artifacts.event_graph
    lag_profile_path = artifacts.lag_profile
    lag_graph_path = artifacts.lag_graph
    transition_graph_path = artifacts.transition_graph
    fused_graph_path = artifacts.fused_graph
    graph_parameter_universe_path = artifacts.graph_parameter_universe
    table_format = execution.table_format
    write_mode = execution.fit_write_mode

    precision_ridge_lambda = settings.graph.precision_ridge_lambda
    min_abs_partial_corr = settings.graph.min_abs_partial_corr
    min_event_count = settings.graph.event.min_count
    min_event_npmi = settings.graph.event.min_npmi
    lag_band_specs = tuple(
        LagBandSpec(
            name=item.name,
            lower_seconds=item.lower_seconds,
            upper_seconds=item.upper_seconds,
            combine_weight=item.combine_weight,
        )
        for item in settings.graph.lag.bands
    )
    lag_tau_max_seconds = max(
        [float(settings.graph.lag.tau_max_seconds)] + [float(item.upper_seconds) for item in lag_band_specs]
    )
    min_lag_count = settings.graph.lag.min_count
    min_transition_count = settings.graph.transition.min_count
    alpha = settings.graph.fusion.alpha
    beta = settings.graph.fusion.beta
    gamma = settings.graph.fusion.gamma
    min_fused_edge_weight = settings.graph.fusion.min_fused_edge_weight
    max_graph_sensor_universe = settings.graph.max_sensor_universe
    graph_evaluation_report_path = "reports/stages/50_build_graph_evaluation.json"

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
    event_sdf = None
    transition_sdf = None
    lag_profile_sdf = None
    lag_sdf = None
    fused_df = None
    parameter_universe_df = None
    graph_evaluation_report: dict[str, object] | None = None
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
            top_k_per_parameter_name=settings.graph.event.top_k_per_parameter_name,
        ).persist(StorageLevel.MEMORY_AND_DISK)
        _materialize_df(event_sdf)
        timing_ms["event_graph_build"] = _elapsed_ms(started)
        started = time.perf_counter()
        transition_sdf = build_transition_graph_spark_table(
            graph_events_df,
            min_count=min_transition_count,
        ).persist(StorageLevel.MEMORY_AND_DISK)
        _materialize_df(transition_sdf)
        timing_ms["transition_graph_build"] = _elapsed_ms(started)
        started = time.perf_counter()
        lag_candidate_sdf = build_lag_candidate_pairs_spark_table(event_sdf, transition_sdf)
        lag_profile_sdf = build_lag_profile_spark_table(
            graph_events_df,
            tau_max_seconds=lag_tau_max_seconds,
            bands=lag_band_specs,
            candidate_pairs_df=lag_candidate_sdf,
        ).persist(StorageLevel.MEMORY_AND_DISK)
        _materialize_df(lag_profile_sdf)
        timing_ms["lag_profile_build"] = _elapsed_ms(started)
        started = time.perf_counter()
        lag_sdf = collapse_lag_profile_spark_table(
            lag_profile_sdf,
            tau_max_seconds=lag_tau_max_seconds,
            bands=lag_band_specs,
            min_count=min_lag_count,
            max_mean_lag_seconds=settings.graph.lag.max_mean_lag_seconds,
            top_k_outgoing=settings.graph.lag.top_k_outgoing,
        ).persist(StorageLevel.MEMORY_AND_DISK)
        _materialize_df(lag_sdf)
        timing_ms["lag_graph_build"] = _elapsed_ms(started)
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
                lag_profile_count_out = int(lag_profile_sdf.count())
                lag_count_out = int(lag_sdf.count())
                transition_count_out = int(transition_sdf.count())
                fused_count_out = int(fused_df.count())
                parameter_universe_count_out = int(parameter_universe_df.count())
                timing_ms["output_counts"] = _elapsed_ms(started)

                started = time.perf_counter()
                write_table(precision_df, path=precision_graph_path, mode=write_mode, fmt=table_format)
                write_table(event_sdf, path=event_graph_path, mode=write_mode, fmt=table_format)
                write_table(lag_profile_sdf, path=lag_profile_path, mode=write_mode, fmt=table_format)
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
                started = time.perf_counter()
                graph_evaluation_report = build_graph_stage_evaluation_report_spark(
                    spark=spark,
                    events_df=graph_events_df,
                    windows_df=graph_windows_df,
                    window_features_df=window_features_df,
                    backbone_df=backbone_df,
                    precision_df=precision_pdf,
                    event_sdf=event_sdf,
                    lag_profile_sdf=lag_profile_sdf,
                    lag_sdf=lag_sdf,
                    transition_sdf=transition_sdf,
                    fused_sdf=fused_df,
                    parameter_universe_df=parameter_universe_df,
                    precision_ridge_lambda=precision_ridge_lambda,
                    min_abs_partial_corr=min_abs_partial_corr,
                    min_event_count=min_event_count,
                    min_event_npmi=min_event_npmi,
                    event_top_k_per_parameter_name=settings.graph.event.top_k_per_parameter_name,
                    lag_tau_max_seconds=lag_tau_max_seconds,
                    lag_bands=lag_band_specs,
                    min_lag_count=min_lag_count,
                    max_mean_lag_seconds=settings.graph.lag.max_mean_lag_seconds,
                    lag_top_k_outgoing=settings.graph.lag.top_k_outgoing,
                    min_transition_count=min_transition_count,
                    alpha=alpha,
                    beta=beta,
                    gamma=gamma,
                    max_graph_sensor_universe=max_graph_sensor_universe,
                    hierarchy_spec=HierarchySpec(
                        min_edge_weight=min_fused_edge_weight,
                        top_k_per_parameter_name=settings.hierarchy.top_k_per_parameter_name,
                        subsystem_min_edge_weight=settings.hierarchy.subsystem_min_edge_weight,
                        system_min_edge_weight=settings.hierarchy.system_min_edge_weight,
                    ),
                )
                timing_ms["graph_evaluation_report"] = _elapsed_ms(started)
            finally:
                if parameter_universe_df is not None:
                    parameter_universe_df.unpersist()
                if fused_df is not None:
                    fused_df.unpersist()
        finally:
            if event_sdf is not None:
                event_sdf.unpersist()
            if lag_profile_sdf is not None:
                lag_profile_sdf.unpersist()
            if lag_sdf is not None:
                lag_sdf.unpersist()
            if transition_sdf is not None:
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
            "lag_band_names": [item.name for item in lag_band_specs],
            "lag_band_upper_seconds": [item.upper_seconds for item in lag_band_specs],
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
            "stage": "50_build_graph",
            "raw_path": raw_path,
            "events_path": events_path,
            "windows_path": windows_path,
            "window_features_path": window_features_path,
            "backbone_path": backbone_path,
            "precision_graph_path": precision_graph_path,
            "event_graph_path": event_graph_path,
            "lag_profile_path": lag_profile_path,
            "lag_graph_path": lag_graph_path,
            "transition_graph_path": transition_graph_path,
            "fused_graph_path": fused_graph_path,
            "graph_parameter_universe_path": graph_parameter_universe_path,
            "graph_evaluation_report_path": graph_evaluation_report_path,
            "precision_edge_count": int(len(precision_pdf)),
            "event_edge_count": event_count_out,
            "lag_profile_edge_count": lag_profile_count_out,
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
        "reports/stages/50_build_graph_summary.json",
    )
    if graph_evaluation_report is not None:
        log_dict_artifact_if_active(
            graph_evaluation_report,
            graph_evaluation_report_path,
        )
    stage_manifest = build_stage_manifest(
        stage_name="50_build_graph",
        config={
            "table_format": table_format,
            "write_mode": write_mode,
            "precision_ridge_lambda": precision_ridge_lambda,
            "min_abs_partial_corr": min_abs_partial_corr,
            "min_event_count": min_event_count,
            "min_event_npmi": min_event_npmi,
            "lag_tau_max_seconds": lag_tau_max_seconds,
            "lag_bands": [
                {
                    "name": item.name,
                    "lower_seconds": item.lower_seconds,
                    "upper_seconds": item.upper_seconds,
                    "combine_weight": item.combine_weight,
                }
                for item in lag_band_specs
            ],
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
            "lag_profile": build_artifact_manifest(path=lag_profile_path, dataframe=lag_profile_sdf, row_count=lag_profile_count_out),
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
                "lag_profile_path": lag_profile_path,
                "lag_graph_path": lag_graph_path,
                "transition_graph_path": transition_graph_path,
                "fused_graph_path": fused_graph_path,
                "graph_parameter_universe_path": graph_parameter_universe_path,
                "graph_evaluation_report_path": graph_evaluation_report_path,
            }
        },
    )
    log_stage_manifest_if_active(stage_manifest, "reports/stages/50_build_graph_manifest.json")
    LOGGER.info(
        "pipeline=build_graph format=%s write_mode=%s precision_edges=%s event_edges=%s lag_profile_edges=%s lag_edges=%s transition_edges=%s fused_edges=%s parameter_universe=%s timing_ms=%s",
        table_format,
        write_mode,
        len(precision_pdf),
        event_count_out,
        lag_profile_count_out,
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
