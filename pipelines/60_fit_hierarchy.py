"""Fit hierarchy artifacts from fused graph and the persisted graph parameter universe."""
import time

from libs.graph import build_hierarchy_from_fused_spark_table
from libs.io.delta import get_spark, read_table, write_table
from libs.io.schemas import HIERARCHY_SENSOR_MAP_SCHEMA
from libs.perf import (
    build_artifact_manifest,
    build_stage_manifest,
    get_logger,
    log_dict_artifact_if_active,
    log_memory_usage,
    log_params_if_active,
    log_stage_manifest_if_active,
    log_wall_time,
    track_mlflow_run,
)
from pipelines.common import build_context, context_artifacts, context_execution, context_settings


LOGGER = get_logger(__name__)


def _elapsed_ms(start_time: float) -> float:
    return (time.perf_counter() - start_time) * 1000.0


@track_mlflow_run(stage_name="60_fit_hierarchy", logger=LOGGER)
@log_memory_usage(logger=LOGGER, label="60_fit_hierarchy")
@log_wall_time(logger=LOGGER)
def run() -> None:
    context = build_context()
    artifacts = context_artifacts(context)
    execution = context_execution(context)
    settings = context_settings(context)
    spark = get_spark("s3ntinel.fit_hierarchy")
    fused_graph_path = artifacts.fused_graph
    graph_parameter_universe_path = artifacts.graph_parameter_universe
    hierarchy_map_path = artifacts.hierarchy_sensor_map
    table_format = execution.table_format
    write_mode = execution.fit_write_mode
    min_fused_edge_weight = settings.graph.fusion.min_fused_edge_weight
    hierarchy_top_k_per_sensor = settings.hierarchy.top_k_per_parameter_name
    hierarchy_subsystem_min_edge_weight = settings.hierarchy.subsystem_min_edge_weight
    hierarchy_system_min_edge_weight = settings.hierarchy.system_min_edge_weight

    fused_df = read_table(spark, fused_graph_path, fmt=table_format)
    parameter_universe_df = read_table(spark, graph_parameter_universe_path, fmt=table_format)

    timing_ms: dict[str, float] = {}
    started = time.perf_counter()
    parameter_names = [
        str(row["parameter_name"])
        for row in parameter_universe_df.select("parameter_name").orderBy("parameter_name").collect()
        if str(row["parameter_name"])
    ]
    timing_ms["parameter_universe_collect"] = _elapsed_ms(started)

    started = time.perf_counter()
    hierarchy_pdf = build_hierarchy_from_fused_spark_table(
        fused_df,
        parameter_names=parameter_names,
        min_fused_edge_weight=min_fused_edge_weight,
        hierarchy_top_k_per_parameter_name=hierarchy_top_k_per_sensor,
        hierarchy_subsystem_min_edge_weight=hierarchy_subsystem_min_edge_weight,
        hierarchy_system_min_edge_weight=hierarchy_system_min_edge_weight,
    )
    timing_ms["hierarchy_build"] = _elapsed_ms(started)
    hierarchy_df = (
        spark.createDataFrame(hierarchy_pdf)
        if not hierarchy_pdf.empty
        else spark.createDataFrame([], schema=HIERARCHY_SENSOR_MAP_SCHEMA)
    )

    started = time.perf_counter()
    write_table(hierarchy_df, path=hierarchy_map_path, mode=write_mode, fmt=table_format)
    timing_ms["output_write"] = _elapsed_ms(started)

    fused_count = int(fused_df.count())
    parameter_universe_count = int(parameter_universe_df.count())
    hierarchy_count = int(hierarchy_df.count())

    log_params_if_active(
        {
            "min_fused_edge_weight": min_fused_edge_weight,
            "hierarchy_top_k_per_sensor": hierarchy_top_k_per_sensor,
            "hierarchy_subsystem_min_edge_weight": hierarchy_subsystem_min_edge_weight,
            "hierarchy_system_min_edge_weight": hierarchy_system_min_edge_weight,
        }
    )
    log_dict_artifact_if_active(
        {
            "stage": "60_fit_hierarchy",
            "fused_graph_path": fused_graph_path,
            "graph_parameter_universe_path": graph_parameter_universe_path,
            "hierarchy_map_path": hierarchy_map_path,
            "fused_edge_count": fused_count,
            "graph_parameter_universe_count": parameter_universe_count,
            "hierarchy_sensor_count": hierarchy_count,
            "min_fused_edge_weight": min_fused_edge_weight,
            "hierarchy_top_k_per_sensor": hierarchy_top_k_per_sensor,
            "hierarchy_subsystem_min_edge_weight": hierarchy_subsystem_min_edge_weight,
            "hierarchy_system_min_edge_weight": hierarchy_system_min_edge_weight,
            "timing_ms": {key: round(value, 3) for key, value in timing_ms.items()},
            "table_format": table_format,
            "write_mode": write_mode,
        },
        "reports/stages/60_fit_hierarchy_summary.json",
    )
    stage_manifest = build_stage_manifest(
        stage_name="60_fit_hierarchy",
        config={
            "table_format": table_format,
            "write_mode": write_mode,
            "min_fused_edge_weight": min_fused_edge_weight,
            "hierarchy_top_k_per_sensor": hierarchy_top_k_per_sensor,
            "hierarchy_subsystem_min_edge_weight": hierarchy_subsystem_min_edge_weight,
            "hierarchy_system_min_edge_weight": hierarchy_system_min_edge_weight,
        },
        input_artifacts={
            "fused_graph": build_artifact_manifest(path=fused_graph_path, dataframe=fused_df, row_count=fused_count),
            "graph_parameter_universe": build_artifact_manifest(
                path=graph_parameter_universe_path,
                dataframe=parameter_universe_df,
                row_count=parameter_universe_count,
            ),
        },
        output_artifacts={
            "hierarchy_sensor_map": build_artifact_manifest(
                path=hierarchy_map_path,
                dataframe=hierarchy_df,
                row_count=hierarchy_count,
            ),
        },
        replayable_from=["fused_graph", "graph_parameter_universe"],
    )
    log_stage_manifest_if_active(stage_manifest, "reports/stages/60_fit_hierarchy_manifest.json")
    LOGGER.info(
        "pipeline=fit_hierarchy format=%s write_mode=%s fused_edges=%s parameter_universe=%s hierarchy_sensors=%s timing_ms=%s",
        table_format,
        write_mode,
        fused_count,
        parameter_universe_count,
        hierarchy_count,
        {key: round(value, 1) for key, value in timing_ms.items()},
    )


if __name__ == "__main__":
    run()
