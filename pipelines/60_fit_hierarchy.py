"""Fit hierarchy artifacts from fused graph and the persisted graph parameter universe."""
import time

from libs.graph import HierarchySensorMapTable
from libs.io.delta import get_spark, read_table, write_table
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
from pipelines.common import build_stage_runtime


LOGGER = get_logger(__name__)


def _elapsed_ms(start_time: float) -> float:
    return (time.perf_counter() - start_time) * 1000.0


@track_mlflow_run(stage_name="60_fit_hierarchy", logger=LOGGER)
@log_memory_usage(logger=LOGGER, label="60_fit_hierarchy")
@log_wall_time(logger=LOGGER)
def run() -> None:
    runtime = build_stage_runtime("60_fit_hierarchy")
    spark = get_spark("s3ntinel.fit_hierarchy")
    fused_graph_path = runtime.artifacts.fused_graph
    graph_parameter_universe_path = runtime.artifacts.graph_parameter_universe
    datatype_profile_path = runtime.artifacts.parameter_datatype_profile
    behavior_profile_path = runtime.artifacts.parameter_behavior_profile
    hierarchy_map_path = runtime.artifacts.hierarchy_sensor_map
    table_format = runtime.execution.table_format
    write_mode = runtime.execution.fit_write_mode
    min_fused_edge_weight = runtime.settings.graph.fusion.min_fused_edge_weight
    hierarchy_top_k_per_sensor = runtime.settings.hierarchy.top_k_per_parameter_name
    hierarchy_subsystem_min_edge_weight = runtime.settings.hierarchy.subsystem_min_edge_weight
    hierarchy_system_min_edge_weight = runtime.settings.hierarchy.system_min_edge_weight

    fused_df = read_table(spark, fused_graph_path, fmt=table_format)
    parameter_universe_df = read_table(spark, graph_parameter_universe_path, fmt=table_format)
    datatype_profile_df = read_table(spark, datatype_profile_path, fmt=table_format)
    behavior_profile_df = read_table(spark, behavior_profile_path, fmt=table_format)

    timing_ms: dict[str, float] = {}
    started = time.perf_counter()
    parameter_names = [
        str(row["parameter_name"])
        for row in parameter_universe_df.select("parameter_name").orderBy("parameter_name").collect()
        if str(row["parameter_name"])
    ]
    timing_ms["parameter_universe_collect"] = _elapsed_ms(started)

    started = time.perf_counter()
    hierarchy_df = HierarchySensorMapTable.from_fused_graph(
        fused_df,
        parameter_names=parameter_names,
        min_fused_edge_weight=min_fused_edge_weight,
        hierarchy_top_k_per_parameter_name=hierarchy_top_k_per_sensor,
        hierarchy_subsystem_min_edge_weight=hierarchy_subsystem_min_edge_weight,
        hierarchy_system_min_edge_weight=hierarchy_system_min_edge_weight,
        datatype_profile_df=datatype_profile_df,
        behavior_profile_df=behavior_profile_df,
    ).to_dataframe()
    timing_ms["hierarchy_build"] = _elapsed_ms(started)

    started = time.perf_counter()
    write_table(hierarchy_df, path=hierarchy_map_path, mode=write_mode, fmt=table_format)
    timing_ms["output_write"] = _elapsed_ms(started)

    fused_count = int(fused_df.count())
    parameter_universe_count = int(parameter_universe_df.count())
    datatype_profile_count = int(datatype_profile_df.count())
    behavior_profile_count = int(behavior_profile_df.count())
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
            "datatype_profile_path": datatype_profile_path,
            "behavior_profile_path": behavior_profile_path,
            "hierarchy_map_path": hierarchy_map_path,
            "fused_edge_count": fused_count,
            "graph_parameter_universe_count": parameter_universe_count,
            "datatype_profile_count": datatype_profile_count,
            "behavior_profile_count": behavior_profile_count,
            "hierarchy_sensor_count": hierarchy_count,
            "min_fused_edge_weight": min_fused_edge_weight,
            "hierarchy_top_k_per_sensor": hierarchy_top_k_per_sensor,
            "hierarchy_subsystem_min_edge_weight": hierarchy_subsystem_min_edge_weight,
            "hierarchy_system_min_edge_weight": hierarchy_system_min_edge_weight,
            "timing_ms": {key: round(value, 3) for key, value in timing_ms.items()},
            "table_format": table_format,
            "write_mode": write_mode,
        },
        runtime.report_paths.summary_artifact_path,
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
            "parameter_datatype_profile": build_artifact_manifest(
                path=datatype_profile_path,
                dataframe=datatype_profile_df,
                row_count=datatype_profile_count,
            ),
            "parameter_behavior_profile": build_artifact_manifest(
                path=behavior_profile_path,
                dataframe=behavior_profile_df,
                row_count=behavior_profile_count,
            ),
        },
        output_artifacts={
            "hierarchy_sensor_map": build_artifact_manifest(
                path=hierarchy_map_path,
                dataframe=hierarchy_df,
                row_count=hierarchy_count,
            ),
        },
        replayable_from=["fused_graph", "graph_parameter_universe", "parameter_datatype_profile", "parameter_behavior_profile"],
    )
    log_stage_manifest_if_active(stage_manifest, runtime.report_paths.manifest_artifact_path)
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
