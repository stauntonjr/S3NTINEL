# File: pipelines/80_window_scores_raw.py
"""Build raw window scores from phase windows and phase baselines."""

from libs.graph.tables import HierarchySensorMapTable
from libs.io.delta import get_spark
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
from libs.phase import PhaseBaselinesTable, PhaseWindowsTable
from libs.scoring import WindowScoresRawTable
from pipelines.common import build_stage_runtime


LOGGER = get_logger(__name__)

@track_mlflow_run(stage_name="80_window_scores_raw", logger=LOGGER)
@log_memory_usage(logger=LOGGER, label="80_window_scores_raw")
@log_wall_time(logger=LOGGER)
def run() -> None:
    runtime = build_stage_runtime("80_window_scores_raw")
    context = runtime.context
    phase_baselines_path = runtime.artifacts.phase_baselines
    phase_windows_path = runtime.artifacts.phase_windows
    hierarchy_sensor_map_path = runtime.artifacts.hierarchy_sensor_map
    scores_path = runtime.artifacts.window_scores_raw
    table_format = runtime.execution.table_format
    write_mode = runtime.execution.write_mode

    spark = get_spark("s3ntinel.window_scores_raw")
    phase_windows = PhaseWindowsTable.read(spark, phase_windows_path, format=table_format)
    phase_baselines = PhaseBaselinesTable.read(spark, phase_baselines_path, format=table_format)
    hierarchy_sensor_map = HierarchySensorMapTable.read(spark, hierarchy_sensor_map_path, format=table_format)
    scores = WindowScoresRawTable.from_phase_tables(
        phase_windows,
        phase_baselines,
        hierarchy_sensor_map,
    ).bind(
        path=scores_path,
        format=table_format,
        partition_by=tuple(context.config["output"]["partition_by"]),
    )
    scores.write(mode=write_mode)
    phase_windows_count = int(phase_windows.to_dataframe().count())
    phase_baselines_count = int(phase_baselines.to_dataframe().count())
    hierarchy_sensor_map_count = int(hierarchy_sensor_map.to_dataframe().count())
    scores_count = int(scores.to_dataframe().count())

    log_params_if_active(
        {
            "pvalue_combine": context.config["scoring"]["combine_pvalues"],
        }
    )
    log_dict_artifact_if_active(
        {
            "stage": "80_window_scores_raw",
            "phase_baselines_path": phase_baselines_path,
            "phase_windows_path": phase_windows_path,
            "hierarchy_sensor_map_path": hierarchy_sensor_map_path,
            "window_scores_raw_path": scores_path,
            "table_format": table_format,
            "write_mode": write_mode,
            "partition_by": list(context.config["output"]["partition_by"]),
        },
        runtime.report_paths.summary_artifact_path,
    )
    stage_manifest = build_stage_manifest(
        stage_name="80_window_scores_raw",
        config={
            "table_format": table_format,
            "write_mode": write_mode,
            "pvalue_combine": context.config["scoring"]["combine_pvalues"],
        },
        input_artifacts={
            "phase_windows": build_artifact_manifest(
                path=phase_windows_path,
                dataframe=phase_windows.to_dataframe(),
                row_count=phase_windows_count,
            ),
            "phase_baselines": build_artifact_manifest(
                path=phase_baselines_path,
                dataframe=phase_baselines.to_dataframe(),
                row_count=phase_baselines_count,
            ),
            "hierarchy_sensor_map": build_artifact_manifest(
                path=hierarchy_sensor_map_path,
                dataframe=hierarchy_sensor_map.to_dataframe(),
                row_count=hierarchy_sensor_map_count,
            ),
        },
        output_artifacts={
            "window_scores_raw": build_artifact_manifest(
                path=scores_path,
                dataframe=scores.to_dataframe(),
                row_count=scores_count,
            ),
        },
        replayable_from=["phase_windows", "phase_baselines", "hierarchy_sensor_map"],
    )
    log_stage_manifest_if_active(stage_manifest, runtime.report_paths.manifest_artifact_path)
    LOGGER.info(
        "pipeline=window_scores_raw format=%s write_mode=%s phase_windows=%s window_scores_raw=%s",
        table_format,
        write_mode,
        phase_windows_path,
        scores_path,
    )


if __name__ == "__main__":
    run()
