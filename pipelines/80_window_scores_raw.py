# File: pipelines/80_window_scores_raw.py
"""Build raw window scores from phase windows and phase baselines."""

from libs.events import EventsTable, ParameterEventProfile
from libs.graph.tables import HierarchySensorMapTable
from libs.io.delta import get_spark
from libs.perf import (
    build_artifact_manifest,
    build_stage_manifest,
    get_logger,
    log_memory_usage,
    log_dict_artifact_if_active,
    log_stage_manifest_if_active,
    log_wall_time,
    track_mlflow_run,
)
from libs.phase import PhaseBaselinesTable, PhaseWindowsTable
from libs.profiling import ParameterBehaviorProfile
from libs.scoring import WindowScoresRawTable
from libs.windows import WindowsTable
from pipelines.common import build_stage_runtime


LOGGER = get_logger(__name__)
_SCORE_ARTIFACT_PARTITION_BY: tuple[str, ...] = ()

@track_mlflow_run(stage_name="80_window_scores_raw", logger=LOGGER)
@log_memory_usage(logger=LOGGER, label="80_window_scores_raw")
@log_wall_time(logger=LOGGER)
def run() -> None:
    runtime = build_stage_runtime("80_window_scores_raw")
    phase_baselines_path = runtime.artifacts.phase_baselines
    phase_windows_path = runtime.artifacts.phase_windows
    hierarchy_sensor_map_path = runtime.artifacts.hierarchy_sensor_map
    windows_path = runtime.artifacts.windows
    events_path = runtime.artifacts.events
    parameter_behavior_profile_path = runtime.artifacts.parameter_behavior_profile
    parameter_event_profile_path = runtime.artifacts.parameter_event_profile
    scores_path = runtime.artifacts.window_scores_raw
    table_format = runtime.execution.table_format
    write_mode = runtime.execution.write_mode

    spark = get_spark("s3ntinel.window_scores_raw")
    phase_windows = PhaseWindowsTable.read(spark, phase_windows_path, format=table_format)
    phase_baselines = PhaseBaselinesTable.read(spark, phase_baselines_path, format=table_format)
    hierarchy_sensor_map = HierarchySensorMapTable.read(spark, hierarchy_sensor_map_path, format=table_format)
    windows = WindowsTable.read(spark, windows_path, format=table_format)
    events = EventsTable.read(spark, events_path, format=table_format)
    parameter_behavior_profile = ParameterBehaviorProfile.read(
        spark,
        parameter_behavior_profile_path,
        format=table_format,
    )
    parameter_event_profile = ParameterEventProfile.read(spark, parameter_event_profile_path, format=table_format)
    scores = WindowScoresRawTable.from_phase_tables(
        phase_windows,
        phase_baselines,
        hierarchy_sensor_map,
        windows=windows,
        events=events,
        parameter_behavior_profile=parameter_behavior_profile,
        parameter_event_profile=parameter_event_profile,
    ).bind(
        path=scores_path,
        format=table_format,
        partition_by=_SCORE_ARTIFACT_PARTITION_BY,
    )
    scores.write(mode=write_mode)
    scores = WindowScoresRawTable.read(
        spark,
        scores_path,
        format=table_format,
        partition_by=_SCORE_ARTIFACT_PARTITION_BY,
    )
    scores_count = int(scores.to_dataframe().count())
    phase_windows_count = int(phase_windows.to_dataframe().count())
    phase_baselines_count = int(phase_baselines.to_dataframe().count())
    hierarchy_sensor_map_count = int(hierarchy_sensor_map.to_dataframe().count())
    windows_count = int(windows.to_dataframe().count())
    events_count = int(events.to_dataframe().count())
    parameter_behavior_profile_count = int(parameter_behavior_profile.to_dataframe().count())
    parameter_event_profile_count = int(parameter_event_profile.to_dataframe().count())
    log_dict_artifact_if_active(
        {
            "stage": "80_window_scores_raw",
            "phase_baselines_path": phase_baselines_path,
            "phase_windows_path": phase_windows_path,
            "hierarchy_sensor_map_path": hierarchy_sensor_map_path,
            "windows_path": windows_path,
            "events_path": events_path,
            "parameter_behavior_profile_path": parameter_behavior_profile_path,
            "parameter_event_profile_path": parameter_event_profile_path,
            "window_scores_raw_path": scores_path,
            "table_format": table_format,
            "write_mode": write_mode,
            "partition_by": list(_SCORE_ARTIFACT_PARTITION_BY),
        },
        runtime.report_paths.summary_artifact_path,
    )
    stage_manifest = build_stage_manifest(
        stage_name="80_window_scores_raw",
        config={
            "table_format": table_format,
            "write_mode": write_mode,
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
            "windows": build_artifact_manifest(
                path=windows_path,
                dataframe=windows.to_dataframe(),
                row_count=windows_count,
            ),
            "events": build_artifact_manifest(
                path=events_path,
                dataframe=events.to_dataframe(),
                row_count=events_count,
            ),
            "parameter_behavior_profile": build_artifact_manifest(
                path=parameter_behavior_profile_path,
                dataframe=parameter_behavior_profile.to_dataframe(),
                row_count=parameter_behavior_profile_count,
            ),
            "parameter_event_profile": build_artifact_manifest(
                path=parameter_event_profile_path,
                dataframe=parameter_event_profile.to_dataframe(),
                row_count=parameter_event_profile_count,
            ),
        },
        output_artifacts={
            "window_scores_raw": build_artifact_manifest(
                path=scores_path,
                dataframe=scores.to_dataframe(),
                row_count=scores_count,
            ),
        },
        replayable_from=[
            "phase_windows",
            "phase_baselines",
            "hierarchy_sensor_map",
            "windows",
            "events",
            "parameter_behavior_profile",
            "parameter_event_profile",
        ],
    )
    log_stage_manifest_if_active(stage_manifest, runtime.report_paths.manifest_artifact_path)
    LOGGER.info(
        "pipeline=window_scores_raw format=%s write_mode=%s phase_windows=%s events=%s window_scores_raw=%s",
        table_format,
        write_mode,
        phase_windows_path,
        events_path,
        scores_path,
    )


if __name__ == "__main__":
    run()
