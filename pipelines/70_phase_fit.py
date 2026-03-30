# File: pipelines/70_phase_fit.py
"""Fit phase baselines and assign detected phases to windows."""

from libs.io.delta import get_spark, read_table
from libs.phase import (
    PhaseDetectionPlan,
    fit_phase_feature_config_from_spark,
)
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
from pipelines.common import build_stage_runtime, require_artifact_path


LOGGER = get_logger(__name__)
def _select_phase_fit_input_columns(
    raw_df: "DataFrame",
    events_df: "DataFrame",
    windows_df: "DataFrame",
) -> tuple["DataFrame", "DataFrame", "DataFrame"]:
    raw_cols = [col for col in ["tail_id", "flight_id", "timestamp_utc", "parameter_name", "parameter_value_clean", "parameter_value", "timestamp"] if col in raw_df.columns]
    event_cols = [
        col
        for col in [
            "tail_id",
            "flight_id",
            "parameter_name",
            "timestamp_utc",
            "event_type_detected",
            "payload",
            "sensor",
            "ts",
        ]
        if col in events_df.columns
    ]
    window_cols = [col for col in ["tail_id", "flight_id", "win_id", "t_start", "t_end", "duration_ms", "event_count", "date_utc"] if col in windows_df.columns]
    return raw_df.select(*raw_cols), events_df.select(*event_cols), windows_df.select(*window_cols)


@track_mlflow_run(stage_name="70_phase_fit", logger=LOGGER)
@log_memory_usage(logger=LOGGER, label="70_phase_fit")
@log_wall_time(logger=LOGGER)
def run() -> None:
    from pyspark import StorageLevel

    runtime = build_stage_runtime("70_phase_fit")
    context = runtime.context
    raw_path = runtime.artifacts.raw_table
    events_path = runtime.artifacts.events
    windows_path = runtime.artifacts.windows
    window_features_path = runtime.artifacts.window_features
    backbone_path = runtime.artifacts.backbone
    phase_baselines_path = runtime.artifacts.phase_baselines
    phase_windows_path = runtime.artifacts.phase_windows
    table_format = runtime.execution.table_format
    write_mode = runtime.execution.write_mode

    spark = get_spark("s3ntinel.phase_fit")
    raw_df = read_table(spark, raw_path, fmt=table_format)
    events_df = read_table(spark, events_path, fmt=table_format)
    windows_df = read_table(spark, windows_path, fmt=table_format)
    backbone_df = read_table(spark, backbone_path, fmt=table_format)
    raw_df, events_df, windows_df = _select_phase_fit_input_columns(raw_df, events_df, windows_df)
    resolved_window_features_path = require_artifact_path(
        window_features_path,
        env_name="S3NTINEL_WINDOW_FEATURES_TABLE_PATH",
        artifact_name="window_features",
    )
    window_features_df = read_table(spark, str(resolved_window_features_path), fmt=table_format).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    phase_count = runtime.settings.phase.phase_count
    phase_detect_sensor_count = runtime.settings.phase.detect_sensor_count
    phase_detect_event_type_count = runtime.settings.phase.detect_event_type_count
    phase_detect_categorical_state_count = runtime.settings.phase.detect_categorical_state_count
    phase_stable_drift_quantile = runtime.settings.phase.stable_drift_quantile
    phase_smoothing_radius = runtime.settings.phase.smoothing_radius
    phase_transition_penalty = runtime.settings.phase.transition_penalty
    phase_min_dwell_windows = runtime.settings.phase.min_dwell_windows

    try:
        phase_config = fit_phase_feature_config_from_spark(
            window_features_df,
            backbone_df=backbone_df,
            phase_detect_sensor_count=phase_detect_sensor_count,
            phase_detect_event_type_count=phase_detect_event_type_count,
            phase_detect_categorical_state_count=phase_detect_categorical_state_count,
        )
        phase_plan = PhaseDetectionPlan(
            phase_count=phase_count,
            phase_stable_drift_quantile=phase_stable_drift_quantile,
            phase_smoothing_radius=phase_smoothing_radius,
            phase_transition_penalty=phase_transition_penalty,
            phase_min_dwell_windows=phase_min_dwell_windows,
        )
        phase_windows = phase_plan.build_phase_windows(
            window_features_df,
            phase_config=phase_config,
        )
        phase_windows = phase_windows.with_dataframe(phase_windows.to_dataframe().persist(StorageLevel.MEMORY_AND_DISK)).bind(
            path=phase_windows_path,
            format=table_format,
            partition_by=tuple(context.config["output"]["partition_by"]),
        )
        try:
            phase_baselines = phase_plan.build_phase_baselines(
                phase_windows,
                phase_config=phase_config,
            )
            phase_baselines = phase_baselines.with_dataframe(
                phase_baselines.to_dataframe().persist(StorageLevel.MEMORY_AND_DISK)
            ).bind(
                path=phase_baselines_path,
                format=table_format,
                partition_by=("tail_id",),
            )
            try:
                phase_windows.write(mode=write_mode)
                phase_baselines.write(mode=write_mode)
                phase_windows_count = int(phase_windows.to_dataframe().count())
                phase_baselines_count = int(phase_baselines.to_dataframe().count())
            finally:
                phase_baselines.to_dataframe().unpersist()
        finally:
            phase_windows.to_dataframe().unpersist()
    finally:
        window_features_df.unpersist()

    log_params_if_active(
        {
            "phase_count": phase_count,
        }
    )
    log_dict_artifact_if_active(
        {
            "stage": "70_phase_fit",
            "raw_path": raw_path,
            "events_path": events_path,
            "windows_path": windows_path,
            "window_features_path": window_features_path,
            "backbone_path": backbone_path,
            "phase_windows_path": phase_windows_path,
            "phase_baselines_path": phase_baselines_path,
            "table_format": table_format,
            "write_mode": write_mode,
            "phase_partition_by": ["tail_id"],
            "phase_windows_partition_by": list(context.config["output"]["partition_by"]),
        },
        runtime.report_paths.summary_artifact_path,
    )
    stage_manifest = build_stage_manifest(
        stage_name="70_phase_fit",
        config={
            "table_format": table_format,
            "write_mode": write_mode,
            "phase_count": phase_count,
            "phase_detect_sensor_count": phase_detect_sensor_count,
            "phase_detect_event_type_count": phase_detect_event_type_count,
            "phase_detect_categorical_state_count": phase_detect_categorical_state_count,
            "phase_stable_drift_quantile": phase_stable_drift_quantile,
            "phase_smoothing_radius": phase_smoothing_radius,
            "phase_transition_penalty": phase_transition_penalty,
            "phase_min_dwell_windows": phase_min_dwell_windows,
        },
        input_artifacts={
            "raw_telemetry": build_artifact_manifest(path=raw_path, dataframe=raw_df),
            "events": build_artifact_manifest(path=events_path, dataframe=events_df),
            "windows": build_artifact_manifest(path=windows_path, dataframe=windows_df),
            "backbone": build_artifact_manifest(path=backbone_path, dataframe=backbone_df),
            "window_features": build_artifact_manifest(
                path=(window_features_path or "window_features::ephemeral"),
                dataframe=window_features_df,
            ),
        },
        output_artifacts={
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
        },
        replayable_from=["window_features", "backbone"],
        cache_artifacts={"phase_fit_cache": {"config_keys": sorted(list(phase_config.keys()))}},
    )
    log_stage_manifest_if_active(stage_manifest, runtime.report_paths.manifest_artifact_path)
    LOGGER.info(
        "pipeline=phase_fit format=%s write_mode=%s phase_windows=%s phase_baselines=%s",
        table_format,
        write_mode,
        phase_windows_path,
        phase_baselines_path,
    )


if __name__ == "__main__":
    run()
