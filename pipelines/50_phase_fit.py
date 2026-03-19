# File: pipelines/50_phase_fit.py
"""Fit phase baselines and assign detected phases to windows."""

import os

from libs.io.delta import get_spark, read_table, write_table
from libs.phase import (
    build_phase_baselines_spark_table,
    build_phase_windows_spark_table,
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
from pipelines.common import build_context, require_artifact_path


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


@track_mlflow_run(stage_name="50_phase_fit", logger=LOGGER)
@log_memory_usage(logger=LOGGER, label="50_phase_fit")
@log_wall_time(logger=LOGGER)
def run() -> None:
    from pyspark import StorageLevel

    context = build_context()
    raw_path = os.getenv("S3NTINEL_RAW_TABLE_PATH", "data/delta/raw_telemetry")
    events_path = os.getenv("S3NTINEL_EVENTS_TABLE_PATH", "data/delta/events")
    windows_path = os.getenv("S3NTINEL_WINDOWS_TABLE_PATH", "data/delta/windows")
    window_features_path = os.getenv("S3NTINEL_WINDOW_FEATURES_TABLE_PATH", "")
    backbone_path = os.getenv("S3NTINEL_BACKBONE_TABLE_PATH", "data/delta/backbone")
    phase_baselines_path = os.getenv("S3NTINEL_PHASE_BASELINES_TABLE_PATH", "data/delta/phase_baselines")
    phase_windows_path = os.getenv("S3NTINEL_PHASE_WINDOWS_TABLE_PATH", "data/delta/phase_windows")
    table_format = os.getenv("S3NTINEL_TABLE_FORMAT", "delta")
    write_mode = os.getenv("S3NTINEL_WRITE_MODE", "append")

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
    phase_count = int(os.getenv("S3NTINEL_PHASE_COUNT", str(context.config.get("simulation", {}).get("phase_count", 4))))
    phase_detect_sensor_count = int(os.getenv("S3NTINEL_PHASE_DETECT_SENSOR_COUNT", "8"))
    phase_detect_event_type_count = int(os.getenv("S3NTINEL_PHASE_DETECT_EVENT_TYPE_COUNT", "6"))
    phase_detect_categorical_state_count = int(os.getenv("S3NTINEL_PHASE_DETECT_CATEGORICAL_STATE_COUNT", "6"))
    phase_stable_drift_quantile = float(os.getenv("S3NTINEL_PHASE_STABLE_DRIFT_QUANTILE", "0.35"))
    phase_smoothing_radius = int(os.getenv("S3NTINEL_PHASE_SMOOTHING_RADIUS", "2"))
    phase_transition_penalty = float(os.getenv("S3NTINEL_PHASE_TRANSITION_PENALTY", "1.5"))
    phase_min_dwell_windows = int(os.getenv("S3NTINEL_PHASE_MIN_DWELL_WINDOWS", "8"))

    try:
        phase_config = fit_phase_feature_config_from_spark(
            window_features_df,
            backbone_df=backbone_df,
            phase_detect_sensor_count=phase_detect_sensor_count,
            phase_detect_event_type_count=phase_detect_event_type_count,
            phase_detect_categorical_state_count=phase_detect_categorical_state_count,
        )
        phase_windows_df = build_phase_windows_spark_table(
            window_features_df,
            phase_config=phase_config,
            phase_count=phase_count,
            phase_stable_drift_quantile=phase_stable_drift_quantile,
            phase_smoothing_radius=phase_smoothing_radius,
            phase_transition_penalty=phase_transition_penalty,
            phase_min_dwell_windows=phase_min_dwell_windows,
        ).persist(StorageLevel.MEMORY_AND_DISK)
        try:
            phase_baselines_df = build_phase_baselines_spark_table(
                phase_windows_df,
                phase_config=phase_config,
            ).persist(StorageLevel.MEMORY_AND_DISK)
            try:
                write_table(
                    phase_windows_df,
                    path=phase_windows_path,
                    mode=write_mode,
                    fmt=table_format,
                    partition_by=context.config["output"]["partition_by"],
                )
                write_table(
                    phase_baselines_df,
                    path=phase_baselines_path,
                    mode=write_mode,
                    fmt=table_format,
                    partition_by=["tail_id"],
                )
                phase_windows_count = int(phase_windows_df.count())
                phase_baselines_count = int(phase_baselines_df.count())
            finally:
                phase_baselines_df.unpersist()
        finally:
            phase_windows_df.unpersist()
    finally:
        window_features_df.unpersist()

    log_params_if_active(
        {
            "phase_count": phase_count,
        }
    )
    log_dict_artifact_if_active(
        {
            "stage": "50_phase_fit",
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
        "reports/stages/50_phase_fit_summary.json",
    )
    stage_manifest = build_stage_manifest(
        stage_name="50_phase_fit",
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
            "phase_windows": build_artifact_manifest(path=phase_windows_path, dataframe=phase_windows_df, row_count=phase_windows_count),
            "phase_baselines": build_artifact_manifest(path=phase_baselines_path, dataframe=phase_baselines_df, row_count=phase_baselines_count),
        },
        replayable_from=["window_features", "backbone"],
        cache_artifacts={"phase_fit_cache": {"config_keys": sorted(list(phase_config.keys()))}},
    )
    log_stage_manifest_if_active(stage_manifest, "reports/stages/50_phase_fit_manifest.json")
    LOGGER.info(
        "pipeline=phase_fit format=%s write_mode=%s phase_windows=%s phase_baselines=%s",
        table_format,
        write_mode,
        phase_windows_path,
        phase_baselines_path,
    )


if __name__ == "__main__":
    run()
