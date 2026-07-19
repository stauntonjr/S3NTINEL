# File: pipelines/70_phase_fit.py
"""Fit phase baselines and assign detected phases to windows."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from libs.io.delta import get_spark, read_table
from libs.phase import (
    PhaseDetectionPlan,
    PhaseReferenceModelTable,
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
    raw_cols = [
        col
        for col in [
            "tail_id",
            "flight_id",
            "timestamp_utc",
            "parameter_name",
            "parameter_value",
            "timestamp",
        ]
        if col in raw_df.columns
    ]
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
    phase_reference_model_path = runtime.artifacts.phase_reference_model
    phase_windows_path = runtime.artifacts.phase_windows
    table_format = runtime.execution.table_format
    write_mode = runtime.execution.write_mode
    execution_mode = str(os.getenv("S3NTINEL_PHASE_EXECUTION_MODE", "fit")).strip().lower()
    if execution_mode not in {"fit", "apply_reference"}:
        raise ValueError(
            "unsupported S3NTINEL_PHASE_EXECUTION_MODE="
            f"{execution_mode!r}; expected 'fit' or 'apply_reference'"
        )

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
    phase_transition_penalty = runtime.settings.phase.transition_penalty
    phase_min_dwell_windows = runtime.settings.phase.min_dwell_windows

    phase_plan = PhaseDetectionPlan(
        phase_count=phase_count,
        phase_stable_drift_quantile=phase_stable_drift_quantile,
        phase_transition_penalty=phase_transition_penalty,
        phase_min_dwell_windows=phase_min_dwell_windows,
    )
    phase_baselines = None
    phase_reference_model = None
    try:
        if execution_mode == "apply_reference":
            phase_reference_model = PhaseReferenceModelTable.read(
                spark,
                phase_reference_model_path,
                format=table_format,
            )
            phase_baselines_df = read_table(spark, phase_baselines_path, fmt=table_format)
            detection_run = phase_plan.run_reference_inference(
                window_features_df,
                reference_model=phase_reference_model,
            )
        else:
            phase_config = fit_phase_feature_config_from_spark(
                window_features_df,
                backbone_df=backbone_df,
                phase_detect_sensor_count=phase_detect_sensor_count,
                phase_detect_event_type_count=phase_detect_event_type_count,
                phase_detect_categorical_state_count=phase_detect_categorical_state_count,
            )
            detection_run = phase_plan.run_detection(window_features_df, phase_config=phase_config)
            phase_baselines = phase_plan.build_phase_baselines(
                detection_run.phase_windows,
                phase_config=detection_run.phase_config,
            )
            phase_reference_model = PhaseReferenceModelTable.from_detection_run(detection_run)
            phase_baselines_df = phase_baselines.to_dataframe().persist(StorageLevel.MEMORY_AND_DISK)
            phase_reference_model_df = phase_reference_model.to_dataframe().persist(StorageLevel.MEMORY_AND_DISK)
        phase_fit_diagnostics = detection_run.diagnostics or {}
        phase_config = detection_run.phase_config.to_dict()
        phase_windows = detection_run.phase_windows.with_dataframe(
            detection_run.phase_windows.to_dataframe().persist(StorageLevel.MEMORY_AND_DISK)
        ).bind(
            path=phase_windows_path,
            format=table_format,
            partition_by=tuple(context.config["output"]["partition_by"]),
        )
        try:
            phase_windows.write(mode=write_mode)
            phase_windows_count = int(phase_windows.to_dataframe().count())
            if execution_mode == "fit":
                phase_baselines = phase_baselines.with_dataframe(phase_baselines_df).bind(
                    path=phase_baselines_path,
                    format=table_format,
                    partition_by=("tail_id",),
                )
                phase_reference_model = phase_reference_model.with_dataframe(phase_reference_model_df).bind(
                    path=phase_reference_model_path,
                    format=table_format,
                    partition_by=("tail_id",),
                )
                phase_baselines.write(mode=write_mode)
                phase_reference_model.write(mode=write_mode)
            phase_baselines_count = int(phase_baselines_df.count())
            phase_reference_model_count = int(phase_reference_model.to_dataframe().count())
        finally:
            phase_windows.to_dataframe().unpersist()
            if execution_mode == "fit":
                phase_baselines_df.unpersist()
                phase_reference_model_df.unpersist()
    finally:
        window_features_df.unpersist()

    log_params_if_active(
        {
            "phase_count": phase_count,
            "phase_execution_mode": execution_mode,
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
            "phase_reference_model_path": phase_reference_model_path,
            "phase_execution_mode": execution_mode,
            "table_format": table_format,
            "write_mode": write_mode,
            "phase_partition_by": ["tail_id"],
            "phase_windows_partition_by": list(context.config["output"]["partition_by"]),
            "phase_fit_flights": phase_fit_diagnostics.get("phase_fit_flights", []),
        },
        runtime.report_paths.summary_artifact_path,
    )
    input_artifacts = {
        "raw_telemetry": build_artifact_manifest(path=raw_path, dataframe=raw_df),
        "events": build_artifact_manifest(path=events_path, dataframe=events_df),
        "windows": build_artifact_manifest(path=windows_path, dataframe=windows_df),
        "backbone": build_artifact_manifest(path=backbone_path, dataframe=backbone_df),
        "window_features": build_artifact_manifest(
            path=(window_features_path or "window_features::ephemeral"),
            dataframe=window_features_df,
        ),
    }
    output_artifacts = {
        "phase_windows": build_artifact_manifest(
            path=phase_windows_path,
            dataframe=phase_windows.to_dataframe(),
            row_count=phase_windows_count,
        ),
    }
    if execution_mode == "apply_reference":
        input_artifacts["phase_baselines"] = build_artifact_manifest(
            path=phase_baselines_path,
            dataframe=phase_baselines_df,
            row_count=phase_baselines_count,
        )
        input_artifacts["phase_reference_model"] = build_artifact_manifest(
            path=phase_reference_model_path,
            dataframe=phase_reference_model.to_dataframe(),
            row_count=phase_reference_model_count,
        )
    else:
        output_artifacts["phase_baselines"] = build_artifact_manifest(
            path=phase_baselines_path,
            dataframe=phase_baselines.to_dataframe(),
            row_count=phase_baselines_count,
        )
        output_artifacts["phase_reference_model"] = build_artifact_manifest(
            path=phase_reference_model_path,
            dataframe=phase_reference_model.to_dataframe(),
            row_count=phase_reference_model_count,
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
            "phase_transition_penalty": phase_transition_penalty,
            "phase_min_dwell_windows": phase_min_dwell_windows,
            "phase_execution_mode": execution_mode,
        },
        input_artifacts=input_artifacts,
        output_artifacts=output_artifacts,
        replayable_from=(
            ["window_features", "phase_baselines", "phase_reference_model"]
            if execution_mode == "apply_reference"
            else ["window_features", "backbone"]
        ),
        cache_artifacts={
            "phase_fit_cache": {
                "config_keys": sorted(list(phase_config.keys())),
                "phase_fit_flights": phase_fit_diagnostics.get("phase_fit_flights", []),
            }
        },
    )
    log_stage_manifest_if_active(stage_manifest, runtime.report_paths.manifest_artifact_path)
    LOGGER.info(
        "pipeline=phase_fit mode=%s format=%s write_mode=%s phase_windows=%s phase_baselines=%s phase_reference_model=%s",
        execution_mode,
        table_format,
        write_mode,
        phase_windows_path,
        phase_baselines_path,
        phase_reference_model_path,
    )


if __name__ == "__main__":
    run()


if TYPE_CHECKING:
    from pyspark.sql import DataFrame
