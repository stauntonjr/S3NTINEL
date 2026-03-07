# File: pipelines/50_phase_fit.py
"""Fit phase baselines and assign detected phases to windows."""

import os

from libs.io.delta import get_spark, read_table, write_table
from libs.phase import (
    build_phase_baselines_spark_table,
    build_phase_windows_spark_table,
    build_window_x_spark_table,
    fit_phase_window_x_config,
)
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
from pipelines.common import build_context


LOGGER = get_logger(__name__)


def _bounded_count(df: "DataFrame", *, limit: int) -> int:
    return int(df.limit(max(int(limit), 0) + 1).count())


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
@log_wall_time(logger=LOGGER)
def run() -> None:
    context = build_context()
    raw_path = os.getenv("S3NTINEL_RAW_TABLE_PATH", "data/delta/raw_telemetry")
    events_path = os.getenv("S3NTINEL_EVENTS_TABLE_PATH", "data/delta/events")
    windows_path = os.getenv("S3NTINEL_WINDOWS_TABLE_PATH", "data/delta/windows")
    phase_baselines_path = os.getenv("S3NTINEL_PHASE_BASELINES_TABLE_PATH", "data/delta/phase_baselines")
    phase_windows_path = os.getenv("S3NTINEL_PHASE_WINDOWS_TABLE_PATH", "data/delta/phase_windows")
    table_format = os.getenv("S3NTINEL_TABLE_FORMAT", "delta")
    write_mode = os.getenv("S3NTINEL_WRITE_MODE", "append")
    max_bridge_rows = int(os.getenv("S3NTINEL_MAX_BRIDGE_PHASE_INPUT_ROWS", "250000"))

    spark = get_spark("s3ntinel.phase_fit")
    raw_df = read_table(spark, raw_path, fmt=table_format)
    events_df = read_table(spark, events_path, fmt=table_format)
    windows_df = read_table(spark, windows_path, fmt=table_format)
    raw_df, events_df, windows_df = _select_phase_fit_input_columns(raw_df, events_df, windows_df)
    raw_count = _bounded_count(raw_df, limit=max_bridge_rows)
    events_count = _bounded_count(events_df, limit=max_bridge_rows)
    windows_count = _bounded_count(windows_df, limit=max_bridge_rows)
    if raw_count > max_bridge_rows or events_count > max_bridge_rows or windows_count > max_bridge_rows:
        raise RuntimeError(
            "50_phase_fit still uses a bounded pandas bridge; input exceeds "
            f"S3NTINEL_MAX_BRIDGE_PHASE_INPUT_ROWS={max_bridge_rows}. "
            "Reduce input size or replace this stage with a distributed implementation."
        )
    window_x_df = build_window_x_spark_table(raw_df, events_df, windows_df)
    window_x_count = _bounded_count(window_x_df, limit=max_bridge_rows)
    if window_x_count > max_bridge_rows:
        raise RuntimeError(
            "50_phase_fit still bridges `window_x` to pandas; "
            f"window_x exceeds S3NTINEL_MAX_BRIDGE_PHASE_INPUT_ROWS={max_bridge_rows}. "
            "Replace the remaining phase fit with a distributed implementation."
        )
    phase_count = int(os.getenv("S3NTINEL_PHASE_COUNT", str(context.config.get("simulation", {}).get("phase_count", 4))))
    backbone_sensor_count = int(os.getenv("S3NTINEL_BACKBONE_SENSOR_COUNT", "8"))
    backbone_ridge_lambda = float(os.getenv("S3NTINEL_BACKBONE_RIDGE_LAMBDA", "1.0"))
    phase_detect_sensor_count = int(os.getenv("S3NTINEL_PHASE_DETECT_SENSOR_COUNT", "8"))
    phase_detect_event_type_count = int(os.getenv("S3NTINEL_PHASE_DETECT_EVENT_TYPE_COUNT", "6"))
    phase_detect_categorical_state_count = int(os.getenv("S3NTINEL_PHASE_DETECT_CATEGORICAL_STATE_COUNT", "6"))
    phase_detect_window_cooccurrence_count = int(os.getenv("S3NTINEL_PHASE_DETECT_WINDOW_COOCCURRENCE_COUNT", "0"))
    phase_stable_drift_quantile = float(os.getenv("S3NTINEL_PHASE_STABLE_DRIFT_QUANTILE", "0.35"))
    phase_smoothing_radius = int(os.getenv("S3NTINEL_PHASE_SMOOTHING_RADIUS", "2"))
    phase_transition_penalty = float(os.getenv("S3NTINEL_PHASE_TRANSITION_PENALTY", "1.5"))
    phase_min_dwell_windows = int(os.getenv("S3NTINEL_PHASE_MIN_DWELL_WINDOWS", "8"))

    phase_config = fit_phase_window_x_config(
        window_x_df.toPandas(),
        backbone_sensor_count=backbone_sensor_count,
        backbone_ridge_lambda=backbone_ridge_lambda,
        phase_detect_sensor_count=phase_detect_sensor_count,
        phase_detect_event_type_count=phase_detect_event_type_count,
        phase_detect_categorical_state_count=phase_detect_categorical_state_count,
        phase_detect_window_cooccurrence_count=phase_detect_window_cooccurrence_count,
    )
    phase_windows_df = build_phase_windows_spark_table(
        window_x_df,
        phase_config=phase_config,
        phase_count=phase_count,
        phase_stable_drift_quantile=phase_stable_drift_quantile,
        phase_smoothing_radius=phase_smoothing_radius,
        phase_transition_penalty=phase_transition_penalty,
        phase_min_dwell_windows=phase_min_dwell_windows,
    )
    phase_baselines_df = build_phase_baselines_spark_table(
        phase_windows_df,
        phase_config=phase_config,
    )

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
            "phase_windows_path": phase_windows_path,
            "phase_baselines_path": phase_baselines_path,
            "table_format": table_format,
            "write_mode": write_mode,
            "max_bridge_phase_input_rows": max_bridge_rows,
            "raw_count_bounded": raw_count,
            "events_count_bounded": events_count,
            "windows_count_bounded": windows_count,
            "window_x_count_bounded": window_x_count,
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
            "backbone_sensor_count": backbone_sensor_count,
            "backbone_ridge_lambda": backbone_ridge_lambda,
            "phase_detect_sensor_count": phase_detect_sensor_count,
            "phase_detect_event_type_count": phase_detect_event_type_count,
            "phase_detect_categorical_state_count": phase_detect_categorical_state_count,
            "phase_detect_window_cooccurrence_count": phase_detect_window_cooccurrence_count,
            "phase_stable_drift_quantile": phase_stable_drift_quantile,
            "phase_smoothing_radius": phase_smoothing_radius,
            "phase_transition_penalty": phase_transition_penalty,
            "phase_min_dwell_windows": phase_min_dwell_windows,
            "max_bridge_phase_input_rows": max_bridge_rows,
        },
        input_artifacts={
            "raw_telemetry": build_artifact_manifest(path=raw_path, dataframe=raw_df, row_count=raw_count),
            "events": build_artifact_manifest(path=events_path, dataframe=events_df, row_count=events_count),
            "windows": build_artifact_manifest(path=windows_path, dataframe=windows_df, row_count=windows_count),
            "window_x": build_artifact_manifest(path="window_x::ephemeral", dataframe=window_x_df, row_count=window_x_count),
        },
        output_artifacts={
            "phase_windows": build_artifact_manifest(path=phase_windows_path, dataframe=phase_windows_df, row_count=phase_windows_count),
            "phase_baselines": build_artifact_manifest(path=phase_baselines_path, dataframe=phase_baselines_df, row_count=phase_baselines_count),
        },
        replayable_from=["window_x"],
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
