# File: pipelines/80_anomaly_attribution.py
"""Emit anomaly attribution tables for anomalous windows."""

import os

from libs.anomaly.attribution import (
    build_anomaly_event_attribution_df,
    build_anomaly_telemetry_attribution_df,
    build_anomaly_window_attribution_df,
)
from libs.io.delta import get_spark, read_table, upsert_table, write_table
from libs.perf import get_logger, log_dict_artifact_if_active, log_params_if_active, log_wall_time, track_mlflow_run
from pipelines.common import build_context


LOGGER = get_logger(__name__)


@track_mlflow_run(stage_name="80_anomaly_attribution", logger=LOGGER)
@log_wall_time(logger=LOGGER)
def run() -> None:
    context = build_context()
    window_scores_calibrated_path = os.getenv("S3NTINEL_WINDOW_SCORES_CALIBRATED_TABLE_PATH", "data/delta/window_scores_calibrated")
    phase_windows_path = os.getenv("S3NTINEL_PHASE_WINDOWS_TABLE_PATH", "data/delta/phase_windows")
    windows_path = os.getenv("S3NTINEL_WINDOWS_TABLE_PATH", "data/delta/windows")
    events_path = os.getenv("S3NTINEL_EVENTS_TABLE_PATH", "data/delta/events")
    hierarchy_sensor_map_path = os.getenv("S3NTINEL_HIERARCHY_SENSOR_MAP_TABLE_PATH", "data/delta/hierarchy_sensor_map")
    raw_path = os.getenv("S3NTINEL_RAW_TABLE_PATH", "data/delta/raw_telemetry")
    anomaly_window_attribution_path = os.getenv(
        "S3NTINEL_ANOMALY_WINDOW_ATTRIBUTION_TABLE_PATH",
        "data/delta/anomaly_window_attribution",
    )
    anomaly_telemetry_attribution_path = os.getenv(
        "S3NTINEL_ANOMALY_TELEMETRY_ATTRIBUTION_TABLE_PATH",
        "data/delta/anomaly_telemetry_attribution",
    )
    anomaly_event_attribution_path = os.getenv(
        "S3NTINEL_ANOMALY_EVENT_ATTRIBUTION_TABLE_PATH",
        "data/delta/anomaly_event_attribution",
    )
    table_format = os.getenv("S3NTINEL_TABLE_FORMAT", "delta")
    write_mode = os.getenv("S3NTINEL_WRITE_MODE", "merge")
    top_k_per_subsystem = int(os.getenv("S3NTINEL_SUBSYSTEM_TOP_SENSORS_K", "5"))

    spark = get_spark("s3ntinel.anomaly_attribution")
    calibrated_df = read_table(spark, window_scores_calibrated_path, fmt=table_format)
    phase_windows_df = read_table(spark, phase_windows_path, fmt=table_format)
    windows_df = read_table(spark, windows_path, fmt=table_format)
    events_df = read_table(spark, events_path, fmt=table_format)
    hierarchy_sensor_map_df = read_table(spark, hierarchy_sensor_map_path, fmt=table_format)
    raw_df = read_table(spark, raw_path, fmt=table_format)

    anomaly_window_attribution_df = build_anomaly_window_attribution_df(
        calibrated_df=calibrated_df,
        phase_windows_df=phase_windows_df,
        windows_df=windows_df,
        events_df=events_df,
        hierarchy_sensor_map_df=hierarchy_sensor_map_df,
        raw_df=raw_df,
        top_k_per_subsystem=top_k_per_subsystem,
    )
    anomaly_telemetry_attribution_df = build_anomaly_telemetry_attribution_df(
        calibrated_df=calibrated_df,
        windows_df=windows_df,
        raw_df=raw_df,
        hierarchy_sensor_map_df=hierarchy_sensor_map_df,
    )
    anomaly_event_attribution_df = build_anomaly_event_attribution_df(
        calibrated_df=calibrated_df,
        windows_df=windows_df,
        events_df=events_df,
        hierarchy_sensor_map_df=hierarchy_sensor_map_df,
    )
    if write_mode.lower() == "merge":
        upsert_table(
            anomaly_window_attribution_df,
            path=anomaly_window_attribution_path,
            merge_keys=context.config["output"]["anomalies_merge_key"],
            fmt=table_format,
            partition_by=context.config["output"]["partition_by"],
        )
        upsert_table(
            anomaly_telemetry_attribution_df,
            path=anomaly_telemetry_attribution_path,
            merge_keys=["tail_id", "flight_id", "win_id", "timestamp_utc", "parameter_name"],
            fmt=table_format,
            partition_by=context.config["output"]["partition_by"],
        )
        upsert_table(
            anomaly_event_attribution_df,
            path=anomaly_event_attribution_path,
            merge_keys=["tail_id", "flight_id", "win_id", "timestamp_utc", "parameter_name", "event_type_detected"],
            fmt=table_format,
            partition_by=context.config["output"]["partition_by"],
        )
    else:
        write_table(
            anomaly_window_attribution_df,
            path=anomaly_window_attribution_path,
            mode=write_mode,
            fmt=table_format,
            partition_by=context.config["output"]["partition_by"],
        )
        write_table(
            anomaly_telemetry_attribution_df,
            path=anomaly_telemetry_attribution_path,
            mode=write_mode,
            fmt=table_format,
            partition_by=context.config["output"]["partition_by"],
        )
        write_table(
            anomaly_event_attribution_df,
            path=anomaly_event_attribution_path,
            mode=write_mode,
            fmt=table_format,
            partition_by=context.config["output"]["partition_by"],
        )

    log_params_if_active(
        {
            "merge_key": context.config["output"]["anomalies_merge_key"],
            "write_mode": write_mode,
            "subsystem_top_sensors_k": top_k_per_subsystem,
            "required_events": 1,
            "required_hierarchy_sensor_map": 1,
            "required_raw_telemetry": 1,
        }
    )
    log_dict_artifact_if_active(
        {
            "stage": "80_anomaly_attribution",
            "window_scores_calibrated_path": window_scores_calibrated_path,
            "phase_windows_path": phase_windows_path,
            "windows_path": windows_path,
            "events_path": events_path,
            "hierarchy_sensor_map_path": hierarchy_sensor_map_path,
            "raw_path": raw_path,
            "anomaly_window_attribution_path": anomaly_window_attribution_path,
            "anomaly_telemetry_attribution_path": anomaly_telemetry_attribution_path,
            "anomaly_event_attribution_path": anomaly_event_attribution_path,
            "table_format": table_format,
            "write_mode": write_mode,
            "merge_key": list(context.config["output"]["anomalies_merge_key"]),
            "partition_by": list(context.config["output"]["partition_by"]),
            "subsystem_top_sensors_k": top_k_per_subsystem,
            "required_inputs": [
                window_scores_calibrated_path,
                phase_windows_path,
                windows_path,
                events_path,
                hierarchy_sensor_map_path,
                raw_path,
            ],
        },
        "reports/stages/80_anomaly_attribution_summary.json",
    )
    LOGGER.info(
        "pipeline=anomaly_attribution merge_key=%s write_mode=%s window_scores_calibrated=%s anomaly_window_attribution=%s anomaly_telemetry_attribution=%s anomaly_event_attribution=%s",
        context.config["output"]["anomalies_merge_key"],
        write_mode,
        window_scores_calibrated_path,
        anomaly_window_attribution_path,
        anomaly_telemetry_attribution_path,
        anomaly_event_attribution_path,
    )


if __name__ == "__main__":
    run()
