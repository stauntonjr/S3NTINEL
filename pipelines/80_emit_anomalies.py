# File: pipelines/80_emit_anomalies.py
"""Emit deterministic anomaly objects to Delta sink."""

import os

from libs.anomaly.build import build_anomalies_df
from libs.io.delta import get_spark, read_table, upsert_table, write_table
from libs.perf import get_logger, log_params_if_active, log_wall_time, track_mlflow_run
from pipelines.common import build_context


LOGGER = get_logger(__name__)


@track_mlflow_run(stage_name="80_emit_anomalies", logger=LOGGER)
@log_wall_time(logger=LOGGER)
def run() -> None:
    context = build_context()
    calibrated_path = os.getenv("S3NTINEL_CALIBRATED_TABLE_PATH", "data/delta/calibrated")
    phase_windows_path = os.getenv("S3NTINEL_PHASE_WINDOWS_TABLE_PATH", "data/delta/phase_windows")
    signatures_path = os.getenv("S3NTINEL_SIGNATURES_TABLE_PATH", "data/delta/signatures")
    windows_path = os.getenv("S3NTINEL_WINDOWS_TABLE_PATH", "data/delta/windows")
    events_path = os.getenv("S3NTINEL_EVENTS_TABLE_PATH", "data/delta/events")
    subsystem_map_path = os.getenv("S3NTINEL_SUBSYSTEM_MAP_TABLE_PATH", "data/delta/sensor_subsystem_map")
    raw_path = os.getenv("S3NTINEL_RAW_TABLE_PATH", "data/delta/raw_telemetry")
    anomalies_path = os.getenv("S3NTINEL_ANOMALIES_TABLE_PATH", "data/delta/anomalies")
    table_format = os.getenv("S3NTINEL_TABLE_FORMAT", "delta")
    write_mode = os.getenv("S3NTINEL_WRITE_MODE", "merge")
    top_k_per_subsystem = int(os.getenv("S3NTINEL_SUBSYSTEM_TOP_SENSORS_K", "5"))

    spark = get_spark("s3ntinel.emit_anomalies")
    calibrated_df = read_table(spark, calibrated_path, fmt=table_format)
    phase_windows_df = read_table(spark, phase_windows_path, fmt=table_format)
    signatures_df = read_table(spark, signatures_path, fmt=table_format)
    windows_df = read_table(spark, windows_path, fmt=table_format)
    events_df = None
    subsystem_map_df = None
    raw_df = None
    try:
        events_df = read_table(spark, events_path, fmt=table_format)
        subsystem_map_df = read_table(spark, subsystem_map_path, fmt=table_format)
    except Exception:
        events_df = None
        subsystem_map_df = None
    try:
        raw_df = read_table(spark, raw_path, fmt=table_format)
    except Exception:
        raw_df = None

    anomalies_df = build_anomalies_df(
        calibrated_df=calibrated_df,
        phase_windows_df=phase_windows_df,
        signatures_df=signatures_df,
        windows_df=windows_df,
        events_df=events_df,
        subsystem_map_df=subsystem_map_df,
        raw_df=raw_df,
        top_k_per_subsystem=top_k_per_subsystem,
    )
    if write_mode.lower() == "merge":
        upsert_table(
            anomalies_df,
            path=anomalies_path,
            merge_keys=context.config["output"]["anomalies_merge_key"],
            fmt=table_format,
            partition_by=context.config["output"]["partition_by"],
        )
    else:
        write_table(
            anomalies_df,
            path=anomalies_path,
            mode=write_mode,
            fmt=table_format,
            partition_by=context.config["output"]["partition_by"],
        )

    log_params_if_active(
        {
            "merge_key": context.config["output"]["anomalies_merge_key"],
            "write_mode": write_mode,
            "subsystem_top_sensors_k": top_k_per_subsystem,
            "subsystem_top_sensors_enabled": int(events_df is not None and subsystem_map_df is not None),
            "panel_context_enabled": int(raw_df is not None),
        }
    )
    LOGGER.info(
        "pipeline=emit_anomalies merge_key=%s write_mode=%s top_sensors_enabled=%s panel_context_enabled=%s calibrated=%s anomalies=%s",
        context.config["output"]["anomalies_merge_key"],
        write_mode,
        events_df is not None and subsystem_map_df is not None,
        raw_df is not None,
        calibrated_path,
        anomalies_path,
    )


if __name__ == "__main__":
    run()
