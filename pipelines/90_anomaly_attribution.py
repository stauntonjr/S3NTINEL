# File: pipelines/90_anomaly_attribution.py
"""Emit anomaly attribution tables for anomalous windows."""

from libs.anomaly import (
    build_anomaly_event_attribution_table,
    build_anomaly_telemetry_attribution_table,
    build_anomaly_window_attribution_table,
)
from libs.io.delta import get_spark, read_table, upsert_table, write_table
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
from pipelines.common import build_context, context_artifacts, context_execution, context_settings


LOGGER = get_logger(__name__)


@track_mlflow_run(stage_name="90_anomaly_attribution", logger=LOGGER)
@log_memory_usage(logger=LOGGER, label="90_anomaly_attribution")
@log_wall_time(logger=LOGGER)
def run() -> None:
    context = build_context()
    artifacts = context_artifacts(context)
    execution = context_execution(context)
    settings = context_settings(context)
    window_scores_calibrated_path = artifacts.window_scores_calibrated
    phase_windows_path = artifacts.phase_windows
    windows_path = artifacts.windows
    events_path = artifacts.events
    hierarchy_sensor_map_path = artifacts.hierarchy_sensor_map
    raw_path = artifacts.raw_table
    anomaly_window_attribution_path = artifacts.anomaly_window_attribution
    anomaly_telemetry_attribution_path = artifacts.anomaly_telemetry_attribution
    anomaly_event_attribution_path = artifacts.anomaly_event_attribution
    table_format = execution.table_format
    write_mode = execution.write_mode
    top_k_per_subsystem = settings.anomaly.subsystem_top_sensors_k

    spark = get_spark("s3ntinel.anomaly_attribution")
    calibrated_df = read_table(spark, window_scores_calibrated_path, fmt=table_format)
    phase_windows_df = read_table(spark, phase_windows_path, fmt=table_format)
    windows_df = read_table(spark, windows_path, fmt=table_format)
    events_df = read_table(spark, events_path, fmt=table_format)
    hierarchy_sensor_map_df = read_table(spark, hierarchy_sensor_map_path, fmt=table_format)
    raw_df = read_table(spark, raw_path, fmt=table_format)

    anomaly_window_attribution_df = build_anomaly_window_attribution_table(
        calibrated_df=calibrated_df,
        phase_windows_df=phase_windows_df,
        windows_df=windows_df,
        events_df=events_df,
        hierarchy_sensor_map_df=hierarchy_sensor_map_df,
        raw_df=raw_df,
        top_k_per_subsystem=top_k_per_subsystem,
    )
    anomaly_telemetry_attribution_df = build_anomaly_telemetry_attribution_table(
        calibrated_df=calibrated_df,
        windows_df=windows_df,
        raw_df=raw_df,
        hierarchy_sensor_map_df=hierarchy_sensor_map_df,
    )
    anomaly_event_attribution_df = build_anomaly_event_attribution_table(
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
    calibrated_count = int(calibrated_df.count())
    phase_windows_count = int(phase_windows_df.count())
    windows_count = int(windows_df.count())
    events_count = int(events_df.count())
    hierarchy_sensor_map_count = int(hierarchy_sensor_map_df.count())
    raw_count = int(raw_df.count())
    anomaly_window_count = int(anomaly_window_attribution_df.count())
    anomaly_telemetry_count = int(anomaly_telemetry_attribution_df.count())
    anomaly_event_count = int(anomaly_event_attribution_df.count())

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
            "stage": "90_anomaly_attribution",
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
        "reports/stages/90_anomaly_attribution_summary.json",
    )
    stage_manifest = build_stage_manifest(
        stage_name="90_anomaly_attribution",
        config={
            "table_format": table_format,
            "write_mode": write_mode,
            "merge_key": list(context.config["output"]["anomalies_merge_key"]),
            "subsystem_top_sensors_k": top_k_per_subsystem,
        },
        input_artifacts={
            "window_scores_calibrated": build_artifact_manifest(
                path=window_scores_calibrated_path,
                dataframe=calibrated_df,
                row_count=calibrated_count,
            ),
            "phase_windows": build_artifact_manifest(
                path=phase_windows_path,
                dataframe=phase_windows_df,
                row_count=phase_windows_count,
            ),
            "windows": build_artifact_manifest(path=windows_path, dataframe=windows_df, row_count=windows_count),
            "events": build_artifact_manifest(path=events_path, dataframe=events_df, row_count=events_count),
            "hierarchy_sensor_map": build_artifact_manifest(
                path=hierarchy_sensor_map_path,
                dataframe=hierarchy_sensor_map_df,
                row_count=hierarchy_sensor_map_count,
            ),
            "raw_telemetry": build_artifact_manifest(path=raw_path, dataframe=raw_df, row_count=raw_count),
        },
        output_artifacts={
            "anomaly_window_attribution": build_artifact_manifest(
                path=anomaly_window_attribution_path,
                dataframe=anomaly_window_attribution_df,
                row_count=anomaly_window_count,
            ),
            "anomaly_telemetry_attribution": build_artifact_manifest(
                path=anomaly_telemetry_attribution_path,
                dataframe=anomaly_telemetry_attribution_df,
                row_count=anomaly_telemetry_count,
            ),
            "anomaly_event_attribution": build_artifact_manifest(
                path=anomaly_event_attribution_path,
                dataframe=anomaly_event_attribution_df,
                row_count=anomaly_event_count,
            ),
        },
        replayable_from=[
            "window_scores_calibrated",
            "phase_windows",
            "windows",
            "events",
            "hierarchy_sensor_map",
            "raw_telemetry",
        ],
    )
    log_stage_manifest_if_active(stage_manifest, "reports/stages/90_anomaly_attribution_manifest.json")
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
