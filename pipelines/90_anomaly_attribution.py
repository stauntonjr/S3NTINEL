# File: pipelines/90_anomaly_attribution.py
"""Emit anomaly attribution tables for anomalous windows."""

from libs.anomaly import AnomalyAttributionPlan
from libs.io.delta import get_spark, read_table
from libs.profiling import ParameterBehaviorProfile
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
from pipelines.common import build_stage_runtime


LOGGER = get_logger(__name__)


def _materialize_output(table, storage_level):
    dataframe = table.to_dataframe()
    if hasattr(dataframe, "persist"):
        dataframe = dataframe.persist(storage_level)
    if hasattr(table, "with_dataframe"):
        table = table.with_dataframe(dataframe)
    return table, int(dataframe.count())


@track_mlflow_run(stage_name="90_anomaly_attribution", logger=LOGGER)
@log_memory_usage(logger=LOGGER, label="90_anomaly_attribution")
@log_wall_time(logger=LOGGER)
def run() -> None:
    from pyspark import StorageLevel

    runtime = build_stage_runtime("90_anomaly_attribution")
    context = runtime.context
    window_scores_calibrated_path = runtime.artifacts.window_scores_calibrated
    phase_windows_path = runtime.artifacts.phase_windows
    windows_path = runtime.artifacts.windows
    events_path = runtime.artifacts.events
    hierarchy_sensor_map_path = runtime.artifacts.hierarchy_sensor_map
    parameter_behavior_profile_path = runtime.artifacts.parameter_behavior_profile
    raw_path = runtime.artifacts.raw_table
    anomaly_window_attribution_path = runtime.artifacts.anomaly_window_attribution
    anomaly_telemetry_attribution_path = runtime.artifacts.anomaly_telemetry_attribution
    anomaly_event_attribution_path = runtime.artifacts.anomaly_event_attribution
    anomaly_parameter_candidate_evidence_path = runtime.artifacts.anomaly_parameter_candidate_evidence
    table_format = runtime.execution.table_format
    write_mode = runtime.execution.write_mode
    top_k_per_subsystem = runtime.settings.anomaly.subsystem_top_sensors_k

    spark = get_spark("s3ntinel.anomaly_attribution")
    calibrated_df = read_table(spark, window_scores_calibrated_path, fmt=table_format)
    phase_windows_df = read_table(spark, phase_windows_path, fmt=table_format)
    windows_df = read_table(spark, windows_path, fmt=table_format)
    events_df = read_table(spark, events_path, fmt=table_format)
    hierarchy_sensor_map_df = read_table(spark, hierarchy_sensor_map_path, fmt=table_format)
    parameter_behavior_profile_df = ParameterBehaviorProfile.read(
        spark,
        parameter_behavior_profile_path,
        format=table_format,
    ).to_dataframe()
    raw_df = read_table(spark, raw_path, fmt=table_format)

    artifacts = AnomalyAttributionPlan(top_k_per_subsystem=top_k_per_subsystem).build(
        calibrated_df=calibrated_df,
        phase_windows_df=phase_windows_df,
        windows_df=windows_df,
        events_df=events_df,
        hierarchy_sensor_map_df=hierarchy_sensor_map_df,
        parameter_behavior_profile_df=parameter_behavior_profile_df,
        raw_df=raw_df,
    )
    anomaly_window_attribution = artifacts.window_attribution.bind(
        path=anomaly_window_attribution_path,
        format=table_format,
        partition_by=tuple(context.config["output"]["partition_by"]),
    )
    anomaly_telemetry_attribution = artifacts.telemetry_attribution.bind(
        path=anomaly_telemetry_attribution_path,
        format=table_format,
        partition_by=tuple(context.config["output"]["partition_by"]),
    )
    anomaly_event_attribution = artifacts.event_attribution.bind(
        path=anomaly_event_attribution_path,
        format=table_format,
        partition_by=tuple(context.config["output"]["partition_by"]),
    )
    anomaly_parameter_candidate_evidence = artifacts.parameter_candidate_evidence.bind(
        path=anomaly_parameter_candidate_evidence_path,
        format=table_format,
        partition_by=tuple(context.config["output"]["partition_by"]),
    )
    # These frames have wide schemas and share expensive attribution lineages.
    # Materialize them once so the empty check, write, and manifest count do not
    # each re-plan and re-execute the complete attribution graph.
    anomaly_window_attribution, anomaly_window_count = _materialize_output(
        anomaly_window_attribution, StorageLevel.DISK_ONLY
    )
    anomaly_telemetry_attribution, anomaly_telemetry_count = _materialize_output(
        anomaly_telemetry_attribution, StorageLevel.DISK_ONLY
    )
    anomaly_event_attribution, anomaly_event_count = _materialize_output(
        anomaly_event_attribution, StorageLevel.DISK_ONLY
    )
    anomaly_parameter_candidate_evidence, anomaly_parameter_candidate_evidence_count = _materialize_output(
        anomaly_parameter_candidate_evidence, StorageLevel.DISK_ONLY
    )
    if write_mode.lower() == "merge":
        anomaly_window_attribution.upsert(merge_keys=context.config["output"]["anomalies_merge_key"])
        anomaly_telemetry_attribution.upsert(merge_keys=["tail_id", "flight_id", "win_id", "timestamp_utc", "parameter_name"])
        anomaly_event_attribution.upsert(
            merge_keys=["tail_id", "flight_id", "win_id", "timestamp_utc", "parameter_name", "event_type_detected"]
        )
        anomaly_parameter_candidate_evidence.upsert(
            merge_keys=["tail_id", "flight_id", "win_id", "parameter_name"]
        )
    else:
        anomaly_window_attribution.write(mode=write_mode)
        anomaly_telemetry_attribution.write(mode=write_mode)
        anomaly_event_attribution.write(mode=write_mode)
        anomaly_parameter_candidate_evidence.write(mode=write_mode)
    calibrated_count = int(calibrated_df.count())
    phase_windows_count = int(phase_windows_df.count())
    windows_count = int(windows_df.count())
    events_count = int(events_df.count())
    hierarchy_sensor_map_count = int(hierarchy_sensor_map_df.count())
    parameter_behavior_profile_count = int(parameter_behavior_profile_df.count())
    raw_count = int(raw_df.count())
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
            "parameter_behavior_profile_path": parameter_behavior_profile_path,
            "raw_path": raw_path,
            "anomaly_window_attribution_path": anomaly_window_attribution_path,
            "anomaly_telemetry_attribution_path": anomaly_telemetry_attribution_path,
            "anomaly_event_attribution_path": anomaly_event_attribution_path,
            "anomaly_parameter_candidate_evidence_path": anomaly_parameter_candidate_evidence_path,
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
        runtime.report_paths.summary_artifact_path,
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
            "parameter_behavior_profile": build_artifact_manifest(
                path=parameter_behavior_profile_path,
                dataframe=parameter_behavior_profile_df,
                row_count=parameter_behavior_profile_count,
            ),
            "raw_telemetry": build_artifact_manifest(path=raw_path, dataframe=raw_df, row_count=raw_count),
        },
        output_artifacts={
            "anomaly_window_attribution": build_artifact_manifest(
                path=anomaly_window_attribution_path,
                dataframe=anomaly_window_attribution.to_dataframe(),
                row_count=anomaly_window_count,
            ),
            "anomaly_telemetry_attribution": build_artifact_manifest(
                path=anomaly_telemetry_attribution_path,
                dataframe=anomaly_telemetry_attribution.to_dataframe(),
                row_count=anomaly_telemetry_count,
            ),
            "anomaly_event_attribution": build_artifact_manifest(
                path=anomaly_event_attribution_path,
                dataframe=anomaly_event_attribution.to_dataframe(),
                row_count=anomaly_event_count,
            ),
            "anomaly_parameter_candidate_evidence": build_artifact_manifest(
                path=anomaly_parameter_candidate_evidence_path,
                dataframe=anomaly_parameter_candidate_evidence.to_dataframe(),
                row_count=anomaly_parameter_candidate_evidence_count,
            ),
        },
        replayable_from=[
            "window_scores_calibrated",
            "phase_windows",
            "windows",
            "events",
            "hierarchy_sensor_map",
            "parameter_behavior_profile",
            "raw_telemetry",
        ],
    )
    log_stage_manifest_if_active(stage_manifest, runtime.report_paths.manifest_artifact_path)
    LOGGER.info(
        "pipeline=anomaly_attribution merge_key=%s write_mode=%s window_scores_calibrated=%s anomaly_window_attribution=%s anomaly_telemetry_attribution=%s anomaly_event_attribution=%s anomaly_parameter_candidate_evidence=%s",
        context.config["output"]["anomalies_merge_key"],
        write_mode,
        window_scores_calibrated_path,
        anomaly_window_attribution_path,
        anomaly_telemetry_attribution_path,
        anomaly_event_attribution_path,
        anomaly_parameter_candidate_evidence_path,
    )


if __name__ == "__main__":
    run()
