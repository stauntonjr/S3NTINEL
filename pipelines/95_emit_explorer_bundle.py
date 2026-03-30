"""Emit a thin explorer-ready bundle for notebook and UI consumers."""

from __future__ import annotations

from pathlib import Path

from libs.io.delta import get_spark, read_table
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
from libs.perf.memory import log_memory_usage
from libs.plotting.explorer_bundle import (
    build_explorer_anomaly_markers_spark_table,
    build_explorer_anomaly_windows_spark_table,
    build_explorer_event_markers_spark_table,
    build_explorer_phase_intervals_spark_table,
    build_explorer_telemetry_spark_table,
    explorer_bundle_manifest_path,
    write_explorer_bundle,
)
from pipelines.common import build_stage_runtime


LOGGER = get_logger(__name__)


@track_mlflow_run(stage_name="95_emit_explorer_bundle", logger=LOGGER)
@log_memory_usage(logger=LOGGER, label="95_emit_explorer_bundle")
@log_wall_time(logger=LOGGER)
def run() -> None:
    runtime = build_stage_runtime("95_emit_explorer_bundle")
    context = runtime.context
    artifacts = runtime.artifacts
    table_format = runtime.execution.table_format
    write_mode = runtime.execution.write_mode
    spark = get_spark("s3ntinel.emit_explorer_bundle")

    raw_df = read_table(spark, artifacts.raw_table, fmt=table_format)
    hierarchy_df = read_table(spark, artifacts.hierarchy_sensor_map, fmt=table_format)
    events_df = read_table(spark, artifacts.events, fmt=table_format)
    phase_windows_df = read_table(spark, artifacts.phase_windows, fmt=table_format)
    anomaly_window_df = read_table(spark, artifacts.anomaly_window_attribution, fmt=table_format)
    anomaly_telemetry_df = read_table(spark, artifacts.anomaly_telemetry_attribution, fmt=table_format)
    anomaly_event_df = read_table(spark, artifacts.anomaly_event_attribution, fmt=table_format)

    telemetry_df, parameter_catalog_df = build_explorer_telemetry_spark_table(raw_df, hierarchy_df)
    event_markers_df = build_explorer_event_markers_spark_table(events_df, anomaly_event_df)
    anomaly_markers_df = build_explorer_anomaly_markers_spark_table(anomaly_telemetry_df)
    anomaly_windows_df = build_explorer_anomaly_windows_spark_table(anomaly_window_df)
    phase_intervals_df = build_explorer_phase_intervals_spark_table(phase_windows_df)
    bundle_manifest = write_explorer_bundle(
        root_dir=artifacts.explorer_bundle,
        telemetry_df=telemetry_df,
        parameter_catalog_df=parameter_catalog_df,
        event_markers_df=event_markers_df,
        anomaly_markers_df=anomaly_markers_df,
        anomaly_windows_df=anomaly_windows_df,
        phase_intervals_df=phase_intervals_df,
        fmt=table_format,
        mode=write_mode,
        partition_by=context.config["output"]["partition_by"],
    )

    log_params_if_active(
        {
            "write_mode": write_mode,
            "table_format": table_format,
            "bundle_version": bundle_manifest["bundle_version"],
        }
    )
    log_dict_artifact_if_active(bundle_manifest, runtime.report_paths.summary_artifact_path)
    stage_manifest = build_stage_manifest(
        stage_name="95_emit_explorer_bundle",
        config={
            "table_format": table_format,
            "write_mode": write_mode,
            "bundle_version": bundle_manifest["bundle_version"],
        },
        input_artifacts={
            "raw_telemetry": build_artifact_manifest(path=artifacts.raw_table, dataframe=raw_df, row_count=int(raw_df.count())),
            "hierarchy_sensor_map": build_artifact_manifest(
                path=artifacts.hierarchy_sensor_map,
                dataframe=hierarchy_df,
                row_count=int(hierarchy_df.count()),
            ),
            "events": build_artifact_manifest(path=artifacts.events, dataframe=events_df, row_count=int(events_df.count())),
            "phase_windows": build_artifact_manifest(
                path=artifacts.phase_windows,
                dataframe=phase_windows_df,
                row_count=int(phase_windows_df.count()),
            ),
            "anomaly_window_attribution": build_artifact_manifest(
                path=artifacts.anomaly_window_attribution,
                dataframe=anomaly_window_df,
                row_count=int(anomaly_window_df.count()),
            ),
            "anomaly_telemetry_attribution": build_artifact_manifest(
                path=artifacts.anomaly_telemetry_attribution,
                dataframe=anomaly_telemetry_df,
                row_count=int(anomaly_telemetry_df.count()),
            ),
            "anomaly_event_attribution": build_artifact_manifest(
                path=artifacts.anomaly_event_attribution,
                dataframe=anomaly_event_df,
                row_count=int(anomaly_event_df.count()),
            ),
        },
        output_artifacts={
            "explorer_bundle": build_artifact_manifest(
                path=artifacts.explorer_bundle,
                dataframe=parameter_catalog_df,
                row_count=int(parameter_catalog_df.count()),
                artifact_version=bundle_manifest["bundle_version"],
                extra={
                    "bundle_manifest_path": str(explorer_bundle_manifest_path(artifacts.explorer_bundle)),
                    "table_counts": dict(bundle_manifest["counts"]),
                },
            ),
        },
        replayable_from=[
            "raw_telemetry",
            "hierarchy_sensor_map",
            "events",
            "phase_windows",
            "anomaly_window_attribution",
            "anomaly_telemetry_attribution",
            "anomaly_event_attribution",
        ],
    )
    log_stage_manifest_if_active(stage_manifest, runtime.report_paths.manifest_artifact_path)
    LOGGER.info(
        "pipeline=emit_explorer_bundle format=%s write_mode=%s explorer_bundle=%s",
        table_format,
        write_mode,
        Path(artifacts.explorer_bundle),
    )


if __name__ == "__main__":
    run()
