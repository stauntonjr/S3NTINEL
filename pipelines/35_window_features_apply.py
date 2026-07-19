"""Materialize window features using an existing continuous scaling profile."""

from libs.io.delta import get_spark, read_table
from libs.perf import (
    build_artifact_manifest,
    build_stage_manifest,
    get_logger,
    log_stage_manifest_if_active,
    log_wall_time,
)
from libs.windows import WindowFeaturesTable
from pipelines.common import build_stage_runtime, require_artifact_path


LOGGER = get_logger(__name__)


@log_wall_time(logger=LOGGER)
def run() -> None:
    runtime = build_stage_runtime("35_window_features_apply")
    raw_path = runtime.artifacts.raw_table
    events_path = runtime.artifacts.events
    windows_path = runtime.artifacts.windows
    scaling_profile_path = require_artifact_path(
        runtime.artifacts.continuous_scaling_profile,
        env_name="S3NTINEL_CONTINUOUS_SCALING_PROFILE_TABLE_PATH",
        artifact_name="continuous_scaling_profile",
    )
    window_features_path = str(runtime.artifacts.window_features).strip()
    if not window_features_path:
        raise RuntimeError("S3NTINEL_WINDOW_FEATURES_TABLE_PATH is required for reference inference")
    table_format = runtime.execution.table_format
    write_mode = runtime.execution.write_mode

    spark = get_spark("s3ntinel.window_features_apply")
    raw_df = read_table(spark, raw_path, fmt=table_format)
    events_df = read_table(spark, events_path, fmt=table_format)
    windows_df = read_table(spark, windows_path, fmt=table_format)
    scaling_profile_df = read_table(spark, str(scaling_profile_path), fmt=table_format)
    window_features = WindowFeaturesTable.from_raw_events_and_windows(
        raw_df,
        events_df,
        windows_df,
        scaling_profile_df=scaling_profile_df,
    ).bind(
        path=window_features_path,
        format=table_format,
        partition_by=tuple(runtime.context.config["output"]["partition_by"]),
    )
    window_features.write(mode=write_mode)
    window_features_count = int(window_features.to_dataframe().count())

    manifest = build_stage_manifest(
        stage_name="35_window_features_apply",
        config={"table_format": table_format, "write_mode": write_mode},
        input_artifacts={
            "raw_telemetry": build_artifact_manifest(path=raw_path, dataframe=raw_df),
            "events": build_artifact_manifest(path=events_path, dataframe=events_df),
            "windows": build_artifact_manifest(path=windows_path, dataframe=windows_df),
            "continuous_scaling_profile": build_artifact_manifest(
                path=str(scaling_profile_path),
                dataframe=scaling_profile_df,
            ),
        },
        output_artifacts={
            "window_features": build_artifact_manifest(
                path=window_features_path,
                dataframe=window_features.to_dataframe(),
                row_count=window_features_count,
            )
        },
        replayable_from=["raw_telemetry", "events", "windows", "continuous_scaling_profile"],
    )
    log_stage_manifest_if_active(manifest, runtime.report_paths.manifest_artifact_path)
    LOGGER.info(
        "pipeline=window_features_apply format=%s write_mode=%s window_features=%s",
        table_format,
        write_mode,
        window_features_path,
    )


if __name__ == "__main__":
    run()
