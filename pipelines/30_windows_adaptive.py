# File: pipelines/30_windows_adaptive.py
"""Build adaptive windows from event thresholds and max duration."""

from libs.io.delta import get_spark, read_table, write_table
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
from libs.windows import WindowPolicy, build_windows_table
from libs.windows.window import DEFAULT_MIN_SAMPLING_RATE_HZ
from pipelines.common import build_context, context_artifacts, context_execution, context_settings


LOGGER = get_logger(__name__)


@track_mlflow_run(stage_name="30_windows_adaptive", logger=LOGGER)
@log_memory_usage(logger=LOGGER, label="30_windows_adaptive")
@log_wall_time(logger=LOGGER)
def run() -> None:
    context = build_context()
    artifacts = context_artifacts(context)
    execution = context_execution(context)
    settings = context_settings(context)
    input_path = artifacts.events
    output_path = artifacts.windows
    table_format = execution.table_format
    write_mode = execution.write_mode

    min_sampling_rate_hz = float(settings.windowing.min_sampling_rate_hz or DEFAULT_MIN_SAMPLING_RATE_HZ)
    derived_max_ms = WindowPolicy.max_ms_from_min_sampling_rate(min_sampling_rate_hz)
    configured_max_ms = int(settings.windowing.max_ms)
    max_ms = int(configured_max_ms if configured_max_ms > 0 else derived_max_ms)
    min_ms = settings.windowing.min_ms
    event_threshold = settings.windowing.event_threshold
    inactivity_timeout_ms = settings.windowing.inactivity_timeout_ms
    window_strategy = settings.windowing.strategy

    spark = get_spark("s3ntinel.windows_adaptive")
    events_df = read_table(spark, input_path, fmt=table_format)
    windows_df = build_windows_table(
        events_df,
        max_ms=max_ms,
        event_threshold=event_threshold,
        min_ms=min_ms,
        inactivity_timeout_ms=inactivity_timeout_ms,
        strategy=window_strategy,
    )
    write_table(
        windows_df,
        path=output_path,
        mode=write_mode,
        fmt=table_format,
        partition_by=context.config["output"]["partition_by"],
    )
    events_count = int(events_df.count())
    windows_count = int(windows_df.count())

    log_params_if_active(
        {
            "max_ms": max_ms,
            "min_sampling_rate_hz": min_sampling_rate_hz,
            "min_ms": min_ms,
            "window_strategy": window_strategy,
            "window_inactivity_timeout_ms": inactivity_timeout_ms,
        }
    )
    log_dict_artifact_if_active(
        {
            "stage": "30_windows_adaptive",
            "input_path": input_path,
            "output_path": output_path,
            "table_format": table_format,
            "write_mode": write_mode,
            "window_strategy": window_strategy,
            "max_ms": max_ms,
            "min_sampling_rate_hz": min_sampling_rate_hz,
            "min_ms": min_ms,
            "event_threshold": event_threshold,
            "inactivity_timeout_ms": inactivity_timeout_ms,
            "partition_by": list(context.config["output"]["partition_by"]),
        },
        "reports/stages/30_windows_adaptive_summary.json",
    )
    stage_manifest = build_stage_manifest(
        stage_name="30_windows_adaptive",
        config={
            "table_format": table_format,
            "write_mode": write_mode,
            "window_strategy": window_strategy,
            "max_ms": max_ms,
            "min_sampling_rate_hz": min_sampling_rate_hz,
            "min_ms": min_ms,
            "event_threshold": event_threshold,
            "inactivity_timeout_ms": inactivity_timeout_ms,
        },
        input_artifacts={
            "events": build_artifact_manifest(path=input_path, dataframe=events_df, row_count=events_count),
        },
        output_artifacts={
            "windows": build_artifact_manifest(path=output_path, dataframe=windows_df, row_count=windows_count),
        },
        replayable_from=["events"],
    )
    log_stage_manifest_if_active(stage_manifest, "reports/stages/30_windows_adaptive_manifest.json")
    LOGGER.info(
        "pipeline=windows_adaptive format=%s write_mode=%s strategy=%s max_ms=%s event_threshold=%s inactivity_timeout_ms=%s input=%s output=%s",
        table_format,
        write_mode,
        window_strategy,
        max_ms,
        event_threshold,
        inactivity_timeout_ms,
        input_path,
        output_path,
    )


if __name__ == "__main__":
    run()
