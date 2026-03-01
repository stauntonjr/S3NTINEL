# File: pipelines/30_windows_adaptive.py
"""Build adaptive windows from event thresholds and max duration."""

import os

from libs.io.delta import get_spark, read_table, write_table
from libs.perf import get_logger, log_params_if_active, log_wall_time, track_mlflow_run
from libs.windows.adaptive import build_adaptive_windows, build_adaptive_windows_stream_parity
from pipelines.common import build_context


LOGGER = get_logger(__name__)


@track_mlflow_run(stage_name="30_windows_adaptive", logger=LOGGER)
@log_wall_time(logger=LOGGER)
def run() -> None:
    context = build_context()
    input_path = os.getenv("S3NTINEL_EVENTS_TABLE_PATH", "data/delta/events")
    output_path = os.getenv("S3NTINEL_WINDOWS_TABLE_PATH", "data/delta/windows")
    table_format = os.getenv("S3NTINEL_TABLE_FORMAT", "delta")
    write_mode = os.getenv("S3NTINEL_WRITE_MODE", "append")

    max_ms = int(context.config["windowing"]["max_ms"])
    min_ms = int(context.config["windowing"]["min_ms"])
    event_threshold = int(context.config["windowing"]["event_threshold"])
    default_inactivity_timeout_ms = int(context.config.get("windowing", {}).get("inactivity_timeout_ms", 0))
    inactivity_timeout_ms = int(os.getenv("S3NTINEL_WINDOW_INACTIVITY_TIMEOUT_MS", str(default_inactivity_timeout_ms)))
    default_strategy = str(context.config.get("windowing", {}).get("strategy", "bucketed"))
    window_strategy = str(os.getenv("S3NTINEL_WINDOW_STRATEGY", default_strategy)).strip().lower()

    spark = get_spark("s3ntinel.windows_adaptive")
    events_df = read_table(spark, input_path, fmt=table_format)
    if window_strategy == "stream_parity":
        windows_df = build_adaptive_windows_stream_parity(
            events_df,
            max_ms=max_ms,
            event_threshold=event_threshold,
            min_ms=min_ms,
            inactivity_timeout_ms=inactivity_timeout_ms,
        )
    else:
        windows_df = build_adaptive_windows(
            events_df,
            max_ms=max_ms,
            event_threshold=event_threshold,
            min_ms=min_ms,
        )
    write_table(
        windows_df,
        path=output_path,
        mode=write_mode,
        fmt=table_format,
        partition_by=context.config["output"]["partition_by"],
    )

    log_params_if_active(
        {
            "max_ms": max_ms,
            "min_ms": min_ms,
            "window_strategy": window_strategy,
            "window_inactivity_timeout_ms": inactivity_timeout_ms,
        }
    )
    LOGGER.info(
        "pipeline=windows_adaptive strategy=%s max_ms=%s event_threshold=%s inactivity_timeout_ms=%s input=%s output=%s",
        window_strategy,
        max_ms,
        event_threshold,
        inactivity_timeout_ms,
        input_path,
        output_path,
    )


if __name__ == "__main__":
    run()
