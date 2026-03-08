# File: pipelines/20_events_extract.py
"""Extract event stream from mixed-rate sensor channels."""

import os

from libs.events import build_events_table
from libs.io.delta import get_spark, read_table, write_table
from libs.perf import get_logger, log_dict_artifact_if_active, log_params_if_active, log_wall_time, track_mlflow_run
from pipelines.common import build_context


LOGGER = get_logger(__name__)


@track_mlflow_run(stage_name="20_events_extract", logger=LOGGER)
@log_wall_time(logger=LOGGER)
def run() -> None:
    context = build_context()
    input_path = os.getenv("S3NTINEL_RAW_TABLE_PATH", "data/delta/raw_telemetry")
    output_path = os.getenv("S3NTINEL_EVENTS_TABLE_PATH", "data/delta/events")
    table_format = os.getenv("S3NTINEL_TABLE_FORMAT", "delta")
    write_mode = os.getenv("S3NTINEL_WRITE_MODE", "append")
    delta_threshold = float(os.getenv("S3NTINEL_EVENT_DELTA_THRESHOLD", "0.0"))
    slope_source = str(os.getenv("S3NTINEL_EVENT_SLOPE_SOURCE", "ema"))
    ema_alpha = float(os.getenv("S3NTINEL_EVENT_EMA_ALPHA", "0.2"))

    spark = get_spark("s3ntinel.events_extract")
    raw_df = read_table(spark, input_path, fmt=table_format)

    events_df = build_events_table(
        raw_df,
        delta_threshold=delta_threshold,
        slope_source=slope_source,
        ema_alpha=ema_alpha,
    )

    write_table(
        events_df,
        path=output_path,
        mode=write_mode,
        fmt=table_format,
        partition_by=context.config["output"]["partition_by"],
    )

    log_params_if_active(
        {
            "event_threshold": context.config["windowing"]["event_threshold"],
        }
    )
    log_dict_artifact_if_active(
        {
            "stage": "20_events_extract",
            "input_path": input_path,
            "output_path": output_path,
            "table_format": table_format,
            "write_mode": write_mode,
            "delta_threshold": delta_threshold,
            "slope_source": slope_source,
            "ema_alpha": ema_alpha,
            "event_threshold": int(context.config["windowing"]["event_threshold"]),
            "partition_by": list(context.config["output"]["partition_by"]),
        },
        "reports/stages/20_events_extract_summary.json",
    )
    LOGGER.info(
        "pipeline=events_extract format=%s write_mode=%s event_threshold=%s delta_threshold=%s slope_source=%s ema_alpha=%s input=%s output=%s",
        table_format,
        write_mode,
        context.config["windowing"]["event_threshold"],
        delta_threshold,
        slope_source,
        ema_alpha,
        input_path,
        output_path,
    )


if __name__ == "__main__":
    run()
