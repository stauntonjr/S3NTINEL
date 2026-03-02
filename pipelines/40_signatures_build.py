# File: pipelines/40_signatures_build.py
"""Build structural signatures for each adaptive window."""

import os

from libs.io.delta import get_spark, read_table, write_table
from libs.signature.blocks import build_signatures_df
from libs.perf import get_logger, log_params_if_active, log_wall_time, track_mlflow_run
from pipelines.common import build_context


LOGGER = get_logger(__name__)


@track_mlflow_run(stage_name="40_signatures_build", logger=LOGGER)
@log_wall_time(logger=LOGGER)
def run() -> None:
    context = build_context()
    raw_path = os.getenv("S3NTINEL_RAW_TABLE_PATH", "data/delta/raw_telemetry")
    events_path = os.getenv("S3NTINEL_EVENTS_TABLE_PATH", "data/delta/events")
    windows_path = os.getenv("S3NTINEL_WINDOWS_TABLE_PATH", "data/delta/windows")
    output_path = os.getenv("S3NTINEL_SIGNATURES_TABLE_PATH", "data/delta/signatures")
    cur_sensor_sample_path = os.getenv("S3NTINEL_CUR_SENSOR_SAMPLE_TABLE_PATH", "data/delta/cur_sensor_sample")
    table_format = os.getenv("S3NTINEL_TABLE_FORMAT", "delta")
    write_mode = os.getenv("S3NTINEL_WRITE_MODE", "append")
    sig_version = int(os.getenv("S3NTINEL_SIGNATURE_VERSION", "1"))
    event_threshold = int(context.config["windowing"]["event_threshold"])

    spark = get_spark("s3ntinel.signatures_build")
    raw_df = read_table(spark, raw_path, fmt=table_format)
    events_df = read_table(spark, events_path, fmt=table_format)
    windows_df = read_table(spark, windows_path, fmt=table_format)
    sampled_sensors_df = None
    try:
        sampled_sensors_df = read_table(spark, cur_sensor_sample_path, fmt=table_format).select("sensor")
    except Exception:
        sampled_sensors_df = None

    signatures_df = build_signatures_df(
        raw_df=raw_df,
        events_df=events_df,
        windows_df=windows_df,
        sampled_sensors_df=sampled_sensors_df,
        sig_version=sig_version,
        event_threshold=event_threshold,
    )

    write_table(
        signatures_df,
        path=output_path,
        mode=write_mode,
        fmt=table_format,
        partition_by=context.config["output"]["partition_by"],
    )

    log_params_if_active(
        {
            "signature_version": sig_version,
            "event_threshold": event_threshold,
            "signature_cur_sensor_sample_path": cur_sensor_sample_path,
            "signature_cur_sensor_sample_present": int(sampled_sensors_df is not None),
        }
    )
    LOGGER.info(
        "pipeline=signatures_build sig_version=%s raw=%s events=%s windows=%s cur_sensor_sample_present=%s output=%s",
        sig_version,
        raw_path,
        events_path,
        windows_path,
        sampled_sensors_df is not None,
        output_path,
    )


if __name__ == "__main__":
    run()
