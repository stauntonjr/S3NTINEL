# File: pipelines/80_emit_anomalies.py
"""Emit deterministic anomaly objects to Delta sink."""

import os

from libs.anomaly.build import build_anomalies_df
from libs.io.delta import get_spark, read_table, write_table
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
    anomalies_path = os.getenv("S3NTINEL_ANOMALIES_TABLE_PATH", "data/delta/anomalies")
    table_format = os.getenv("S3NTINEL_TABLE_FORMAT", "delta")
    write_mode = os.getenv("S3NTINEL_WRITE_MODE", "append")

    spark = get_spark("s3ntinel.emit_anomalies")
    calibrated_df = read_table(spark, calibrated_path, fmt=table_format)
    phase_windows_df = read_table(spark, phase_windows_path, fmt=table_format)
    signatures_df = read_table(spark, signatures_path, fmt=table_format)
    windows_df = read_table(spark, windows_path, fmt=table_format)

    anomalies_df = build_anomalies_df(
        calibrated_df=calibrated_df,
        phase_windows_df=phase_windows_df,
        signatures_df=signatures_df,
        windows_df=windows_df,
    )
    write_table(
        anomalies_df,
        path=anomalies_path,
        mode=write_mode,
        fmt=table_format,
        partition_by=context.config["output"]["partition_by"],
    )

    log_params_if_active({"merge_key": context.config["output"]["anomalies_merge_key"]})
    LOGGER.info(
        "pipeline=emit_anomalies merge_key=%s calibrated=%s anomalies=%s",
        context.config["output"]["anomalies_merge_key"],
        calibrated_path,
        anomalies_path,
    )


if __name__ == "__main__":
    run()
