# File: pipelines/50_phase_detect.py
"""Detect phase transitions and maintain per-tail centroids."""

import os

from libs.io.delta import get_spark, read_table, write_table
from libs.phase.drift import build_phase_centroids, build_phase_windows
from libs.perf import get_logger, log_params_if_active, log_wall_time, track_mlflow_run
from pipelines.common import build_context


LOGGER = get_logger(__name__)


@track_mlflow_run(stage_name="50_phase_detect", logger=LOGGER)
@log_wall_time(logger=LOGGER)
def run() -> None:
    context = build_context()
    signatures_path = os.getenv("S3NTINEL_SIGNATURES_TABLE_PATH", "data/delta/signatures")
    phases_path = os.getenv("S3NTINEL_PHASES_TABLE_PATH", "data/delta/phases")
    phase_windows_path = os.getenv("S3NTINEL_PHASE_WINDOWS_TABLE_PATH", "data/delta/phase_windows")
    table_format = os.getenv("S3NTINEL_TABLE_FORMAT", "delta")
    write_mode = os.getenv("S3NTINEL_WRITE_MODE", "append")

    tau_near_q = float(context.config["phase"]["tau_near_quantile"])
    tau_far_q = float(context.config["phase"]["tau_far_quantile"])
    persistence_q = float(context.config["phase"]["persistence_quantile"])

    spark = get_spark("s3ntinel.phase_detect")
    signatures_df = read_table(spark, signatures_path, fmt=table_format)
    phase_windows_df = build_phase_windows(
        signatures_df=signatures_df,
        tau_near_q=tau_near_q,
        tau_far_q=tau_far_q,
        persistence_q=persistence_q,
    )
    phases_df = build_phase_centroids(phase_windows_df, version=1)

    write_table(
        phase_windows_df,
        path=phase_windows_path,
        mode=write_mode,
        fmt=table_format,
        partition_by=context.config["output"]["partition_by"],
    )
    write_table(
        phases_df,
        path=phases_path,
        mode=write_mode,
        fmt=table_format,
        partition_by=["tail_id"],
    )

    log_params_if_active(
        {
            "tau_near_q": tau_near_q,
            "tau_far_q": tau_far_q,
            "persistence_q": persistence_q,
        }
    )
    LOGGER.info(
        "pipeline=phase_detect signatures=%s phase_windows=%s phases=%s",
        signatures_path,
        phase_windows_path,
        phases_path,
    )


if __name__ == "__main__":
    run()
