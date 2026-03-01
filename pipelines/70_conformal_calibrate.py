# File: pipelines/70_conformal_calibrate.py
"""Apply phase-conditioned conformal calibration with warm-up buffers."""

import os

from libs.conformal.build import build_calibrated_scores_df
from libs.io.delta import get_spark, read_table, write_table
from libs.perf import get_logger, log_params_if_active, log_wall_time, track_mlflow_run
from pipelines.common import build_context


LOGGER = get_logger(__name__)


@track_mlflow_run(stage_name="70_conformal_calibrate", logger=LOGGER)
@log_wall_time(logger=LOGGER)
def run() -> None:
    context = build_context()
    scores_path = os.getenv("S3NTINEL_SCORES_TABLE_PATH", "data/delta/scores")
    calibrated_path = os.getenv("S3NTINEL_CALIBRATED_TABLE_PATH", "data/delta/calibrated")
    table_format = os.getenv("S3NTINEL_TABLE_FORMAT", "delta")
    write_mode = os.getenv("S3NTINEL_WRITE_MODE", "append")

    min_warm = int(os.getenv("S3NTINEL_MIN_WARM", str(context.config["conformal"]["min_warm"])))

    spark = get_spark("s3ntinel.conformal_calibrate")
    scores_df = read_table(spark, scores_path, fmt=table_format)
    calibrated_df = build_calibrated_scores_df(scores_df=scores_df, min_warm=min_warm)

    write_table(
        calibrated_df,
        path=calibrated_path,
        mode=write_mode,
        fmt=table_format,
        partition_by=context.config["output"]["partition_by"],
    )

    log_params_if_active({"min_warm": min_warm})
    LOGGER.info(
        "pipeline=conformal_calibrate min_warm=%s scores=%s calibrated=%s",
        min_warm,
        scores_path,
        calibrated_path,
    )


if __name__ == "__main__":
    run()
