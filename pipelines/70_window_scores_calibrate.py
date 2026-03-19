# File: pipelines/70_window_scores_calibrate.py
"""Calibrate raw window scores with phase-conditioned conformal calibration."""

import os

from libs.conformal import build_calibrated_window_scores_table
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
from pipelines.common import build_context


LOGGER = get_logger(__name__)


@track_mlflow_run(stage_name="70_window_scores_calibrate", logger=LOGGER)
@log_memory_usage(logger=LOGGER, label="70_window_scores_calibrate")
@log_wall_time(logger=LOGGER)
def run() -> None:
    context = build_context()
    scores_path = os.getenv("S3NTINEL_WINDOW_SCORES_RAW_TABLE_PATH", "data/delta/window_scores_raw")
    window_scores_calibrated_path = os.getenv("S3NTINEL_WINDOW_SCORES_CALIBRATED_TABLE_PATH", "data/delta/window_scores_calibrated")
    table_format = os.getenv("S3NTINEL_TABLE_FORMAT", "delta")
    write_mode = os.getenv("S3NTINEL_WRITE_MODE", "append")

    min_warm = int(os.getenv("S3NTINEL_MIN_WARM", str(context.config["conformal"]["min_warm"])))

    spark = get_spark("s3ntinel.window_scores_calibrate")
    scores_df = read_table(spark, scores_path, fmt=table_format)
    calibrated_df = build_calibrated_window_scores_table(scores_df=scores_df, min_warm=min_warm)

    write_table(
        calibrated_df,
        path=window_scores_calibrated_path,
        mode=write_mode,
        fmt=table_format,
        partition_by=context.config["output"]["partition_by"],
    )
    scores_count = int(scores_df.count())
    calibrated_count = int(calibrated_df.count())

    log_params_if_active({"min_warm": min_warm})
    log_dict_artifact_if_active(
        {
            "stage": "70_window_scores_calibrate",
            "window_scores_raw_path": scores_path,
            "window_scores_calibrated_path": window_scores_calibrated_path,
            "table_format": table_format,
            "write_mode": write_mode,
            "min_warm": min_warm,
            "partition_by": list(context.config["output"]["partition_by"]),
        },
        "reports/stages/70_window_scores_calibrate_summary.json",
    )
    stage_manifest = build_stage_manifest(
        stage_name="70_window_scores_calibrate",
        config={
            "table_format": table_format,
            "write_mode": write_mode,
            "min_warm": min_warm,
        },
        input_artifacts={
            "window_scores_raw": build_artifact_manifest(path=scores_path, dataframe=scores_df, row_count=scores_count),
        },
        output_artifacts={
            "window_scores_calibrated": build_artifact_manifest(
                path=window_scores_calibrated_path,
                dataframe=calibrated_df,
                row_count=calibrated_count,
            ),
        },
        replayable_from=["window_scores_raw"],
    )
    log_stage_manifest_if_active(stage_manifest, "reports/stages/70_window_scores_calibrate_manifest.json")
    LOGGER.info(
        "pipeline=window_scores_calibrate format=%s write_mode=%s min_warm=%s window_scores_raw=%s window_scores_calibrated=%s",
        table_format,
        write_mode,
        min_warm,
        scores_path,
        window_scores_calibrated_path,
    )


if __name__ == "__main__":
    run()
