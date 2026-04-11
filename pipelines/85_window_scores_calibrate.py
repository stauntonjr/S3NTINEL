# File: pipelines/85_window_scores_calibrate.py
"""Calibrate raw window scores with phase-conditioned conformal calibration."""

from libs.io.delta import get_spark
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
from libs.scoring import WindowScoresCalibratedTable, WindowScoresRawTable
from pipelines.common import build_stage_runtime


LOGGER = get_logger(__name__)
_SCORE_ARTIFACT_PARTITION_BY: tuple[str, ...] = ()


@track_mlflow_run(stage_name="85_window_scores_calibrate", logger=LOGGER)
@log_memory_usage(logger=LOGGER, label="85_window_scores_calibrate")
@log_wall_time(logger=LOGGER)
def run() -> None:
    runtime = build_stage_runtime("85_window_scores_calibrate")
    context = runtime.context
    scores_path = runtime.artifacts.window_scores_raw
    window_scores_calibrated_path = runtime.artifacts.window_scores_calibrated
    table_format = runtime.execution.table_format
    write_mode = runtime.execution.write_mode

    min_warm = runtime.settings.scoring.min_warm

    spark = get_spark("s3ntinel.window_scores_calibrate")
    scores = WindowScoresRawTable.read(spark, scores_path, format=table_format)
    calibrated = WindowScoresCalibratedTable.from_scores(scores.to_dataframe(), min_warm=min_warm).bind(
        path=window_scores_calibrated_path,
        format=table_format,
        partition_by=_SCORE_ARTIFACT_PARTITION_BY,
    )
    calibrated.write(mode=write_mode)
    scores_df = scores.to_dataframe()
    scores_count = int(scores_df.count())
    calibrated = WindowScoresCalibratedTable.read(
        spark,
        window_scores_calibrated_path,
        format=table_format,
        partition_by=_SCORE_ARTIFACT_PARTITION_BY,
    )
    calibrated_df = calibrated.to_dataframe()
    calibrated_count = int(calibrated_df.count())

    log_params_if_active({"min_warm": min_warm})
    log_dict_artifact_if_active(
        {
            "stage": "85_window_scores_calibrate",
            "window_scores_raw_path": scores_path,
            "window_scores_calibrated_path": window_scores_calibrated_path,
            "table_format": table_format,
            "write_mode": write_mode,
            "min_warm": min_warm,
            "partition_by": list(_SCORE_ARTIFACT_PARTITION_BY),
        },
        runtime.report_paths.summary_artifact_path,
    )
    stage_manifest = build_stage_manifest(
        stage_name="85_window_scores_calibrate",
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
    log_stage_manifest_if_active(stage_manifest, runtime.report_paths.manifest_artifact_path)
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
