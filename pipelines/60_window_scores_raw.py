# File: pipelines/60_window_scores_raw.py
"""Build raw window scores from phase windows and phase baselines."""

import os

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
from libs.scoring import build_window_scores_raw_spark_table
from pipelines.common import build_context


LOGGER = get_logger(__name__)


def _bounded_count(df: "DataFrame", *, limit: int) -> int:
    return int(df.limit(max(int(limit), 0) + 1).count())


@track_mlflow_run(stage_name="60_window_scores_raw", logger=LOGGER)
@log_memory_usage(logger=LOGGER, label="60_window_scores_raw")
@log_wall_time(logger=LOGGER)
def run() -> None:
    context = build_context()
    phase_baselines_path = os.getenv("S3NTINEL_PHASE_BASELINES_TABLE_PATH", "data/delta/phase_baselines")
    phase_windows_path = os.getenv("S3NTINEL_PHASE_WINDOWS_TABLE_PATH", "data/delta/phase_windows")
    hierarchy_sensor_map_path = os.getenv("S3NTINEL_HIERARCHY_SENSOR_MAP_TABLE_PATH", "data/delta/hierarchy_sensor_map")
    scores_path = os.getenv("S3NTINEL_WINDOW_SCORES_RAW_TABLE_PATH", "data/delta/window_scores_raw")
    table_format = os.getenv("S3NTINEL_TABLE_FORMAT", "delta")
    write_mode = os.getenv("S3NTINEL_WRITE_MODE", "append")
    max_reference_rows = int(os.getenv("S3NTINEL_MAX_BRIDGE_REFERENCE_ROWS", "10000"))

    spark = get_spark("s3ntinel.window_scores_raw")
    phase_windows_df = read_table(spark, phase_windows_path, fmt=table_format)
    phase_baselines_df = read_table(spark, phase_baselines_path, fmt=table_format)
    hierarchy_sensor_map_df = read_table(spark, hierarchy_sensor_map_path, fmt=table_format)
    phase_baselines_count = _bounded_count(phase_baselines_df, limit=max_reference_rows)
    hierarchy_sensor_map_count = _bounded_count(hierarchy_sensor_map_df, limit=max_reference_rows)
    if phase_baselines_count > max_reference_rows or hierarchy_sensor_map_count > max_reference_rows:
        raise RuntimeError(
            "60_window_scores_raw only allows bounded reference-table collection; "
            f"phase baselines / hierarchy map exceed S3NTINEL_MAX_BRIDGE_REFERENCE_ROWS={max_reference_rows}. "
            "Replace the bridge with a fully distributed implementation."
        )
    scores_df = build_window_scores_raw_spark_table(
        phase_windows_df,
        phase_baselines_df,
        hierarchy_sensor_map_df,
    )
    write_table(
        scores_df,
        path=scores_path,
        mode=write_mode,
        fmt=table_format,
        partition_by=context.config["output"]["partition_by"],
    )
    phase_windows_count = int(phase_windows_df.count())
    scores_count = int(scores_df.count())

    log_params_if_active(
        {
            "pvalue_combine": context.config["scoring"]["combine_pvalues"],
        }
    )
    log_dict_artifact_if_active(
        {
            "stage": "60_window_scores_raw",
            "phase_baselines_path": phase_baselines_path,
            "phase_windows_path": phase_windows_path,
            "hierarchy_sensor_map_path": hierarchy_sensor_map_path,
            "window_scores_raw_path": scores_path,
            "table_format": table_format,
            "write_mode": write_mode,
            "max_bridge_reference_rows": max_reference_rows,
            "phase_baselines_count_bounded": phase_baselines_count,
            "hierarchy_sensor_map_count_bounded": hierarchy_sensor_map_count,
            "partition_by": list(context.config["output"]["partition_by"]),
        },
        "reports/stages/60_window_scores_raw_summary.json",
    )
    stage_manifest = build_stage_manifest(
        stage_name="60_window_scores_raw",
        config={
            "table_format": table_format,
            "write_mode": write_mode,
            "pvalue_combine": context.config["scoring"]["combine_pvalues"],
            "max_bridge_reference_rows": max_reference_rows,
        },
        input_artifacts={
            "phase_windows": build_artifact_manifest(path=phase_windows_path, dataframe=phase_windows_df, row_count=phase_windows_count),
            "phase_baselines": build_artifact_manifest(path=phase_baselines_path, dataframe=phase_baselines_df, row_count=phase_baselines_count),
            "hierarchy_sensor_map": build_artifact_manifest(
                path=hierarchy_sensor_map_path,
                dataframe=hierarchy_sensor_map_df,
                row_count=hierarchy_sensor_map_count,
            ),
        },
        output_artifacts={
            "window_scores_raw": build_artifact_manifest(path=scores_path, dataframe=scores_df, row_count=scores_count),
        },
        replayable_from=["phase_windows", "phase_baselines", "hierarchy_sensor_map"],
    )
    log_stage_manifest_if_active(stage_manifest, "reports/stages/60_window_scores_raw_manifest.json")
    LOGGER.info(
        "pipeline=window_scores_raw format=%s write_mode=%s phase_windows=%s window_scores_raw=%s",
        table_format,
        write_mode,
        phase_windows_path,
        scores_path,
    )


if __name__ == "__main__":
    run()
