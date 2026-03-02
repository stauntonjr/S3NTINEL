# File: pipelines/60_anomaly_score.py
"""Score anomalies by block and subsystem and compute global score."""

import os

from libs.io.delta import get_spark, read_table, write_table
from libs.perf import get_logger, log_params_if_active, log_wall_time, track_mlflow_run
from libs.scoring.build import build_scores_df, build_window_subsystem_evidence_df
from pipelines.common import build_context


LOGGER = get_logger(__name__)


@track_mlflow_run(stage_name="60_anomaly_score", logger=LOGGER)
@log_wall_time(logger=LOGGER)
def run() -> None:
    context = build_context()
    signatures_path = os.getenv("S3NTINEL_SIGNATURES_TABLE_PATH", "data/delta/signatures")
    phase_windows_path = os.getenv("S3NTINEL_PHASE_WINDOWS_TABLE_PATH", "data/delta/phase_windows")
    events_path = os.getenv("S3NTINEL_EVENTS_TABLE_PATH", "data/delta/events")
    windows_path = os.getenv("S3NTINEL_WINDOWS_TABLE_PATH", "data/delta/windows")
    subsystem_map_path = os.getenv("S3NTINEL_SUBSYSTEM_MAP_TABLE_PATH", "data/delta/sensor_subsystem_map")
    scores_path = os.getenv("S3NTINEL_SCORES_TABLE_PATH", "data/delta/scores")
    table_format = os.getenv("S3NTINEL_TABLE_FORMAT", "delta")
    write_mode = os.getenv("S3NTINEL_WRITE_MODE", "append")

    spark = get_spark("s3ntinel.anomaly_score")
    signatures_df = read_table(spark, signatures_path, fmt=table_format)
    phase_windows_df = read_table(spark, phase_windows_path, fmt=table_format)
    subsystem_evidence_df = None
    try:
        events_df = read_table(spark, events_path, fmt=table_format)
        windows_df = read_table(spark, windows_path, fmt=table_format)
        subsystem_map_df = read_table(spark, subsystem_map_path, fmt=table_format)
        subsystem_evidence_df = build_window_subsystem_evidence_df(
            events_df=events_df,
            windows_df=windows_df,
            subsystem_map_df=subsystem_map_df,
        )
    except Exception:
        subsystem_evidence_df = None

    scores_df = build_scores_df(
        signatures_df=signatures_df,
        phase_windows_df=phase_windows_df,
        subsystem_evidence_df=subsystem_evidence_df,
    )
    write_table(
        scores_df,
        path=scores_path,
        mode=write_mode,
        fmt=table_format,
        partition_by=context.config["output"]["partition_by"],
    )

    log_params_if_active(
        {
            "pvalue_combine": context.config["scoring"]["combine_pvalues"],
            "subsystem_map_path": subsystem_map_path,
            "dominant_subsystem_enabled": int(subsystem_evidence_df is not None),
        }
    )
    LOGGER.info(
        "pipeline=anomaly_score signatures=%s phase_windows=%s dominant_subsystem_enabled=%s scores=%s",
        signatures_path,
        phase_windows_path,
        subsystem_evidence_df is not None,
        scores_path,
    )


if __name__ == "__main__":
    run()
