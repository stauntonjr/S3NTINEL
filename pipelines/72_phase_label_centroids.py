# File: pipelines/72_phase_label_centroids.py
"""Build validation-only centroids from truth-labeled phase windows."""

from libs.io.delta import get_spark, read_table
from libs.perf import (
    build_artifact_manifest,
    build_stage_manifest,
    get_logger,
    log_dict_artifact_if_active,
    log_stage_manifest_if_active,
    log_wall_time,
    track_mlflow_run,
)
from libs.phase import PhaseLabelCentroidsTable, build_phase_centroid_comparison_summary_from_tables
from pipelines.common import build_stage_runtime, require_artifact_path


LOGGER = get_logger(__name__)


@track_mlflow_run(stage_name="72_phase_label_centroids", logger=LOGGER)
@log_wall_time(logger=LOGGER)
def run() -> None:
    from pyspark import StorageLevel

    runtime = build_stage_runtime("72_phase_label_centroids")
    phase_windows_path = runtime.artifacts.phase_windows
    phase_baselines_path = runtime.artifacts.phase_baselines
    phase_labels_path = require_artifact_path(
        runtime.artifacts.phase_labels,
        env_name="S3NTINEL_PHASE_LABELS_TABLE_PATH",
        artifact_name="phase_labels",
    )
    phase_label_centroids_path = runtime.artifacts.phase_label_centroids
    table_format = runtime.execution.table_format
    write_mode = runtime.execution.write_mode
    comparison_report_path = "reports/stages/72_phase_label_centroids_comparison.json"

    spark = get_spark("s3ntinel.phase_label_centroids")
    phase_windows_df = read_table(spark, phase_windows_path, fmt=table_format)
    phase_baselines_df = read_table(spark, phase_baselines_path, fmt=table_format)
    phase_labels_df = read_table(spark, str(phase_labels_path), fmt=table_format)
    phase_label_centroids = PhaseLabelCentroidsTable.from_phase_windows_and_labels(
        phase_windows_df,
        phase_labels_df,
    )
    phase_label_centroids = phase_label_centroids.with_dataframe(
        phase_label_centroids.to_dataframe().persist(StorageLevel.MEMORY_AND_DISK)
    ).bind(
        path=phase_label_centroids_path,
        format=table_format,
        partition_by=("tail_id",),
    )
    try:
        phase_label_centroids.write(mode=write_mode)
        phase_label_centroids_count = int(phase_label_centroids.to_dataframe().count())
    finally:
        phase_label_centroids.to_dataframe().unpersist()

    comparison_summary = build_phase_centroid_comparison_summary_from_tables(
        phase_windows_df=phase_windows_df.select(
            "tail_id",
            "flight_id",
            "win_id",
            "t_start",
            "t_end",
            "phase_id_detected",
            "phase_state_detected",
            "phase_confidence_detected",
            "distance_to_centroid_detected",
            "drift_magnitude",
            "s_w",
        ).toPandas(),
        phase_labels_df=phase_labels_df.select("tail_id", "flight_id", "timestamp_utc", "phase_label").toPandas(),
        phase_baselines_df=phase_baselines_df.select(
            "tail_id",
            "phase_id_detected",
            "phase_name_detected",
            "stable_window_count",
            "feature_names",
            "s_w_centroid",
        ).toPandas(),
    )

    log_dict_artifact_if_active(
        {
            "stage": "72_phase_label_centroids",
            "phase_windows_path": phase_windows_path,
            "phase_baselines_path": phase_baselines_path,
            "phase_labels_path": str(phase_labels_path),
            "phase_label_centroids_path": phase_label_centroids_path,
            "phase_label_centroids_comparison_path": comparison_report_path,
            "table_format": table_format,
            "write_mode": write_mode,
            "centroid_vector_column": "s_w",
            "label_assignment_contract": "majority_overlap_label",
            "comparison_status": comparison_summary.get("status"),
        },
        runtime.report_paths.summary_artifact_path,
    )
    log_dict_artifact_if_active(
        comparison_summary,
        comparison_report_path,
    )
    stage_manifest = build_stage_manifest(
        stage_name="72_phase_label_centroids",
        config={
            "table_format": table_format,
            "write_mode": write_mode,
            "centroid_vector_column": "s_w",
            "label_assignment_contract": "majority_overlap_label",
        },
        input_artifacts={
            "phase_windows": build_artifact_manifest(path=phase_windows_path, dataframe=phase_windows_df),
            "phase_baselines": build_artifact_manifest(path=phase_baselines_path, dataframe=phase_baselines_df),
            "phase_labels": build_artifact_manifest(path=str(phase_labels_path), dataframe=phase_labels_df),
        },
        output_artifacts={
            "phase_label_centroids": build_artifact_manifest(
                path=phase_label_centroids_path,
                dataframe=phase_label_centroids.to_dataframe(),
                row_count=phase_label_centroids_count,
            ),
        },
        replayable_from=["phase_windows"],
        cache_artifacts={
            "phase_label_centroid_validation": {
                "comparison_report_path": comparison_report_path,
            }
        },
    )
    log_stage_manifest_if_active(stage_manifest, runtime.report_paths.manifest_artifact_path)
    LOGGER.info(
        "pipeline=phase_label_centroids format=%s write_mode=%s phase_label_centroids=%s",
        table_format,
        write_mode,
        phase_label_centroids_path,
    )


if __name__ == "__main__":
    run()
