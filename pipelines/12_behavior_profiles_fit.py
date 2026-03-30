"""Fit behavior primitive and family profile artifacts from raw telemetry."""

from __future__ import annotations

from libs.io.delta import get_spark, read_table, write_table
from libs.perf import (
    build_artifact_manifest,
    build_stage_manifest,
    get_logger,
    log_dict_artifact_if_active,
    log_memory_usage,
    log_params_if_active,
    log_stage_manifest_if_active,
    log_wall_time,
    track_mlflow_run,
)
from libs.profiling import TelemetryProfilingPlan
from pipelines.common import build_stage_runtime


LOGGER = get_logger(__name__)


@track_mlflow_run(stage_name="12_behavior_profiles_fit", logger=LOGGER)
@log_memory_usage(logger=LOGGER, label="12_behavior_profiles_fit")
@log_wall_time(logger=LOGGER)
def run() -> None:
    from pyspark import StorageLevel

    runtime = build_stage_runtime("12_behavior_profiles_fit")
    context = runtime.context
    raw_path = runtime.artifacts.raw_table
    datatype_profile_path = runtime.artifacts.parameter_datatype_profile
    scaling_profile_path = runtime.artifacts.continuous_scaling_profile
    primitive_profile_path = runtime.artifacts.parameter_behavior_primitive_profile
    behavior_profile_path = runtime.artifacts.parameter_behavior_profile
    table_format = runtime.execution.table_format
    write_mode = runtime.execution.fit_write_mode
    profiling = runtime.settings.profiling

    spark = get_spark("s3ntinel.behavior_profiles_fit")
    raw_df = read_table(spark, raw_path, fmt=table_format).persist(StorageLevel.MEMORY_AND_DISK)
    datatype_profile_df = read_table(spark, datatype_profile_path, fmt=table_format).persist(StorageLevel.MEMORY_AND_DISK)
    scaling_profile_df = read_table(spark, scaling_profile_path, fmt=table_format).persist(StorageLevel.MEMORY_AND_DISK)

    profiling_plan = TelemetryProfilingPlan.from_raw_input(
        raw_df,
        numeric_ratio_threshold=profiling.numeric_ratio_threshold,
        categorical_cardinality_max=profiling.categorical_cardinality_max,
        behavior_significant_diff_threshold=profiling.behavior_significant_diff_threshold,
        behavior_center_band_width=profiling.behavior_center_band_width,
        behavior_soft_bound_width=profiling.behavior_soft_bound_width,
        behavior_hard_bound_width=profiling.behavior_hard_bound_width,
        behavior_mixed_unknown_low_score_threshold=profiling.behavior_mixed_unknown_low_score_threshold,
        behavior_mixed_unknown_ambiguous_score_threshold=profiling.behavior_mixed_unknown_ambiguous_score_threshold,
        behavior_mixed_unknown_ambiguous_margin_threshold=profiling.behavior_mixed_unknown_ambiguous_margin_threshold,
    )
    primitive_profile_df = profiling_plan.build_behavior_primitive_profile(
        datatype_profile_df,
        scaling_profile_df,
    ).to_dataframe().persist(StorageLevel.MEMORY_AND_DISK)
    behavior_profile_df = profiling_plan.build_behavior_profile(
        primitive_profile_df,
    ).to_dataframe().persist(StorageLevel.MEMORY_AND_DISK)

    try:
        raw_count = int(raw_df.count())
        datatype_count = int(datatype_profile_df.count())
        scaling_count = int(scaling_profile_df.count())
        primitive_count = int(primitive_profile_df.count())
        behavior_count = int(behavior_profile_df.count())

        write_table(primitive_profile_df, path=primitive_profile_path, mode=write_mode, fmt=table_format)
        write_table(behavior_profile_df, path=behavior_profile_path, mode=write_mode, fmt=table_format)
    finally:
        behavior_profile_df.unpersist()
        primitive_profile_df.unpersist()
        scaling_profile_df.unpersist()
        datatype_profile_df.unpersist()
        raw_df.unpersist()

    log_params_if_active(
        {
            "runtime_mode": context.config["runtime"]["mode"],
            "table_format": table_format,
            "behavior_significant_diff_threshold": profiling.behavior_significant_diff_threshold,
            "behavior_center_band_width": profiling.behavior_center_band_width,
            "behavior_soft_bound_width": profiling.behavior_soft_bound_width,
            "behavior_hard_bound_width": profiling.behavior_hard_bound_width,
            "behavior_mixed_unknown_low_score_threshold": profiling.behavior_mixed_unknown_low_score_threshold,
            "behavior_mixed_unknown_ambiguous_score_threshold": profiling.behavior_mixed_unknown_ambiguous_score_threshold,
            "behavior_mixed_unknown_ambiguous_margin_threshold": profiling.behavior_mixed_unknown_ambiguous_margin_threshold,
        }
    )
    log_dict_artifact_if_active(
        {
            "stage": "12_behavior_profiles_fit",
            "raw_path": raw_path,
            "parameter_datatype_profile_path": datatype_profile_path,
            "continuous_scaling_profile_path": scaling_profile_path,
            "parameter_behavior_primitive_profile_path": primitive_profile_path,
            "parameter_behavior_profile_path": behavior_profile_path,
            "table_format": table_format,
            "write_mode": write_mode,
            "behavior_significant_diff_threshold": profiling.behavior_significant_diff_threshold,
            "behavior_center_band_width": profiling.behavior_center_band_width,
            "behavior_soft_bound_width": profiling.behavior_soft_bound_width,
            "behavior_hard_bound_width": profiling.behavior_hard_bound_width,
            "behavior_mixed_unknown_low_score_threshold": profiling.behavior_mixed_unknown_low_score_threshold,
            "behavior_mixed_unknown_ambiguous_score_threshold": profiling.behavior_mixed_unknown_ambiguous_score_threshold,
            "behavior_mixed_unknown_ambiguous_margin_threshold": profiling.behavior_mixed_unknown_ambiguous_margin_threshold,
            "raw_count": raw_count,
            "datatype_profile_count": datatype_count,
            "scaling_profile_count": scaling_count,
            "primitive_profile_count": primitive_count,
            "behavior_profile_count": behavior_count,
        },
        runtime.report_paths.summary_artifact_path,
    )
    stage_manifest = build_stage_manifest(
        stage_name="12_behavior_profiles_fit",
        config={
            "table_format": table_format,
            "write_mode": write_mode,
            "behavior_significant_diff_threshold": profiling.behavior_significant_diff_threshold,
            "behavior_center_band_width": profiling.behavior_center_band_width,
            "behavior_soft_bound_width": profiling.behavior_soft_bound_width,
            "behavior_hard_bound_width": profiling.behavior_hard_bound_width,
            "behavior_mixed_unknown_low_score_threshold": profiling.behavior_mixed_unknown_low_score_threshold,
            "behavior_mixed_unknown_ambiguous_score_threshold": profiling.behavior_mixed_unknown_ambiguous_score_threshold,
            "behavior_mixed_unknown_ambiguous_margin_threshold": profiling.behavior_mixed_unknown_ambiguous_margin_threshold,
        },
        input_artifacts={
            "raw_telemetry": build_artifact_manifest(path=raw_path, dataframe=raw_df, row_count=raw_count),
            "parameter_datatype_profile": build_artifact_manifest(
                path=datatype_profile_path,
                dataframe=datatype_profile_df,
                row_count=datatype_count,
                artifact_version="PARAMETER_DATATYPE_PROFILE_V2",
            ),
            "continuous_scaling_profile": build_artifact_manifest(
                path=scaling_profile_path,
                dataframe=scaling_profile_df,
                row_count=scaling_count,
                artifact_version="CONTINUOUS_SCALING_PROFILE_V2",
            ),
        },
        output_artifacts={
            "parameter_behavior_primitive_profile": build_artifact_manifest(
                path=primitive_profile_path,
                dataframe=primitive_profile_df,
                row_count=primitive_count,
                artifact_version="PARAMETER_BEHAVIOR_PRIMITIVE_PROFILE_V1",
            ),
            "parameter_behavior_profile": build_artifact_manifest(
                path=behavior_profile_path,
                dataframe=behavior_profile_df,
                row_count=behavior_count,
                artifact_version="PARAMETER_BEHAVIOR_PROFILE_V3",
            ),
        },
        replayable_from=["raw_telemetry", "parameter_datatype_profile", "continuous_scaling_profile"],
    )
    log_stage_manifest_if_active(stage_manifest, runtime.report_paths.manifest_artifact_path)
    LOGGER.info(
        "pipeline=behavior_profiles_fit format=%s write_mode=%s raw=%s datatype_profile=%s scaling_profile=%s primitive_profile=%s behavior_profile=%s",
        table_format,
        write_mode,
        raw_path,
        datatype_profile_path,
        scaling_profile_path,
        primitive_profile_path,
        behavior_profile_path,
    )


if __name__ == "__main__":
    run()
