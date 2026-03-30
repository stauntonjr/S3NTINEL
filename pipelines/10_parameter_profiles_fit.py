"""Fit datatype and scaling profile artifacts from raw telemetry."""

from __future__ import annotations

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
from libs.profiling import (
    TelemetryProfilingPlan,
)
from pipelines.common import build_stage_runtime


LOGGER = get_logger(__name__)


@track_mlflow_run(stage_name="10_parameter_profiles_fit", logger=LOGGER)
@log_memory_usage(logger=LOGGER, label="10_parameter_profiles_fit")
@log_wall_time(logger=LOGGER)
def run() -> None:
    from pyspark import StorageLevel

    runtime = build_stage_runtime("10_parameter_profiles_fit")
    context = runtime.context
    raw_path = runtime.artifacts.raw_table
    datatype_profile_path = runtime.artifacts.parameter_datatype_profile
    scaling_profile_path = runtime.artifacts.continuous_scaling_profile
    table_format = runtime.execution.table_format
    write_mode = runtime.execution.fit_write_mode
    numeric_ratio_threshold = runtime.settings.profiling.numeric_ratio_threshold
    categorical_cardinality_max = runtime.settings.profiling.categorical_cardinality_max

    spark = get_spark("s3ntinel.parameter_profiles_fit")
    raw_df = read_table(spark, raw_path, fmt=table_format).persist(StorageLevel.MEMORY_AND_DISK)

    profiling_plan = TelemetryProfilingPlan.from_raw_input(
        raw_df,
        numeric_ratio_threshold=numeric_ratio_threshold,
        categorical_cardinality_max=categorical_cardinality_max,
    )
    datatype_profile_df = profiling_plan.build_datatype_profile().to_dataframe().persist(StorageLevel.MEMORY_AND_DISK)
    scaling_profile_df = profiling_plan.build_scaling_profile(datatype_profile_df).to_dataframe().persist(StorageLevel.MEMORY_AND_DISK)

    try:
        raw_count = int(raw_df.count())
        datatype_count = int(datatype_profile_df.count())
        scaling_count = int(scaling_profile_df.count())

        write_table(datatype_profile_df, path=datatype_profile_path, mode=write_mode, fmt=table_format)
        write_table(scaling_profile_df, path=scaling_profile_path, mode=write_mode, fmt=table_format)
    finally:
        scaling_profile_df.unpersist()
        datatype_profile_df.unpersist()
        raw_df.unpersist()

    log_params_if_active(
        {
            "runtime_mode": context.config["runtime"]["mode"],
            "table_format": table_format,
            "numeric_ratio_threshold": numeric_ratio_threshold,
            "categorical_cardinality_max": categorical_cardinality_max,
        }
    )
    log_dict_artifact_if_active(
        {
            "stage": "10_parameter_profiles_fit",
            "raw_path": raw_path,
            "parameter_datatype_profile_path": datatype_profile_path,
            "continuous_scaling_profile_path": scaling_profile_path,
            "table_format": table_format,
            "write_mode": write_mode,
            "numeric_ratio_threshold": numeric_ratio_threshold,
            "categorical_cardinality_max": categorical_cardinality_max,
            "raw_count": raw_count,
            "datatype_profile_count": datatype_count,
            "scaling_profile_count": scaling_count,
        },
        runtime.report_paths.summary_artifact_path,
    )
    stage_manifest = build_stage_manifest(
        stage_name="10_parameter_profiles_fit",
        config={
            "table_format": table_format,
            "write_mode": write_mode,
            "numeric_ratio_threshold": numeric_ratio_threshold,
            "categorical_cardinality_max": categorical_cardinality_max,
        },
        input_artifacts={
            "raw_telemetry": build_artifact_manifest(path=raw_path, dataframe=raw_df, row_count=raw_count),
        },
        output_artifacts={
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
        replayable_from=["raw_telemetry"],
    )
    log_stage_manifest_if_active(stage_manifest, runtime.report_paths.manifest_artifact_path)
    LOGGER.info(
        "pipeline=parameter_profiles_fit format=%s write_mode=%s raw=%s datatype_profile=%s scaling_profile=%s",
        table_format,
        write_mode,
        raw_path,
        datatype_profile_path,
        scaling_profile_path,
    )


if __name__ == "__main__":
    run()
