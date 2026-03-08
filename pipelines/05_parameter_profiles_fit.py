"""Fit datatype, scaling, and behavior profile artifacts from raw telemetry."""

from __future__ import annotations

import os

from libs.io.delta import get_spark, read_table, write_table
from libs.perf import (
    build_artifact_manifest,
    build_stage_manifest,
    get_logger,
    log_dict_artifact_if_active,
    log_params_if_active,
    log_stage_manifest_if_active,
    log_wall_time,
    track_mlflow_run,
)
from libs.profiling import (
    build_continuous_scaling_profile_table,
    build_parameter_behavior_profile_table,
    build_parameter_datatype_profile_table,
)
from pipelines.common import build_context


LOGGER = get_logger(__name__)


@track_mlflow_run(stage_name="05_parameter_profiles_fit", logger=LOGGER)
@log_wall_time(logger=LOGGER)
def run() -> None:
    context = build_context()
    raw_path = os.getenv("S3NTINEL_RAW_TABLE_PATH", "data/delta/raw_telemetry")
    datatype_profile_path = os.getenv("S3NTINEL_PARAMETER_DATATYPE_PROFILE_TABLE_PATH", "data/delta/parameter_datatype_profile")
    scaling_profile_path = os.getenv("S3NTINEL_CONTINUOUS_SCALING_PROFILE_TABLE_PATH", "data/delta/continuous_scaling_profile")
    behavior_profile_path = os.getenv("S3NTINEL_PARAMETER_BEHAVIOR_PROFILE_TABLE_PATH", "data/delta/parameter_behavior_profile")
    table_format = os.getenv("S3NTINEL_TABLE_FORMAT", "delta")
    write_mode = os.getenv("S3NTINEL_FIT_WRITE_MODE", "overwrite")

    spark = get_spark("s3ntinel.parameter_profiles_fit")
    raw_df = read_table(spark, raw_path, fmt=table_format)

    datatype_profile_df = build_parameter_datatype_profile_table(raw_df)
    scaling_profile_df = build_continuous_scaling_profile_table(raw_df, datatype_profile_df)
    behavior_profile_df = build_parameter_behavior_profile_table(raw_df, datatype_profile_df)

    write_table(datatype_profile_df, path=datatype_profile_path, mode=write_mode, fmt=table_format)
    write_table(scaling_profile_df, path=scaling_profile_path, mode=write_mode, fmt=table_format)
    write_table(behavior_profile_df, path=behavior_profile_path, mode=write_mode, fmt=table_format)

    raw_count = int(raw_df.count())
    datatype_count = int(datatype_profile_df.count())
    scaling_count = int(scaling_profile_df.count())
    behavior_count = int(behavior_profile_df.count())

    log_params_if_active(
        {
            "runtime_mode": context.config["runtime"]["mode"],
            "table_format": table_format,
        }
    )
    log_dict_artifact_if_active(
        {
            "stage": "05_parameter_profiles_fit",
            "raw_path": raw_path,
            "parameter_datatype_profile_path": datatype_profile_path,
            "continuous_scaling_profile_path": scaling_profile_path,
            "parameter_behavior_profile_path": behavior_profile_path,
            "table_format": table_format,
            "write_mode": write_mode,
            "raw_count": raw_count,
            "datatype_profile_count": datatype_count,
            "scaling_profile_count": scaling_count,
            "behavior_profile_count": behavior_count,
        },
        "reports/stages/05_parameter_profiles_fit_summary.json",
    )
    stage_manifest = build_stage_manifest(
        stage_name="05_parameter_profiles_fit",
        config={
            "table_format": table_format,
            "write_mode": write_mode,
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
            "parameter_behavior_profile": build_artifact_manifest(
                path=behavior_profile_path,
                dataframe=behavior_profile_df,
                row_count=behavior_count,
                artifact_version="PARAMETER_BEHAVIOR_PROFILE_V2",
            ),
        },
        replayable_from=["raw_telemetry"],
    )
    log_stage_manifest_if_active(stage_manifest, "reports/stages/05_parameter_profiles_fit_manifest.json")
    LOGGER.info(
        "pipeline=parameter_profiles_fit format=%s write_mode=%s raw=%s datatype_profile=%s scaling_profile=%s behavior_profile=%s",
        table_format,
        write_mode,
        raw_path,
        datatype_profile_path,
        scaling_profile_path,
        behavior_profile_path,
    )


if __name__ == "__main__":
    run()
