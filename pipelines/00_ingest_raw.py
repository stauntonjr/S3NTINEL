# File: pipelines/00_ingest_raw.py
"""Ingest raw parquet telemetry into normalized Delta bronze/silver tables."""

from libs.perf import (
    build_artifact_manifest,
    build_stage_manifest,
    get_logger,
    log_dataframe_dataset_if_active,
    log_memory_usage,
    log_dict_artifact_if_active,
    log_params_if_active,
    log_stage_manifest_if_active,
    log_wall_time,
    track_mlflow_run,
)
from libs.io.delta import get_spark, read_parquet, write_table
from libs.io.transforms import normalize_raw_telemetry
from pipelines.common import build_context, build_stage_runtime, context_execution


LOGGER = get_logger(__name__)


def resolve_output_format() -> str:
    return context_execution(build_context()).raw_output_format


@track_mlflow_run(stage_name="00_ingest_raw", logger=LOGGER)
@log_memory_usage(logger=LOGGER, label="00_ingest_raw")
@log_wall_time(logger=LOGGER)
def run() -> None:
    runtime = build_stage_runtime("00_ingest_raw")
    context = runtime.context
    input_path = runtime.artifacts.raw_input
    output_path = runtime.artifacts.raw_table
    output_format = runtime.execution.raw_output_format
    write_mode = runtime.execution.write_mode

    spark = get_spark("s3ntinel.ingest_raw")
    raw_df = read_parquet(spark, input_path)
    log_dataframe_dataset_if_active(
        name="stage00_raw_input",
        dataframe=raw_df,
        context="stage00_input",
        logger=LOGGER,
    )
    normalized_df = normalize_raw_telemetry(raw_df)
    log_dataframe_dataset_if_active(
        name="stage00_normalized_output",
        dataframe=normalized_df,
        context="stage00_output",
        logger=LOGGER,
    )
    write_table(
        normalized_df,
        path=output_path,
        mode=write_mode,
        fmt=output_format,
        partition_by=context.config["output"]["partition_by"],
    )
    raw_count = int(raw_df.count())
    normalized_count = int(normalized_df.count())

    log_params_if_active({"runtime_mode": context.config["runtime"]["mode"]})
    log_dict_artifact_if_active(
        {
            "stage": "00_ingest_raw",
            "input_path": input_path,
            "output_path": output_path,
            "output_format": output_format,
            "write_mode": write_mode,
            "partition_by": list(context.config["output"]["partition_by"]),
        },
        runtime.report_paths.summary_artifact_path,
    )
    stage_manifest = build_stage_manifest(
        stage_name="00_ingest_raw",
        config={
            "output_format": output_format,
            "write_mode": write_mode,
        },
        input_artifacts={
            "raw_input": build_artifact_manifest(path=input_path, dataframe=raw_df, row_count=raw_count),
        },
        output_artifacts={
            "raw_telemetry": build_artifact_manifest(
                path=output_path,
                dataframe=normalized_df,
                row_count=normalized_count,
            ),
        },
        replayable_from=["raw_input"],
    )
    log_stage_manifest_if_active(stage_manifest, runtime.report_paths.manifest_artifact_path)
    LOGGER.info(
        "pipeline=ingest_raw project=%s format=%s write_mode=%s input=%s output=%s",
        context.config["project"]["name"],
        output_format,
        write_mode,
        input_path,
        output_path,
    )


if __name__ == "__main__":
    run()
