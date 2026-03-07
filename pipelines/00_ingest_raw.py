# File: pipelines/00_ingest_raw.py
"""Ingest raw parquet telemetry into normalized Delta bronze/silver tables."""

import os

from libs.perf import (
    get_logger,
    log_dataframe_dataset_if_active,
    log_dict_artifact_if_active,
    log_params_if_active,
    log_wall_time,
    track_mlflow_run,
)
from libs.io.delta import get_spark, read_parquet, write_table
from libs.io.transforms import normalize_raw_telemetry
from libs.profiling import build_sensor_datatype_profile
from pipelines.common import build_context


LOGGER = get_logger(__name__)


def resolve_output_format() -> str:
    return os.getenv("S3NTINEL_RAW_OUTPUT_FORMAT", os.getenv("S3NTINEL_TABLE_FORMAT", "delta"))


@track_mlflow_run(stage_name="00_ingest_raw", logger=LOGGER)
@log_wall_time(logger=LOGGER)
def run() -> None:
    context = build_context()
    input_path = os.getenv("S3NTINEL_RAW_INPUT_PATH", "data/input/raw_telemetry")
    output_path = os.getenv("S3NTINEL_RAW_TABLE_PATH", "data/delta/raw_telemetry")
    sensor_type_profile_path = os.getenv("S3NTINEL_SENSOR_TYPE_PROFILE_TABLE_PATH", "data/delta/sensor_type_profile")
    output_format = resolve_output_format()
    write_mode = os.getenv("S3NTINEL_WRITE_MODE", "append")

    spark = get_spark("s3ntinel.ingest_raw")
    raw_df = read_parquet(spark, input_path)
    log_dataframe_dataset_if_active(
        name="stage00_raw_input",
        dataframe=raw_df,
        context="stage00_input",
        logger=LOGGER,
    )
    normalized_df = normalize_raw_telemetry(raw_df)
    sensor_type_profile_df = build_sensor_datatype_profile(normalized_df)
    log_dataframe_dataset_if_active(
        name="stage00_normalized_output",
        dataframe=normalized_df,
        context="stage00_output",
        logger=LOGGER,
    )
    log_dataframe_dataset_if_active(
        name="stage00_sensor_type_profile",
        dataframe=sensor_type_profile_df,
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
    write_table(
        sensor_type_profile_df,
        path=sensor_type_profile_path,
        mode=write_mode,
        fmt=output_format,
    )

    log_params_if_active({"runtime_mode": context.config["runtime"]["mode"]})
    log_dict_artifact_if_active(
        {
            "stage": "00_ingest_raw",
            "input_path": input_path,
            "output_path": output_path,
            "sensor_type_profile_path": sensor_type_profile_path,
            "output_format": output_format,
            "write_mode": write_mode,
            "partition_by": list(context.config["output"]["partition_by"]),
        },
        "reports/stages/00_ingest_raw_summary.json",
    )
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
