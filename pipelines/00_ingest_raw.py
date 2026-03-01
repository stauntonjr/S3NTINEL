# File: pipelines/00_ingest_raw.py
"""Ingest raw parquet telemetry into normalized Delta bronze/silver tables."""

import os

from libs.perf import get_logger, log_params_if_active, log_wall_time, track_mlflow_run
from libs.io.delta import get_spark, read_parquet, write_table
from libs.io.transforms import normalize_raw_telemetry
from pipelines.common import build_context


LOGGER = get_logger(__name__)


@track_mlflow_run(stage_name="00_ingest_raw", logger=LOGGER)
@log_wall_time(logger=LOGGER)
def run() -> None:
    context = build_context()
    input_path = os.getenv("S3NTINEL_RAW_INPUT_PATH", "data/input/raw_telemetry")
    output_path = os.getenv("S3NTINEL_RAW_TABLE_PATH", "data/delta/raw_telemetry")
    output_format = os.getenv("S3NTINEL_RAW_OUTPUT_FORMAT", "delta")
    write_mode = os.getenv("S3NTINEL_WRITE_MODE", "append")

    spark = get_spark("s3ntinel.ingest_raw")
    raw_df = read_parquet(spark, input_path)
    normalized_df = normalize_raw_telemetry(raw_df)
    write_table(
        normalized_df,
        path=output_path,
        mode=write_mode,
        fmt=output_format,
        partition_by=context.config["output"]["partition_by"],
    )

    log_params_if_active({"runtime_mode": context.config["runtime"]["mode"]})
    LOGGER.info(
        "pipeline=ingest_raw project=%s input=%s output=%s",
        context.config["project"]["name"],
        input_path,
        output_path,
    )


if __name__ == "__main__":
    run()
