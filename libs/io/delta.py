# File: libs/io/delta.py
"""Delta table read/write helpers for Spark pipelines."""

from __future__ import annotations

from collections.abc import Sequence

from libs.perf.annotations import hot_path


def read_table(spark: "SparkSession", path: str, fmt: str = "delta") -> "DataFrame":
    return spark.read.format(fmt).load(path)


def read_parquet(spark: "SparkSession", path: str) -> "DataFrame":
    return spark.read.parquet(path)


@hot_path
def write_table(
    df: "DataFrame",
    path: str,
    mode: str = "append",
    fmt: str = "delta",
    partition_by: Sequence[str] | None = None,
) -> None:
    # HOT PATH: table write orchestration must avoid small-file amplification and expensive per-row operations.
    writer = df.write.format(fmt).mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def get_spark(app_name: str) -> "SparkSession":
    from pyspark.sql import SparkSession

    return SparkSession.builder.appName(app_name).getOrCreate()

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
