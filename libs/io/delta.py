# File: libs/io/delta.py
"""Delta table read/write helpers for Spark pipelines."""

from __future__ import annotations

import os
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
    if partition_by and len(df.take(1)) == 0:
        df.sparkSession.createDataFrame([], df.schema).write.format(fmt).mode(mode).save(path)
        return

    writer = df.write.format(fmt).mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


@hot_path
def upsert_table(
    df: "DataFrame",
    path: str,
    merge_keys: Sequence[str],
    fmt: str = "delta",
    partition_by: Sequence[str] | None = None,
) -> None:
    if fmt != "delta":
        raise ValueError("upsert_table currently supports fmt='delta' only")
    if not merge_keys:
        raise ValueError("merge_keys must contain at least one key column")

    spark = df.sparkSession
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    j_path = jvm.org.apache.hadoop.fs.Path(path)

    if not fs.exists(j_path):
        write_table(df, path=path, mode="append", fmt=fmt, partition_by=partition_by)
        return

    from delta.tables import DeltaTable

    target = DeltaTable.forPath(spark, path)
    merge_condition = " AND ".join([f"t.`{key}` <=> s.`{key}`" for key in merge_keys])
    target.alias("t").merge(df.alias("s"), merge_condition).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


def get_spark(app_name: str) -> "SparkSession":
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)
    table_format = str(os.getenv("S3NTINEL_TABLE_FORMAT", "delta")).strip().lower()
    write_mode = str(os.getenv("S3NTINEL_WRITE_MODE", "append")).strip().lower()
    should_enable_delta = table_format == "delta" or write_mode == "merge"

    if should_enable_delta:
        builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        builder = builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        try:
            from delta import configure_spark_with_delta_pip

            builder = configure_spark_with_delta_pip(builder)
        except Exception:
            pass

    return builder.getOrCreate()

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
