# File: libs/io/delta.py
"""Delta table read/write helpers for Spark pipelines."""

from __future__ import annotations

import os
from collections.abc import Sequence
from pathlib import Path

from libs.perf.annotations import hot_path


def _jar_list_from_env(value: str | None) -> list[str]:
    if value is None:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


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

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")

    builder = SparkSession.builder.appName(app_name)
    builder = builder.master(os.getenv("S3NTINEL_SPARK_MASTER", "local[2]"))
    builder = builder.config("spark.driver.host", os.getenv("SPARK_LOCAL_IP", "127.0.0.1"))
    builder = builder.config("spark.driver.bindAddress", os.getenv("SPARK_LOCAL_IP", "127.0.0.1"))
    builder = builder.config("spark.ui.enabled", os.getenv("S3NTINEL_SPARK_UI_ENABLED", "false"))
    builder = builder.config("spark.sql.shuffle.partitions", os.getenv("S3NTINEL_SPARK_SHUFFLE_PARTITIONS", "8"))
    builder = builder.config("spark.default.parallelism", os.getenv("S3NTINEL_SPARK_DEFAULT_PARALLELISM", "8"))

    warehouse_dir = Path(os.getenv("S3NTINEL_SPARK_WAREHOUSE_DIR", "/tmp/s3ntinel-spark-warehouse"))
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    builder = builder.config("spark.sql.warehouse.dir", str(warehouse_dir))

    table_format = str(os.getenv("S3NTINEL_TABLE_FORMAT", "delta")).strip().lower()
    write_mode = str(os.getenv("S3NTINEL_WRITE_MODE", "append")).strip().lower()
    should_enable_delta = table_format == "delta" or write_mode == "merge"

    explicit_jars = _jar_list_from_env(os.getenv("S3NTINEL_SPARK_EXTRA_JARS"))
    explicit_jars.extend(_jar_list_from_env(os.getenv("S3NTINEL_DELTA_JAR_PATH")))
    if explicit_jars:
        builder = builder.config("spark.jars", ",".join(explicit_jars))

    if should_enable_delta:
        ivy_dir = Path(os.getenv("S3NTINEL_SPARK_IVY_DIR", "/tmp/s3ntinel-ivy"))
        ivy_cache_dir = ivy_dir / "cache"
        ivy_jars_dir = ivy_dir / "jars"
        ivy_cache_dir.mkdir(parents=True, exist_ok=True)
        ivy_jars_dir.mkdir(parents=True, exist_ok=True)
        builder = builder.config("spark.jars.ivy", str(ivy_dir))
        builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        builder = builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        allow_maven_delta = str(os.getenv("S3NTINEL_DELTA_ALLOW_MAVEN", "true")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        if allow_maven_delta and not explicit_jars:
            try:
                from delta import configure_spark_with_delta_pip

                builder = configure_spark_with_delta_pip(builder)
            except Exception:
                pass

    return builder.getOrCreate()

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
