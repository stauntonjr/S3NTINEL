# File: libs/io/delta.py
"""Delta table read/write helpers for Spark pipelines."""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from pathlib import Path

from libs.perf.annotations import hot_path


_SPARK_PROFILE_DEFAULTS: dict[str, dict[str, str]] = {
    "laptop_large_sim": {
        "spark.master": "local[4]",
        "spark.driver.memory": "8g",
        "spark.driver.maxResultSize": "2g",
        "spark.sql.shuffle.partitions": "16",
        "spark.default.parallelism": "8",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.localShuffleReader.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.local.dir": "/tmp/s3ntinel-spark-local",
    },
    "laptop_large_sim_large_segments": {
        "spark.master": "local[4]",
        "spark.driver.memory": "8g",
        "spark.driver.maxResultSize": "2g",
        "spark.sql.shuffle.partitions": "16",
        "spark.default.parallelism": "8",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.localShuffleReader.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.local.dir": "/tmp/s3ntinel-spark-local",
    },
}

_SPARK_CONF_ENV_BY_NAME: dict[str, str] = {
    "S3NTINEL_SPARK_MASTER": "spark.master",
    "S3NTINEL_SPARK_UI_ENABLED": "spark.ui.enabled",
    "S3NTINEL_SPARK_SHUFFLE_PARTITIONS": "spark.sql.shuffle.partitions",
    "S3NTINEL_SPARK_DEFAULT_PARALLELISM": "spark.default.parallelism",
    "S3NTINEL_SPARK_DRIVER_MEMORY": "spark.driver.memory",
    "S3NTINEL_SPARK_DRIVER_MAX_RESULT_SIZE": "spark.driver.maxResultSize",
    "S3NTINEL_SPARK_EXECUTOR_MEMORY": "spark.executor.memory",
    "S3NTINEL_SPARK_EXECUTOR_MEMORY_OVERHEAD": "spark.executor.memoryOverhead",
    "S3NTINEL_SPARK_LOCAL_DIR": "spark.local.dir",
    "S3NTINEL_SPARK_SERIALIZER": "spark.serializer",
    "S3NTINEL_SPARK_SQL_ADAPTIVE_ENABLED": "spark.sql.adaptive.enabled",
    "S3NTINEL_SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS_ENABLED": "spark.sql.adaptive.coalescePartitions.enabled",
    "S3NTINEL_SPARK_SQL_ADAPTIVE_LOCAL_SHUFFLE_READER_ENABLED": "spark.sql.adaptive.localShuffleReader.enabled",
    "S3NTINEL_SPARK_SQL_AUTO_BROADCAST_JOIN_THRESHOLD": "spark.sql.autoBroadcastJoinThreshold",
    "S3NTINEL_SPARK_SQL_FILES_MAX_PARTITION_BYTES": "spark.sql.files.maxPartitionBytes",
    "S3NTINEL_SPARK_SQL_BROADCAST_TIMEOUT": "spark.sql.broadcastTimeout",
    "S3NTINEL_SPARK_WAREHOUSE_DIR": "spark.sql.warehouse.dir",
}


def _jar_list_from_env(value: str | None) -> list[str]:
    if value is None:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _normalize_spark_profile_name(value: str | None) -> str:
    return str(value or "").strip().lower()


def _spark_profile_defaults(profile_name: str) -> dict[str, str]:
    if not profile_name:
        return {}
    defaults = _SPARK_PROFILE_DEFAULTS.get(profile_name)
    if defaults is None:
        known = ", ".join(sorted(_SPARK_PROFILE_DEFAULTS))
        raise ValueError(f"unsupported S3NTINEL_SPARK_PROFILE={profile_name!r}; expected one of: {known}")
    return dict(defaults)


def describe_spark_runtime_config(environ: Mapping[str, str] | None = None) -> dict[str, str]:
    env = environ or os.environ
    config: dict[str, str] = {
        "spark.master": "local[2]",
        "spark.ui.enabled": "false",
        "spark.sql.shuffle.partitions": "8",
        "spark.default.parallelism": "8",
    }
    profile_name = _normalize_spark_profile_name(env.get("S3NTINEL_SPARK_PROFILE"))
    config.update(_spark_profile_defaults(profile_name))
    for env_name, spark_conf_key in _SPARK_CONF_ENV_BY_NAME.items():
        raw_value = str(env.get(env_name, "")).strip()
        if raw_value:
            config[spark_conf_key] = raw_value
    return config


def _ensure_spark_local_paths(runtime_config: Mapping[str, str]) -> None:
    for key in ("spark.local.dir", "spark.sql.warehouse.dir"):
        raw_value = str(runtime_config.get(key, "")).strip()
        if not raw_value:
            continue
        for item in raw_value.split(","):
            path_text = item.strip()
            if path_text:
                Path(path_text).mkdir(parents=True, exist_ok=True)


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

    runtime_config = describe_spark_runtime_config()
    runtime_config.setdefault("spark.sql.warehouse.dir", os.getenv("S3NTINEL_SPARK_WAREHOUSE_DIR", "/tmp/s3ntinel-spark-warehouse"))
    _ensure_spark_local_paths(runtime_config)

    builder = SparkSession.builder.appName(app_name)
    builder = builder.master(runtime_config["spark.master"])
    builder = builder.config("spark.driver.host", os.getenv("SPARK_LOCAL_IP", "127.0.0.1"))
    builder = builder.config("spark.driver.bindAddress", os.getenv("SPARK_LOCAL_IP", "127.0.0.1"))
    for spark_conf_key, value in runtime_config.items():
        if spark_conf_key == "spark.master":
            continue
        builder = builder.config(spark_conf_key, value)

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
