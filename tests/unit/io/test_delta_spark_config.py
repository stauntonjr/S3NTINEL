from __future__ import annotations

import pytest

from libs.io.delta import describe_spark_runtime_config


def test_describe_spark_runtime_config_uses_repo_defaults():
    config = describe_spark_runtime_config({})

    assert config["spark.master"] == "local[2]"
    assert config["spark.ui.enabled"] == "false"
    assert config["spark.sql.shuffle.partitions"] == "8"
    assert config["spark.default.parallelism"] == "8"


def test_describe_spark_runtime_config_supports_laptop_large_profile():
    config = describe_spark_runtime_config({"S3NTINEL_SPARK_PROFILE": "laptop_large_sim"})

    assert config["spark.master"] == "local[4]"
    assert config["spark.driver.memory"] == "8g"
    assert config["spark.driver.maxResultSize"] == "2g"
    assert config["spark.sql.shuffle.partitions"] == "16"
    assert config["spark.default.parallelism"] == "8"
    assert config["spark.sql.adaptive.enabled"] == "true"
    assert config["spark.sql.adaptive.coalescePartitions.enabled"] == "true"
    assert config["spark.sql.adaptive.localShuffleReader.enabled"] == "true"
    assert config["spark.serializer"] == "org.apache.spark.serializer.KryoSerializer"
    assert config["spark.local.dir"] == "/tmp/s3ntinel-spark-local"


def test_describe_spark_runtime_config_supports_laptop_large_segments_profile():
    config = describe_spark_runtime_config({"S3NTINEL_SPARK_PROFILE": "laptop_large_sim_large_segments"})

    assert config["spark.master"] == "local[4]"
    assert config["spark.driver.memory"] == "8g"
    assert config["spark.driver.maxResultSize"] == "2g"
    assert config["spark.sql.shuffle.partitions"] == "16"
    assert config["spark.default.parallelism"] == "8"
    assert config["spark.sql.adaptive.enabled"] == "true"
    assert config["spark.sql.adaptive.coalescePartitions.enabled"] == "true"
    assert config["spark.sql.adaptive.localShuffleReader.enabled"] == "true"
    assert config["spark.serializer"] == "org.apache.spark.serializer.KryoSerializer"
    assert config["spark.local.dir"] == "/tmp/s3ntinel-spark-local"


def test_describe_spark_runtime_config_allows_explicit_env_overrides():
    config = describe_spark_runtime_config(
        {
            "S3NTINEL_SPARK_PROFILE": "laptop_large_sim",
            "S3NTINEL_SPARK_MASTER": "local[6]",
            "S3NTINEL_SPARK_DRIVER_MEMORY": "10g",
            "S3NTINEL_SPARK_SHUFFLE_PARTITIONS": "24",
        }
    )

    assert config["spark.master"] == "local[6]"
    assert config["spark.driver.memory"] == "10g"
    assert config["spark.sql.shuffle.partitions"] == "24"


def test_describe_spark_runtime_config_rejects_unknown_profile():
    with pytest.raises(ValueError, match="unsupported S3NTINEL_SPARK_PROFILE"):
        describe_spark_runtime_config({"S3NTINEL_SPARK_PROFILE": "does_not_exist"})
