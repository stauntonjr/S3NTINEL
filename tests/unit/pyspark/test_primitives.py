from __future__ import annotations

from unittest.mock import Mock

import pytest

from libs.pyspark import Frame, Table


def _test_schema():
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    return StructType(
        [
            StructField("tail_id", StringType(), False),
            StructField("win_id", IntegerType(), False),
        ]
    )


class SchemaFrame(Frame):
    @classmethod
    def spark_schema(cls):
        return _test_schema()


class SchemaTable(Table):
    @classmethod
    def spark_schema(cls):
        return _test_schema()


def test_frame_from_dataframe_preserves_wrapped_dataframe(spark):
    dataframe = spark.createDataFrame([("tail-1", 1)], ["tail_id", "win_id"])

    frame = Frame.from_dataframe(dataframe)

    assert frame.dataframe is dataframe


def test_frame_to_dataframe_returns_wrapped_dataframe(spark):
    dataframe = spark.createDataFrame([("tail-1", 1)], ["tail_id", "win_id"])

    frame = Frame(dataframe=dataframe)

    assert frame.to_dataframe() is dataframe


def test_frame_validate_schema_is_noop_when_schema_is_not_declared(spark):
    dataframe = spark.createDataFrame([("tail-1", 1)], ["tail_id", "win_id"])

    Frame(dataframe=dataframe).validate_schema()


def test_frame_validate_schema_passes_when_schema_matches(spark):
    dataframe = spark.createDataFrame([("tail-1", 1)], schema=_test_schema())

    SchemaFrame(dataframe=dataframe).validate_schema()


def test_frame_validate_schema_raises_when_schema_differs(spark):
    dataframe = spark.createDataFrame([(1, "tail-1")], ["win_id", "tail_id"])

    with pytest.raises(ValueError, match="schema mismatch"):
        SchemaFrame(dataframe=dataframe).validate_schema()


def test_table_base_requires_schema_override():
    with pytest.raises(NotImplementedError, match="Table.spark_schema"):
        Table.spark_schema()


def test_table_read_loads_and_validates(monkeypatch, spark):
    dataframe = spark.createDataFrame([("tail-1", 1)], schema=_test_schema())
    read_table = Mock(return_value=dataframe)
    monkeypatch.setattr("libs.pyspark.table.delta.read_table", read_table)

    table = SchemaTable.read(
        spark,
        "/tmp/window_features",
        format="parquet",
        partition_by=["tail_id"],
    )

    read_table.assert_called_once_with(spark, path="/tmp/window_features", fmt="parquet")
    assert table.dataframe is dataframe
    assert table.path == "/tmp/window_features"
    assert table.format == "parquet"
    assert table.partition_by == ("tail_id",)


def test_table_read_raises_on_schema_mismatch(monkeypatch, spark):
    dataframe = spark.createDataFrame([(1, "tail-1")], ["win_id", "tail_id"])
    monkeypatch.setattr("libs.pyspark.table.delta.read_table", Mock(return_value=dataframe))

    with pytest.raises(ValueError, match="schema mismatch"):
        SchemaTable.read(spark, "/tmp/window_features")


def test_table_write_validates_schema_and_delegates(monkeypatch, spark):
    dataframe = spark.createDataFrame([("tail-1", 1)], schema=_test_schema())
    write_table = Mock()
    monkeypatch.setattr("libs.pyspark.table.delta.write_table", write_table)

    table = SchemaTable(
        dataframe=dataframe,
        path="/tmp/window_features",
        format="parquet",
        partition_by=("tail_id",),
    )

    table.write(mode="overwrite")

    write_table.assert_called_once_with(
        dataframe,
        path="/tmp/window_features",
        mode="overwrite",
        fmt="parquet",
        partition_by=("tail_id",),
    )


def test_table_replace_uses_overwrite_mode(monkeypatch, spark):
    dataframe = spark.createDataFrame([("tail-1", 1)], schema=_test_schema())
    write_table = Mock()
    monkeypatch.setattr("libs.pyspark.table.delta.write_table", write_table)

    SchemaTable(
        dataframe=dataframe,
        path="/tmp/window_features",
    ).replace()

    write_table.assert_called_once_with(
        dataframe,
        path="/tmp/window_features",
        mode="overwrite",
        fmt="delta",
        partition_by=(),
    )


def test_table_upsert_validates_schema_and_delegates(monkeypatch, spark):
    dataframe = spark.createDataFrame([("tail-1", 1)], schema=_test_schema())
    upsert_table = Mock()
    monkeypatch.setattr("libs.pyspark.table.delta.upsert_table", upsert_table)

    table = SchemaTable(
        dataframe=dataframe,
        path="/tmp/window_features",
        partition_by=("tail_id",),
    )

    table.upsert(merge_keys=["tail_id", "win_id"])

    upsert_table.assert_called_once_with(
        dataframe,
        path="/tmp/window_features",
        merge_keys=["tail_id", "win_id"],
        fmt="delta",
        partition_by=("tail_id",),
    )
