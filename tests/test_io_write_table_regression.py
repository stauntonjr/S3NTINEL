import pytest

pytest.importorskip("pyspark")

from pyspark.sql import types as T

from libs.io.delta import read_table, write_table


def test_write_table_empty_partitioned_output_is_readable(spark, tmp_path):
    schema = T.StructType(
        [
            T.StructField("tail_id", T.StringType(), True),
            T.StructField("phase_id_detected", T.IntegerType(), True),
            T.StructField("name", T.StringType(), True),
        ]
    )
    empty_df = spark.createDataFrame([], schema)
    output_path = str(tmp_path / "phases_empty")

    write_table(
        empty_df,
        path=output_path,
        mode="overwrite",
        fmt="parquet",
        partition_by=["tail_id"],
    )

    read_back = read_table(spark, path=output_path, fmt="parquet")
    assert read_back.count() == 0
    assert read_back.columns == ["tail_id", "phase_id_detected", "name"]
