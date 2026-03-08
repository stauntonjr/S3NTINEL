import pytest

pytest.importorskip("pyspark")

from libs.profiling import build_parameter_datatype_profile


def test_build_parameter_datatype_profile_infers_numeric_binary_and_categorical(spark):
    rows = [
        ("S_NUM", "1.1"),
        ("S_NUM", "2.5"),
        ("S_NUM", "3.2"),
        ("S_NUM", ""),
        ("S_BIN", "ON"),
        ("S_BIN", "OFF"),
        ("S_BIN", "ON"),
        ("S_CAT", "A"),
        ("S_CAT", "B"),
        ("S_CAT", "C"),
        ("S_CAT", "A"),
    ]
    telemetry_df = spark.createDataFrame(rows, schema="sensor string, parameter_value string")

    prof_df = build_parameter_datatype_profile(telemetry_df)
    results = {row["sensor"]: row["detected_type"] for row in prof_df.select("parameter_name", "detected_type").collect()}

    assert results["S_NUM"] == "numeric"
    assert results["S_BIN"] == "binary"
    assert results["S_CAT"] == "categorical"


def test_build_parameter_datatype_profile_supports_parameter_name_column(spark):
    rows = [
        ("P1", "10.0"),
        ("P1", "11.0"),
        ("P2", "ON"),
        ("P2", "OFF"),
    ]
    telemetry_df = spark.createDataFrame(rows, schema="parameter_name string, parameter_value string")

    prof_df = build_parameter_datatype_profile(telemetry_df)
    result_columns = set(prof_df.columns)

    assert {"sensor", "detected_type", "total_count", "missing_count", "missing_rate", "numeric_rate", "distinct_value_count"}.issubset(result_columns)
    assert prof_df.where("sensor = 'P1'").select("detected_type").first()[0] == "binary"
