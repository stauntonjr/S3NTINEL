import pytest

pytest.importorskip("pyspark")

from libs.profiling import ParameterDatatypeProfile, ParameterProfile


def test_build_parameter_datatype_profile_infers_numeric_and_small_categorical(spark):
    rows = [
        ("S_NUM", "2025-01-01T00:00:00", "1.1"),
        ("S_NUM", "2025-01-01T00:00:01", "2.5"),
        ("S_NUM", "2025-01-01T00:00:02", "3.2"),
        ("S_NUM", "2025-01-01T00:00:03", ""),
        ("S_BIN", "2025-01-01T00:00:00", "ON"),
        ("S_BIN", "2025-01-01T00:00:01", "OFF"),
        ("S_BIN", "2025-01-01T00:00:02", "ON"),
        ("S_CAT", "2025-01-01T00:00:00", "A"),
        ("S_CAT", "2025-01-01T00:00:01", "B"),
        ("S_CAT", "2025-01-01T00:00:02", "C"),
        ("S_CAT", "2025-01-01T00:00:03", "A"),
    ]
    telemetry_df = spark.createDataFrame(
        rows,
        schema="parameter_name string, timestamp_utc string, parameter_value string",
    )

    prof_df = ParameterDatatypeProfile.from_parameter_profile(ParameterProfile.build_dataframe(telemetry_df)).to_dataframe()
    results = {
        row["parameter_name"]: row["parameter_datatype_profiled"]
        for row in prof_df.select("parameter_name", "parameter_datatype_profiled").collect()
    }

    assert results["S_NUM"] == "numeric"
    assert results["S_BIN"] == "categorical"
    assert results["S_CAT"] == "categorical"


def test_build_parameter_datatype_profile_supports_parameter_name_column(spark):
    rows = [
        ("P1", "2025-01-01T00:00:00", "10.0"),
        ("P1", "2025-01-01T00:00:01", "11.0"),
        ("P2", "2025-01-01T00:00:00", "ON"),
        ("P2", "2025-01-01T00:00:01", "OFF"),
    ]
    telemetry_df = spark.createDataFrame(
        rows,
        schema="parameter_name string, timestamp_utc string, parameter_value string",
    )

    prof_df = ParameterDatatypeProfile.from_parameter_profile(ParameterProfile.build_dataframe(telemetry_df)).to_dataframe()
    result_columns = set(prof_df.columns)

    assert {
        "parameter_name",
        "parameter_datatype_profiled",
        "total_count",
        "missing_count",
        "missing_rate",
        "numeric_rate",
        "distinct_value_count",
    }.issubset(result_columns)
    assert prof_df.where("parameter_name = 'P1'").select("parameter_datatype_profiled").first()[0] == "numeric"
