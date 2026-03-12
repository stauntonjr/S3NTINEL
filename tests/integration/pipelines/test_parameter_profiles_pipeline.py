from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from libs.profiling import (
    build_continuous_scaling_profile_table,
    build_parameter_behavior_profile_table,
    build_parameter_datatype_profile_table,
)


def test_build_parameter_datatype_profile_table_exposes_canonical_columns(spark):
    rows = [
        ("P_NUM", "2026-01-01T00:00:00", "1.0"),
        ("P_NUM", "2026-01-01T00:00:01", "2.0"),
        ("P_NUM", "2026-01-01T00:00:02", "3.0"),
        ("P_STATE", "2026-01-01T00:00:00", "ON"),
        ("P_STATE", "2026-01-01T00:00:01", "OFF"),
    ]
    raw_df = spark.createDataFrame(rows, schema="parameter_name string, timestamp_utc string, parameter_value string")

    result_df = build_parameter_datatype_profile_table(raw_df)
    assert {
        "parameter_name",
        "parameter_datatype_profiled",
        "sampling_rate_profiled_hz",
        "median_interval_ms",
    }.issubset(result_df.columns)
    result = {row["parameter_name"]: row["parameter_datatype_profiled"] for row in result_df.select("parameter_name", "parameter_datatype_profiled").collect()}
    assert result["P_NUM"] == "numeric"
    assert result["P_STATE"] == "binary"


def test_build_continuous_scaling_profile_table_returns_numeric_profile_only(spark):
    rows = [
        ("P_NUM", "2026-01-01T00:00:00", "1.0"),
        ("P_NUM", "2026-01-01T00:00:01", "3.0"),
        ("P_NUM", "2026-01-01T00:00:02", "5.0"),
        ("P_STATE", "2026-01-01T00:00:00", "ON"),
        ("P_STATE", "2026-01-01T00:00:01", "OFF"),
    ]
    raw_df = spark.createDataFrame(rows, schema="parameter_name string, timestamp_utc string, parameter_value string")
    datatype_profile_df = spark.createDataFrame(
        [
            ("P_NUM", "numeric"),
            ("P_STATE", "binary"),
        ],
        schema="parameter_name string, parameter_datatype_profiled string",
    )

    scaling_df = build_continuous_scaling_profile_table(raw_df, datatype_profile_df)
    rows = scaling_df.collect()
    assert len(rows) == 1
    row = rows[0].asDict()
    assert row["parameter_name"] == "P_NUM"
    assert row["scaling_center_median"] == 3.0
    assert row["scaling_iqr"] > 0.0


def test_build_parameter_behavior_profile_table_profiles_numeric_and_state_channels(spark):
    rows = [
        ("P_REG", "2026-01-01T00:00:00", "100.0"),
        ("P_REG", "2026-01-01T00:00:01", "100.1"),
        ("P_REG", "2026-01-01T00:00:02", "99.9"),
        ("P_REG", "2026-01-01T00:00:03", "100.0"),
        ("P_STATE", "2026-01-01T00:00:00", "ON"),
        ("P_STATE", "2026-01-01T00:00:01", "ON"),
        ("P_STATE", "2026-01-01T00:00:02", "OFF"),
        ("P_STATE", "2026-01-01T00:00:03", "OFF"),
    ]
    raw_df = spark.createDataFrame(rows, schema="parameter_name string, timestamp_utc string, parameter_value string")
    datatype_profile_df = spark.createDataFrame(
        [
            ("P_REG", "numeric"),
            ("P_STATE", "binary"),
        ],
        schema="parameter_name string, parameter_datatype_profiled string",
    )

    profile_df = build_parameter_behavior_profile_table(raw_df, datatype_profile_df)
    rows = {row["parameter_name"]: row.asDict() for row in profile_df.collect()}
    assert rows["P_STATE"]["behavior_family_profiled"] == "discrete_state"
    assert rows["P_STATE"]["discrete_state_score_profiled"] > 0.0
    assert rows["P_REG"]["behavior_family_profiled"] in {"regulated", "inertial", "accumulative", "mixed_unknown"}
    assert rows["P_REG"]["sample_count"] == 4


def test_build_parameter_behavior_profile_table_can_identify_accumulative_channel(spark):
    rows = [
        ("P_ACC", "2026-01-01T00:00:00", "10.0"),
        ("P_ACC", "2026-01-01T00:00:01", "11.0"),
        ("P_ACC", "2026-01-01T00:00:02", "12.0"),
        ("P_ACC", "2026-01-01T00:00:03", "13.0"),
        ("P_ACC", "2026-01-01T00:00:04", "14.0"),
        ("P_ACC", "2026-01-01T00:00:05", "15.0"),
    ]
    raw_df = spark.createDataFrame(rows, schema="parameter_name string, timestamp_utc string, parameter_value string")
    datatype_profile_df = spark.createDataFrame(
        [("P_ACC", "numeric")],
        schema="parameter_name string, parameter_datatype_profiled string",
    )

    profile_df = build_parameter_behavior_profile_table(raw_df, datatype_profile_df)
    row = profile_df.collect()[0].asDict()
    assert row["parameter_name"] == "P_ACC"
    assert row["accumulative_score_profiled"] > 0.0
    assert row["behavior_family_profiled"] in {"accumulative", "mixed_unknown"}
