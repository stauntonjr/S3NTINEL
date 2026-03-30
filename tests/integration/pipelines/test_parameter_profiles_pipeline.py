from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from libs.profiling import (
    ContinuousScalingProfile,
    ParameterBehaviorPrimitiveProfile,
    ParameterBehaviorProfile,
    ParameterDatatypeProfile,
    ParameterProfile,
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

    result_df = ParameterDatatypeProfile.from_parameter_profile(
        ParameterProfile.build_dataframe(raw_df)
    ).to_dataframe()
    assert {
        "parameter_name",
        "parameter_datatype_profiled",
        "sampling_rate_profiled_hz",
        "median_interval_ms",
    }.issubset(result_df.columns)
    result = {row["parameter_name"]: row["parameter_datatype_profiled"] for row in result_df.select("parameter_name", "parameter_datatype_profiled").collect()}
    assert result["P_NUM"] == "numeric"
    assert result["P_STATE"] == "categorical"


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
            ("P_STATE", "categorical"),
        ],
        schema="parameter_name string, parameter_datatype_profiled string",
    )

    scaling_df = ContinuousScalingProfile.from_raw_input(raw_df, datatype_profile_df).to_dataframe()
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
            ("P_STATE", "categorical"),
        ],
        schema="parameter_name string, parameter_datatype_profiled string",
    )

    scaling_df = ContinuousScalingProfile.from_raw_input(raw_df, datatype_profile_df).to_dataframe()
    primitive_df = ParameterBehaviorPrimitiveProfile.from_raw_input(raw_df, datatype_profile_df, scaling_df).to_dataframe()
    profile_df = ParameterBehaviorProfile.from_primitive_profile(primitive_df).to_dataframe()
    rows = {row["parameter_name"]: row.asDict() for row in profile_df.collect()}
    assert rows["P_STATE"]["behavior_family_profiled"] == "discrete_state"
    assert rows["P_STATE"]["discrete_state_score_profiled"] > 0.0
    assert rows["P_REG"]["behavior_family_profiled"] in {"regulated", "tracking", "inertial", "accumulative", "mixed_unknown"}
    assert rows["P_REG"]["sample_count"] == 4
    assert "persistent_run_strength_profiled" in rows["P_REG"]


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

    scaling_df = ContinuousScalingProfile.from_raw_input(raw_df, datatype_profile_df).to_dataframe()
    primitive_df = ParameterBehaviorPrimitiveProfile.from_raw_input(raw_df, datatype_profile_df, scaling_df).to_dataframe()
    profile_df = ParameterBehaviorProfile.from_primitive_profile(primitive_df).to_dataframe()
    row = profile_df.collect()[0].asDict()
    assert row["parameter_name"] == "P_ACC"
    assert row["accumulative_score_profiled"] > 0.0
    assert row["accumulative_score_profiled"] > row["inertial_score_profiled"]
    assert row["behavior_family_profiled"] == "accumulative"


def test_build_parameter_behavior_profile_table_prefers_regulated_over_inertial_for_mean_reverting_channel(spark):
    rows = [
        ("P_REG", "2026-01-01T00:00:00", "100.0"),
        ("P_REG", "2026-01-01T00:00:01", "100.6"),
        ("P_REG", "2026-01-01T00:00:02", "99.7"),
        ("P_REG", "2026-01-01T00:00:03", "100.3"),
        ("P_REG", "2026-01-01T00:00:04", "99.8"),
        ("P_REG", "2026-01-01T00:00:05", "100.1"),
        ("P_REG", "2026-01-01T00:00:06", "99.9"),
        ("P_REG", "2026-01-01T00:00:07", "100.0"),
    ]
    raw_df = spark.createDataFrame(rows, schema="parameter_name string, timestamp_utc string, parameter_value string")
    datatype_profile_df = spark.createDataFrame(
        [("P_REG", "numeric")],
        schema="parameter_name string, parameter_datatype_profiled string",
    )

    scaling_df = ContinuousScalingProfile.from_raw_input(raw_df, datatype_profile_df).to_dataframe()
    primitive_df = ParameterBehaviorPrimitiveProfile.from_raw_input(raw_df, datatype_profile_df, scaling_df).to_dataframe()
    profile_df = ParameterBehaviorProfile.from_primitive_profile(primitive_df).to_dataframe()
    row = profile_df.collect()[0].asDict()
    assert row["parameter_name"] == "P_REG"
    assert row["regulated_score_profiled"] >= row["inertial_score_profiled"]
    assert row["behavior_family_profiled"] in {"regulated", "tracking"}


def test_build_parameter_behavior_primitive_profile_table_emits_tracking_evidence(spark):
    rows = [
        ("P_TRACK", "2026-01-01T00:00:00", "0.0"),
        ("P_TRACK", "2026-01-01T00:00:01", "1.0"),
        ("P_TRACK", "2026-01-01T00:00:02", "2.0"),
        ("P_TRACK", "2026-01-01T00:00:03", "1.2"),
        ("P_TRACK", "2026-01-01T00:00:04", "1.0"),
        ("P_TRACK", "2026-01-01T00:00:05", "1.05"),
    ]
    raw_df = spark.createDataFrame(rows, schema="parameter_name string, timestamp_utc string, parameter_value string")
    datatype_profile_df = spark.createDataFrame([("P_TRACK", "numeric")], schema="parameter_name string, parameter_datatype_profiled string")
    scaling_df = ContinuousScalingProfile.from_raw_input(raw_df, datatype_profile_df).to_dataframe()

    primitive_df = ParameterBehaviorPrimitiveProfile.from_raw_input(raw_df, datatype_profile_df, scaling_df).to_dataframe()
    row = primitive_df.collect()[0].asDict()
    assert row["parameter_name"] == "P_TRACK"
    assert row["tracking_recovery_score_profiled"] is not None
    assert row["lagged_response_score_profiled"] is not None
