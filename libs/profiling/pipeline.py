"""Fitting-stage profiling artifact adapters for the active V2 path."""

from __future__ import annotations

from typing import TYPE_CHECKING

from libs.profiling.model import (
    ContinuousScalingProfile,
    ParameterBehaviorProfile,
    ParameterDatatypeProfile,
    ParameterProfile,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def build_parameter_datatype_profile_table(raw_input_df: "DataFrame") -> "DataFrame":
    """Build the canonical datatype/rate profile artifact."""
    return ParameterDatatypeProfile.from_parameter_profile(ParameterProfile.build_dataframe(raw_input_df))


def build_continuous_scaling_profile_table(raw_input_df: "DataFrame", datatype_profile_df: "DataFrame") -> "DataFrame":
    """Build robust scaling metadata for continuous parameters."""
    return ContinuousScalingProfile.build_dataframe(raw_input_df, datatype_profile_df)


def build_parameter_behavior_profile_table(
    raw_input_df: "DataFrame",
    datatype_profile_df: "DataFrame",
) -> "DataFrame":
    """Build the canonical behavior profile artifact from observed telemetry."""
    return ParameterBehaviorProfile.build_dataframe(raw_input_df, datatype_profile_df)
