"""Class-oriented profiling artifact builders for the active Spark path."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from libs.profiling.profiles import (
    ContinuousScalingProfile,
    ParameterBehaviorProfile,
    ParameterDatatypeProfile,
    ParameterProfile,
    TelemetryProfileSource,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

@dataclass(frozen=True)
class TelemetryProfilingArtifacts:
    datatype_profile_df: "DataFrame"
    scaling_profile_df: "DataFrame"
    behavior_profile_df: "DataFrame"


@dataclass(frozen=True)
class TelemetryProfilingPlan:
    source: TelemetryProfileSource

    def parameter_profile_df(self) -> "DataFrame":
        return ParameterProfile.build_dataframe(self.source.raw_input_df)

    def build_datatype_profile(self) -> "DataFrame":
        return ParameterDatatypeProfile.from_parameter_profile(self.parameter_profile_df())

    def build_scaling_profile(self, datatype_profile_df: "DataFrame") -> "DataFrame":
        return ContinuousScalingProfile.build_dataframe(self.source.raw_input_df, datatype_profile_df)

    def build_behavior_profile(self, datatype_profile_df: "DataFrame") -> "DataFrame":
        return ParameterBehaviorProfile.build_dataframe(self.source.raw_input_df, datatype_profile_df)

    def build(self) -> TelemetryProfilingArtifacts:
        datatype_profile_df = self.build_datatype_profile()
        scaling_profile_df = self.build_scaling_profile(datatype_profile_df)
        behavior_profile_df = self.build_behavior_profile(datatype_profile_df)
        return TelemetryProfilingArtifacts(
            datatype_profile_df=datatype_profile_df,
            scaling_profile_df=scaling_profile_df,
            behavior_profile_df=behavior_profile_df,
        )


def build_parameter_datatype_profile_table(raw_input_df: "DataFrame") -> "DataFrame":
    """Stage adapter for the canonical datatype/rate profile artifact."""
    return TelemetryProfilingPlan(TelemetryProfileSource(raw_input_df)).build_datatype_profile()


def build_continuous_scaling_profile_table(raw_input_df: "DataFrame", datatype_profile_df: "DataFrame") -> "DataFrame":
    """Stage adapter for robust scaling metadata for continuous parameters."""
    return TelemetryProfilingPlan(TelemetryProfileSource(raw_input_df)).build_scaling_profile(datatype_profile_df)


def build_parameter_behavior_profile_table(
    raw_input_df: "DataFrame",
    datatype_profile_df: "DataFrame",
) -> "DataFrame":
    """Stage adapter for the canonical behavior profile artifact."""
    return TelemetryProfilingPlan(TelemetryProfileSource(raw_input_df)).build_behavior_profile(datatype_profile_df)
