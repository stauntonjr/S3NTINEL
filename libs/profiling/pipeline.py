"""Class-oriented profiling artifact builders for the active Spark path."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from libs.profiling.profiles import (
    ContinuousScalingProfile,
    ParameterBehaviorPrimitiveProfile,
    ParameterBehaviorProfile,
    ParameterDatatypeProfile,
    ParameterProfile,
    TelemetryProfileSource,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

@dataclass(frozen=True)
class TelemetryProfilingArtifacts:
    datatype_profile: ParameterDatatypeProfile
    scaling_profile: ContinuousScalingProfile
    primitive_profile: ParameterBehaviorPrimitiveProfile
    behavior_profile: ParameterBehaviorProfile


@dataclass(frozen=True)
class TelemetryProfilingPlan:
    source: TelemetryProfileSource
    numeric_ratio_threshold: float = 0.8
    categorical_cardinality_max: int = 200
    behavior_significant_diff_threshold: float = ParameterBehaviorPrimitiveProfile.NUMERIC_SIGNIFICANT_DIFF_THRESHOLD
    behavior_center_band_width: float = ParameterBehaviorPrimitiveProfile.CENTER_BAND_WIDTH
    behavior_soft_bound_width: float = ParameterBehaviorPrimitiveProfile.SOFT_BOUND_WIDTH
    behavior_hard_bound_width: float = ParameterBehaviorPrimitiveProfile.HARD_BOUND_WIDTH
    behavior_mixed_unknown_low_score_threshold: float = 0.38
    behavior_mixed_unknown_ambiguous_score_threshold: float = 0.55
    behavior_mixed_unknown_ambiguous_margin_threshold: float = 0.03

    @classmethod
    def from_raw_input(
        cls,
        raw_input_df: "DataFrame",
        *,
        numeric_ratio_threshold: float = 0.8,
        categorical_cardinality_max: int = 200,
        behavior_significant_diff_threshold: float = ParameterBehaviorPrimitiveProfile.NUMERIC_SIGNIFICANT_DIFF_THRESHOLD,
        behavior_center_band_width: float = ParameterBehaviorPrimitiveProfile.CENTER_BAND_WIDTH,
        behavior_soft_bound_width: float = ParameterBehaviorPrimitiveProfile.SOFT_BOUND_WIDTH,
        behavior_hard_bound_width: float = ParameterBehaviorPrimitiveProfile.HARD_BOUND_WIDTH,
        behavior_mixed_unknown_low_score_threshold: float = 0.38,
        behavior_mixed_unknown_ambiguous_score_threshold: float = 0.55,
        behavior_mixed_unknown_ambiguous_margin_threshold: float = 0.03,
    ) -> "TelemetryProfilingPlan":
        return cls(
            source=TelemetryProfileSource(raw_input_df=raw_input_df),
            numeric_ratio_threshold=float(numeric_ratio_threshold),
            categorical_cardinality_max=int(categorical_cardinality_max),
            behavior_significant_diff_threshold=float(behavior_significant_diff_threshold),
            behavior_center_band_width=float(behavior_center_band_width),
            behavior_soft_bound_width=float(behavior_soft_bound_width),
            behavior_hard_bound_width=float(behavior_hard_bound_width),
            behavior_mixed_unknown_low_score_threshold=float(behavior_mixed_unknown_low_score_threshold),
            behavior_mixed_unknown_ambiguous_score_threshold=float(behavior_mixed_unknown_ambiguous_score_threshold),
            behavior_mixed_unknown_ambiguous_margin_threshold=float(behavior_mixed_unknown_ambiguous_margin_threshold),
        )

    def parameter_profile_df(self) -> "DataFrame":
        return ParameterProfile.build_dataframe(
            self.source.raw_input_df,
            numeric_ratio_threshold=float(self.numeric_ratio_threshold),
            categorical_cardinality_max=int(self.categorical_cardinality_max),
        )

    def build_datatype_profile(self) -> ParameterDatatypeProfile:
        return ParameterDatatypeProfile.from_parameter_profile(self.parameter_profile_df())

    def build_scaling_profile(self, datatype_profile: "ParameterDatatypeProfile | DataFrame") -> ContinuousScalingProfile:
        datatype_profile_df = (
            datatype_profile.to_dataframe() if isinstance(datatype_profile, ParameterDatatypeProfile) else datatype_profile
        )
        return ContinuousScalingProfile.from_raw_input(self.source.raw_input_df, datatype_profile_df)

    def build_behavior_primitive_profile(
        self,
        datatype_profile: "ParameterDatatypeProfile | DataFrame",
        scaling_profile: "ContinuousScalingProfile | DataFrame",
    ) -> ParameterBehaviorPrimitiveProfile:
        datatype_profile_df = (
            datatype_profile.to_dataframe() if isinstance(datatype_profile, ParameterDatatypeProfile) else datatype_profile
        )
        scaling_profile_df = (
            scaling_profile.to_dataframe() if isinstance(scaling_profile, ContinuousScalingProfile) else scaling_profile
        )
        return ParameterBehaviorPrimitiveProfile.from_raw_input(
            self.source.raw_input_df,
            datatype_profile_df,
            scaling_profile_df,
            significant_diff_threshold=float(self.behavior_significant_diff_threshold),
            center_band_width=float(self.behavior_center_band_width),
            soft_bound_width=float(self.behavior_soft_bound_width),
            hard_bound_width=float(self.behavior_hard_bound_width),
        )

    def build_behavior_profile(
        self,
        primitive_profile: "ParameterBehaviorPrimitiveProfile | DataFrame",
    ) -> ParameterBehaviorProfile:
        primitive_profile_df = (
            primitive_profile.to_dataframe()
            if isinstance(primitive_profile, ParameterBehaviorPrimitiveProfile)
            else primitive_profile
        )
        return ParameterBehaviorProfile.from_primitive_profile_with_thresholds(
            primitive_profile_df,
            mixed_unknown_low_score_threshold=float(self.behavior_mixed_unknown_low_score_threshold),
            mixed_unknown_ambiguous_score_threshold=float(self.behavior_mixed_unknown_ambiguous_score_threshold),
            mixed_unknown_ambiguous_margin_threshold=float(self.behavior_mixed_unknown_ambiguous_margin_threshold),
        )

    def build(self) -> TelemetryProfilingArtifacts:
        datatype_profile = self.build_datatype_profile()
        scaling_profile = self.build_scaling_profile(datatype_profile)
        primitive_profile = self.build_behavior_primitive_profile(datatype_profile, scaling_profile)
        behavior_profile = self.build_behavior_profile(primitive_profile)
        return TelemetryProfilingArtifacts(
            datatype_profile=datatype_profile,
            scaling_profile=scaling_profile,
            primitive_profile=primitive_profile,
            behavior_profile=behavior_profile,
        )
