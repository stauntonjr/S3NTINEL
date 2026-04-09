# File: libs/windows/__init__.py
"""Adaptive windowing package."""

from libs.windows.buffer import WindowSensorBuffer
from libs.windows.coverage import WindowCoverageSampler
from libs.windows.window import Window, WindowPolicy
from libs.windows.pipeline import (
    AdaptiveWindowArtifactSet,
    AdaptiveWindowPlan,
    AdaptiveWindowPolicy,
    AdaptiveWindowSegmentState,
    AdaptiveWindowTransition,
    OpenWindowState,
    build_window_coverage_timestamps,
)
from libs.windows.policy_profile import (
    WindowPolicyEvaluationSpec,
    WindowPolicyProfile,
    WindowPolicyProfileSpec,
    build_window_truth_phase_coverage_summary,
    build_window_policy_profile_evaluation_report_spark,
)
from libs.windows.features import WindowFeaturesDiagnostics, WindowFeaturesPlan, WindowFeatureVectorSpec
from libs.windows.tables import WindowFeaturesTable, WindowPolicyProfileTable, WindowProfileRowsFrame, WindowsTable

__all__ = [
    "build_window_policy_profile_evaluation_report_spark",
    "build_window_truth_phase_coverage_summary",
    "build_window_coverage_timestamps",
    "AdaptiveWindowPolicy",
    "OpenWindowState",
    "AdaptiveWindowSegmentState",
    "AdaptiveWindowTransition",
    "AdaptiveWindowPlan",
    "AdaptiveWindowArtifactSet",
    "WindowFeatureVectorSpec",
    "WindowFeaturesDiagnostics",
    "WindowFeaturesPlan",
    "WindowFeaturesTable",
    "WindowPolicyProfileTable",
    "WindowProfileRowsFrame",
    "WindowSensorBuffer",
    "WindowPolicy",
    "WindowsTable",
    "Window",
    "WindowCoverageSampler",
    "WindowPolicyEvaluationSpec",
    "WindowPolicyProfile",
    "WindowPolicyProfileSpec",
]
