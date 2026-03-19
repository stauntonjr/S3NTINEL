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
    build_windows_table,
)
from libs.windows.features import (
    WindowFeaturesDiagnostics,
    WindowFeaturesPlan,
    WindowFeatureVectorSpec,
    build_window_features_spark_table,
    build_window_features_with_diagnostics_spark_table,
)

__all__ = [
    "build_window_features_spark_table",
    "build_window_features_with_diagnostics_spark_table",
    "build_windows_table",
    "AdaptiveWindowPolicy",
    "OpenWindowState",
    "AdaptiveWindowSegmentState",
    "AdaptiveWindowTransition",
    "AdaptiveWindowPlan",
    "AdaptiveWindowArtifactSet",
    "WindowFeatureVectorSpec",
    "WindowFeaturesDiagnostics",
    "WindowFeaturesPlan",
    "WindowSensorBuffer",
    "WindowPolicy",
    "Window",
    "WindowCoverageSampler",
]
