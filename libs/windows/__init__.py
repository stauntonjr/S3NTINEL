# File: libs/windows/__init__.py
"""Adaptive windowing package."""

from libs.windows.representations import (
    build_continuous_robust_scaler,
    build_window_s_rows,
    build_window_x_row,
    top_window_cooccurrence_sensor_pairs,
    top_categorical_state_pairs,
    top_phase_event_types,
)
from libs.windows.pipeline import build_windows_table
from libs.windows.window_x import WINDOW_X_SCHEMA, build_window_x_spark_table, build_window_x_table
from libs.windows.sampling import sample_windows_for_coverage

__all__ = [
    "build_continuous_robust_scaler",
    "build_window_s_rows",
    "build_window_x_row",
    "build_window_x_spark_table",
    "build_window_x_table",
    "build_windows_table",
    "WINDOW_X_SCHEMA",
    "sample_windows_for_coverage",
    "top_window_cooccurrence_sensor_pairs",
    "top_categorical_state_pairs",
    "top_phase_event_types",
]
