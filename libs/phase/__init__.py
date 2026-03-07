# File: libs/phase/__init__.py
"""Phase detection package."""

from libs.phase.detect import build_structure_vectors, detect_phases_from_windows, evaluate_detected_phases
from libs.phase.diagnostics import compute_phase_behavior_diagnostics
from libs.phase.pipeline import (
    PHASE_BASELINES_SCHEMA,
    PHASE_WINDOWS_SCHEMA,
    build_phase_artifact_tables,
    build_phase_artifacts_from_window_x_table,
    build_phase_baselines_spark_table,
    build_phase_windows_spark_table,
    fit_phase_window_x_config,
)
from libs.windows import build_window_x_spark_table, build_window_x_table

__all__ = [
    "build_structure_vectors",
    "build_phase_artifact_tables",
    "build_phase_artifacts_from_window_x_table",
    "build_phase_baselines_spark_table",
    "build_phase_windows_spark_table",
    "build_window_x_spark_table",
    "build_window_x_table",
    "compute_phase_behavior_diagnostics",
    "detect_phases_from_windows",
    "evaluate_detected_phases",
    "fit_phase_window_x_config",
    "PHASE_BASELINES_SCHEMA",
    "PHASE_WINDOWS_SCHEMA",
]
