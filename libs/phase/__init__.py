# File: libs/phase/__init__.py
"""Phase detection package."""

from libs.phase.analysis import analyze_phase_behavior
from libs.phase.model import PhaseFeatureConfig, PhaseFeatures
from libs.phase.pipeline import (
    build_phase_artifact_tables,
    build_phase_features_from_window_features_dataframe,
    build_phase_baselines_spark_table,
    build_phase_windows_spark_table,
    fit_phase_feature_config,
    fit_phase_feature_config_from_spark,
)
from libs.phase.runtime import (
    Phase,
    PhaseBuffer,
    PhaseClusterAssignment,
    PhaseClustering,
    PhaseDetectionPolicy,
    PhaseStream,
)
from libs.phase.validator import (
    build_phase_validation_assignments,
    evaluate_detected_phases,
    validate_detected_phases_from_tables,
)
from libs.windows import build_window_features_spark_dataframe, build_window_features_dataframe

__all__ = [
    "build_phase_artifact_tables",
    "build_phase_features_from_window_features_dataframe",
    "build_phase_baselines_spark_table",
    "build_phase_windows_spark_table",
    "build_window_features_spark_dataframe",
    "build_window_features_dataframe",
    "analyze_phase_behavior",
    "evaluate_detected_phases",
    "Phase",
    "PhaseBuffer",
    "PhaseClusterAssignment",
    "PhaseClustering",
    "PhaseDetectionPolicy",
    "PhaseFeatureConfig",
    "PhaseFeatures",
    "PhaseStream",
    "fit_phase_feature_config",
    "fit_phase_feature_config_from_spark",
    "build_phase_validation_assignments",
    "validate_detected_phases_from_tables",
]
