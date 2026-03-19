# File: libs/phase/__init__.py
"""Phase detection package."""

from libs.phase.analysis import analyze_phase_behavior
from libs.phase.feature_config import PhaseFeatureConfig
from libs.phase.pipeline import (
    PhaseArtifactSet,
    PhaseClusterModel,
    PhaseDetectionPlan,
    PhaseFeatureFrame,
    build_phase_baselines_spark_table,
    build_phase_windows_spark_table,
    fit_phase_feature_config_from_spark,
    fit_phase_feature_config_with_diagnostics_from_spark,
)
from libs.phase.validator import (
    build_phase_validation_assignments,
    evaluate_detected_phases,
    validate_detected_phases_from_tables,
)

__all__ = [
    "PhaseArtifactSet",
    "PhaseClusterModel",
    "PhaseDetectionPlan",
    "PhaseFeatureFrame",
    "build_phase_baselines_spark_table",
    "build_phase_windows_spark_table",
    "analyze_phase_behavior",
    "evaluate_detected_phases",
    "PhaseFeatureConfig",
    "fit_phase_feature_config_from_spark",
    "fit_phase_feature_config_with_diagnostics_from_spark",
    "build_phase_validation_assignments",
    "validate_detected_phases_from_tables",
]
