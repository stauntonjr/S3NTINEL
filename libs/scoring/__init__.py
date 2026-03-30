"""Window scoring package for the active V2 pipeline."""

from libs.scoring.artifacts import WindowScoreArtifacts
from libs.scoring.rules import build_phase_window_score_baselines, score_phase_window_rows
from libs.scoring.tables import WindowScoresCalibratedTable, WindowScoresRawTable
from libs.scoring.validator import (
    extract_fault_truth_windows,
    extract_misbehavior_truth_windows,
    summarize_fault_window_detection,
    summarize_misbehavior_window_detection,
    validate_scores_against_fault_windows,
    validate_scores_against_misbehavior_windows,
)

__all__ = [
    "build_phase_window_score_baselines",
    "extract_fault_truth_windows",
    "extract_misbehavior_truth_windows",
    "WindowScoresCalibratedTable",
    "WindowScoreArtifacts",
    "WindowScoresRawTable",
    "score_phase_window_rows",
    "summarize_fault_window_detection",
    "summarize_misbehavior_window_detection",
    "validate_scores_against_fault_windows",
    "validate_scores_against_misbehavior_windows",
]
