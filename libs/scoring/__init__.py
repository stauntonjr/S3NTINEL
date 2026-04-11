"""Window scoring package for the active V2 pipeline."""

from libs.scoring.channels import (
    BOUND_VIOLATION_CHANNEL,
    COHERENCE_BREAK_CHANNEL,
    EVENT_DISCORDANCE_CHANNEL,
    RECONSTRUCTION_ERROR_CHANNEL,
    REGIME_DEVIATION_CHANNEL,
    RESPONSE_VIOLATION_CHANNEL,
    SCORE_COMPONENT_NAMES,
    STATE_VIOLATION_CHANNEL,
    dominant_score_component,
    score_component_scores_with_updates,
    zero_score_component_scores,
)
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
    "BOUND_VIOLATION_CHANNEL",
    "COHERENCE_BREAK_CHANNEL",
    "EVENT_DISCORDANCE_CHANNEL",
    "extract_fault_truth_windows",
    "extract_misbehavior_truth_windows",
    "RECONSTRUCTION_ERROR_CHANNEL",
    "REGIME_DEVIATION_CHANNEL",
    "RESPONSE_VIOLATION_CHANNEL",
    "SCORE_COMPONENT_NAMES",
    "STATE_VIOLATION_CHANNEL",
    "dominant_score_component",
    "score_component_scores_with_updates",
    "WindowScoresCalibratedTable",
    "WindowScoresRawTable",
    "zero_score_component_scores",
    "summarize_fault_window_detection",
    "summarize_misbehavior_window_detection",
    "validate_scores_against_fault_windows",
    "validate_scores_against_misbehavior_windows",
]
