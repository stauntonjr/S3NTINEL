"""Window scoring package for the active V2 pipeline."""

from libs.scoring.model import WindowScoreArtifacts
from libs.scoring.pipeline import build_window_scores_raw_spark_table, build_window_scores_raw_table
from libs.scoring.validator import extract_fault_truth_windows, summarize_fault_window_detection, validate_scores_against_fault_windows
from libs.scoring.window_scores import build_phase_score_baselines, score_window_s_rows

__all__ = [
    "build_phase_score_baselines",
    "build_window_scores_raw_spark_table",
    "build_window_scores_raw_table",
    "extract_fault_truth_windows",
    "WindowScoreArtifacts",
    "score_window_s_rows",
    "summarize_fault_window_detection",
    "validate_scores_against_fault_windows",
]
