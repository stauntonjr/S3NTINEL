from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]

HOT_PATH_FILES = [
    "libs/profiling/profiles.py",
    "libs/events/pipeline.py",
    "libs/events/continuous.py",
    "libs/events/categorical.py",
    "libs/windows/pipeline.py",
    "libs/windows/features.py",
    "libs/scoring/pipeline.py",
    "pipelines/10_parameter_profiles_fit.py",
    "pipelines/20_events_extract.py",
    "pipelines/30_windows_adaptive.py",
    "pipelines/80_window_scores_raw.py",
]

WAVE2_FILES = [
    "libs/backbone/pipeline.py",
    "libs/graph/event.py",
    "libs/graph/lag.py",
    "pipelines/40_backbone_fit.py",
    "libs/graph/pipeline.py",
    "libs/graph/transition.py",
    "libs/graph/hierarchy_artifacts.py",
    "pipelines/50_build_graph.py",
]

WAVE3_FILES = [
    "libs/phase/pipeline.py",
]

FORBIDDEN_TOKENS = [
    "applyInPandas",
    "mapInPandas",
    "toPandas(",
    ".collect(",
    "pandas_udf",
]

FORBIDDEN_PANDAS_TOKENS = [
    "applyInPandas",
    "mapInPandas",
    "toPandas(",
]


def test_wave1_hot_path_modules_do_not_use_python_dataframe_bridges() -> None:
    for relative_path in HOT_PATH_FILES:
        content = (REPO_ROOT / relative_path).read_text()
        for token in FORBIDDEN_TOKENS:
            assert token not in content, f"{relative_path} still contains forbidden hot-path boundary: {token}"


def test_wave2_backbone_modules_do_not_use_pandas_dataframe_bridges() -> None:
    for relative_path in WAVE2_FILES:
        content = (REPO_ROOT / relative_path).read_text()
        for token in FORBIDDEN_PANDAS_TOKENS:
            assert token not in content, f"{relative_path} still contains forbidden backbone hot-path boundary: {token}"


def test_wave3_phase_modules_do_not_use_grouped_pandas_pair_count_bridges() -> None:
    for relative_path in WAVE3_FILES:
        content = (REPO_ROOT / relative_path).read_text()
        for token in ["udf(", "applyInPandas", "mapInPandas", "toPandas("]:
            assert token not in content, f"{relative_path} still contains forbidden phase hot-path boundary: {token}"
