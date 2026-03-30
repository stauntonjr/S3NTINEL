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
    "libs/scoring/rules.py",
    "pipelines/10_parameter_profiles_fit.py",
    "pipelines/12_behavior_profiles_fit.py",
    "pipelines/15_event_profiles_fit.py",
    "pipelines/20_events_extract.py",
    "libs/windows/policy_profile.py",
    "pipelines/25_window_policy_profile.py",
    "pipelines/30_windows_adaptive.py",
    "pipelines/80_window_scores_raw.py",
]

WAVE2_FILES = [
    "libs/backbone/fit.py",
    "libs/graph/event.py",
    "libs/graph/lag.py",
    "pipelines/40_backbone_fit.py",
    "libs/graph/pipeline.py",
    "libs/graph/transition.py",
    "libs/graph/hierarchy_artifacts.py",
]

# `pipelines/50_build_graph.py` still routes graph evaluation through
# `libs.graph.evaluation`, which uses `toPandas()` for small artifact/report
# construction. Keep that stage out of this contract until the evaluation path is
# refactored off the driver bridge.

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


def _read_repo_file(relative_path: str) -> str:
    path = REPO_ROOT / relative_path
    assert path.exists(), f"hot-path contract references missing file: {relative_path}"
    return path.read_text()


def test_wave1_hot_path_modules_do_not_use_python_dataframe_bridges() -> None:
    for relative_path in HOT_PATH_FILES:
        content = _read_repo_file(relative_path)
        for token in FORBIDDEN_TOKENS:
            assert token not in content, f"{relative_path} still contains forbidden hot-path boundary: {token}"


def test_wave2_backbone_modules_do_not_use_pandas_dataframe_bridges() -> None:
    for relative_path in WAVE2_FILES:
        content = _read_repo_file(relative_path)
        for token in FORBIDDEN_PANDAS_TOKENS:
            assert token not in content, f"{relative_path} still contains forbidden backbone hot-path boundary: {token}"


def test_wave3_phase_modules_do_not_use_grouped_pandas_pair_count_bridges() -> None:
    for relative_path in WAVE3_FILES:
        content = _read_repo_file(relative_path)
        for token in ["udf(", "applyInPandas", "mapInPandas", "toPandas("]:
            assert token not in content, f"{relative_path} still contains forbidden phase hot-path boundary: {token}"
