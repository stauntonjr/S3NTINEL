from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]

HOT_PATH_FILES = [
    "libs/profiling/profiles.py",
    "libs/events/pipeline.py",
    "libs/events/continuous.py",
    "libs/events/categorical.py",
    "libs/windows/pipeline.py",
    "libs/windows/features.py",
    "libs/scoring/tables.py",
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
]

# `pipelines/50_build_graph.py` still routes graph evaluation through
# `libs.graph.evaluation`, which uses `toPandas()` for small artifact/report
# construction. Keep that stage out of this contract until the evaluation path is
# refactored off the driver bridge.
#
# `libs/graph/hierarchy_artifacts.py` remains a single canonical bounded local
# clustering step for hierarchy rollup. This contract targets duplicate local
# modeling paths, not the current internal implementation strategy of that one
# canonical owner.

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

REMOVED_SCORING_FILES = [
    "libs/scoring/artifacts.py",
    "libs/scoring/rules.py",
]

REMOVED_LOCAL_SCORING_SYMBOLS = [
    "WindowScoreArtifacts",
    "build_phase_window_score_baselines",
    "score_phase_window_rows",
]

AUTHORITATIVE_DOC_FILES = [
    "README.md",
    "docs/README.md",
    "docs/current/computational_complexity_report.md",
    "libs/README.md",
    "libs/scoring/README.md",
    "libs/graph/README.md",
]


def _read_repo_file(relative_path: str) -> str:
    path = REPO_ROOT / relative_path
    assert path.exists(), f"hot-path contract references missing file: {relative_path}"
    return path.read_text()


def _iter_repo_python_files(*relative_roots: str) -> list[Path]:
    files: list[Path] = []
    for relative_root in relative_roots:
        root = REPO_ROOT / relative_root
        if root.is_file():
            files.append(root)
            continue
        files.extend(sorted(root.rglob("*.py")))
    return files


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


def test_removed_local_scoring_files_do_not_exist() -> None:
    for relative_path in REMOVED_SCORING_FILES:
        assert not (REPO_ROOT / relative_path).exists(), f"duplicate local scoring file still exists: {relative_path}"


def test_repo_python_code_does_not_reference_removed_local_scoring_symbols() -> None:
    current_file = Path(__file__).resolve()
    for path in _iter_repo_python_files("libs", "pipelines", "tests"):
        if path == current_file:
            continue
        content = path.read_text()
        for symbol in REMOVED_LOCAL_SCORING_SYMBOLS:
            assert symbol not in content, f"{path.relative_to(REPO_ROOT)} still references removed local scoring symbol: {symbol}"


def test_graph_public_surface_is_spark_canonical() -> None:
    content = _read_repo_file("libs/graph/__init__.py")
    for symbol in [
        "EventGraph",
        "EventGraphSpec",
        "FusedGraph",
        "FusedGraphSpec",
        "GraphHierarchy",
        "PrecisionGraph",
        "PrecisionGraphSpec",
        "TransitionGraph",
        "TransitionGraphSpec",
    ]:
        assert re.search(rf"\b{symbol}\b", content) is None, f"libs/graph/__init__.py still exports local graph symbol: {symbol}"


def test_authoritative_docs_do_not_describe_removed_local_modeling_paths() -> None:
    for relative_path in AUTHORITATIVE_DOC_FILES:
        content = _read_repo_file(relative_path)
        for symbol in REMOVED_LOCAL_SCORING_SYMBOLS:
            assert symbol not in content, f"{relative_path} still references removed local scoring API: {symbol}"
    scoring_readme = _read_repo_file("libs/scoring/README.md")
    assert "in-memory score assembly" not in scoring_readme
    assert "rules.py" not in scoring_readme
    readme = _read_repo_file("README.md")
    assert "Stage 60 also emits `subsystem_scores`" not in readme


def test_dead_scoring_config_surface_is_removed() -> None:
    assert "combine_pvalues" not in _read_repo_file("conf/defaults.yaml")
    assert "pvalue_combine" not in _read_repo_file("pipelines/80_window_scores_raw.py")


def test_computational_complexity_report_covers_time_and_space_and_current_paths() -> None:
    content = _read_repo_file("docs/current/computational_complexity_report.md")
    assert "time-complexity" in content
    assert "space-complexity" in content
    assert "Space / materialization envelope" in content
    for stale_path in [
        "libs/scoring/pipeline.py",
        "libs/conformal/pipeline.py",
        "libs/anomaly/artifacts.py",
        "libs/anomaly/subsystem_context.py",
        "libs/anomaly/panel_context.py",
    ]:
        assert stale_path not in content, f"computational complexity report still references stale path: {stale_path}"


def test_docs_readme_lists_doc_subdirectories() -> None:
    content = _read_repo_file("docs/README.md")
    for directory_name in [
        "current/",
        "reference/",
        "design/",
        "simulation/",
        "plans/",
        "research/",
        "architecture/",
    ]:
        assert directory_name in content, f"docs/README.md is missing docs subdirectory section: {directory_name}"


def test_simulation_guidance_requires_hardware_preflight_before_spark_profile_selection() -> None:
    required_hardware_checks = ["nproc", "free -h", "df -h"]
    for relative_path in [
        ".codex/skills/resume-repository/SKILL.md",
        ".codex/skills/repo-agent-loop/SKILL.md",
        "scripts/README.md",
    ]:
        content = _read_repo_file(relative_path)
        for expected_check in required_hardware_checks:
            assert expected_check in content, f"{relative_path} is missing hardware preflight check: {expected_check}"

    agent_guidance = _read_repo_file("AGENTS.md")
    for expected_resource in ["CPU count", "total/available memory", "swap", "free space"]:
        assert expected_resource in agent_guidance, f"AGENTS.md is missing hardware guidance: {expected_resource}"


def test_plan_docs_have_index_and_status_headers() -> None:
    plans_index = _read_repo_file("docs/plans/README.md")
    assert "libs/README.md" in plans_index

    library_plans_index = _read_repo_file("docs/plans/libs/README.md")
    for expected_plan in [
        "anomaly.md",
        "simulation.md",
        "phase.md",
        "windows.md",
    ]:
        assert expected_plan in library_plans_index, f"docs/plans/libs/README.md is missing plan reference: {expected_plan}"
    for expected_plan in [
        "anomaly.md",
        "simulation.md",
        "phase.md",
        "windows.md",
    ]:
        assert expected_plan in plans_index, f"docs/plans/README.md is missing plan reference: {expected_plan}"

    root_plan_files = sorted(path.name for path in (REPO_ROOT / "docs" / "plans").glob("*.md"))
    assert root_plan_files == ["README.md"], f"docs/plans root should only contain README.md, found: {root_plan_files}"

    plans_dir = REPO_ROOT / "docs" / "plans"
    for path in sorted(plans_dir.rglob("*.md")):
        if path.name == "README.md":
            continue
        content = path.read_text()
        assert "Status: Plan" in content, f"{path.relative_to(REPO_ROOT)} is missing plan status header"
        assert "Authority: Non-authoritative roadmap." in content, (
            f"{path.relative_to(REPO_ROOT)} is missing plan authority header"
        )


def test_simulation_medium_term_plan_does_not_reference_removed_duplicate_paths() -> None:
    content = _read_repo_file("docs/plans/libs/simulation.md")
    for stale_path in [
        "libs/scoring/pipeline.py",
        "pandas and Spark boundaries are still duplicated across",
    ]:
        assert stale_path not in content, f"simulation medium-term plan still references stale path: {stale_path}"
