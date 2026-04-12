from __future__ import annotations

import json
from pathlib import Path
from typing import Any

VALIDATION_REPORT_FILENAMES = (
    "profile_validation_summary.json",
    "event_validation_summary.json",
    "label_contract_summary.json",
    "phase_validation_summary.json",
    "hierarchy_validation_summary.json",
    "coupling_validation_summary.json",
    "score_validation_summary.json",
    "misbehavior_score_validation_summary.json",
    "misbehavior_window_validation_summary.json",
    "misbehavior_attribution_validation_summary.json",
    "fault_window_validation_summary.json",
    "attribution_validation_summary.json",
    "simulation_benchmark_audit_summary.json",
)

OPTIONAL_PIPELINE_SUMMARY_FILENAMES = (
    "profile_pipeline_run_summary.json",
    "event_pipeline_run_summary.json",
    "structural_pipeline_run_summary.json",
    "pipeline_run_summary.json",
)


def load_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def load_text_if_exists(path: Path) -> str | None:
    if not path.exists():
        return None
    return path.read_text(encoding="utf-8")


def path_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return int(path.stat().st_size)
    return sum(int(child.stat().st_size) for child in path.rglob("*") if child.is_file())
