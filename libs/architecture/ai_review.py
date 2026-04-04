"""Prompt packaging for optional AI architecture review."""

from __future__ import annotations

import json
from pathlib import Path

from libs.architecture.model import ArchitectureBundle


AI_REVIEW_FILENAMES = (
    "architecture_ai_review_input.json",
    "architecture_ai_review_prompt.md",
    "architecture_ai_draft_annotations.yaml",
)


def render_ai_review_prompt(bundle: ArchitectureBundle) -> str:
    lines = [
        "# Architecture AI Review Prompt",
        "",
        "Review the attached architecture facts and propose conservative improvements to the checked-in annotations.",
        "",
        "Goals:",
        "- normalize container and component naming",
        "- improve descriptions where package READMEs or docs make intent clearer",
        "- suggest missing external systems or data stores",
        "- identify oversized or hub-like components that may justify refactoring",
        "",
        "Constraints:",
        "- do not invent systems or flows unsupported by code or local docs",
        "- keep the current container boundaries unless the facts strongly contradict them",
        "- treat LOC skew as a signal, not proof of a design problem",
        "- output reviewable YAML edits only; do not rewrite generated artifacts directly",
        "",
        "Context:",
        f"- Workspace: {bundle.annotations.workspace_name}",
        f"- System LOC: {bundle.metrics.system_loc_span}",
        f"- Top component share: {bundle.metrics.top_component_loc_share:.2%}",
        f"- Top three component share: {bundle.metrics.top_three_component_loc_share:.2%}",
        "",
        "Suggested output sections:",
        "1. Annotation changes",
        "2. New or revised relationships",
        "3. Refactoring candidates justified by LOC or dependency skew",
        "",
    ]
    return "\n".join(lines)


def render_draft_annotations(bundle: ArchitectureBundle) -> str:
    return "\n".join(
        [
            "# Draft annotation edits",
            "# This file is a review scratchpad produced by `ai-draft`.",
            "# Promote changes into `annotations.yaml` only after review.",
            "",
            "suggested_changes: []",
            "",
        ]
    )


def write_ai_review_bundle(bundle: ArchitectureBundle, output_dir: Path) -> list[str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    files = {
        "architecture_ai_review_input.json": json.dumps(bundle.to_payload(), indent=2, sort_keys=True) + "\n",
        "architecture_ai_review_prompt.md": render_ai_review_prompt(bundle),
        "architecture_ai_draft_annotations.yaml": render_draft_annotations(bundle),
    }
    written: list[str] = []
    for name, content in files.items():
        (output_dir / name).write_text(content, encoding="utf-8")
        written.append(name)
    return written

