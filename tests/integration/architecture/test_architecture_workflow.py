from __future__ import annotations

import json
from pathlib import Path

from tools import architecture_workflow


def test_architecture_workflow_renders_and_checks_live_repo(tmp_path):
    root = Path(__file__).resolve().parents[3]
    output_dir = tmp_path / "architecture"
    annotations_path = root / "docs" / "architecture" / "annotations.yaml"

    written = architecture_workflow.run_render(root, output_dir, annotations_path)

    assert "workspace.dsl" in written
    assert (output_dir / "workspace.dsl").exists()
    assert (output_dir / "architecture_metrics.md").exists()
    assert (output_dir / "core_library_semantics.md").exists()
    assert (output_dir / "pipeline_stage_catalog.md").exists()

    payload = json.loads((output_dir / "architecture_facts.json").read_text(encoding="utf-8"))
    element_ids = {element["id"] for element in payload["elements"]}
    module_names = {module["module"] for module in payload["modules"]}
    workspace_dsl = (output_dir / "workspace.dsl").read_text(encoding="utf-8")
    assert "pipeline_runtime" in element_ids
    assert "core_libraries" in element_ids
    assert "simulation_clis" in element_ids
    assert "scripts.run_sim_pipeline" in module_names
    assert payload["metrics"]["system_loc_span"] > 0
    assert 'softwareSystem "Core Library Semantics"' in workspace_dsl
    assert 'component semantics.phase_semantics "phase_semantics"' in workspace_dsl

    assert architecture_workflow.run_check(root, output_dir, annotations_path) == 0


def test_architecture_workflow_ai_draft_writes_review_bundle(tmp_path):
    root = Path(__file__).resolve().parents[3]
    output_dir = tmp_path / "architecture"
    annotations_path = root / "docs" / "architecture" / "annotations.yaml"

    written = architecture_workflow.run_ai_draft(root, output_dir, annotations_path)

    assert "ai_review/architecture_ai_review_input.json" in written
    assert (output_dir / "ai_review" / "architecture_ai_review_prompt.md").exists()
