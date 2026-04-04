from __future__ import annotations

from pathlib import Path

from libs.architecture.extract import build_architecture_bundle
from libs.architecture.render import (
    render_core_library_semantics_markdown,
    render_metrics_markdown,
    render_pipeline_layered_architecture_markdown,
    render_pipeline_stage_catalog_markdown,
    render_workspace_dsl,
)


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _seed_repo(tmp_path: Path) -> Path:
    root = tmp_path
    _write(root / "README.md", "# Demo Repo\n\nTop-level summary.\n")
    _write(root / "libs" / "README.md", "# Libraries\n\n## Purpose\n\nReusable libraries.\n")
    _write(root / "libs" / "alpha" / "README.md", "# Alpha\n\n## Purpose\n\nAlpha domain logic.\n")
    _write(
        root / "libs" / "alpha" / "__init__.py",
        '"""Alpha package."""\n',
    )
    _write(
        root / "libs" / "alpha" / "core.py",
        "\n".join(
            [
                '"""Alpha core."""',
                "",
                "from dataclasses import dataclass",
                "",
                "@dataclass(frozen=True)",
                "class AlphaSpec:",
                '    """Immutable semantic spec for alpha processing."""',
                "    threshold: float = 0.5",
                "    label: str = 'alpha'",
                "",
                "class Engine:",
                "    def run(self) -> int:",
                "        value = 1",
                "        return value",
                "",
                "def helper() -> int:",
                "    return 1",
                "",
            ]
        ),
    )
    _write(root / "pipelines" / "README.md", "# Pipelines\n\n## Purpose\n\nPipeline entrypoints.\n")
    _write(
        root / "pipelines" / "00_ingest_raw.py",
        "\n".join(
            [
                '"""Ingest stage."""',
                "",
                "from libs.alpha.core import helper",
                "",
                "def run() -> int:",
                "    return helper()",
                "",
            ]
        ),
    )
    _write(
        root / "pipelines" / "97_run_full_pipeline.py",
        "\n".join(
            [
                '"""Run full pipeline."""',
                "",
                "from libs.alpha.core import Engine",
                "",
                "def run() -> int:",
                "    return Engine().run()",
                "",
            ]
        ),
    )
    _write(
        root / "pipelines" / "10_parameter_profiles_fit.py",
        "\n".join(
            [
                '"""Profile stage."""',
                "",
                "from libs.alpha.core import Engine",
                "",
                "def run() -> int:",
                "    return Engine().run()",
                "",
            ]
        ),
    )
    _write(root / "scripts" / "README.md", "# Scripts\n\n## Purpose\n\nUtility scripts.\n")
    _write(
        root / "scripts" / "run_sim_pipeline.py",
        "\n".join(
            [
                '"""Simulation CLI."""',
                "",
                "from libs.alpha.core import helper",
                "",
                "def main() -> int:",
                "    return helper()",
                "",
            ]
        ),
    )
    _write(root / "tools" / "__init__.py", '"""Tools."""\n')
    _write(
        root / "docs" / "v2_architecture.md",
        "# Demo Architecture\n\n## Purpose\n\nArchitecture notes.\n",
    )
    _write(
        root / "docs" / "simulation_codepath_wire_diagram.md",
        "# Demo Simulation Flow\n\nSimulation path.\n",
    )
    _write(
        root / "docs" / "simulation_architecture.md",
        "# Demo Simulation Architecture\n\nSimulation architecture notes.\n",
    )
    _write(
        root / "docs" / "architecture" / "annotations.yaml",
        "\n".join(
            [
                "workspace:",
                "  name: Demo Architecture",
                "  description: Demo system.",
                "focus_paths:",
                "  - libs/",
                "  - pipelines/",
                "  - scripts/",
                "doc_paths:",
                "  - README.md",
                "  - docs/v2_architecture.md",
                "people:",
                "  - id: engineer",
                "    name: Engineer",
                "    description: Runs the system.",
                "containers:",
                "  - id: pipeline_runtime",
                "    name: Pipeline Runtime",
                "    description: Pipeline entrypoints.",
                "    technology: Python",
                "    selectors:",
                "      module_prefixes:",
                "        - pipelines",
                "    components:",
                "      - id: grouped_runners",
                "        name: Grouped Runners",
                "        description: Pipeline runner.",
                "        technology: Python",
                "        selectors:",
                "          module_names:",
                "            - pipelines.00_ingest_raw",
                "            - pipelines.10_parameter_profiles_fit",
                "            - pipelines.97_run_full_pipeline",
                "  - id: simulation_clis",
                "    name: Simulation CLIs",
                "    description: Simulation scripts.",
                "    technology: Python",
                "    selectors:",
                "      path_prefixes:",
                "        - scripts/",
                "    components:",
                "      - id: simulation_runner",
                "        name: Simulation Runner",
                "        description: Runs the simulation.",
                "        technology: Python",
                "        selectors:",
                "          module_names:",
                "            - scripts.run_sim_pipeline",
                "  - id: core_libraries",
                "    name: Core Libraries",
                "    description: Reusable libraries.",
                "    technology: Python",
                "    selectors:",
                "      module_prefixes:",
                "        - libs",
                "    auto_components:",
                "      group_by: second_segment",
                "      prefix: libs.",
                "      include_groups:",
                "        - alpha",
                "manual_relationships:",
                "  - source: engineer",
                "    destination: pipeline_runtime",
                "    description: Uses the pipeline.",
                "views:",
                "  pipeline_component_include:",
                "    - pipeline_runtime.grouped_runners",
                "  core_library_component_include:",
                "    - core_libraries.alpha",
                "",
            ]
        ),
    )
    return root


def test_build_architecture_bundle_rolls_up_loc_and_relationships(tmp_path):
    root = _seed_repo(tmp_path)

    bundle = build_architecture_bundle(root, root / "docs" / "architecture" / "annotations.yaml")

    element_by_id = {element.id: element for element in bundle.elements}

    assert element_by_id["core_libraries.alpha"].loc_span > 0
    assert element_by_id["pipeline_runtime.grouped_runners"].loc_span > 0
    assert any(
        relationship.source_id == "pipeline_runtime.grouped_runners"
        and relationship.destination_id == "core_libraries.alpha"
        for relationship in bundle.relationships
    )
    assert bundle.metrics.system_loc_span >= element_by_id["core_libraries.alpha"].loc_span


def test_render_outputs_include_loc_metadata_and_component_views(tmp_path):
    root = _seed_repo(tmp_path)
    bundle = build_architecture_bundle(root, root / "docs" / "architecture" / "annotations.yaml")

    dsl = render_workspace_dsl(bundle)
    metrics_markdown = render_metrics_markdown(bundle)
    library_semantics_markdown = render_core_library_semantics_markdown(bundle)
    layered_markdown = render_pipeline_layered_architecture_markdown(bundle)
    stage_catalog_markdown = render_pipeline_stage_catalog_markdown(bundle)

    assert 'component s3ntinel.core_libraries "core_library_components"' in dsl
    assert 'component s3ntinel.pipeline_runtime "pipeline_layers"' in dsl
    assert 'softwareSystem "Core Library Semantics"' in dsl
    assert 'component semantics.alpha_semantics "alpha_semantics"' in dsl
    assert 'Alpha Semantics' in dsl
    assert 'Modules grouped under `libs.config`.' not in dsl
    assert "stage_00_ingest_raw" in dsl
    assert '"00 Ingest Raw"' in dsl
    assert '"Ingest stage."' in dsl
    assert 'Flows into next pipeline layer" "Pipeline order"' in dsl
    assert '"loc_span"' in dsl
    assert '"purpose"' in dsl
    assert '"function_count"' in dsl
    assert '"class_count"' in dsl
    assert '"library_layers" "Alpha"' in dsl
    assert '"library_layer_summary" "Alpha: Alpha domain logic"' in dsl
    assert "core_libraries.alpha" in dsl
    assert "Largest Modules" in metrics_markdown
    assert "libs/alpha/core.py" in metrics_markdown
    assert "Core Library Semantics" in library_semantics_markdown
    assert "## Alpha" in library_semantics_markdown
    assert "AlphaSpec" in library_semantics_markdown
    assert "Immutable semantic spec for alpha processing" in library_semantics_markdown
    assert "Carries threshold: float = 0.5, label: str = 'alpha'." in library_semantics_markdown
    assert "| Field | Type | Default | Role |" in library_semantics_markdown
    assert "| threshold | float | 0.5 | model parameter or coefficient |" in library_semantics_markdown
    assert "| label | str | 'alpha' | descriptive or categorical value |" in library_semantics_markdown
    assert "Layered Pipeline Architecture" in layered_markdown
    assert "Layer 1: 00 Ingest Raw" in layered_markdown
    assert "Purpose: Ingest stage." in layered_markdown
    assert "Functions: 1 | Classes: 0" in layered_markdown
    assert "pipelines/97_run_full_pipeline.py" not in layered_markdown
    assert "Pipeline Stage Catalog" in stage_catalog_markdown
    assert "| Order | Stage | Module | Wrapper Purpose | Library Layers | Layer Summary | LOC | Functions | Classes | Detail View |" in stage_catalog_markdown
    assert "| 1 | 00 Ingest Raw | `pipelines.00_ingest_raw` | Ingest stage." in stage_catalog_markdown
    assert "| Alpha | Alpha: Alpha domain logic |" in stage_catalog_markdown
    assert "`pipeline_stage_00_ingest_raw`" in stage_catalog_markdown
