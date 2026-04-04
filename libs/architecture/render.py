"""Rendering helpers for architecture artifacts."""

from __future__ import annotations

import csv
import io
import json
import re
from dataclasses import dataclass
from pathlib import Path

from libs.architecture.model import ArchitectureBundle, ArchitectureElement, CodeClassFact, CodeClassFieldFact


RENDERED_FILENAMES = (
    "workspace.dsl",
    "architecture_metrics.json",
    "architecture_metrics.md",
    "core_library_semantics.md",
    "pipeline_data_flow.md",
    "pipeline_layered_architecture.md",
    "pipeline_stage_catalog.md",
    "taxonomy_diagram.md",
    "view_index.md",
    "lucid_nodes.csv",
    "lucid_edges.csv",
)

SECONDARY_LIBRARY_GROUPS = {
    "common",
    "config",
    "pyspark",
    "spark_sequence",
    "testing",
}


@dataclass(frozen=True)
class PipelineStageLibraryLayer:
    group_id: str
    display_name: str
    summary: str
    module_count: int
    loc_span: int
    direct: bool


@dataclass(frozen=True)
class PipelineStageRenderFact:
    index: int
    module_name: str
    module_path: str
    display_name: str
    component_id: str
    view_id: str
    purpose: str
    loc_span: int
    function_count: int
    class_count: int
    library_layers: tuple[PipelineStageLibraryLayer, ...]


@dataclass(frozen=True)
class SemanticDataclassRow:
    module_name: str
    module_label: str
    class_fact: CodeClassFact
    class_name: str
    kind: str
    summary: str
    payload_shape: str
    field_count: int
    loc_span: int


def _pipeline_stage_modules(bundle: ArchitectureBundle) -> list:
    stages = []
    for module in bundle.modules:
        if not module.module.startswith("pipelines.") or not module.path.endswith(".py"):
            continue
        filename = module.path.split("/")[-1]
        stage_prefix = filename.split("_", 1)[0]
        if not stage_prefix.isdigit():
            continue
        stage_number = int(stage_prefix)
        if stage_number >= 97:
            continue
        stages.append((stage_number, module))
    stages.sort(key=lambda item: (item[0], item[1].path))
    return [module for _, module in stages]


def _dsl_identifier(name: str) -> str:
    chars: list[str] = []
    for char in name:
        if char.isalnum() or char == "_":
            chars.append(char.lower())
        else:
            chars.append("_")
    out = "".join(chars).strip("_")
    return out or "element"


def _dsl_ref(element: ArchitectureElement) -> str:
    if element.kind == "software_system":
        return _dsl_identifier(element.id)
    if element.kind == "container":
        return f"s3ntinel.{_dsl_identifier(element.id)}"
    if element.kind == "component":
        parent_id = element.parent_id or ""
        local_id = element.id.split(".")[-1]
        return f"s3ntinel.{_dsl_identifier(parent_id)}.{_dsl_identifier(local_id)}"
    return _dsl_identifier(element.id)


def _dsl_properties(properties: dict[str, str], *, indent: str = "      ") -> list[str]:
    if not properties:
        return []
    lines = [f"{indent}properties {{"]
    for key, value in sorted(properties.items()):
        lines.append(f'{indent}  "{key}" "{value}"')
    lines.append(f"{indent}}}")
    return lines


def _pipeline_stage_component_id(module_path: str) -> str:
    filename = module_path.split("/")[-1]
    return f"stage_{_dsl_identifier(filename[:-3])}"


def _pipeline_stage_display_name(module_path: str) -> str:
    stem = module_path.split("/")[-1][:-3]
    parts = stem.split("_")
    if not parts:
        return stem
    stage_number = parts[0]
    title = " ".join(part.capitalize() for part in parts[1:]) if len(parts) > 1 else "Stage"
    return f"{stage_number} {title}".strip()


def _summarize_text(text: str | None, *, fallback: str) -> str:
    if not text:
        return fallback
    cleaned = " ".join(text.strip().split())
    return cleaned or fallback


def _titleize_identifier(value: str) -> str:
    spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value.replace("_", " ").replace("-", " "))
    return " ".join(spaced.split()).title()


def _summary_clause(text: str) -> str:
    clause = " ".join(text.replace("`", "").split())
    if clause.endswith("."):
        clause = clause[:-1]
    return clause


def _library_group_id(module_name: str) -> str | None:
    parts = module_name.split(".")
    if len(parts) < 2 or parts[0] != "libs":
        return None
    return parts[1]


def _library_group_summary_lookup(bundle: ArchitectureBundle) -> dict[str, str]:
    summaries: dict[str, str] = {}
    for element in bundle.elements:
        if element.kind != "component" or element.parent_id != "core_libraries":
            continue
        group_id = element.id.split(".")[-1]
        summaries[group_id] = _summary_clause(element.description)

    for package_doc in bundle.package_docs:
        if not package_doc.package.startswith("libs."):
            continue
        group_id = package_doc.package.split(".")[1]
        if group_id in summaries:
            continue
        if package_doc.summary:
            summaries[group_id] = _summary_clause(package_doc.summary)
    return summaries


def _transitive_internal_dependencies(bundle: ArchitectureBundle, root_module_name: str) -> tuple[str, ...]:
    module_lookup = {module.module: module for module in bundle.modules}
    seen: set[str] = set()
    queue = [root_module_name]
    ordered: list[str] = []

    while queue:
        current = queue.pop(0)
        current_module = module_lookup.get(current)
        if current_module is None:
            continue
        for dependency in current_module.internal_dependencies:
            if dependency in seen:
                continue
            if dependency not in module_lookup:
                continue
            seen.add(dependency)
            ordered.append(dependency)
            queue.append(dependency)
    return tuple(ordered)


def _stage_library_layers(bundle: ArchitectureBundle, stage_module_name: str) -> tuple[PipelineStageLibraryLayer, ...]:
    module_lookup = {module.module: module for module in bundle.modules}
    summary_lookup = _library_group_summary_lookup(bundle)
    direct_groups = {
        group_id
        for dependency in module_lookup[stage_module_name].internal_dependencies
        for group_id in [_library_group_id(dependency)]
        if group_id is not None
    }

    grouped_modules: dict[str, list] = {}
    for dependency in _transitive_internal_dependencies(bundle, stage_module_name):
        group_id = _library_group_id(dependency)
        if group_id is None:
            continue
        grouped_modules.setdefault(group_id, []).append(module_lookup[dependency])

    layers: list[PipelineStageLibraryLayer] = []
    for group_id, modules in grouped_modules.items():
        ordered_modules = sorted(modules, key=lambda item: item.module)
        layers.append(
            PipelineStageLibraryLayer(
                group_id=group_id,
                display_name=_titleize_identifier(group_id),
                summary=summary_lookup.get(
                    group_id,
                    f"{_titleize_identifier(group_id)} library functionality.",
                ),
                module_count=len(ordered_modules),
                loc_span=sum(module.span.span_loc for module in ordered_modules),
                direct=group_id in direct_groups,
            )
        )
    layers.sort(
        key=lambda item: (
            item.group_id in SECONDARY_LIBRARY_GROUPS,
            not item.direct,
            -item.loc_span,
            item.display_name,
        )
    )
    return tuple(layers)


def _stage_description(
    *,
    module_name: str,
    loc_span: int,
    function_count: int,
    class_count: int,
    library_layers: tuple[PipelineStageLibraryLayer, ...],
) -> str:
    del module_name, loc_span, function_count, class_count, library_layers
    return ""


def _pipeline_stage_facts(bundle: ArchitectureBundle) -> tuple[PipelineStageRenderFact, ...]:
    facts: list[PipelineStageRenderFact] = []
    for index, stage in enumerate(_pipeline_stage_modules(bundle), start=1):
        stem = stage.path.split("/")[-1][:-3]
        purpose = _summarize_text(
            stage.doc,
            fallback=f"Pipeline layer for `{stage.module}`.",
        )
        library_layers = _stage_library_layers(bundle, stage.module)
        facts.append(
            PipelineStageRenderFact(
                index=index,
                module_name=stage.module,
                module_path=stage.path,
                display_name=_pipeline_stage_display_name(stage.path),
                component_id=_pipeline_stage_component_id(stage.path),
                view_id=f"pipeline_stage_{_dsl_identifier(stem)}",
                purpose=purpose,
                loc_span=stage.span.span_loc,
                function_count=len(stage.functions),
                class_count=len(stage.classes),
                library_layers=library_layers,
            )
        )
    return tuple(facts)


def _render_pipeline_stage_components(
    stage_facts: tuple[PipelineStageRenderFact, ...],
) -> tuple[list[str], list[str], list[str]]:
    stage_lines: list[str] = []
    relationship_lines: list[str] = []
    include_lines: list[str] = []
    previous_component_id: str | None = None

    for stage in stage_facts:
        description = stage.purpose
        stage_lines.append(
            '        '
            f'{stage.component_id} = component "{stage.display_name}" "{description}" "Python CLI + PySpark" {{'
        )
        for property_line in _dsl_properties(
            {
                "class_count": str(stage.class_count),
                "function_count": str(stage.function_count),
                "library_layers": ", ".join(layer.display_name for layer in stage.library_layers),
                "library_layer_summary": "; ".join(
                    f"{layer.display_name}: {layer.summary}" for layer in stage.library_layers[:4]
                ),
                "loc_span": str(stage.loc_span),
                "module_name": stage.module_name,
                "purpose": stage.purpose,
                "source_paths": stage.module_path,
            },
            indent="        ",
        ):
            stage_lines.append(property_line)
        stage_lines.append("        }")
        include_lines.append(f"      include s3ntinel.pipeline_runtime.{stage.component_id}")
        if previous_component_id is not None:
            relationship_lines.append(
                "    s3ntinel.pipeline_runtime."
                f"{previous_component_id} -> s3ntinel.pipeline_runtime.{stage.component_id} "
                '"Flows into next pipeline layer" "Pipeline order"'
            )
        previous_component_id = stage.component_id

    return stage_lines, relationship_lines, include_lines


def _semantic_system_container_id(component: ArchitectureElement) -> str:
    return f"{component.id.split('.')[-1]}_semantics"


def _semantic_component_id(class_name: str) -> str:
    return _dsl_identifier(class_name)


def _render_semantic_software_system(
    bundle: ArchitectureBundle,
    core_components: tuple[ArchitectureElement, ...],
) -> tuple[list[str], list[str]]:
    lines = [
        '    semantics = softwareSystem "Core Library Semantics" "Synthetic semantic views for core-library dataclasses and payload shapes." {'
    ]
    view_lines: list[str] = []

    for component in core_components:
        rows = _semantic_dataclass_rows(bundle, component)
        if not rows:
            continue
        group_id = component.id.split(".")[-1]
        container_id = _semantic_system_container_id(component)
        description = _core_library_component_description(component)
        lines.append(
            f'      {container_id} = container "{component.name} Semantics" "{description}" "Python Dataclasses" {{'
        )
        for property_line in _dsl_properties(
            {
                "dataclass_count": str(len(rows)),
                "source_component": component.id,
            },
            indent="        ",
        ):
            lines.append(property_line)

        visible_rows = rows[:15]
        remaining = len(rows) - len(visible_rows)
        for row in visible_rows:
            lines.append(
                '        '
                f'{_semantic_component_id(row.class_name)} = component "{row.class_name}" '
                f'"{row.summary}. {row.payload_shape}" "Dataclass" {{'
            )
            for property_line in _dsl_properties(
                {
                    "field_count": str(row.field_count),
                    "module_name": row.module_name,
                    "payload_shape": row.payload_shape,
                    "semantic_kind": row.kind,
                },
                indent="        ",
            ):
                lines.append(property_line)
            lines.append("        }")
        if remaining > 0:
            lines.append(
                f'        additional_dataclasses = component "Additional Dataclasses" "{remaining} more dataclasses are cataloged in core_library_semantics.md." "Generated Summary"'
            )
        lines.append("      }")

        view_lines.extend(
            [
                f'    component semantics.{container_id} "{group_id}_semantics" {{',
                "      include *",
                "      autoLayout tb",
                "    }",
                "",
            ]
        )

    lines.append("    }")
    return lines, view_lines


def render_workspace_dsl(bundle: ArchitectureBundle) -> str:
    elements = {element.id: element for element in bundle.elements}
    stage_facts = _pipeline_stage_facts(bundle)
    containers = [element for element in bundle.elements if element.kind == "container"]
    core_components = tuple(
        element
        for element in bundle.elements
        if element.kind == "component" and element.parent_id == "core_libraries"
    )
    components_by_container: dict[str, list[ArchitectureElement]] = {}
    for element in bundle.elements:
        if element.kind != "component" or element.parent_id is None:
            continue
        components_by_container.setdefault(element.parent_id, []).append(element)

    lines = [
        f'workspace "{bundle.annotations.workspace_name}" "{bundle.annotations.workspace_description}" {{',
        "  !identifiers hierarchical",
        "",
        "  model {",
    ]

    for element in bundle.elements:
        if element.kind != "person":
            continue
        lines.append(
            f'    {_dsl_identifier(element.id)} = person "{element.name}" "{element.description}"'
        )

    for element in bundle.elements:
        if element.kind == "external_system":
            lines.append(
                f'    {_dsl_identifier(element.id)} = softwareSystem "{element.name}" "{element.description}"'
            )
        if element.kind == "data_store":
            lines.append(
                f'    {_dsl_identifier(element.id)} = softwareSystem "{element.name}" "{element.description}"'
            )

    lines.append(
        f'    s3ntinel = softwareSystem "{bundle.annotations.workspace_name}" "{bundle.annotations.workspace_description}" {{'
    )
    for container in sorted(containers, key=lambda item: item.id):
        tech = f' "{container.technology}"' if container.technology else ""
        lines.append(
            f'      {_dsl_identifier(container.id)} = container "{container.name}" "{container.description}"{tech} {{'
        )
        for property_line in _dsl_properties(
            {
                **container.properties,
                "source_paths": ",".join(container.source_paths),
            },
            indent="        ",
        ):
            lines.append(property_line)
        for component in sorted(components_by_container.get(container.id, []), key=lambda item: item.id):
            local_id = component.id.split(".")[-1]
            tech = f' "{component.technology}"' if component.technology else ""
            description = (
                _core_library_component_description(component)
                if container.id == "core_libraries"
                else component.description
            )
            lines.append(
                f'        {_dsl_identifier(local_id)} = component "{component.name}" "{description}"{tech} {{'
            )
            for property_line in _dsl_properties(
                {
                    **component.properties,
                    "source_paths": ",".join(component.source_paths),
                },
                indent="        ",
            ):
                lines.append(property_line)
            lines.append("        }")
        if container.id == "pipeline_runtime":
            stage_lines, _, _ = _render_pipeline_stage_components(stage_facts)
            lines.extend(stage_lines)
        lines.append("      }")
    lines.append("    }")
    semantic_system_lines, semantic_view_lines = _render_semantic_software_system(bundle, core_components)
    lines.extend(semantic_system_lines)
    lines.append("")

    for relationship in bundle.relationships:
        source = elements.get(relationship.source_id)
        destination = elements.get(relationship.destination_id)
        if source is None or destination is None:
            continue
        tech = f' "{relationship.technology}"' if relationship.technology else ""
        description = relationship.description or "Uses"
        lines.append(
            f'    {_dsl_ref(source)} -> {_dsl_ref(destination)} "{description}"{tech}'
        )
    _, stage_relationship_lines, _ = _render_pipeline_stage_components(stage_facts)
    lines.extend(stage_relationship_lines)

    lines.extend(
        [
            "  }",
            "",
            "  views {",
            '    systemContext s3ntinel "system_context" {',
            "      include *",
            "      autoLayout lr",
            "    }",
            "",
            '    container s3ntinel "container_view" {',
            "      include *",
            "      autoLayout lr",
            "    }",
            "",
        ]
    )

    pipeline_components = bundle.annotations.views.pipeline_component_include or tuple(
        element.id for element in bundle.elements if element.parent_id == "pipeline_runtime"
    )
    lines.append('    component s3ntinel.pipeline_runtime "pipeline_components" {')
    for element_id in pipeline_components:
        if element_id in elements:
            lines.append(f"      include {_dsl_ref(elements[element_id])}")
    lines.append("      autoLayout lr")
    lines.append("    }")
    lines.append("")

    lines.append('    component s3ntinel.pipeline_runtime "pipeline_layers" {')
    _, _, stage_include_lines = _render_pipeline_stage_components(stage_facts)
    lines.extend(stage_include_lines)
    lines.append("      autoLayout tb")
    lines.append("    }")
    lines.append("")

    core_component_ids = bundle.annotations.views.core_library_component_include or tuple(
        element.id for element in bundle.elements if element.parent_id == "core_libraries"
    )
    lines.append('    component s3ntinel.core_libraries "core_library_components" {')
    for element_id in core_component_ids:
        if element_id in elements:
            lines.append(f"      include {_dsl_ref(elements[element_id])}")
    lines.append("      autoLayout lr")
    lines.append("    }")
    lines.append("")
    lines.extend(semantic_view_lines)
    lines.extend(
        [
            "    styles {",
            '      element "Software System" {',
            "        background #0b6e4f",
            "        color #ffffff",
            "      }",
            '      element "Container" {',
            "        background #145da0",
            "        color #ffffff",
            "      }",
            '      element "Component" {',
            "        background #f0f4f8",
            "        color #1f2933",
            "      }",
            '      element "Dataclass" {',
            "        background #fff3c4",
            "        color #5d3a00",
            "        shape roundedbox",
            "      }",
            '      element "Person" {',
            "        background #d9e2ec",
            "        color #102a43",
            "        shape person",
            "      }",
            "    }",
            "  }",
            "}",
            "",
        ]
    )
    return "\n".join(lines)


def render_metrics_markdown(bundle: ArchitectureBundle) -> str:
    metrics = bundle.metrics
    lines = [
        "# Architecture Metrics",
        "",
        f"- System AST span LOC: `{metrics.system_loc_span}`",
        f"- Largest module LOC share: `{metrics.top_module_loc_share:.2%}`",
        f"- Largest component LOC share: `{metrics.top_component_loc_share:.2%}`",
        f"- Top three components LOC share: `{metrics.top_three_component_loc_share:.2%}`",
        "",
        "## Largest Modules",
        "",
        "| Module | Path | LOC | Container | Component |",
        "| --- | --- | ---: | --- | --- |",
    ]
    for row in metrics.largest_modules:
        lines.append(
            "| {module} | {path} | {loc_span} | {container_id} | {component_id} |".format(
                module=row["module"],
                path=row["path"],
                loc_span=row["loc_span"],
                container_id=row["container_id"] or "",
                component_id=row["component_id"] or "",
            )
        )

    lines.extend(
        [
            "",
            "## Largest Classes",
            "",
            "| Qualified Name | Path | LOC |",
            "| --- | --- | ---: |",
        ]
    )
    for row in metrics.largest_classes:
        lines.append(
            "| {qualified_name} | {path} | {loc_span} |".format(
                qualified_name=row["qualified_name"],
                path=row["path"],
                loc_span=row["loc_span"],
            )
        )

    lines.extend(
        [
            "",
            "## Largest Functions And Methods",
            "",
            "| Qualified Name | Path | LOC |",
            "| --- | --- | ---: |",
        ]
    )
    for row in metrics.largest_functions:
        lines.append(
            "| {qualified_name} | {path} | {loc_span} |".format(
                qualified_name=row["qualified_name"],
                path=row["path"],
                loc_span=row["loc_span"],
            )
        )

    lines.extend(
        [
            "",
            "## Component Size Table",
            "",
            "| Component | Container | LOC | Module Count | System Share |",
            "| --- | --- | ---: | ---: | ---: |",
        ]
    )
    for row in metrics.component_size_table:
        lines.append(
            "| {name} | {container_id} | {loc_span} | {module_count} | {share:.2%} |".format(
                name=row["name"],
                container_id=row["container_id"],
                loc_span=row["loc_span"],
                module_count=row["module_count"],
                share=row["system_loc_share"],
            )
        )

    lines.extend(
        [
            "",
            "## Dependency Vs Size",
            "",
            "| Component | LOC | Incoming | Outgoing | Combined |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in metrics.dependency_size_table:
        lines.append(
            "| {name} | {loc_span} | {incoming} | {outgoing} | {combined} |".format(
                name=row["name"],
                loc_span=row["loc_span"],
                incoming=row["incoming_relationship_count"],
                outgoing=row["outgoing_relationship_count"],
                combined=row["combined_relationship_count"],
            )
        )
    lines.append("")
    return "\n".join(lines)


def render_pipeline_flow_markdown(bundle: ArchitectureBundle) -> str:
    stages = _pipeline_stage_modules(bundle)
    lines = [
        "# Pipeline Data Flow",
        "",
        "```mermaid",
        "flowchart LR",
        '    raw["Raw Telemetry / Simulation Output"]',
    ]
    previous = "raw"
    for index, stage in enumerate(stages, start=1):
        node = f"stage_{index}"
        lines.append(f'    {node}["{stage.path}"]')
        lines.append(f"    {previous} --> {node}")
        previous = node
    lines.append('    artifacts["Persisted Artifacts / Reports"]')
    lines.append(f"    {previous} --> artifacts")
    lines.append("```")
    lines.append("")
    return "\n".join(lines)


def render_pipeline_layered_architecture_markdown(bundle: ArchitectureBundle) -> str:
    stages = _pipeline_stage_facts(bundle)
    lines = [
        "# Layered Pipeline Architecture",
        "",
        "Each non-aggregate pipeline stage is shown as its own layer.",
        "Aggregate runners such as `97_run_fitting_pipeline.py`, `98_run_inference_pipeline.py`, and `99_run_full_pipeline.py` are intentionally excluded.",
        "",
        "```mermaid",
        "flowchart TB",
    ]
    previous_node = ""
    for stage in stages:
        node_name = f"layer_{stage.index}"
        label = (
            f"Layer {stage.index}: {stage.display_name}<br/>"
            f"Purpose: {stage.purpose}<br/>"
            f"Module: {stage.module_name}<br/>"
            f"LOC: {stage.loc_span} | Functions: {stage.function_count} | Classes: {stage.class_count}"
        )
        lines.append(f'    {node_name}["{label}"]')
        if previous_node:
            lines.append(f"    {previous_node} --> {node_name}")
        previous_node = node_name
    lines.append("```")
    lines.append("")
    return "\n".join(lines)


def render_pipeline_stage_catalog_markdown(bundle: ArchitectureBundle) -> str:
    lines = [
        "# Pipeline Stage Catalog",
        "",
        "Each non-aggregate pipeline stage is listed with the library layers it coordinates, the wrapper module, and the generated detail view id.",
        "",
        "| Order | Stage | Module | Wrapper Purpose | Library Layers | Layer Summary | LOC | Functions | Classes | Detail View |",
        "| ---: | --- | --- | --- | --- | --- | ---: | ---: | ---: | --- |",
    ]
    for stage in _pipeline_stage_facts(bundle):
        lines.append(
            "| {index} | {display_name} | `{module_name}` | {purpose} | {library_layers} | {layer_summary} | {loc_span} | {function_count} | {class_count} | `{view_id}` |".format(
                index=stage.index,
                display_name=stage.display_name,
                module_name=stage.module_name,
                purpose=stage.purpose,
                library_layers=", ".join(layer.display_name for layer in stage.library_layers),
                layer_summary="; ".join(
                    f"{layer.display_name}: {layer.summary}" for layer in stage.library_layers[:4]
                ),
                loc_span=stage.loc_span,
                function_count=stage.function_count,
                class_count=stage.class_count,
                view_id=stage.view_id,
            )
        )
    lines.append("")
    return "\n".join(lines)


def _is_dataclass_decorator(decorator_name: str) -> bool:
    return "dataclass" in decorator_name


def _semantic_kind(class_name: str) -> str:
    suffix_map = {
        "Table": "Table Artifact",
        "Frame": "Frame Artifact",
        "Spec": "Specification",
        "Plan": "Execution Plan",
        "Policy": "Policy",
        "Profile": "Profile",
        "Config": "Configuration",
        "Settings": "Configuration",
        "Model": "Model",
        "Bundle": "Bundle",
        "ArtifactSet": "Artifact Bundle",
        "Artifacts": "Artifact Bundle",
        "State": "Runtime State",
        "Event": "Domain Event",
    }
    for suffix, label in suffix_map.items():
        if class_name.endswith(suffix):
            return label
    return "Domain Dataclass"


def _semantic_summary(
    *,
    class_fact: CodeClassFact,
    class_doc: str | None,
    module_doc: str | None,
    library_summary: str,
) -> str:
    if class_doc:
        return _summary_clause(class_doc)

    stem = class_fact.name
    for suffix in (
        "ArtifactSet",
        "Settings",
        "Profile",
        "Config",
        "Table",
        "Frame",
        "Model",
        "Bundle",
        "Policy",
        "State",
        "Event",
        "Spec",
        "Plan",
    ):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)] or class_fact.name
            kind = _semantic_kind(class_fact.name).lower()
            return f"{kind} for {_titleize_identifier(stem)} within {library_summary.lower()}"
    if module_doc:
        return f"{_titleize_identifier(class_fact.name)} within {_summary_clause(module_doc).lower()}"
    return f"{_titleize_identifier(class_fact.name)} within {library_summary.lower()}"


def _field_type_label(field: CodeClassFieldFact) -> str:
    annotation = field.annotation.strip()
    if annotation:
        return annotation
    default = field.default.strip()
    if not default:
        return "unknown"
    if default in {"None", "null"}:
        return "optional"
    if default.startswith(("'", '"')):
        return "str"
    if default in {"True", "False"}:
        return "bool"
    if default.startswith(("[", "list(", "tuple(", "set(", "{")):
        return "collection"
    if default.startswith(("dict(", "{")):
        return "mapping"
    if default.replace(".", "", 1).isdigit():
        return "number"
    return "derived"


def _field_display(field: CodeClassFieldFact) -> str:
    detail = field.name
    annotation = field.annotation.strip()
    if annotation:
        detail += f": {annotation}"
    default = field.default.strip()
    if default:
        detail += f" = {default}"
    return detail


def _payload_shape_summary(class_fact: CodeClassFact) -> str:
    if not class_fact.fields:
        return "No extracted dataclass fields."
    primary_fields = ", ".join(
        _field_display(field) for field in class_fact.fields[:4]
    )
    extra_count = len(class_fact.fields) - 4
    if extra_count > 0:
        primary_fields += f", +{extra_count} more"
    return f"Carries {primary_fields}."


def _field_semantic_role(field: CodeClassFieldFact) -> str:
    name = field.name.lower()
    annotation = _field_type_label(field).lower()
    artifact_reference_suffixes = (
        "_table",
        "_frame",
        "_bundle",
        "_artifact",
        "_artifacts",
        "_profile",
        "_profiles",
        "_graph",
        "_windows",
        "_window",
        "_events",
        "_baselines",
        "_centroids",
        "_features",
        "_attribution",
        "_backbone",
        "_hierarchy",
    )
    if name in {"partition_by", "partition_columns", "partition_keys"}:
        return "partitioning contract"
    if name.startswith(("selected_", "phase_selected_")):
        if "sensor" in name:
            return "selected sensor set"
        if "event" in name:
            return "selected event feature set"
        if "pair" in name:
            return "selected pair-feature set"
        return "selected feature set"
    if name in {"selected_sensors_c", "all_sensors"}:
        return "sensor set"
    if name.endswith(artifact_reference_suffixes):
        return "artifact or table reference"
    if any(token in name for token in ("model", "policy", "plan", "spec")) and annotation not in {"str", "literal"}:
        return "domain model or execution contract"
    if any(token in name for token in ("weight", "weights", "lambda", "alpha", "beta", "threshold")):
        return "model parameter or coefficient"
    if name.endswith(("_id", "id")):
        return "identity / key"
    if "path" in name:
        return "artifact path or location"
    if "time" in name or "timestamp" in name:
        return "temporal marker"
    if "score" in name or "weight" in name or "prob" in name:
        return "quantitative measure"
    if any(token in name for token in ("sensor", "feature", "event", "phase")) and annotation.startswith(("list", "tuple", "set")):
        return "domain feature set"
    if annotation.startswith(("list", "tuple", "set")) or annotation == "collection":
        return "ordered or grouped values"
    if annotation.startswith("dict") or annotation == "mapping":
        return "lookup or grouped mapping"
    if annotation.endswith(("model", "plan", "policy", "spec", "table", "frame")):
        return "domain model or execution contract"
    if annotation in {"str", "Literal", "optional"}:
        return "descriptive or categorical value"
    if annotation in {"int", "float", "number"}:
        return "numeric value"
    return "domain payload field"


def _module_basename(module_name: str) -> str:
    return module_name.rsplit(".", 1)[-1]


def _core_library_components(bundle: ArchitectureBundle) -> tuple[ArchitectureElement, ...]:
    return tuple(
        sorted(
            (
                element
                for element in bundle.elements
                if element.kind == "component" and element.parent_id == "core_libraries"
            ),
            key=lambda item: item.id,
        )
    )


def _core_library_component_description(component: ArchitectureElement) -> str:
    description = _summary_clause(component.description)
    generic_prefixes = (
        "Modules grouped under libs.",
        "This package contains the simulation domain model only. It does not own",
        "This package answers",
    )
    if description.startswith(generic_prefixes):
        fallback_map = {
            "config": "Owns typed pipeline execution settings, artifact path resolution, and stage-level configuration dataclasses",
            "pyspark": "Owns typed Spark frame and table wrappers used at repository boundaries",
            "reporting": "Owns thin report payload wrappers used by generated summaries and diagnostics",
            "spark_sequence": "Owns deterministic sequence ordering and segmentation policies for long Spark streams",
            "simulation": "Owns the simulation domain model: aircraft, coupling, faults, fleets, phases, and runtime bundles",
            "tuning": "Owns benchmark search, objective specifications, comparable run ranking, and tuning reports",
        }
        group_id = component.id.split(".")[-1]
        return fallback_map.get(group_id, description)
    return description


def _semantic_dataclass_rows(
    bundle: ArchitectureBundle,
    component: ArchitectureElement,
) -> list[SemanticDataclassRow]:
    module_lookup = {module.module: module for module in bundle.modules}
    library_summary = _core_library_component_description(component)
    rows: list[SemanticDataclassRow] = []
    for module_name in component.assigned_modules:
        module = module_lookup.get(module_name)
        if module is None:
            continue
        for cls in module.classes:
            if not any(_is_dataclass_decorator(dec) for dec in cls.decorators):
                continue
            rows.append(
                SemanticDataclassRow(
                    module_name=module.module,
                    module_label=_module_basename(module.module),
                    class_fact=cls,
                    class_name=cls.name,
                    kind=_semantic_kind(cls.name),
                    summary=_semantic_summary(
                        class_fact=cls,
                        class_doc=cls.doc,
                        module_doc=module.doc,
                        library_summary=library_summary,
                    ),
                    payload_shape=_payload_shape_summary(cls),
                    field_count=len(cls.fields),
                    loc_span=cls.span.span_loc,
                )
            )
    rows.sort(key=lambda item: (-item.field_count, -item.loc_span, item.class_name))
    return rows


def _truncate_text(text: str, *, limit: int = 88) -> str:
    cleaned = " ".join(text.split())
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 3].rstrip() + "..."


def render_core_library_semantics_markdown(bundle: ArchitectureBundle) -> str:
    module_lookup = {module.module: module for module in bundle.modules}
    lines = [
        "# Core Library Semantics",
        "",
        "Each section summarizes one `core_libraries` component and the dataclasses it uses to represent domain semantics.",
        "",
    ]

    for component in _core_library_components(bundle):
        library_name = component.name
        library_slug = component.id.split(".")[-1]
        library_summary = _summary_clause(component.description)
        lines.extend(
            [
                f"## {library_name}",
                "",
                library_summary + ".",
                "",
            ]
        )

        dataclass_rows: list[dict[str, object]] = []
        for module_name in component.assigned_modules:
            module = module_lookup.get(module_name)
            if module is None:
                continue
            for cls in module.classes:
                if not any(_is_dataclass_decorator(dec) for dec in cls.decorators):
                    continue
                payload_shape = _payload_shape_summary(cls)
                dataclass_rows.append(
                    {
                        "class_fact": cls,
                        "module_name": module.module,
                        "module_label": _module_basename(module.module),
                        "class_name": cls.name,
                        "kind": _semantic_kind(cls.name),
                        "summary": _semantic_summary(
                            class_fact=cls,
                            class_doc=cls.doc,
                            module_doc=module.doc,
                            library_summary=library_summary,
                        ),
                        "payload_shape": payload_shape,
                        "field_count": len(cls.fields),
                        "loc_span": cls.span.span_loc,
                    }
                )

        if not dataclass_rows:
            lines.extend(
                [
                    "No dataclasses were detected in this library component.",
                    "",
                ]
            )
            continue

        lines.extend(
            [
                f"Dataclasses detected: `{len(dataclass_rows)}`",
                "",
                "```mermaid",
                "flowchart TB",
                f'    {_dsl_identifier(component.id)}["{library_name}\\n{_truncate_text(library_summary)}"]',
            ]
        )

        modules_seen: set[str] = set()
        for row in dataclass_rows:
            module_node = _dsl_identifier(f"{component.id}_{row['module_name']}")
            if row["module_name"] not in modules_seen:
                lines.append(
                    f'    {module_node}["{row["module_label"]}.py"]'
                )
                lines.append(
                    f"    {_dsl_identifier(component.id)} --> {module_node}"
                )
                modules_seen.add(row["module_name"])
            class_node = _dsl_identifier(
                f"{component.id}_{row['module_name']}_{row['class_name']}"
            )
            lines.append(
                f'    {class_node}["{row["class_name"]}\\n{_truncate_text(str(row["payload_shape"]), limit=72)}"]'
            )
            lines.append(f"    {module_node} --> {class_node}")

        lines.extend(
            [
                "```",
                "",
                "| Dataclass | Module | Semantic Kind | Represents | Payload Shape | Fields | LOC |",
                "| --- | --- | --- | --- | --- | ---: | ---: |",
            ]
        )
        for row in dataclass_rows:
            lines.append(
                "| {class_name} | `{module_name}` | {kind} | {summary} | {payload_shape} | {field_count} | {loc_span} |".format(
                    class_name=row["class_name"],
                    module_name=row["module_name"],
                    kind=row["kind"],
                    summary=row["summary"],
                    payload_shape=row["payload_shape"],
                    field_count=row["field_count"],
                    loc_span=row["loc_span"],
                )
            )
        lines.append("")
        lines.append("### Dataclass Fields")
        lines.append("")
        for row in dataclass_rows:
            class_fact = row["class_fact"]
            lines.append(f"#### {row['class_name']}")
            lines.append("")
            lines.append(f"- Module: `{row['module_name']}`")
            lines.append(f"- Semantic kind: {row['kind']}")
            lines.append(f"- Represents: {row['summary']}")
            lines.append(f"- Payload shape: {row['payload_shape']}")
            if class_fact.fields:
                lines.extend(
                    [
                        "",
                        "| Field | Type | Default | Role |",
                        "| --- | --- | --- | --- |",
                    ]
                )
                for field in class_fact.fields:
                    lines.append(
                        "| {name} | {annotation} | {default} | {role} |".format(
                            name=field.name,
                            annotation=_field_type_label(field),
                            default=field.default or "",
                            role=_field_semantic_role(field),
                        )
                    )
            else:
                lines.append("")
                lines.append("No extracted dataclass fields.")
            lines.append("")

    return "\n".join(lines)


def render_taxonomy_markdown(bundle: ArchitectureBundle) -> str:
    lines = [
        "# Architecture Taxonomy",
        "",
        "```mermaid",
        "flowchart TB",
        '    system["S3NTINEL"]',
    ]
    containers = [element for element in bundle.elements if element.kind == "container"]
    components = [element for element in bundle.elements if element.kind == "component"]
    for container in sorted(containers, key=lambda item: item.id):
        container_node = _dsl_identifier(container.id)
        lines.append(f'    {container_node}["{container.name}\\nLOC {container.loc_span}"]')
        lines.append(f"    system --> {container_node}")
        child_components = [component for component in components if component.parent_id == container.id]
        for component in sorted(child_components, key=lambda item: item.id):
            component_node = _dsl_identifier(component.id.replace(".", "_"))
            lines.append(f'    {component_node}["{component.name}\\nLOC {component.loc_span}"]')
            lines.append(f"    {container_node} --> {component_node}")
    lines.append("```")
    lines.append("")
    return "\n".join(lines)


def _csv_content(rows: list[dict[str, object]], fieldnames: list[str]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue()


def render_lucid_nodes_csv(bundle: ArchitectureBundle) -> str:
    rows = [
        {
            "id": element.id,
            "parent_id": element.parent_id or "",
            "kind": element.kind,
            "name": element.name,
            "description": element.description,
            "technology": element.technology,
            "loc_span": element.loc_span,
            "module_count": element.module_count,
            "source_paths": ",".join(element.source_paths),
        }
        for element in bundle.elements
    ]
    return _csv_content(
        rows,
        fieldnames=[
            "id",
            "parent_id",
            "kind",
            "name",
            "description",
            "technology",
            "loc_span",
            "module_count",
            "source_paths",
        ],
    )


def render_lucid_edges_csv(bundle: ArchitectureBundle) -> str:
    rows = [
        {
            "source_id": relationship.source_id,
            "destination_id": relationship.destination_id,
            "description": relationship.description,
            "technology": relationship.technology,
            "import_edge_count": relationship.import_edge_count,
            "tags": ",".join(relationship.tags),
        }
        for relationship in bundle.relationships
    ]
    return _csv_content(
        rows,
        fieldnames=[
            "source_id",
            "destination_id",
            "description",
            "technology",
            "import_edge_count",
            "tags",
        ],
    )


def render_view_index_markdown(bundle: ArchitectureBundle) -> str:
    return "\n".join(
        [
            "# Architecture View Index",
            "",
            "- `workspace.dsl`: canonical C4 workspace source",
            "- `architecture_metrics.md`: LOC and skew summary",
            "- `core_library_semantics.md`: per-core-library dataclass semantics diagrams and catalogs",
            "- `pipeline_data_flow.md`: pipeline-stage data flow diagram",
            "- `pipeline_layered_architecture.md`: one-layer-per-stage pipeline architecture diagram",
            "- `pipeline_stage_catalog.md`: purpose and view index for each pipeline stage",
            "- `taxonomy_diagram.md`: container and component taxonomy",
            "- `lucid_nodes.csv` and `lucid_edges.csv`: Lucidchart-friendly exports",
            "- `workspace.dsl` views: `system_context`, `container_view`, `pipeline_components`, `pipeline_layers`, and `core_library_components`",
            "",
            "## Source Docs",
            "",
            *[f"- `{document.path}`: {document.title}" for document in bundle.documents],
            "",
        ]
    )


def write_render_outputs(bundle: ArchitectureBundle, output_dir: Path) -> list[str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[str] = []
    files = {
        "workspace.dsl": render_workspace_dsl(bundle),
        "architecture_metrics.json": json.dumps(bundle.metrics.to_payload(), indent=2, sort_keys=True) + "\n",
        "architecture_metrics.md": render_metrics_markdown(bundle),
        "core_library_semantics.md": render_core_library_semantics_markdown(bundle),
        "pipeline_data_flow.md": render_pipeline_flow_markdown(bundle),
        "pipeline_layered_architecture.md": render_pipeline_layered_architecture_markdown(bundle),
        "pipeline_stage_catalog.md": render_pipeline_stage_catalog_markdown(bundle),
        "taxonomy_diagram.md": render_taxonomy_markdown(bundle),
        "view_index.md": render_view_index_markdown(bundle),
        "lucid_nodes.csv": render_lucid_nodes_csv(bundle),
        "lucid_edges.csv": render_lucid_edges_csv(bundle),
    }
    for name, content in files.items():
        (output_dir / name).write_text(content, encoding="utf-8")
        written.append(name)
    return written
