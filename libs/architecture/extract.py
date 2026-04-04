"""Architecture extraction and normalization."""

from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path

from libs.architecture.annotations import load_annotation_spec
from libs.architecture.model import (
    ArchitectureBundle,
    ArchitectureElement,
    ArchitectureMetrics,
    ArchitectureRelationship,
    CodeClassFact,
    CodeClassFieldFact,
    CodeFunctionFact,
    CodeSpan,
    DependencyEdge,
    DocumentFact,
    ModuleFact,
    PackageDocFact,
)
from tools import module_deps, repo_schematic


def _as_posix(path: Path) -> str:
    return path.as_posix()


def _package_name(module_name: str) -> str:
    if "." not in module_name:
        return module_name
    return module_name.rsplit(".", 1)[0]


def _matches_focus_path(relative_path: str, focus_paths: tuple[str, ...]) -> bool:
    if not focus_paths:
        return True
    return relative_path.startswith(focus_paths)


def _to_code_span(lineno: int | None, end_lineno: int | None, span_loc: int) -> CodeSpan:
    return CodeSpan(lineno=lineno, end_lineno=end_lineno, span_loc=span_loc)


def _to_function_fact(payload: repo_schematic.FunctionInfo) -> CodeFunctionFact:
    return CodeFunctionFact(
        name=payload.name,
        signature=payload.signature,
        decorators=tuple(payload.decorators),
        doc=payload.doc,
        span=_to_code_span(payload.lineno, payload.end_lineno, payload.span_loc),
    )


def _to_class_field_fact(payload: repo_schematic.ClassFieldInfo) -> CodeClassFieldFact:
    return CodeClassFieldFact(
        name=payload.name,
        annotation=payload.annotation,
        default=payload.default,
    )


def _to_class_fact(payload: repo_schematic.ClassInfo) -> CodeClassFact:
    return CodeClassFact(
        name=payload.name,
        decorators=tuple(payload.decorators),
        bases=tuple(payload.bases),
        doc=payload.doc,
        class_attributes=tuple(payload.class_attributes),
        fields=tuple(_to_class_field_fact(field) for field in payload.fields),
        methods=tuple(_to_function_fact(method) for method in payload.methods),
        span=_to_code_span(payload.lineno, payload.end_lineno, payload.span_loc),
    )


def _read_markdown_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def _markdown_title(text: str, *, fallback: str) -> str:
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("# "):
            return stripped[2:].strip()
    return fallback


def _markdown_summary(text: str) -> str | None:
    lines = text.splitlines()
    for section_name in ("## Purpose", "## Summary"):
        for index, line in enumerate(lines):
            if line.strip() != section_name:
                continue
            collected: list[str] = []
            bullets: list[str] = []
            for section_line in lines[index + 1 :]:
                stripped = section_line.strip()
                if stripped.startswith("#"):
                    break
                if not stripped:
                    if collected or bullets:
                        break
                    continue
                if stripped.startswith("- "):
                    bullets.append(stripped[2:].strip())
                    continue
                collected.append(stripped)
            if collected or bullets:
                summary = " ".join(collected).rstrip(":")
                if bullets:
                    bullet_summary = "; ".join(bullets[:4])
                    if summary:
                        summary = f"{summary}: {bullet_summary}"
                    else:
                        summary = bullet_summary
                return summary or None

    collected = []
    bullets = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("- "):
            bullets.append(stripped[2:].strip())
            continue
        collected.append(stripped)
        if len(collected) >= 2:
            break
    if collected or bullets:
        summary = " ".join(collected).rstrip(":")
        if bullets:
            bullet_summary = "; ".join(bullets[:4])
            if summary:
                summary = f"{summary}: {bullet_summary}"
            else:
                summary = bullet_summary
        return summary or None
    return None


def _package_doc_name(path: Path, root: Path) -> str:
    parent = path.parent.relative_to(root)
    if str(parent) == ".":
        return "root"
    return str(parent).replace("/", ".")


def collect_package_docs(root: Path, focus_paths: tuple[str, ...]) -> tuple[PackageDocFact, ...]:
    docs: list[PackageDocFact] = []
    for path in sorted(root.rglob("README.md")):
        rel = _as_posix(path.relative_to(root))
        if not _matches_focus_path(rel, focus_paths):
            continue
        text = _read_markdown_text(path)
        docs.append(
            PackageDocFact(
                package=_package_doc_name(path, root),
                path=rel,
                title=_markdown_title(text, fallback=path.parent.name or "README"),
                summary=_markdown_summary(text),
            )
        )
    return tuple(docs)


def collect_documents(root: Path, doc_paths: tuple[str, ...]) -> tuple[DocumentFact, ...]:
    docs: list[DocumentFact] = []
    for doc_path in doc_paths:
        path = root / doc_path
        if not path.exists():
            continue
        text = _read_markdown_text(path)
        docs.append(
            DocumentFact(
                path=_as_posix(path.relative_to(root)),
                title=_markdown_title(text, fallback=path.stem),
                summary=_markdown_summary(text),
            )
        )
    return tuple(docs)


def collect_module_facts(root: Path, focus_paths: tuple[str, ...]) -> tuple[ModuleFact, ...]:
    excludes = set(module_deps.DEFAULT_EXCLUDES) | {".codex"}
    module_index = module_deps.build_module_index(root, excludes)

    schematic_by_path: dict[Path, repo_schematic.ModuleInfo] = {}
    deps_by_path: dict[Path, module_deps.ModuleDeps] = {}

    for path in module_deps.iter_python_files(root, excludes):
        rel_path = path.relative_to(root)
        rel_posix = _as_posix(rel_path)
        if not _matches_focus_path(rel_posix, focus_paths):
            continue
        schematic_by_path[rel_path] = repo_schematic.parse_module(path, root)
        deps_by_path[rel_path] = module_deps.parse_module_deps(root, path, module_index)

    module_facts: list[ModuleFact] = []
    for rel_path in sorted(schematic_by_path):
        schematic = schematic_by_path[rel_path]
        deps = deps_by_path[rel_path]
        module_facts.append(
            ModuleFact(
                module=deps.module,
                path=_as_posix(rel_path),
                package=_package_name(deps.module),
                doc=schematic.doc,
                imports=tuple(schematic.imports),
                internal_dependencies=tuple(
                    sorted(
                        {
                            imp.resolved
                            for imp in deps.imports
                            if imp.resolved and imp.resolved != deps.module
                        }
                    )
                ),
                unresolved_imports=tuple(
                    sorted({imp.raw for imp in deps.imports if imp.resolved is None})
                ),
                constants=tuple(sorted(set(schematic.constants))),
                functions=tuple(_to_function_fact(fn) for fn in schematic.functions),
                classes=tuple(_to_class_fact(cls) for cls in schematic.classes),
                span=_to_code_span(schematic.lineno, schematic.end_lineno, schematic.span_loc),
                parse_error=schematic.parse_error,
            )
        )
    return tuple(module_facts)


def collect_dependency_edges(modules: tuple[ModuleFact, ...]) -> tuple[DependencyEdge, ...]:
    module_names = {module.module for module in modules}
    edges = {
        (module.module, dependency)
        for module in modules
        for dependency in module.internal_dependencies
        if dependency in module_names and dependency != module.module
    }
    return tuple(
        DependencyEdge(source_module=source, target_module=target)
        for source, target in sorted(edges)
    )


def _selector_modules(
    modules: tuple[ModuleFact, ...],
    selector,
    *,
    exclude_modules: set[str] | None = None,
) -> tuple[ModuleFact, ...]:
    excluded = exclude_modules or set()
    return tuple(
        module
        for module in modules
        if module.module not in excluded and selector.matches(module)
    )


def _summed_loc(modules: tuple[ModuleFact, ...]) -> int:
    return sum(module.span.span_loc for module in modules)


def _top_level_group(module_name: str, prefix: str) -> str | None:
    if prefix and module_name.startswith(prefix):
        remainder = module_name[len(prefix) :]
        parts = [part for part in remainder.split(".") if part]
        if parts:
            return parts[0]
        return None
    parts = module_name.split(".")
    if len(parts) >= 2:
        return parts[1]
    return None


def _title_case(name: str) -> str:
    return name.replace("_", " ").replace("-", " ").title()


def _package_doc_lookup(package_docs: tuple[PackageDocFact, ...]) -> dict[str, PackageDocFact]:
    return {doc.package: doc for doc in package_docs}


def build_architecture_bundle(root: Path, annotations_path: Path) -> ArchitectureBundle:
    annotations = load_annotation_spec(annotations_path)
    modules = collect_module_facts(root, annotations.focus_paths)
    dependency_edges = collect_dependency_edges(modules)
    package_docs = collect_package_docs(root, annotations.focus_paths)
    package_doc_lookup = _package_doc_lookup(package_docs)
    documents = collect_documents(root, annotations.doc_paths)

    module_lookup = {module.module: module for module in modules}
    module_to_container: dict[str, str] = {}
    module_to_component: dict[str, str] = {}
    elements: list[ArchitectureElement] = []

    system_loc = _summed_loc(modules)
    elements.append(
        ArchitectureElement(
            id="s3ntinel",
            kind="software_system",
            name=annotations.workspace_name,
            description=annotations.workspace_description,
            assigned_modules=tuple(module.module for module in modules),
            source_paths=tuple(sorted(module.path for module in modules)),
            loc_span=system_loc,
            module_count=len(modules),
            properties={
                "loc_span": str(system_loc),
                "module_count": str(len(modules)),
            },
        )
    )

    for item in annotations.people:
        elements.append(
            ArchitectureElement(
                id=item.id,
                kind="person",
                name=item.name,
                description=item.description,
                technology=item.technology,
                tags=item.tags,
            )
        )

    for item in annotations.external_systems:
        elements.append(
            ArchitectureElement(
                id=item.id,
                kind="external_system",
                name=item.name,
                description=item.description,
                technology=item.technology,
                tags=item.tags,
            )
        )

    for item in annotations.data_stores:
        elements.append(
            ArchitectureElement(
                id=item.id,
                kind="data_store",
                name=item.name,
                description=item.description,
                technology=item.technology,
                tags=item.tags,
            )
        )

    for container_spec in annotations.containers:
        container_modules = _selector_modules(modules, container_spec.selectors)
        container_module_names = tuple(module.module for module in container_modules)
        for module_name in container_module_names:
            module_to_container[module_name] = container_spec.id
        elements.append(
            ArchitectureElement(
                id=container_spec.id,
                kind="container",
                name=container_spec.name,
                description=container_spec.description,
                technology=container_spec.technology,
                tags=container_spec.tags,
                parent_id="s3ntinel",
                assigned_modules=container_module_names,
                source_paths=tuple(sorted(module.path for module in container_modules)),
                loc_span=_summed_loc(container_modules),
                module_count=len(container_modules),
                properties={
                    "loc_span": str(_summed_loc(container_modules)),
                    "module_count": str(len(container_modules)),
                },
            )
        )

        claimed_modules: set[str] = set()
        for component_spec in container_spec.components:
            component_modules = _selector_modules(
                container_modules,
                component_spec.selectors,
                exclude_modules=claimed_modules,
            )
            claimed_modules.update(module.module for module in component_modules)
            component_id = f"{container_spec.id}.{component_spec.id}"
            for module in component_modules:
                module_to_component[module.module] = component_id
            elements.append(
                ArchitectureElement(
                    id=component_id,
                    kind="component",
                    name=component_spec.name,
                    description=component_spec.description,
                    technology=component_spec.technology,
                    tags=component_spec.tags,
                    parent_id=container_spec.id,
                    assigned_modules=tuple(module.module for module in component_modules),
                    source_paths=tuple(sorted(module.path for module in component_modules)),
                    loc_span=_summed_loc(component_modules),
                    module_count=len(component_modules),
                    properties={
                        "loc_span": str(_summed_loc(component_modules)),
                        "module_count": str(len(component_modules)),
                    },
                )
            )

        auto_spec = container_spec.auto_components
        if auto_spec is None:
            continue

        grouped_modules: dict[str, list[ModuleFact]] = defaultdict(list)
        for module in container_modules:
            if module.module in claimed_modules:
                continue
            group = _top_level_group(module.module, auto_spec.prefix)
            if not group:
                continue
            if auto_spec.include_groups and group not in auto_spec.include_groups:
                continue
            if auto_spec.exclude_groups and group in auto_spec.exclude_groups:
                continue
            grouped_modules[group].append(module)

        for group in sorted(grouped_modules):
            component_modules = tuple(sorted(grouped_modules[group], key=lambda item: item.module))
            component_id = f"{container_spec.id}.{group}"
            for module in component_modules:
                module_to_component[module.module] = component_id
            package_name = (
                auto_spec.prefix.rstrip(".") + f".{group}"
                if auto_spec.prefix
                else group
            )
            package_doc = package_doc_lookup.get(package_name)
            description = package_doc.summary if package_doc and package_doc.summary else f"Modules grouped under `{package_name}`."
            if auto_spec.description_suffix:
                description = f"{description} {auto_spec.description_suffix}".strip()
            elements.append(
                ArchitectureElement(
                    id=component_id,
                    kind="component",
                    name=_title_case(group),
                    description=description,
                    technology=container_spec.technology,
                    tags=("auto_component",),
                    parent_id=container_spec.id,
                    assigned_modules=tuple(module.module for module in component_modules),
                    source_paths=tuple(sorted(module.path for module in component_modules)),
                    loc_span=_summed_loc(component_modules),
                    module_count=len(component_modules),
                    properties={
                        "loc_span": str(_summed_loc(component_modules)),
                        "module_count": str(len(component_modules)),
                        "package_name": package_name,
                    },
                )
            )

    relationship_counts: dict[tuple[str, str], int] = Counter()
    component_relationship_counts: dict[tuple[str, str], int] = Counter()

    for edge in dependency_edges:
        source_container = module_to_container.get(edge.source_module)
        target_container = module_to_container.get(edge.target_module)
        if source_container and target_container and source_container != target_container:
            relationship_counts[(source_container, target_container)] += 1

        source_component = module_to_component.get(edge.source_module)
        target_component = module_to_component.get(edge.target_module)
        if source_component and target_component and source_component != target_component:
            component_relationship_counts[(source_component, target_component)] += 1

    relationships: list[ArchitectureRelationship] = []
    for (source_id, destination_id), count in sorted(relationship_counts.items()):
        relationships.append(
            ArchitectureRelationship(
                source_id=source_id,
                destination_id=destination_id,
                description="Uses",
                technology="Python imports",
                tags=("internal",),
                import_edge_count=count,
            )
        )

    for (source_id, destination_id), count in sorted(component_relationship_counts.items()):
        relationships.append(
            ArchitectureRelationship(
                source_id=source_id,
                destination_id=destination_id,
                description="Uses",
                technology="Python imports",
                tags=("internal", "component"),
                import_edge_count=count,
            )
        )

    for spec in annotations.manual_relationships:
        relationships.append(
            ArchitectureRelationship(
                source_id=spec.source,
                destination_id=spec.destination,
                description=spec.description,
                technology=spec.technology,
                tags=spec.tags,
            )
        )

    metrics = _build_metrics(
        modules=modules,
        elements=tuple(elements),
        relationships=tuple(relationships),
        module_to_container=module_to_container,
        module_to_component=module_to_component,
    )

    return ArchitectureBundle(
        root=_as_posix(root),
        annotations=annotations,
        modules=modules,
        dependency_edges=dependency_edges,
        package_docs=package_docs,
        documents=documents,
        elements=tuple(elements),
        relationships=tuple(relationships),
        metrics=metrics,
    )


def _share(value: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round(value / total, 4)


def _build_metrics(
    *,
    modules: tuple[ModuleFact, ...],
    elements: tuple[ArchitectureElement, ...],
    relationships: tuple[ArchitectureRelationship, ...],
    module_to_container: dict[str, str],
    module_to_component: dict[str, str],
) -> ArchitectureMetrics:
    system_loc = _summed_loc(modules)
    largest_modules = sorted(modules, key=lambda item: (-item.span.span_loc, item.module))

    classes: list[dict[str, object]] = []
    functions: list[dict[str, object]] = []
    for module in modules:
        for cls in module.classes:
            classes.append(
                {
                    "module": module.module,
                    "path": module.path,
                    "name": cls.name,
                    "qualified_name": f"{module.module}.{cls.name}",
                    "loc_span": cls.span.span_loc,
                }
            )
            for method in cls.methods:
                functions.append(
                    {
                        "module": module.module,
                        "path": module.path,
                        "name": method.name,
                        "qualified_name": f"{module.module}.{cls.name}.{method.name}",
                        "loc_span": method.span.span_loc,
                    }
                )
        for function in module.functions:
            functions.append(
                {
                    "module": module.module,
                    "path": module.path,
                    "name": function.name,
                    "qualified_name": f"{module.module}.{function.name}",
                    "loc_span": function.span.span_loc,
                }
            )

    largest_classes = tuple(sorted(classes, key=lambda item: (-int(item["loc_span"]), str(item["qualified_name"])))[:10])
    largest_functions = tuple(sorted(functions, key=lambda item: (-int(item["loc_span"]), str(item["qualified_name"])))[:10])

    components = [element for element in elements if element.kind == "component"]
    containers = [element for element in elements if element.kind == "container"]
    component_sizes = sorted((component.loc_span for component in components), reverse=True)

    component_incoming = Counter()
    component_outgoing = Counter()
    for relationship in relationships:
        source_id = relationship.source_id
        destination_id = relationship.destination_id
        if "." in source_id:
            component_outgoing[source_id] += 1
        if "." in destination_id:
            component_incoming[destination_id] += 1

    component_size_table = tuple(
        {
            "id": component.id,
            "name": component.name,
            "container_id": component.parent_id,
            "loc_span": component.loc_span,
            "module_count": component.module_count,
            "system_loc_share": _share(component.loc_span, system_loc),
        }
        for component in sorted(components, key=lambda item: (-item.loc_span, item.id))
    )

    container_size_table = tuple(
        {
            "id": container.id,
            "name": container.name,
            "loc_span": container.loc_span,
            "module_count": container.module_count,
            "system_loc_share": _share(container.loc_span, system_loc),
        }
        for container in sorted(containers, key=lambda item: (-item.loc_span, item.id))
    )

    dependency_size_table = tuple(
        {
            "id": component.id,
            "name": component.name,
            "loc_span": component.loc_span,
            "incoming_relationship_count": component_incoming[component.id],
            "outgoing_relationship_count": component_outgoing[component.id],
            "combined_relationship_count": component_incoming[component.id] + component_outgoing[component.id],
            "container_id": component.parent_id,
        }
        for component in sorted(
            components,
            key=lambda item: (
                -(component_incoming[item.id] + component_outgoing[item.id]),
                -item.loc_span,
                item.id,
            ),
        )
    )

    largest_module_rows = tuple(
        {
            "module": module.module,
            "path": module.path,
            "loc_span": module.span.span_loc,
            "container_id": module_to_container.get(module.module),
            "component_id": module_to_component.get(module.module),
        }
        for module in largest_modules[:10]
    )

    return ArchitectureMetrics(
        system_loc_span=system_loc,
        top_module_loc_share=_share(largest_modules[0].span.span_loc, system_loc) if largest_modules else 0.0,
        top_component_loc_share=_share(component_sizes[0], system_loc) if component_sizes else 0.0,
        top_three_component_loc_share=_share(sum(component_sizes[:3]), system_loc) if component_sizes else 0.0,
        largest_modules=largest_module_rows,
        largest_classes=largest_classes,
        largest_functions=largest_functions,
        component_size_table=component_size_table,
        container_size_table=container_size_table,
        dependency_size_table=dependency_size_table,
    )
