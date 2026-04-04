"""Stable payload models for architecture extraction and rendering."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class CodeSpan:
    lineno: int | None = None
    end_lineno: int | None = None
    span_loc: int = 0

    def to_payload(self) -> dict[str, int | None]:
        return {
            "lineno": self.lineno,
            "end_lineno": self.end_lineno,
            "span_loc": self.span_loc,
        }


@dataclass(frozen=True)
class CodeFunctionFact:
    name: str
    signature: str
    decorators: tuple[str, ...] = ()
    doc: str | None = None
    span: CodeSpan = field(default_factory=CodeSpan)

    def to_payload(self) -> dict[str, object]:
        return {
            "name": self.name,
            "signature": self.signature,
            "decorators": list(self.decorators),
            "doc": self.doc,
            "span": self.span.to_payload(),
        }


@dataclass(frozen=True)
class CodeClassFieldFact:
    name: str
    annotation: str = ""
    default: str = ""

    def to_payload(self) -> dict[str, object]:
        return {
            "name": self.name,
            "annotation": self.annotation,
            "default": self.default,
        }


@dataclass(frozen=True)
class CodeClassFact:
    name: str
    decorators: tuple[str, ...] = ()
    bases: tuple[str, ...] = ()
    doc: str | None = None
    class_attributes: tuple[str, ...] = ()
    fields: tuple[CodeClassFieldFact, ...] = ()
    methods: tuple[CodeFunctionFact, ...] = ()
    span: CodeSpan = field(default_factory=CodeSpan)

    def to_payload(self) -> dict[str, object]:
        return {
            "name": self.name,
            "decorators": list(self.decorators),
            "bases": list(self.bases),
            "doc": self.doc,
            "class_attributes": list(self.class_attributes),
            "fields": [field.to_payload() for field in self.fields],
            "methods": [method.to_payload() for method in self.methods],
            "span": self.span.to_payload(),
        }


@dataclass(frozen=True)
class ModuleFact:
    module: str
    path: str
    package: str
    doc: str | None = None
    imports: tuple[str, ...] = ()
    internal_dependencies: tuple[str, ...] = ()
    unresolved_imports: tuple[str, ...] = ()
    constants: tuple[str, ...] = ()
    functions: tuple[CodeFunctionFact, ...] = ()
    classes: tuple[CodeClassFact, ...] = ()
    span: CodeSpan = field(default_factory=CodeSpan)
    parse_error: str | None = None

    def to_payload(self) -> dict[str, object]:
        return {
            "module": self.module,
            "path": self.path,
            "package": self.package,
            "doc": self.doc,
            "imports": list(self.imports),
            "internal_dependencies": list(self.internal_dependencies),
            "unresolved_imports": list(self.unresolved_imports),
            "constants": list(self.constants),
            "functions": [function.to_payload() for function in self.functions],
            "classes": [cls.to_payload() for cls in self.classes],
            "span": self.span.to_payload(),
            "parse_error": self.parse_error,
        }


@dataclass(frozen=True)
class DependencyEdge:
    source_module: str
    target_module: str

    def to_payload(self) -> dict[str, str]:
        return {
            "source_module": self.source_module,
            "target_module": self.target_module,
        }


@dataclass(frozen=True)
class PackageDocFact:
    package: str
    path: str
    title: str
    summary: str | None = None

    def to_payload(self) -> dict[str, object]:
        return {
            "package": self.package,
            "path": self.path,
            "title": self.title,
            "summary": self.summary,
        }


@dataclass(frozen=True)
class DocumentFact:
    path: str
    title: str
    summary: str | None = None

    def to_payload(self) -> dict[str, object]:
        return {
            "path": self.path,
            "title": self.title,
            "summary": self.summary,
        }


@dataclass(frozen=True)
class SelectorSpec:
    module_prefixes: tuple[str, ...] = ()
    module_names: tuple[str, ...] = ()
    path_prefixes: tuple[str, ...] = ()
    exclude_module_prefixes: tuple[str, ...] = ()
    exclude_module_names: tuple[str, ...] = ()
    exclude_path_prefixes: tuple[str, ...] = ()

    def matches(self, module_fact: ModuleFact) -> bool:
        include = True
        if self.module_prefixes or self.module_names or self.path_prefixes:
            include = False
            if self.module_names and module_fact.module in self.module_names:
                include = True
            if self.module_prefixes and module_fact.module.startswith(self.module_prefixes):
                include = True
            if self.path_prefixes and module_fact.path.startswith(self.path_prefixes):
                include = True
        if not include:
            return False
        if self.exclude_module_names and module_fact.module in self.exclude_module_names:
            return False
        if self.exclude_module_prefixes and module_fact.module.startswith(self.exclude_module_prefixes):
            return False
        if self.exclude_path_prefixes and module_fact.path.startswith(self.exclude_path_prefixes):
            return False
        return True

    def to_payload(self) -> dict[str, object]:
        return {
            "module_prefixes": list(self.module_prefixes),
            "module_names": list(self.module_names),
            "path_prefixes": list(self.path_prefixes),
            "exclude_module_prefixes": list(self.exclude_module_prefixes),
            "exclude_module_names": list(self.exclude_module_names),
            "exclude_path_prefixes": list(self.exclude_path_prefixes),
        }


@dataclass(frozen=True)
class ComponentSpec:
    id: str
    name: str
    description: str
    technology: str = ""
    tags: tuple[str, ...] = ()
    selectors: SelectorSpec = field(default_factory=SelectorSpec)

    def to_payload(self) -> dict[str, object]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "technology": self.technology,
            "tags": list(self.tags),
            "selectors": self.selectors.to_payload(),
        }


@dataclass(frozen=True)
class AutoComponentSpec:
    group_by: str = "second_segment"
    prefix: str = ""
    include_groups: tuple[str, ...] = ()
    exclude_groups: tuple[str, ...] = ()
    description_suffix: str = ""

    def to_payload(self) -> dict[str, object]:
        return {
            "group_by": self.group_by,
            "prefix": self.prefix,
            "include_groups": list(self.include_groups),
            "exclude_groups": list(self.exclude_groups),
            "description_suffix": self.description_suffix,
        }


@dataclass(frozen=True)
class ContainerSpec:
    id: str
    name: str
    description: str
    technology: str = ""
    tags: tuple[str, ...] = ()
    selectors: SelectorSpec = field(default_factory=SelectorSpec)
    components: tuple[ComponentSpec, ...] = ()
    auto_components: AutoComponentSpec | None = None

    def to_payload(self) -> dict[str, object]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "technology": self.technology,
            "tags": list(self.tags),
            "selectors": self.selectors.to_payload(),
            "components": [component.to_payload() for component in self.components],
            "auto_components": None if self.auto_components is None else self.auto_components.to_payload(),
        }


@dataclass(frozen=True)
class StaticElementSpec:
    id: str
    name: str
    description: str
    technology: str = ""
    tags: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, object]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "technology": self.technology,
            "tags": list(self.tags),
        }


@dataclass(frozen=True)
class ManualRelationshipSpec:
    source: str
    destination: str
    description: str
    technology: str = ""
    tags: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, object]:
        return {
            "source": self.source,
            "destination": self.destination,
            "description": self.description,
            "technology": self.technology,
            "tags": list(self.tags),
        }


@dataclass(frozen=True)
class ViewSpec:
    core_library_component_include: tuple[str, ...] = ()
    pipeline_component_include: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, object]:
        return {
            "core_library_component_include": list(self.core_library_component_include),
            "pipeline_component_include": list(self.pipeline_component_include),
        }


@dataclass(frozen=True)
class AnnotationSpec:
    workspace_name: str
    workspace_description: str
    focus_paths: tuple[str, ...]
    doc_paths: tuple[str, ...]
    people: tuple[StaticElementSpec, ...] = ()
    external_systems: tuple[StaticElementSpec, ...] = ()
    data_stores: tuple[StaticElementSpec, ...] = ()
    containers: tuple[ContainerSpec, ...] = ()
    manual_relationships: tuple[ManualRelationshipSpec, ...] = ()
    views: ViewSpec = field(default_factory=ViewSpec)

    def to_payload(self) -> dict[str, object]:
        return {
            "workspace_name": self.workspace_name,
            "workspace_description": self.workspace_description,
            "focus_paths": list(self.focus_paths),
            "doc_paths": list(self.doc_paths),
            "people": [item.to_payload() for item in self.people],
            "external_systems": [item.to_payload() for item in self.external_systems],
            "data_stores": [item.to_payload() for item in self.data_stores],
            "containers": [container.to_payload() for container in self.containers],
            "manual_relationships": [item.to_payload() for item in self.manual_relationships],
            "views": self.views.to_payload(),
        }


@dataclass(frozen=True)
class ArchitectureElement:
    id: str
    kind: str
    name: str
    description: str
    technology: str = ""
    tags: tuple[str, ...] = ()
    parent_id: str | None = None
    assigned_modules: tuple[str, ...] = ()
    source_paths: tuple[str, ...] = ()
    loc_span: int = 0
    module_count: int = 0
    properties: dict[str, str] = field(default_factory=dict)

    def to_payload(self) -> dict[str, object]:
        return {
            "id": self.id,
            "kind": self.kind,
            "name": self.name,
            "description": self.description,
            "technology": self.technology,
            "tags": list(self.tags),
            "parent_id": self.parent_id,
            "assigned_modules": list(self.assigned_modules),
            "source_paths": list(self.source_paths),
            "loc_span": self.loc_span,
            "module_count": self.module_count,
            "properties": dict(self.properties),
        }


@dataclass(frozen=True)
class ArchitectureRelationship:
    source_id: str
    destination_id: str
    description: str
    technology: str = ""
    tags: tuple[str, ...] = ()
    import_edge_count: int = 0

    def to_payload(self) -> dict[str, object]:
        return {
            "source_id": self.source_id,
            "destination_id": self.destination_id,
            "description": self.description,
            "technology": self.technology,
            "tags": list(self.tags),
            "import_edge_count": self.import_edge_count,
        }


@dataclass(frozen=True)
class ArchitectureMetrics:
    system_loc_span: int
    top_module_loc_share: float
    top_component_loc_share: float
    top_three_component_loc_share: float
    largest_modules: tuple[dict[str, object], ...]
    largest_classes: tuple[dict[str, object], ...]
    largest_functions: tuple[dict[str, object], ...]
    component_size_table: tuple[dict[str, object], ...]
    container_size_table: tuple[dict[str, object], ...]
    dependency_size_table: tuple[dict[str, object], ...]

    def to_payload(self) -> dict[str, object]:
        return {
            "system_loc_span": self.system_loc_span,
            "top_module_loc_share": self.top_module_loc_share,
            "top_component_loc_share": self.top_component_loc_share,
            "top_three_component_loc_share": self.top_three_component_loc_share,
            "largest_modules": list(self.largest_modules),
            "largest_classes": list(self.largest_classes),
            "largest_functions": list(self.largest_functions),
            "component_size_table": list(self.component_size_table),
            "container_size_table": list(self.container_size_table),
            "dependency_size_table": list(self.dependency_size_table),
        }


@dataclass(frozen=True)
class ArchitectureBundle:
    root: str
    annotations: AnnotationSpec
    modules: tuple[ModuleFact, ...]
    dependency_edges: tuple[DependencyEdge, ...]
    package_docs: tuple[PackageDocFact, ...]
    documents: tuple[DocumentFact, ...]
    elements: tuple[ArchitectureElement, ...]
    relationships: tuple[ArchitectureRelationship, ...]
    metrics: ArchitectureMetrics

    def to_payload(self) -> dict[str, object]:
        return {
            "root": self.root,
            "annotations": self.annotations.to_payload(),
            "modules": [module.to_payload() for module in self.modules],
            "dependency_edges": [edge.to_payload() for edge in self.dependency_edges],
            "package_docs": [doc.to_payload() for doc in self.package_docs],
            "documents": [doc.to_payload() for doc in self.documents],
            "elements": [element.to_payload() for element in self.elements],
            "relationships": [relationship.to_payload() for relationship in self.relationships],
            "metrics": self.metrics.to_payload(),
        }
