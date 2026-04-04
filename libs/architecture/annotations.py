"""Annotation loading for architecture generation."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from libs.architecture.model import (
    AnnotationSpec,
    AutoComponentSpec,
    ComponentSpec,
    ContainerSpec,
    ManualRelationshipSpec,
    SelectorSpec,
    StaticElementSpec,
    ViewSpec,
)


def _tuple_text(payload: Any) -> tuple[str, ...]:
    if payload is None:
        return ()
    if isinstance(payload, str):
        return (payload,)
    return tuple(str(item) for item in payload)


def _selector_spec(payload: dict[str, Any] | None) -> SelectorSpec:
    if payload is None:
        return SelectorSpec()
    return SelectorSpec(
        module_prefixes=_tuple_text(payload.get("module_prefixes")),
        module_names=_tuple_text(payload.get("module_names")),
        path_prefixes=_tuple_text(payload.get("path_prefixes")),
        exclude_module_prefixes=_tuple_text(payload.get("exclude_module_prefixes")),
        exclude_module_names=_tuple_text(payload.get("exclude_module_names")),
        exclude_path_prefixes=_tuple_text(payload.get("exclude_path_prefixes")),
    )


def _static_elements(payload: list[dict[str, Any]] | None) -> tuple[StaticElementSpec, ...]:
    items = payload or []
    return tuple(
        StaticElementSpec(
            id=str(item["id"]),
            name=str(item["name"]),
            description=str(item.get("description", "")),
            technology=str(item.get("technology", "")),
            tags=_tuple_text(item.get("tags")),
        )
        for item in items
    )


def _component_specs(payload: list[dict[str, Any]] | None) -> tuple[ComponentSpec, ...]:
    items = payload or []
    return tuple(
        ComponentSpec(
            id=str(item["id"]),
            name=str(item["name"]),
            description=str(item.get("description", "")),
            technology=str(item.get("technology", "")),
            tags=_tuple_text(item.get("tags")),
            selectors=_selector_spec(item.get("selectors")),
        )
        for item in items
    )


def _auto_component_spec(payload: dict[str, Any] | None) -> AutoComponentSpec | None:
    if payload is None:
        return None
    return AutoComponentSpec(
        group_by=str(payload.get("group_by", "second_segment")),
        prefix=str(payload.get("prefix", "")),
        include_groups=_tuple_text(payload.get("include_groups")),
        exclude_groups=_tuple_text(payload.get("exclude_groups")),
        description_suffix=str(payload.get("description_suffix", "")),
    )


def _container_specs(payload: list[dict[str, Any]] | None) -> tuple[ContainerSpec, ...]:
    items = payload or []
    return tuple(
        ContainerSpec(
            id=str(item["id"]),
            name=str(item["name"]),
            description=str(item.get("description", "")),
            technology=str(item.get("technology", "")),
            tags=_tuple_text(item.get("tags")),
            selectors=_selector_spec(item.get("selectors")),
            components=_component_specs(item.get("components")),
            auto_components=_auto_component_spec(item.get("auto_components")),
        )
        for item in items
    )


def _manual_relationships(payload: list[dict[str, Any]] | None) -> tuple[ManualRelationshipSpec, ...]:
    items = payload or []
    return tuple(
        ManualRelationshipSpec(
            source=str(item["source"]),
            destination=str(item["destination"]),
            description=str(item.get("description", "")),
            technology=str(item.get("technology", "")),
            tags=_tuple_text(item.get("tags")),
        )
        for item in items
    )


def _view_spec(payload: dict[str, Any] | None) -> ViewSpec:
    if payload is None:
        return ViewSpec()
    return ViewSpec(
        core_library_component_include=_tuple_text(payload.get("core_library_component_include")),
        pipeline_component_include=_tuple_text(payload.get("pipeline_component_include")),
    )


def load_annotation_spec(path: Path) -> AnnotationSpec:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    workspace = payload.get("workspace", {})
    return AnnotationSpec(
        workspace_name=str(workspace.get("name", "S3NTINEL Architecture")),
        workspace_description=str(workspace.get("description", "")),
        focus_paths=_tuple_text(payload.get("focus_paths")),
        doc_paths=_tuple_text(payload.get("doc_paths")),
        people=_static_elements(payload.get("people")),
        external_systems=_static_elements(payload.get("external_systems")),
        data_stores=_static_elements(payload.get("data_stores")),
        containers=_container_specs(payload.get("containers")),
        manual_relationships=_manual_relationships(payload.get("manual_relationships")),
        views=_view_spec(payload.get("views")),
    )

