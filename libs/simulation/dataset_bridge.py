"""Bridge helpers for adapting native assembly specs into dataset-facing compatibility rows."""

from __future__ import annotations

from typing import Any

import pandas as pd

from libs.simulation.flatten import flatten_module_specs
from libs.simulation.setup_builders import build_default_parameter_behavior
from libs.simulation.specs import HierarchyAssemblySpec
from libs.simulation.subsystem_slices import build_native_subsystem_slice


def _build_parameter_coupling_summaries(
    assembly_spec: HierarchyAssemblySpec,
) -> tuple[dict[tuple[str, str], dict[str, Any]], dict[tuple[str, str], dict[str, Any]]]:
    incoming_by_parameter: dict[tuple[str, str], dict[str, Any]] = {}
    outgoing_by_parameter: dict[tuple[str, str], dict[str, Any]] = {}

    def _bucket(store: dict[tuple[str, str], dict[str, Any]], key: tuple[str, str]) -> dict[str, Any]:
        return store.setdefault(
            key,
            {
                "count": 0,
                "relation_types": set(),
                "module_ids": set(),
            },
        )

    output_port_to_parameter_by_module: dict[str, dict[str, str]] = {}
    input_port_to_parameters_by_module: dict[str, dict[str, list[str]]] = {}
    for module_spec in assembly_spec.module_specs:
        module_id = str(module_spec.module_id)
        output_port_to_parameter_by_module[module_id] = {
            str(parameter_spec.output_port_name): str(parameter_spec.parameter_name)
            for parameter_spec in module_spec.parameters
            if parameter_spec.output_port_name
        }
        input_port_to_parameters: dict[str, list[str]] = {}
        for parameter_spec in module_spec.parameters:
            for input_port_name in parameter_spec.input_port_names:
                input_port_to_parameters.setdefault(str(input_port_name), []).append(str(parameter_spec.parameter_name))
        input_port_to_parameters_by_module[module_id] = input_port_to_parameters

    for coupling in assembly_spec.inter_module_couplings:
        source_module_id = str(coupling.source_module_id)
        target_module_id = str(coupling.target_module_id)
        relation_type = str(coupling.relation_type)

        source_parameter_name = output_port_to_parameter_by_module.get(source_module_id, {}).get(str(coupling.source_port_name))
        if source_parameter_name:
            item = _bucket(outgoing_by_parameter, (source_module_id, source_parameter_name))
            item["count"] = int(item["count"]) + 1
            item["relation_types"].add(relation_type)
            item["module_ids"].add(target_module_id)

        for target_parameter_name in input_port_to_parameters_by_module.get(target_module_id, {}).get(str(coupling.target_port_name), []):
            item = _bucket(incoming_by_parameter, (target_module_id, target_parameter_name))
            item["count"] = int(item["count"]) + 1
            item["relation_types"].add(relation_type)
            item["module_ids"].add(source_module_id)

    return incoming_by_parameter, outgoing_by_parameter


def flatten_assembly_spec(assembly_spec: HierarchyAssemblySpec) -> pd.DataFrame:
    rows = flatten_module_specs(assembly_spec.module_specs)
    if not rows:
        return pd.DataFrame(
            columns=[
                "system_id",
                "subsystem_id",
                "module_id",
                "sensor",
                "parameter_name",
                "parameter_datatype",
                "parameter_datatype_label",
                "behavior_family_label",
                "unit",
                "sampling_rate_hz",
                "input_port_names",
                "output_port_name",
                "incoming_coupling_count",
                "outgoing_coupling_count",
                "incoming_relation_types",
                "outgoing_relation_types",
                "upstream_module_ids",
                "downstream_module_ids",
            ]
        )

    incoming_by_parameter, outgoing_by_parameter = _build_parameter_coupling_summaries(assembly_spec)
    hierarchy_df = pd.DataFrame.from_records(rows)
    hierarchy_df["sensor"] = hierarchy_df["parameter_name"].astype(str)
    hierarchy_df["parameter_datatype"] = hierarchy_df["parameter_datatype_label"].astype(str)

    parameter_keys = [
        (str(module_id), str(parameter_name))
        for module_id, parameter_name in zip(hierarchy_df["module_id"], hierarchy_df["parameter_name"], strict=False)
    ]

    hierarchy_df["incoming_coupling_count"] = [
        int(incoming_by_parameter.get(key, {}).get("count", 0))
        for key in parameter_keys
    ]
    hierarchy_df["outgoing_coupling_count"] = [
        int(outgoing_by_parameter.get(key, {}).get("count", 0))
        for key in parameter_keys
    ]
    hierarchy_df["incoming_relation_types"] = [
        sorted(incoming_by_parameter.get(key, {}).get("relation_types", set()))
        for key in parameter_keys
    ]
    hierarchy_df["outgoing_relation_types"] = [
        sorted(outgoing_by_parameter.get(key, {}).get("relation_types", set()))
        for key in parameter_keys
    ]
    hierarchy_df["upstream_module_ids"] = [
        sorted(incoming_by_parameter.get(key, {}).get("module_ids", set()))
        for key in parameter_keys
    ]
    hierarchy_df["downstream_module_ids"] = [
        sorted(outgoing_by_parameter.get(key, {}).get("module_ids", set()))
        for key in parameter_keys
    ]

    ordered_columns = [
        "system_id",
        "subsystem_id",
        "module_id",
        "sensor",
        "parameter_name",
        "parameter_datatype",
        "parameter_datatype_label",
        "behavior_family_label",
        "unit",
        "sampling_rate_hz",
        "input_port_names",
        "output_port_name",
        "incoming_coupling_count",
        "outgoing_coupling_count",
        "incoming_relation_types",
        "outgoing_relation_types",
        "upstream_module_ids",
        "downstream_module_ids",
    ]
    return (
        hierarchy_df[ordered_columns]
        .sort_values(["system_id", "subsystem_id", "module_id", "parameter_name"])
        .reset_index(drop=True)
    )


def build_subsystem_slice_hierarchy_df(slice_name: str) -> pd.DataFrame:
    return flatten_assembly_spec(build_native_subsystem_slice(slice_name))


def resolve_parameter_behavior_for_assembly(
    *,
    assembly_spec: HierarchyAssemblySpec,
    parameter_behavior: dict[str, dict] | None = None,
    parameter_behavior_profile_df: pd.DataFrame | None = None,
    continuous_scaling_profile_df: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, dict[str, dict]]:
    hierarchy_df = flatten_assembly_spec(assembly_spec)
    resolved_parameter_behavior = parameter_behavior or build_default_parameter_behavior(
        hierarchy_df,
        parameter_behavior_profile_df=parameter_behavior_profile_df,
        continuous_scaling_profile_df=continuous_scaling_profile_df,
    )
    return hierarchy_df, resolved_parameter_behavior
