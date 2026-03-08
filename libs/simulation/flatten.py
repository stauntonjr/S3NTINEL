"""Flatten native simulation specs into tabular compatibility rows."""

from __future__ import annotations

from typing import Any

from libs.simulation.specs import InterModuleCouplingSpec, ModuleSpec


def flatten_module_specs(module_specs: tuple[ModuleSpec, ...]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for module_spec in module_specs:
        for parameter_spec in module_spec.parameters:
            rows.append(
                {
                    "system_id": module_spec.system_id,
                    "subsystem_id": module_spec.subsystem_id,
                    "module_id": module_spec.module_id,
                    "parameter_name": parameter_spec.parameter_name,
                    "parameter_datatype_label": parameter_spec.parameter_datatype_label,
                    "behavior_family_label": parameter_spec.behavior_family_label,
                    "unit": parameter_spec.unit,
                    "sampling_rate_hz": parameter_spec.sampling_rate_hz,
                    "input_port_names": list(parameter_spec.input_port_names),
                    "output_port_name": parameter_spec.output_port_name,
                }
            )
    return rows


def flatten_inter_module_couplings(
    inter_module_couplings: tuple[InterModuleCouplingSpec, ...],
) -> list[dict[str, Any]]:
    return [
        {
            "source_module_id": coupling.source_module_id,
            "source_port_name": coupling.source_port_name,
            "target_module_id": coupling.target_module_id,
            "target_port_name": coupling.target_port_name,
            "relation_type": coupling.relation_type,
            "gain": coupling.gain,
            "sign": coupling.sign,
            "lag_seconds": coupling.lag_seconds,
            "time_constant_seconds": coupling.time_constant_seconds,
            "phase_gate": list(coupling.phase_gate),
            "mode_gate": list(coupling.mode_gate),
            "source_mode_name": coupling.source_mode_name,
            "source_mode_gate": list(coupling.source_mode_gate),
            "target_mode_name": coupling.target_mode_name,
            "target_mode_gate": list(coupling.target_mode_gate),
            "shared_noise_group": coupling.shared_noise_group,
        }
        for coupling in inter_module_couplings
    ]
