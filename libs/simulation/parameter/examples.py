"""Example parameter specs."""

from __future__ import annotations

from typing import Any

from libs.simulation.parameter.spec import ParameterSpec


def build_numeric_parameter_spec(
    *,
    parameter_name: str,
    module_id: str,
    subsystem_id: str,
    system_id: str,
    behavior_family_label: str,
    unit: str = "",
    sampling_rate_hz: float | None = None,
    allowed_fault_families: tuple[str, ...] = (),
    input_port_names: tuple[str, ...] = (),
    output_port_name: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> ParameterSpec:
    return ParameterSpec(
        parameter_name=parameter_name,
        system_id=system_id,
        subsystem_id=subsystem_id,
        module_id=module_id,
        parameter_datatype_label="numeric",
        unit=unit,
        behavior_family_label=behavior_family_label,
        sampling_rate_hz=(None if sampling_rate_hz is None else float(sampling_rate_hz)),
        allowed_fault_families=tuple(str(name) for name in allowed_fault_families),
        input_port_names=tuple(str(name) for name in input_port_names),
        output_port_name=(str(output_port_name) if output_port_name else None),
        metadata=dict(metadata or {}),
    )


def build_categorical_parameter_spec(
    *,
    parameter_name: str,
    module_id: str,
    subsystem_id: str,
    system_id: str,
    behavior_family_label: str = "discrete_state",
    unit: str = "state",
    sampling_rate_hz: float | None = None,
    allowed_fault_families: tuple[str, ...] = (),
    input_port_names: tuple[str, ...] = (),
    output_port_name: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> ParameterSpec:
    return ParameterSpec(
        parameter_name=parameter_name,
        system_id=system_id,
        subsystem_id=subsystem_id,
        module_id=module_id,
        parameter_datatype_label="categorical",
        unit=unit,
        behavior_family_label=behavior_family_label,
        sampling_rate_hz=(None if sampling_rate_hz is None else float(sampling_rate_hz)),
        allowed_fault_families=tuple(str(name) for name in allowed_fault_families),
        input_port_names=tuple(str(name) for name in input_port_names),
        output_port_name=(str(output_port_name) if output_port_name else None),
        metadata=dict(metadata or {}),
    )
