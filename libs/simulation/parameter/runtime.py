"""Live parameter runtime objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from libs.behavior import Behavior, BehaviorRegistry, build_default_behavior_registry
from libs.simulation.parameter.spec import ParameterSpec


@dataclass(slots=True)
class Parameter:
    name: str
    system_id: str
    subsystem_id: str
    module_id: str
    datatype_label: str
    unit: str = ""
    behavior_family_label: str | None = None
    allowed_fault_families: tuple[str, ...] = ()
    input_port_names: tuple[str, ...] = ()
    output_port_name: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    behavior: Behavior | None = None
    parameter_value: object | None = None
    parameter_value_clean: object | None = None
    timestamp_utc: datetime | None = None

    @classmethod
    def from_spec(
        cls,
        spec: ParameterSpec,
        *,
        behavior_registry: BehaviorRegistry | None = None,
    ) -> "Parameter":
        behavior = None
        if spec.behavior_family_label:
            resolved_behavior_registry = behavior_registry or build_default_behavior_registry()
            try:
                behavior = resolved_behavior_registry.get(str(spec.behavior_family_label))
            except KeyError as exc:
                raise ValueError(
                    f"parameter {spec.parameter_name!r} references unknown behavior_family_label="
                    f"{spec.behavior_family_label!r}"
                ) from exc
        return cls(
            name=str(spec.parameter_name),
            system_id=str(spec.system_id),
            subsystem_id=str(spec.subsystem_id),
            module_id=str(spec.module_id),
            datatype_label=str(spec.parameter_datatype_label),
            unit=str(spec.unit),
            behavior_family_label=spec.behavior_family_label,
            allowed_fault_families=tuple(str(item) for item in spec.allowed_fault_families),
            input_port_names=tuple(str(item) for item in spec.input_port_names),
            output_port_name=(None if spec.output_port_name is None else str(spec.output_port_name)),
            metadata=dict(spec.metadata),
            behavior=behavior,
        )

    def step(
        self,
        *,
        parameter_value: object,
        parameter_value_clean: object | None = None,
        timestamp_utc: datetime | None = None,
    ) -> None:
        self.parameter_value = parameter_value
        self.parameter_value_clean = parameter_value_clean
        self.timestamp_utc = timestamp_utc
