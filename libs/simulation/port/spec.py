"""Port specification objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


PortDirection = Literal["input", "output"]


@dataclass(frozen=True, slots=True)
class PortSpec:
    port_name: str
    direction: PortDirection
    value_datatype_label: str
    unit: str = ""
    description: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def input(
        cls,
        *,
        port_name: str,
        value_datatype_label: str,
        unit: str = "",
        description: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> "PortSpec":
        return cls(
            port_name=port_name,
            direction="input",
            value_datatype_label=value_datatype_label,
            unit=unit,
            description=description,
            metadata=dict(metadata or {}),
        )

    @classmethod
    def output(
        cls,
        *,
        port_name: str,
        value_datatype_label: str,
        unit: str = "",
        description: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> "PortSpec":
        return cls(
            port_name=port_name,
            direction="output",
            value_datatype_label=value_datatype_label,
            unit=unit,
            description=description,
            metadata=dict(metadata or {}),
        )
