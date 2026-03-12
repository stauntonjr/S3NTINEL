"""Live port runtime objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from libs.simulation.port.spec import PortSpec


@dataclass(slots=True)
class Port:
    name: str
    direction: str
    value_datatype_label: str
    unit: str = ""
    description: str = ""
    current_value: object | None = None
    timestamp_utc: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_spec(cls, spec: PortSpec) -> "Port":
        return cls(
            name=str(spec.port_name),
            direction=str(spec.direction),
            value_datatype_label=str(spec.value_datatype_label),
            unit=str(spec.unit),
            description=str(spec.description),
            metadata=dict(spec.metadata),
        )
