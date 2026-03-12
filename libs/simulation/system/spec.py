"""System specification objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from libs.simulation.subsystem.spec import SubsystemSpec


@dataclass(frozen=True, slots=True)
class SystemSpec:
    system_id: str
    subsystems: tuple[SubsystemSpec, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)
