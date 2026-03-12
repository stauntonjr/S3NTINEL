"""Subsystem specification objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from libs.simulation.module.spec import ModuleSpec


@dataclass(frozen=True, slots=True)
class SubsystemSpec:
    subsystem_id: str
    system_id: str
    modules: tuple[ModuleSpec, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)
