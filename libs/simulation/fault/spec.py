"""Fault program specification objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class FaultWindowSpec:
    module_id: str
    parameter_name: str
    start_step: int
    end_step_exclusive: int
    context: dict[str, Any]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class FaultProgramSpec:
    windows: tuple[FaultWindowSpec, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)
