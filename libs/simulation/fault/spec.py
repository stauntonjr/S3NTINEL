"""Misbehavior program specification objects with deprecated fault aliases."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass(frozen=True, slots=True)
class MisbehaviorWindowSpec:
    start_step: int
    end_step_exclusive: int
    context: dict[str, Any]
    subject_kind: Literal["parameter", "coupling"] = "parameter"
    module_id: str | None = None
    parameter_name: str | None = None
    coupling_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        subject_kind = str(self.subject_kind or "parameter")
        if subject_kind not in {"parameter", "coupling"}:
            raise ValueError(f"unsupported subject_kind={subject_kind!r}")
        object.__setattr__(self, "subject_kind", subject_kind)
        if subject_kind == "parameter":
            if not self.module_id or not self.parameter_name:
                raise ValueError("parameter misbehavior windows require module_id and parameter_name")
        elif not self.coupling_id:
            raise ValueError("coupling misbehavior windows require coupling_id")


@dataclass(frozen=True, slots=True)
class MisbehaviorProgramSpec:
    windows: tuple[MisbehaviorWindowSpec, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)


FaultWindowSpec = MisbehaviorWindowSpec
FaultProgramSpec = MisbehaviorProgramSpec
