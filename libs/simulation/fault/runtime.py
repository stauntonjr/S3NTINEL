"""Fault program runtime helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from libs.simulation.fault.spec import FaultProgramSpec


@dataclass(slots=True)
class FaultProgram:
    spec: FaultProgramSpec

    @classmethod
    def from_spec(cls, spec: FaultProgramSpec | None) -> "FaultProgram":
        return cls(spec=spec or FaultProgramSpec())

    def context_for_step(self, step_index: int) -> dict[str, dict[str, dict[str, Any]]]:
        resolved: dict[str, dict[str, dict[str, Any]]] = {}
        for offset, window in enumerate(self.spec.windows, start=1):
            if int(window.start_step) <= int(step_index) < int(window.end_step_exclusive):
                metadata = dict(window.metadata)
                fault_window_id = str(
                    metadata.get(
                        "fault_window_id",
                        f"{window.module_id}:{window.parameter_name}:{int(window.start_step)}:{int(window.end_step_exclusive)}:{offset}",
                    )
                )
                context = dict(window.context)
                context.setdefault("fault_active", True)
                context.setdefault("fault_window_id", fault_window_id)
                context.setdefault("fault_type", metadata.get("fault_type", context.get("violation_type")))
                context.setdefault("fault_family_label", metadata.get("fault_family_label"))
                context.setdefault("fault_start_step", int(window.start_step))
                context.setdefault("fault_end_step_exclusive", int(window.end_step_exclusive))
                resolved.setdefault(str(window.module_id), {})[str(window.parameter_name)] = context
        return resolved
