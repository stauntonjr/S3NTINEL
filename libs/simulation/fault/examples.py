"""Example fault programs."""

from __future__ import annotations

from typing import Any

from libs.simulation.fault.spec import FaultProgramSpec, FaultWindowSpec


def build_fault_window_spec(
    *,
    module_id: str,
    parameter_name: str,
    start_step: int,
    end_step_exclusive: int,
    context: dict[str, Any],
    metadata: dict[str, Any] | None = None,
) -> FaultWindowSpec:
    return FaultWindowSpec(
        module_id=module_id,
        parameter_name=parameter_name,
        start_step=int(start_step),
        end_step_exclusive=int(end_step_exclusive),
        context=dict(context),
        metadata=dict(metadata or {}),
    )


def build_fault_program_spec(*, windows: tuple[FaultWindowSpec, ...], metadata: dict[str, Any] | None = None) -> FaultProgramSpec:
    return FaultProgramSpec(
        windows=tuple(windows),
        metadata=dict(metadata or {}),
    )


def build_no_fault_program_spec() -> FaultProgramSpec:
    return FaultProgramSpec()
