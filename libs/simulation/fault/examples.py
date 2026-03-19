"""Example misbehavior programs with deprecated fault builder aliases."""

from __future__ import annotations

from typing import Any

from libs.simulation.fault.spec import MisbehaviorProgramSpec, MisbehaviorWindowSpec


def build_misbehavior_window_spec(
    *,
    start_step: int,
    end_step_exclusive: int,
    context: dict[str, Any],
    subject_kind: str = "parameter",
    module_id: str | None = None,
    parameter_name: str | None = None,
    coupling_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> MisbehaviorWindowSpec:
    return MisbehaviorWindowSpec(
        start_step=int(start_step),
        end_step_exclusive=int(end_step_exclusive),
        context=dict(context),
        subject_kind=str(subject_kind),
        module_id=None if module_id is None else str(module_id),
        parameter_name=None if parameter_name is None else str(parameter_name),
        coupling_id=None if coupling_id is None else str(coupling_id),
        metadata=dict(metadata or {}),
    )


def build_misbehavior_program_spec(
    *,
    windows: tuple[MisbehaviorWindowSpec, ...],
    metadata: dict[str, Any] | None = None,
) -> MisbehaviorProgramSpec:
    return MisbehaviorProgramSpec(
        windows=tuple(windows),
        metadata=dict(metadata or {}),
    )


def build_no_misbehavior_program_spec() -> MisbehaviorProgramSpec:
    return MisbehaviorProgramSpec()


def build_fault_window_spec(
    *,
    module_id: str,
    parameter_name: str,
    start_step: int,
    end_step_exclusive: int,
    context: dict[str, Any],
    metadata: dict[str, Any] | None = None,
) -> MisbehaviorWindowSpec:
    return build_misbehavior_window_spec(
        module_id=module_id,
        parameter_name=parameter_name,
        start_step=start_step,
        end_step_exclusive=end_step_exclusive,
        context=context,
        metadata=metadata,
    )


def build_fault_program_spec(
    *,
    windows: tuple[MisbehaviorWindowSpec, ...],
    metadata: dict[str, Any] | None = None,
) -> MisbehaviorProgramSpec:
    return build_misbehavior_program_spec(
        windows=windows,
        metadata=metadata,
    )


def build_no_fault_program_spec() -> MisbehaviorProgramSpec:
    return build_no_misbehavior_program_spec()
