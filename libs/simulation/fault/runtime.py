"""Misbehavior program runtime helpers with deprecated fault aliases."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from libs.simulation.fault.spec import MisbehaviorProgramSpec, MisbehaviorWindowSpec


_EVENT_TYPE_BY_MISBEHAVIOR = {
    "bias": "threshold",
    "drift": "drift_guard",
    "dwell_violation": "dwell_violation",
    "illegal_transition": "illegal_transition",
    "offset": "threshold",
    "oscillation": "oscillation",
    "saturation": "threshold",
    "state_chatter": "transition",
}


def _default_event_type_label(misbehavior_detail_label: str) -> str | None:
    label = _EVENT_TYPE_BY_MISBEHAVIOR.get(str(misbehavior_detail_label or ""))
    return None if not label else str(label)


@dataclass(frozen=True, slots=True)
class MisbehaviorStepContext:
    parameter_context_by_module: dict[str, dict[str, dict[str, Any]]]
    coupling_context_by_id: dict[str, dict[str, Any]]


def _normalized_window_id(window: MisbehaviorWindowSpec, *, offset: int) -> str:
    metadata = dict(window.metadata)
    window_id = metadata.get("misbehavior_window_id") or metadata.get("fault_window_id")
    if window_id:
        return str(window_id)
    if window.subject_kind == "coupling":
        return (
            f"coupling:{window.coupling_id}:{int(window.start_step)}:"
            f"{int(window.end_step_exclusive)}:{offset}"
        )
    return (
        f"{window.module_id}:{window.parameter_name}:{int(window.start_step)}:"
        f"{int(window.end_step_exclusive)}:{offset}"
    )


def _resolved_context(window: MisbehaviorWindowSpec, *, offset: int) -> dict[str, Any]:
    metadata = dict(window.metadata)
    context = dict(window.context)
    misbehavior_window_id = _normalized_window_id(window, offset=offset)
    misbehavior_family_label = str(
        metadata.get("misbehavior_family_label")
        or context.get("misbehavior_family_label")
        or context.get("violation_type")
        or metadata.get("fault_type")
        or ""
    )
    misbehavior_detail_label = str(
        metadata.get("misbehavior_detail_label")
        or context.get("misbehavior_detail_label")
        or context.get("violation_type")
        or metadata.get("fault_type")
        or misbehavior_family_label
        or ""
    )
    behavior_family_label = str(
        metadata.get("behavior_family_label")
        or metadata.get("fault_family_label")
        or context.get("behavior_family_label")
        or context.get("fault_family_label")
        or ""
    )
    event_type_label = metadata.get("event_type_label") or context.get("event_type_label")
    if not event_type_label:
        event_type_label = _default_event_type_label(misbehavior_detail_label)
    context.setdefault("misbehavior_active", True)
    context.setdefault("misbehavior_window_id", misbehavior_window_id)
    context.setdefault("misbehavior_family_label", misbehavior_family_label)
    context.setdefault("misbehavior_detail_label", misbehavior_detail_label)
    context.setdefault("misbehavior_start_step", int(window.start_step))
    context.setdefault("misbehavior_end_step_exclusive", int(window.end_step_exclusive))
    context.setdefault("event_type_label", event_type_label)
    context.setdefault("anomaly_type_label", misbehavior_family_label or None)
    if context.get("anomaly_score_label") is None and misbehavior_family_label:
        context["anomaly_score_label"] = 1.0

    # Compatibility aliases during the migration window.
    context.setdefault("fault_active", bool(context.get("misbehavior_active", False)))
    context.setdefault("fault_window_id", metadata.get("fault_window_id", misbehavior_window_id))
    context.setdefault("fault_type", metadata.get("fault_type", misbehavior_detail_label))
    context.setdefault("fault_family_label", behavior_family_label)
    context.setdefault("fault_start_step", int(window.start_step))
    context.setdefault("fault_end_step_exclusive", int(window.end_step_exclusive))
    return context


@dataclass(slots=True)
class MisbehaviorProgram:
    spec: MisbehaviorProgramSpec

    @classmethod
    def from_spec(cls, spec: MisbehaviorProgramSpec | None) -> "MisbehaviorProgram":
        return cls(spec=spec or MisbehaviorProgramSpec())

    def step_context_for_step(self, step_index: int) -> MisbehaviorStepContext:
        parameter_context_by_module: dict[str, dict[str, dict[str, Any]]] = {}
        coupling_context_by_id: dict[str, dict[str, Any]] = {}
        for offset, window in enumerate(self.spec.windows, start=1):
            if not (int(window.start_step) <= int(step_index) < int(window.end_step_exclusive)):
                continue
            context = _resolved_context(window, offset=offset)
            if window.subject_kind == "coupling":
                coupling_context_by_id[str(window.coupling_id)] = context
            else:
                parameter_context_by_module.setdefault(str(window.module_id), {})[str(window.parameter_name)] = context
        return MisbehaviorStepContext(
            parameter_context_by_module=parameter_context_by_module,
            coupling_context_by_id=coupling_context_by_id,
        )

    def context_for_step(self, step_index: int) -> dict[str, dict[str, dict[str, Any]]]:
        return self.step_context_for_step(step_index).parameter_context_by_module

    def coupling_context_for_step(self, step_index: int) -> dict[str, dict[str, Any]]:
        return self.step_context_for_step(step_index).coupling_context_by_id


FaultProgram = MisbehaviorProgram
