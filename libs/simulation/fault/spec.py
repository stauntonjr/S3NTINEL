"""Misbehavior program specification objects with deprecated fault aliases."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


BENCHMARK_RECOVERABILITY_LADDER = (
    "detection_only",
    "parameter_visible_only",
    "module_recoverable",
    "subsystem_recoverable",
)
BENCHMARK_RECOVERABILITY_TARGETS = BENCHMARK_RECOVERABILITY_LADDER
BENCHMARK_OPTIMIZATION_SCOPE_ORDER = (
    "detection",
    "parameter",
    "module",
    "subsystem",
)
_BENCHMARK_ELIGIBLE_TARGETS_BY_SCOPE = {
    "detection": BENCHMARK_RECOVERABILITY_TARGETS,
    "parameter": (
        "parameter_visible_only",
        "module_recoverable",
        "subsystem_recoverable",
    ),
    "module": ("module_recoverable",),
    "subsystem": (
        "module_recoverable",
        "subsystem_recoverable",
    ),
}

OBSERVED_RECOVERABILITY_STRENGTH_TIERS = (
    "undetected",
    "detection_only",
    "parameter_visible_only",
    "subsystem_recoverable",
    "module_recoverable",
)
_OBSERVED_RECOVERABILITY_STRENGTH_RANK = {
    tier: index
    for index, tier in enumerate(OBSERVED_RECOVERABILITY_STRENGTH_TIERS)
}


def observed_recoverability_strength_rank(label: str | None) -> int:
    return _OBSERVED_RECOVERABILITY_STRENGTH_RANK.get(str(label or ""), -1)


def recoverability_target_alignment_status(*, observed_tier: str | None, declared_target: str | None) -> str:
    observed_rank = observed_recoverability_strength_rank(observed_tier)
    declared_rank = observed_recoverability_strength_rank(declared_target)
    if declared_rank < 0:
        return "undeclared"
    if observed_rank < declared_rank:
        return "missed_target"
    if observed_rank == declared_rank:
        return "met_target"
    return "exceeded_target"


def benchmark_eligible_declared_tiers_for_scope(scope: str | None) -> tuple[str, ...]:
    return tuple(_BENCHMARK_ELIGIBLE_TARGETS_BY_SCOPE.get(str(scope or ""), ()))


def benchmark_scope_includes_declared_tier(*, scope: str | None, declared_tier: str | None) -> bool:
    return str(declared_tier or "") in benchmark_eligible_declared_tiers_for_scope(scope)


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
        recoverability_target = resolve_window_benchmark_recoverability_target(self)
        if recoverability_target and recoverability_target not in BENCHMARK_RECOVERABILITY_TARGETS:
            raise ValueError(
                "unsupported benchmark_recoverability_target="
                f"{recoverability_target!r}; expected one of {BENCHMARK_RECOVERABILITY_TARGETS}"
            )


@dataclass(frozen=True, slots=True)
class MisbehaviorProgramSpec:
    windows: tuple[MisbehaviorWindowSpec, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)


FaultWindowSpec = MisbehaviorWindowSpec
FaultProgramSpec = MisbehaviorProgramSpec


def resolve_window_fault_window_id(window: Any) -> str | None:
    metadata = dict(getattr(window, "metadata", {}) or {})
    value = metadata.get("fault_window_id") or metadata.get("misbehavior_window_id")
    if value is None:
        return None
    text = str(value)
    return text or None


def resolve_window_benchmark_recoverability_target(window: Any) -> str | None:
    metadata = dict(getattr(window, "metadata", {}) or {})
    context = dict(getattr(window, "context", {}) or {})
    value = metadata.get("benchmark_recoverability_target") or context.get("benchmark_recoverability_target")
    if value is None:
        return None
    text = str(value)
    return text or None


def resolve_window_fault_type(window: Any) -> str | None:
    metadata = dict(getattr(window, "metadata", {}) or {})
    context = dict(getattr(window, "context", {}) or {})
    value = (
        metadata.get("fault_type")
        or metadata.get("misbehavior_detail_label")
        or context.get("misbehavior_detail_label")
        or context.get("violation_type")
    )
    if value is None:
        return None
    text = str(value)
    return text or None
