"""Phase program specification objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class PhaseSegmentSpec:
    phase_label: str
    duration_steps: int
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PhaseScheduleSpec:
    segments: tuple[PhaseSegmentSpec, ...]
    repeat: bool = False


@dataclass(frozen=True, slots=True)
class PhaseEnvelopeSpec:
    phase_label: str
    step_input_context_by_module: dict[str, dict[str, dict[str, Any]]] = field(default_factory=dict)
    mode_state_by_module: dict[str, dict[str, Any]] = field(default_factory=dict)
    latent_state_by_module: dict[str, dict[str, float]] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PhaseProgramSpec:
    explicit_labels_by_step: tuple[str | None, ...] = ()
    schedule: PhaseScheduleSpec | None = None
    envelopes: tuple[PhaseEnvelopeSpec, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)
