"""Example phase programs."""

from __future__ import annotations

from libs.simulation.phase.spec import (
    PhaseProgramSpec,
    PhaseScheduleSpec,
    PhaseSegmentSpec,
)


def build_constant_phase_program_spec(phase_label: str) -> PhaseProgramSpec:
    return PhaseProgramSpec(
        schedule=PhaseScheduleSpec(
            segments=(PhaseSegmentSpec(phase_label=phase_label, duration_steps=1),),
            repeat=True,
        ),
    )
