"""Flight specification objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from libs.simulation.aircraft.spec import AircraftSpec
from libs.simulation.fault.spec import FaultProgramSpec
from libs.simulation.phase.spec import PhaseProgramSpec


@dataclass(frozen=True, slots=True)
class StepInputSpec:
    context: dict[str, Any] = field(default_factory=dict)
    latent_state: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class InputProgramSpec:
    steps: tuple[dict[str, dict[str, StepInputSpec]], ...]
    hold_last_step: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class InitialStateSpec:
    values_by_module: dict[str, dict[str, object]] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class FlightSpec:
    aircraft_spec: AircraftSpec
    input_program_spec: InputProgramSpec
    initial_state_spec: InitialStateSpec = field(default_factory=InitialStateSpec)
    phase_program_spec: PhaseProgramSpec | None = None
    fault_program_spec: FaultProgramSpec | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
