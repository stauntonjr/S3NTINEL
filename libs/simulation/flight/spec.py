"""Flight specification objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from libs.simulation.aircraft.spec import AircraftSpec
from libs.simulation.fault.spec import FaultProgramSpec, MisbehaviorProgramSpec
from libs.simulation.phase.spec import PhaseProgramSpec


def _window_detail_label(window: Any) -> str:
    context = dict(getattr(window, "context", {}) or {})
    metadata = dict(getattr(window, "metadata", {}) or {})
    return str(
        metadata.get("misbehavior_detail_label")
        or context.get("misbehavior_detail_label")
        or context.get("violation_type")
        or metadata.get("fault_type")
        or metadata.get("misbehavior_family_label")
        or context.get("misbehavior_family_label")
        or ""
    )


def _validate_misbehavior_windows(
    *,
    aircraft_spec: AircraftSpec,
    misbehavior_program_spec: MisbehaviorProgramSpec | None,
) -> None:
    if misbehavior_program_spec is None:
        return
    module_by_id = {module_spec.module_id: module_spec for module_spec in aircraft_spec.iter_module_specs()}
    parameter_by_key = {
        (str(module_spec.module_id), str(parameter_spec.parameter_name)): parameter_spec
        for module_spec in aircraft_spec.iter_module_specs()
        for parameter_spec in module_spec.parameters
    }
    coupling_by_id = {coupling.coupling_id: coupling for coupling in aircraft_spec.couplings}

    for window in misbehavior_program_spec.windows:
        detail_label = _window_detail_label(window)
        if window.subject_kind == "parameter":
            module_spec = module_by_id.get(str(window.module_id))
            if module_spec is None:
                raise ValueError(f"misbehavior window references unknown module_id={window.module_id!r}")
            parameter_spec = parameter_by_key.get((str(window.module_id), str(window.parameter_name)))
            if parameter_spec is None:
                raise ValueError(
                    "misbehavior window references unknown parameter_name="
                    f"{window.parameter_name!r} on module_id={window.module_id!r}"
                )
            allowed = tuple(str(name) for name in getattr(parameter_spec, "allowed_fault_families", ()) or ())
            if detail_label and allowed and detail_label not in allowed:
                raise ValueError(
                    f"misbehavior detail {detail_label!r} is not allowed for "
                    f"{window.module_id!r}.{window.parameter_name!r}; expected one of {allowed}"
                )
            continue

        coupling_spec = coupling_by_id.get(str(window.coupling_id))
        if coupling_spec is None:
            raise ValueError(f"misbehavior window references unknown coupling_id={window.coupling_id!r}")
        allowed = tuple(str(name) for name in getattr(coupling_spec, "allowed_misbehavior_families", ()) or ())
        if detail_label and allowed and detail_label not in allowed:
            raise ValueError(
                f"misbehavior detail {detail_label!r} is not allowed for coupling_id={window.coupling_id!r}; "
                f"expected one of {allowed}"
            )


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
    misbehavior_program_spec: MisbehaviorProgramSpec | None = None
    fault_program_spec: FaultProgramSpec | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.misbehavior_program_spec is not None and self.fault_program_spec is not None:
            if self.misbehavior_program_spec != self.fault_program_spec:
                raise ValueError("misbehavior_program_spec and fault_program_spec must match when both are provided")
        canonical = self.misbehavior_program_spec or self.fault_program_spec
        object.__setattr__(self, "misbehavior_program_spec", canonical)
        object.__setattr__(self, "fault_program_spec", canonical)
        _validate_misbehavior_windows(
            aircraft_spec=self.aircraft_spec,
            misbehavior_program_spec=canonical,
        )
