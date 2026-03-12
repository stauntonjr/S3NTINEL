"""Live flight runtime objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from libs.behavior import BehaviorRegistry, BehaviorSample, BehaviorStepInput
from libs.simulation.aircraft.runtime import Aircraft
from libs.simulation.fault.runtime import FaultProgram
from libs.simulation.flight.spec import FlightSpec, InputProgramSpec, StepInputSpec
from libs.simulation.phase.runtime import PhaseProgram


DEFAULT_START_TIMESTAMP_UTC = datetime(2025, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True, slots=True)
class FlightTick:
    tail_id: str
    flight_id: str
    step_index: int
    timestamp_utc: datetime
    phase_label: str | None
    samples_by_module_id: dict[str, list[BehaviorSample]]
    fault_context_by_module: dict[str, dict[str, dict[str, Any]]]

    def telemetry_rows(self) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        for module_id, samples in self.samples_by_module_id.items():
            for sample in samples:
                metadata = dict(sample.metadata)
                rows.append(
                    {
                        "tail_id": self.tail_id,
                        "flight_id": self.flight_id,
                        "step_index": self.step_index,
                        "timestamp_utc": self.timestamp_utc,
                        "date_utc": self.timestamp_utc.date(),
                        "system_id": metadata.get("system_id"),
                        "subsystem_id": metadata.get("subsystem_id"),
                        "module_id": module_id,
                        "parameter_name": sample.parameter_name,
                        "sensor": sample.parameter_name,
                        "behavior_family_label": metadata.get("behavior_family_label"),
                        "parameter_datatype_label": metadata.get("parameter_datatype_label"),
                        "parameter_value_clean": sample.parameter_value_clean,
                        "parameter_value": None if sample.parameter_value is None else str(sample.parameter_value),
                        "phase_label": self.phase_label,
                        "target_source": metadata.get("target_source"),
                        "fault_active": bool(metadata.get("fault_active", False)),
                        "fault_applied": bool(metadata.get("fault_applied", False)),
                        "fault_family_label": metadata.get("fault_family_label", ""),
                        "fault_type": metadata.get("fault_type", ""),
                        "fault_window_id": metadata.get("fault_window_id", ""),
                    }
                )
        return rows

    def phase_row(self) -> dict[str, object]:
        return {
            "tail_id": self.tail_id,
            "flight_id": self.flight_id,
            "step_index": self.step_index,
            "timestamp_utc": self.timestamp_utc,
            "phase_label": self.phase_label,
            "date_utc": self.timestamp_utc.date(),
        }


@dataclass(slots=True)
class InputProgram:
    spec: InputProgramSpec

    @classmethod
    def from_spec(cls, spec: InputProgramSpec) -> "InputProgram":
        return cls(spec=spec)

    def _step_template(self, step_index: int) -> dict[str, dict[str, StepInputSpec]]:
        if not self.spec.steps:
            return {}
        if 0 <= int(step_index) < len(self.spec.steps):
            return self.spec.steps[int(step_index)]
        if self.spec.hold_last_step:
            return self.spec.steps[-1]
        return {}

    def step_inputs_for_step(
        self,
        *,
        step_index: int,
        dt_seconds: float,
    ) -> dict[str, dict[str, BehaviorStepInput]]:
        template = self._step_template(step_index)
        resolved: dict[str, dict[str, BehaviorStepInput]] = {}
        for module_id, parameters in template.items():
            resolved[str(module_id)] = {
                str(parameter_name): BehaviorStepInput(
                    dt_seconds=float(dt_seconds),
                    latent_state=dict(step_input_spec.latent_state),
                    context=dict(step_input_spec.context),
                )
                for parameter_name, step_input_spec in parameters.items()
            }
        return resolved


@dataclass(slots=True)
class Flight:
    spec: FlightSpec
    aircraft: Aircraft
    tail_id: str
    flight_id: str
    start_timestamp_utc: datetime
    input_program: InputProgram
    phase_program: PhaseProgram
    fault_program: FaultProgram
    step_index: int = 0
    current_timestamp_utc: datetime | None = None
    current_phase_label: str | None = None
    _initial_state_applied: bool = field(default=False, repr=False)

    @classmethod
    def from_spec(
        cls,
        spec: FlightSpec,
        *,
        tail_id: str = "",
        flight_id: str = "",
        start_timestamp_utc: datetime | None = None,
        behavior_registry: BehaviorRegistry | None = None,
    ) -> "Flight":
        return cls(
            spec=spec,
            aircraft=Aircraft.from_spec(
                spec.aircraft_spec,
                behavior_registry=behavior_registry,
            ),
            tail_id=str(tail_id),
            flight_id=str(flight_id),
            start_timestamp_utc=start_timestamp_utc or DEFAULT_START_TIMESTAMP_UTC,
            input_program=InputProgram.from_spec(spec.input_program_spec),
            phase_program=PhaseProgram.from_spec(spec.phase_program_spec),
            fault_program=FaultProgram.from_spec(spec.fault_program_spec),
        )

    def step(
        self,
        *,
        dt_seconds: float,
        apply_faults: bool = True,
    ) -> FlightTick:
        resolved_step_index = int(self.step_index)
        timestamp_utc = self.start_timestamp_utc + timedelta(seconds=float(resolved_step_index) * float(dt_seconds))
        phase_label = self.phase_program.label_for_step(resolved_step_index)
        self.phase_program.apply_to_aircraft(self.aircraft, step_index=resolved_step_index)
        step_inputs_by_module = self.phase_program.apply_to_step_inputs(
            self.aircraft,
            step_index=resolved_step_index,
            step_inputs_by_module=self.input_program.step_inputs_for_step(
                step_index=resolved_step_index,
                dt_seconds=dt_seconds,
            ),
            default_dt_seconds=float(dt_seconds),
        )
        initial_state_by_module = (
            dict(self.spec.initial_state_spec.values_by_module)
            if not self._initial_state_applied
            else {}
        )
        fault_context_by_module = (
            self.fault_program.context_for_step(resolved_step_index)
            if apply_faults
            else {}
        )
        samples_by_module_id = self.aircraft.step(
            step_inputs_by_module=step_inputs_by_module,
            initial_state_by_module=initial_state_by_module,
            fault_context_by_module=fault_context_by_module,
            apply_faults=apply_faults,
            timestamp_utc=timestamp_utc,
            current_phase_label=phase_label,
        )
        self.step_index = resolved_step_index + 1
        self.current_timestamp_utc = timestamp_utc
        self.current_phase_label = phase_label
        self._initial_state_applied = True
        return FlightTick(
            tail_id=self.tail_id,
            flight_id=self.flight_id,
            step_index=resolved_step_index,
            timestamp_utc=timestamp_utc,
            phase_label=phase_label,
            samples_by_module_id=samples_by_module_id,
            fault_context_by_module=fault_context_by_module,
        )

    def iter_ticks(
        self,
        *,
        n_steps: int,
        dt_seconds: float,
        apply_faults: bool = True,
    ):
        for _ in range(int(max(n_steps, 0))):
            yield self.step(
                dt_seconds=dt_seconds,
                apply_faults=apply_faults,
            )

    def simulate_rows(
        self,
        *,
        n_steps: int,
        dt_seconds: float,
        apply_faults: bool = True,
    ) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        telemetry_rows: list[dict[str, object]] = []
        phase_rows: list[dict[str, object]] = []
        for tick in self.iter_ticks(
            n_steps=n_steps,
            dt_seconds=dt_seconds,
            apply_faults=apply_faults,
        ):
            telemetry_rows.extend(tick.telemetry_rows())
            phase_rows.append(tick.phase_row())
        return telemetry_rows, phase_rows
