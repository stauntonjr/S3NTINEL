"""Live flight runtime objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from libs.behavior import BehaviorRegistry, BehaviorSample, BehaviorStepInput
from libs.simulation.event_truth import annotate_event_type_labels
from libs.simulation.fault.runtime import FaultProgram, MisbehaviorProgram, MisbehaviorStepContext
from libs.simulation.flight.spec import FlightSpec, InputProgramSpec, StepInputSpec
from libs.simulation.phase.runtime import PhaseProgram
from libs.simulation.tail.runtime import Tail

if TYPE_CHECKING:
    from libs.simulation.aircraft.runtime import Aircraft


DEFAULT_START_TIMESTAMP_UTC = datetime(2025, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True, slots=True)
class FlightTick:
    tail_id: str
    flight_id: str
    step_index: int
    timestamp_utc: datetime
    dt_seconds: float
    phase_label: str | None
    samples_by_module_id: dict[str, list[BehaviorSample]]
    step_misbehavior_context: MisbehaviorStepContext

    @property
    def misbehavior_context_by_module(self) -> dict[str, dict[str, dict[str, Any]]]:
        return self.step_misbehavior_context.parameter_context_by_module

    @property
    def coupling_misbehavior_context_by_id(self) -> dict[str, dict[str, Any]]:
        return self.step_misbehavior_context.coupling_context_by_id

    @property
    def fault_context_by_module(self) -> dict[str, dict[str, dict[str, Any]]]:
        return self.misbehavior_context_by_module

    def telemetry_rows(self) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        for module_id, samples in self.samples_by_module_id.items():
            for sample in samples:
                metadata = dict(sample.metadata)
                rate_hz = metadata.get("rate_hz")
                if not self._should_emit_sample(rate_hz):
                    continue
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
                        "unit": metadata.get("unit"),
                        "rate_hz": rate_hz,
                        "behavior_family_label": metadata.get("behavior_family_label"),
                        "parameter_datatype_label": metadata.get("parameter_datatype_label"),
                        "parameter_value_clean": sample.parameter_value_clean,
                        "parameter_value": None if sample.parameter_value is None else str(sample.parameter_value),
                        "phase_label": self.phase_label,
                        "target_source": metadata.get("target_source"),
                        "misbehavior_active": bool(metadata.get("misbehavior_active", False)),
                        "misbehavior_applied": bool(metadata.get("misbehavior_applied", False)),
                        "misbehavior_family_label": metadata.get("misbehavior_family_label", ""),
                        "misbehavior_detail_label": metadata.get("misbehavior_detail_label", ""),
                        "misbehavior_window_id": metadata.get("misbehavior_window_id", ""),
                        "event_type_label": metadata.get("event_type_label", ""),
                        "event_misbehavior_label": metadata.get("event_misbehavior_label", ""),
                        "anomaly_type_label": metadata.get("anomaly_type_label", ""),
                        "anomaly_score_label": metadata.get("anomaly_score_label"),
                        "coupling_id_label": metadata.get("coupling_id_label", ""),
                        "fault_active": bool(metadata.get("fault_active", False)),
                        "fault_applied": bool(metadata.get("fault_applied", False)),
                        "fault_family_label": metadata.get("fault_family_label", ""),
                        "fault_type": metadata.get("fault_type", ""),
                        "fault_window_id": metadata.get("fault_window_id", ""),
                    }
                )
        return rows

    def _should_emit_sample(self, rate_hz: object) -> bool:
        if rate_hz in (None, "", 0, 0.0):
            return True
        try:
            resolved_rate_hz = float(rate_hz)
        except Exception:
            return True
        if resolved_rate_hz <= 0.0:
            return True
        steps_per_sample = max(int(round(1.0 / (resolved_rate_hz * max(self.dt_seconds, 1e-9)))), 1)
        return int(self.step_index) % steps_per_sample == 0

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
                    context={
                        **dict(step_input_spec.context),
                        "step_index": int(step_index),
                    },
                )
                for parameter_name, step_input_spec in parameters.items()
            }
        return resolved


@dataclass(slots=True)
class Flight:
    spec: FlightSpec
    tail: Tail
    flight_id: str
    start_timestamp_utc: datetime
    input_program: InputProgram
    phase_program: PhaseProgram
    misbehavior_program: MisbehaviorProgram
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
        tail = Tail.from_spec(
            spec.aircraft_spec,
            tail_id=str(tail_id),
            behavior_registry=behavior_registry,
        )
        return cls.from_tail(
            tail,
            spec,
            flight_id=flight_id,
            start_timestamp_utc=start_timestamp_utc,
        )

    @classmethod
    def from_tail(
        cls,
        tail: Tail,
        spec: FlightSpec,
        *,
        flight_id: str = "",
        start_timestamp_utc: datetime | None = None,
    ) -> "Flight":
        return cls(
            spec=spec,
            tail=tail,
            flight_id=str(flight_id),
            start_timestamp_utc=start_timestamp_utc or DEFAULT_START_TIMESTAMP_UTC,
            input_program=InputProgram.from_spec(spec.input_program_spec),
            phase_program=PhaseProgram.from_spec(spec.phase_program_spec),
            misbehavior_program=MisbehaviorProgram.from_spec(spec.misbehavior_program_spec),
        )

    @property
    def aircraft(self) -> "Aircraft":
        return self.tail.aircraft

    @property
    def fault_program(self) -> FaultProgram:
        return self.misbehavior_program

    @property
    def tail_id(self) -> str:
        return self.tail.id

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
        step_misbehavior_context = (
            self.misbehavior_program.step_context_for_step(resolved_step_index)
            if apply_faults
            else MisbehaviorStepContext(parameter_context_by_module={}, coupling_context_by_id={})
        )
        samples_by_module_id = self.aircraft.step(
            step_inputs_by_module=step_inputs_by_module,
            initial_state_by_module=initial_state_by_module,
            fault_context_by_module=step_misbehavior_context.parameter_context_by_module,
            coupling_misbehavior_context_by_id=step_misbehavior_context.coupling_context_by_id,
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
            dt_seconds=float(dt_seconds),
            phase_label=phase_label,
            samples_by_module_id=samples_by_module_id,
            step_misbehavior_context=step_misbehavior_context,
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
        return annotate_event_type_labels(telemetry_rows), phase_rows
