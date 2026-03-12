"""Phase program runtime and helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, TYPE_CHECKING

from libs.behavior import BehaviorStepInput
from libs.simulation.phase.catalog import CANONICAL_PHASE_IDS_BY_LABEL, CANONICAL_PHASE_LABELS
from libs.simulation.phase.spec import (
    PhaseEnvelopeSpec,
    PhaseProgramSpec,
    PhaseScheduleSpec,
    PhaseSegmentSpec,
)

if TYPE_CHECKING:
    from libs.simulation.aircraft.runtime import Aircraft


def validate_phase_label(phase_label: str | None) -> str | None:
    if phase_label is None:
        return None
    resolved = str(phase_label)
    if resolved not in CANONICAL_PHASE_IDS_BY_LABEL:
        allowed = ", ".join(CANONICAL_PHASE_LABELS)
        raise ValueError(f"unknown phase label '{resolved}'; expected one of: {allowed}")
    return resolved


def _validated_phase_segment(segment: PhaseSegmentSpec) -> PhaseSegmentSpec:
    resolved_label = validate_phase_label(segment.phase_label)
    if int(segment.duration_steps) <= 0:
        raise ValueError("duration_steps must be positive")
    if resolved_label == segment.phase_label:
        return segment
    return PhaseSegmentSpec(
        phase_label=str(resolved_label),
        duration_steps=int(segment.duration_steps),
        metadata=dict(segment.metadata),
    )


def _validated_phase_envelope(envelope: PhaseEnvelopeSpec) -> PhaseEnvelopeSpec:
    resolved_label = validate_phase_label(envelope.phase_label)
    if resolved_label == envelope.phase_label:
        return envelope
    return PhaseEnvelopeSpec(
        phase_label=str(resolved_label),
        step_input_context_by_module=dict(envelope.step_input_context_by_module),
        mode_state_by_module=dict(envelope.mode_state_by_module),
        latent_state_by_module=dict(envelope.latent_state_by_module),
        metadata=dict(envelope.metadata),
    )


def resolve_phase_label_for_step(
    phase_schedule: PhaseScheduleSpec | None,
    step_index: int,
) -> str | None:
    if phase_schedule is None:
        return None
    if step_index < 0:
        raise ValueError("step_index must be non-negative")
    if not phase_schedule.segments:
        raise ValueError("phase schedule must contain at least one segment")

    resolved_segments = tuple(_validated_phase_segment(segment) for segment in phase_schedule.segments)
    total_duration = sum(int(segment.duration_steps) for segment in resolved_segments)
    if total_duration <= 0:
        raise ValueError("phase schedule duration must be positive")
    resolved_step_index = int(step_index)
    if phase_schedule.repeat:
        resolved_step_index %= total_duration

    elapsed = 0
    for segment in resolved_segments:
        elapsed += int(segment.duration_steps)
        if resolved_step_index < elapsed:
            return segment.phase_label
    return resolved_segments[-1].phase_label


def index_phase_envelopes_by_label(
    phase_envelopes: tuple[PhaseEnvelopeSpec, ...],
) -> dict[str, PhaseEnvelopeSpec]:
    indexed: dict[str, PhaseEnvelopeSpec] = {}
    for envelope in phase_envelopes:
        resolved = _validated_phase_envelope(envelope)
        if resolved.phase_label in indexed:
            raise ValueError(f"duplicate phase envelope for phase_label='{resolved.phase_label}'")
        indexed[resolved.phase_label] = resolved
    return indexed


@dataclass(slots=True)
class PhaseProgram:
    explicit_labels_by_step: tuple[str | None, ...]
    schedule: PhaseScheduleSpec | None
    envelopes_by_label: dict[str, PhaseEnvelopeSpec]

    @classmethod
    def from_spec(cls, spec: PhaseProgramSpec | None) -> "PhaseProgram":
        if spec is None:
            return cls(
                explicit_labels_by_step=(),
                schedule=None,
                envelopes_by_label={},
            )
        explicit_labels = tuple(validate_phase_label(item) for item in spec.explicit_labels_by_step)
        return cls(
            explicit_labels_by_step=explicit_labels,
            schedule=spec.schedule,
            envelopes_by_label=index_phase_envelopes_by_label(tuple(spec.envelopes)),
        )

    def label_for_step(self, step_index: int) -> str | None:
        if self.explicit_labels_by_step:
            capped_index = min(max(int(step_index), 0), len(self.explicit_labels_by_step) - 1)
            return self.explicit_labels_by_step[capped_index]
        return resolve_phase_label_for_step(self.schedule, int(step_index))

    def envelope_for_step(self, step_index: int) -> PhaseEnvelopeSpec | None:
        phase_label = self.label_for_step(step_index)
        if phase_label is None:
            return None
        return self.envelopes_by_label.get(str(phase_label))

    def apply_to_aircraft(self, aircraft: "Aircraft", *, step_index: int) -> None:
        envelope = self.envelope_for_step(step_index)
        if envelope is None:
            return
        for module_id, latent_updates in envelope.latent_state_by_module.items():
            module = aircraft.module(str(module_id))
            for latent_name, latent_value in latent_updates.items():
                module.latent_state_by_name[str(latent_name)] = float(latent_value)
        for module_id, mode_updates in envelope.mode_state_by_module.items():
            module = aircraft.module(str(module_id))
            for mode_name, mode_value in mode_updates.items():
                module.mode_state_by_name[str(mode_name)] = mode_value

    def apply_to_step_inputs(
        self,
        aircraft: "Aircraft",
        *,
        step_index: int,
        step_inputs_by_module: Mapping[str, Mapping[str, BehaviorStepInput]],
        default_dt_seconds: float,
    ) -> dict[str, dict[str, BehaviorStepInput]]:
        resolved: dict[str, dict[str, BehaviorStepInput]] = {
            str(module_id): {
                str(parameter_name): step_input
                for parameter_name, step_input in parameter_inputs.items()
            }
            for module_id, parameter_inputs in step_inputs_by_module.items()
        }
        envelope = self.envelope_for_step(step_index)
        if envelope is None:
            return resolved
        for module_id, parameter_contexts in envelope.step_input_context_by_module.items():
            module_key = str(module_id)
            module = aircraft.module(module_key)
            module_inputs = resolved.setdefault(module_key, {})
            for parameter_name, context_updates in parameter_contexts.items():
                parameter_key = str(parameter_name)
                step_input = module_inputs.get(parameter_key)
                if step_input is not None:
                    merged_context = dict(step_input.context)
                    merged_context.update(dict(context_updates))
                    module_inputs[parameter_key] = BehaviorStepInput(
                        dt_seconds=float(step_input.dt_seconds),
                        latent_state=dict(step_input.latent_state),
                        context=merged_context,
                    )
                    continue
                module_inputs[parameter_key] = BehaviorStepInput(
                    dt_seconds=default_dt_seconds,
                    latent_state=dict(module.latent_state_by_name),
                    context=dict(context_updates),
                )
            for parameter_name in module.parameters:
                module_inputs.setdefault(
                    str(parameter_name),
                    BehaviorStepInput(
                        dt_seconds=default_dt_seconds,
                        latent_state=dict(module.latent_state_by_name),
                        context={},
                    ),
                )
        return resolved
