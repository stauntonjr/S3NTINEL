"""Single-tick adapters over stream-capable behavior components."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping

from libs.behavior.base import BehaviorGenerator, BehaviorSample, BehaviorStepInput, BehaviorViolator


def single_step_input_stream(step_input: BehaviorStepInput) -> Iterator[BehaviorStepInput]:
    yield step_input


def iter_tick_samples(
    *,
    parameter_name: str,
    generator: BehaviorGenerator,
    step_input: BehaviorStepInput,
    initial_state: Any = None,
    violator: BehaviorViolator | None = None,
    violation_context: Mapping[str, Any] | None = None,
) -> Iterator[BehaviorSample]:
    generated_stream = generator.generate_stream(
        parameter_name=parameter_name,
        step_inputs=single_step_input_stream(step_input),
        initial_state=initial_state,
    )
    if violator is None or violation_context is None:
        yield from generated_stream
        return
    yield from violator.violate_stream(
        parameter_name=parameter_name,
        generated_stream=generated_stream,
        context=violation_context,
    )


def materialize_behavior_samples(samples: Iterable[BehaviorSample]) -> tuple[BehaviorSample, ...]:
    return tuple(samples)
