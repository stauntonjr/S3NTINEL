"""Shared protocols and value objects for behavior-local simulation/profiling."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol

import pandas as pd


@dataclass(frozen=True)
class BehaviorContract:
    behavior_family: str
    defining_primitives: tuple[str, ...]
    expected_traits: tuple[str, ...]
    supported_datatypes: tuple[str, ...]
    allowed_fault_families: tuple[str, ...]


@dataclass(frozen=True)
class BehaviorProfileResult:
    behavior_family_profiled: str
    behavior_profile_confidence: float
    score_by_family: Mapping[str, float]
    profiled_features: Mapping[str, float | str | None]


@dataclass(frozen=True)
class BehaviorStepInput:
    dt_seconds: float
    latent_state: Mapping[str, float]
    context: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BehaviorSample:
    parameter_name: str
    parameter_value_clean: object | None
    parameter_value: object | None
    state: Any = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


def behavior_samples_to_frame(samples: Iterable[BehaviorSample]) -> pd.DataFrame:
    rows = [
        {
            "parameter_name": sample.parameter_name,
            "parameter_value_clean": sample.parameter_value_clean,
            "parameter_value": sample.parameter_value,
            "state": sample.state,
            **{str(key): value for key, value in dict(sample.metadata).items()},
        }
        for sample in samples
    ]
    return pd.DataFrame(rows)


class BehaviorFeatureExtractor(Protocol):
    def compute_features(
        self,
        *,
        parameter_name: str,
        telemetry_pdf: pd.DataFrame,
    ) -> dict[str, float | str | None]:
        ...


class BehaviorGenerator(Protocol):
    def generate_stream(
        self,
        *,
        parameter_name: str,
        step_inputs: Iterable[BehaviorStepInput],
        initial_state: Any = None,
    ) -> Iterator[BehaviorSample]:
        ...


class BehaviorProfiler(Protocol):
    def profile(
        self,
        *,
        parameter_name: str,
        features: Mapping[str, float | str | None],
    ) -> BehaviorProfileResult:
        ...


class BehaviorValidator(Protocol):
    def validate_stream(
        self,
        *,
        parameter_name: str,
        generated_stream: Iterable[BehaviorSample],
        profile_result: BehaviorProfileResult,
    ) -> dict[str, float | bool | str]:
        ...


class BehaviorViolator(Protocol):
    def violate_stream(
        self,
        *,
        parameter_name: str,
        generated_stream: Iterable[BehaviorSample],
        context: Mapping[str, Any],
    ) -> Iterator[BehaviorSample]:
        ...


class BehaviorExpectation(Protocol):
    def evaluate(
        self,
        *,
        generated_rows: pd.DataFrame,
        profile_result: BehaviorProfileResult,
    ) -> dict[str, float | bool | str]:
        ...


class Behavior(Protocol):
    contract: BehaviorContract
    feature_extractor: BehaviorFeatureExtractor
    generator: BehaviorGenerator
    profiler: BehaviorProfiler
    validator: BehaviorValidator
    violator: BehaviorViolator
    expectation: BehaviorExpectation
