"""Discrete-state behavior bundle: generator, profiler, validator, and violator."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any, Mapping

import pandas as pd
import numpy as np

from libs.behavior.base import (
    Behavior,
    BehaviorContract,
    BehaviorExpectation,
    BehaviorFeatureExtractor,
    BehaviorGenerator,
    BehaviorProfileResult,
    BehaviorProfiler,
    BehaviorSample,
    BehaviorStepInput,
    BehaviorViolator,
)
from libs.behavior.utils import clip01
from libs.behavior.validation import FamilyValidator


@dataclass(frozen=True)
class DiscreteStateContract(BehaviorContract):
    behavior_family: str = "discrete_state"
    expected_traits: tuple[str, ...] = ("finite_alphabet", "state_dwell", "abrupt_transitions")
    supported_datatypes: tuple[str, ...] = ("binary", "categorical", "high_cardinality")
    allowed_fault_families: tuple[str, ...] = ("illegal_transition", "dwell_violation", "state_chatter", "stuck_state")


class DiscreteStateFeatureExtractor(BehaviorFeatureExtractor):
    def compute_features(
        self,
        *,
        parameter_name: str,
        telemetry_pdf: pd.DataFrame,
    ) -> dict[str, float | str | None]:
        values = telemetry_pdf.get("parameter_value", pd.Series(dtype="object")).fillna("").astype(str)
        if len(values) < 2:
            return {
                "sample_count_profiled": float(len(values)),
                "distinct_state_count_profiled": float(values[values != ""].nunique()),
                "transition_rate_profiled": None,
                "mean_dwell_profiled": None,
                "dominant_state_ratio_profiled": None,
            }
        distinct_state_count = float(values[values != ""].nunique())
        transition_count = max(int((values != values.shift(1)).sum()) - 1, 0)
        transition_rate = transition_count / float(max(len(values) - 1, 1))
        run_lengths: list[int] = []
        current_run = 0
        previous = None
        for value in values.tolist():
            if value == previous:
                current_run += 1
            else:
                if current_run:
                    run_lengths.append(current_run)
                current_run = 1
                previous = value
        if current_run:
            run_lengths.append(current_run)
        mean_dwell = float(sum(run_lengths) / max(len(run_lengths), 1))
        dominant_state_ratio = float(values.value_counts(normalize=True, dropna=False).iloc[0]) if len(values) else 0.0
        return {
            "sample_count_profiled": float(len(values)),
            "distinct_state_count_profiled": distinct_state_count,
            "transition_rate_profiled": transition_rate,
            "mean_dwell_profiled": mean_dwell,
            "dominant_state_ratio_profiled": dominant_state_ratio,
        }


class DiscreteStateGenerator(BehaviorGenerator):
    def generate_stream(
        self,
        *,
        parameter_name: str,
        step_inputs: Iterable[BehaviorStepInput],
        initial_state: Any = None,
    ) -> Iterator[BehaviorSample]:
        current = initial_state if initial_state is not None else ""
        for step_input in step_inputs:
            context = dict(step_input.context)
            latent_target_name = context.get("latent_target_name")
            if latent_target_name is not None and str(latent_target_name) in step_input.latent_state:
                current = str(step_input.latent_state[str(latent_target_name)])
                context["target_source"] = "latent_state"
            elif "state_value" in context:
                current = str(context["state_value"])
                context["target_source"] = "context"
            elif "target_state" in context:
                current = str(context["target_state"])
                context["target_source"] = "context"
            yield BehaviorSample(
                parameter_name=parameter_name,
                parameter_value_clean=None if current == "" else current,
                parameter_value=None if current == "" else current,
                state=current,
                metadata=dict(context),
            )


class DiscreteStateProfiler(BehaviorProfiler):
    def profile(
        self,
        *,
        parameter_name: str,
        features: Mapping[str, float | str | None],
    ) -> BehaviorProfileResult:
        distinct_count = float(features.get("distinct_state_count_profiled") or 0.0)
        transition_rate = float(features.get("transition_rate_profiled") or 0.0)
        mean_dwell = float(features.get("mean_dwell_profiled") or 0.0)
        dominant_ratio = float(features.get("dominant_state_ratio_profiled") or 0.0)
        low_cardinality = clip01(1.0 - min(max(distinct_count - 1.0, 0.0), 9.0) / 9.0)
        low_transition = clip01(1.0 - transition_rate)
        dwell_score = clip01(min(mean_dwell, 10.0) / 10.0)
        concentration_score = clip01(dominant_ratio)
        discrete_state_score = clip01((0.3 * low_cardinality) + (0.25 * low_transition) + (0.25 * dwell_score) + (0.2 * concentration_score))
        mixed_unknown = clip01(1.0 - discrete_state_score)
        scores = {
            "regulated": 0.0,
            "inertial": 0.0,
            "accumulative": 0.0,
            "discrete_state": discrete_state_score,
            "mixed_unknown": mixed_unknown,
        }
        best_family = max(scores, key=scores.get)
        return BehaviorProfileResult(
            behavior_family_profiled=best_family,
            behavior_profile_confidence=float(scores[best_family]),
            score_by_family=scores,
            profiled_features=dict(features),
        )


class DiscreteStateViolator(BehaviorViolator):
    def violate_stream(
        self,
        *,
        parameter_name: str,
        generated_stream: Iterable[BehaviorSample],
        context: Mapping[str, Any],
    ) -> Iterator[BehaviorSample]:
        anomaly_rate = clip01(float(context.get("anomaly_rate", 0.0)))
        violating_state = str(context.get("violating_state", "__ILLEGAL__"))
        rng = np.random.default_rng(int(context.get("rng_seed", 0)))
        for sample in generated_stream:
            apply_violation = bool(rng.random() < anomaly_rate)
            perturbed = violating_state if apply_violation else sample.parameter_value
            metadata = dict(sample.metadata)
            metadata["misbehavior_applied"] = apply_violation
            metadata["misbehavior_family_label"] = "illegal_transition" if apply_violation else None
            yield BehaviorSample(
                parameter_name=parameter_name,
                parameter_value_clean=sample.parameter_value_clean,
                parameter_value=perturbed,
                state=perturbed,
                metadata=metadata,
            )


class DiscreteStateExpectation(BehaviorExpectation):
    def evaluate(
        self,
        *,
        generated_rows: pd.DataFrame,
        profile_result: BehaviorProfileResult,
    ) -> dict[str, float | bool | str]:
        return {
            "behavior_expected": "discrete_state",
            "self_classified": profile_result.behavior_family_profiled == "discrete_state",
            "confidence_at_least_half": float(profile_result.behavior_profile_confidence) >= 0.5,
        }


class DiscreteStateBehavior(Behavior):
    def __init__(self) -> None:
        self.contract = DiscreteStateContract()
        self.feature_extractor = DiscreteStateFeatureExtractor()
        self.generator = DiscreteStateGenerator()
        self.profiler = DiscreteStateProfiler()
        self.validator = FamilyValidator(expected_family="discrete_state")
        self.violator = DiscreteStateViolator()
        self.expectation = DiscreteStateExpectation()
