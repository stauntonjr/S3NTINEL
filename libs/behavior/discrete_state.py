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
from libs.behavior.primitives import (
    BEHAVIOR_FAMILY_DEFINITIONS,
    build_discrete_primitive_evidence,
    choose_behavior_family,
    score_behavior_families_from_primitives,
)
from libs.behavior.utils import clip01
from libs.behavior.validation import FamilyValidator


@dataclass(frozen=True)
class DiscreteStateContract(BehaviorContract):
    behavior_family: str = "discrete_state"
    defining_primitives: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS["discrete_state"].defining_primitives
    expected_traits: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS["discrete_state"].expected_traits
    supported_datatypes: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS["discrete_state"].supported_datatypes
    allowed_fault_families: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS["discrete_state"].allowed_fault_families


class DiscreteStateFeatureExtractor(BehaviorFeatureExtractor):
    def compute_features(
        self,
        *,
        parameter_name: str,
        telemetry_pdf: pd.DataFrame,
    ) -> dict[str, float | str | None]:
        return build_discrete_primitive_evidence(parameter_name=parameter_name, telemetry_pdf=telemetry_pdf)


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
        scores = score_behavior_families_from_primitives(
            primitive_evidence=features,
            parameter_datatype_profiled="categorical",
        )
        best_family, confidence = choose_behavior_family(scores)
        return BehaviorProfileResult(
            behavior_family_profiled=best_family,
            behavior_profile_confidence=confidence,
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
        violation_type = str(context.get("violation_type") or "illegal_transition")
        anomaly_rate = clip01(float(context.get("anomaly_rate", 0.0)))
        violating_state = str(context.get("violating_state", "__ILLEGAL__"))
        rng = np.random.default_rng(int(context.get("rng_seed", 0)))
        stuck_state = str(context.get("stuck_state", "")) or None
        chatter_states = tuple(str(item) for item in context.get("chatter_states", ()) if str(item))
        chatter_cycle_steps = max(int(context.get("chatter_cycle_steps", 1) or 1), 1)
        extra_dwell_steps = max(int(context.get("extra_dwell_steps", 1)), 1)
        step_index = int(context.get("step_index", 0) or 0)
        held_state = None
        held_remaining = 0
        for sample in generated_stream:
            apply_violation = bool(rng.random() < anomaly_rate)
            base_state = sample.parameter_value
            if apply_violation and violation_type == "illegal_transition":
                perturbed = violating_state
            elif apply_violation and violation_type == "dwell_violation":
                if held_state is None:
                    held_state = base_state
                    held_remaining = extra_dwell_steps
                elif base_state != held_state and held_remaining > 0:
                    held_remaining -= 1
                else:
                    held_state = base_state
                    held_remaining = extra_dwell_steps
                perturbed = held_state
            elif apply_violation and violation_type == "state_chatter":
                if chatter_states:
                    perturbed = chatter_states[(step_index // chatter_cycle_steps) % len(chatter_states)]
                else:
                    perturbed = base_state if (step_index // chatter_cycle_steps) % 2 else violating_state
            elif apply_violation and violation_type == "stuck_state":
                if stuck_state is None:
                    stuck_state = str(base_state)
                perturbed = stuck_state
            else:
                perturbed = base_state
            metadata = dict(sample.metadata)
            metadata["misbehavior_applied"] = apply_violation
            metadata["misbehavior_family_label"] = violation_type if apply_violation else None
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
