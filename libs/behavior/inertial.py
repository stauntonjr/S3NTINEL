"""Inertial behavior bundle: generator, profiler, validator, and violator."""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Iterable, Iterator
from typing import Any, Mapping

import pandas as pd

from libs.behavior.base import (
    Behavior,
    BehaviorContract,
    BehaviorExpectation,
    BehaviorFeatureExtractor,
    BehaviorGenerator,
    BehaviorSample,
    BehaviorStepInput,
    BehaviorProfileResult,
    BehaviorProfiler,
    BehaviorViolator,
)
from libs.behavior.primitives import (
    BEHAVIOR_FAMILY_DEFINITIONS,
    build_numeric_primitive_evidence,
    choose_behavior_family,
    score_behavior_families_from_primitives,
)
from libs.behavior.utils import clip01, numeric_series
from libs.behavior.validation import FamilyValidator


@dataclass(frozen=True)
class InertialContract(BehaviorContract):
    behavior_family: str = "inertial"
    defining_primitives: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS["inertial"].defining_primitives
    expected_traits: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS["inertial"].expected_traits
    supported_datatypes: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS["inertial"].supported_datatypes
    allowed_fault_families: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS["inertial"].allowed_fault_families


class InertialFeatureExtractor(BehaviorFeatureExtractor):
    def compute_features(
        self,
        *,
        parameter_name: str,
        telemetry_pdf: pd.DataFrame,
    ) -> dict[str, float | str | None]:
        primitive = build_numeric_primitive_evidence(parameter_name=parameter_name, telemetry_pdf=telemetry_pdf)
        return primitive


class InertialGenerator(BehaviorGenerator):
    def generate_stream(
        self,
        *,
        parameter_name: str,
        step_inputs: Iterable[BehaviorStepInput],
        initial_state: Any = None,
    ) -> Iterator[BehaviorSample]:
        current = float(initial_state if initial_state is not None else 0.0)
        for step_input in step_inputs:
            context = dict(step_input.context)
            latent_target_name = context.get("latent_target_name")
            if latent_target_name is not None and str(latent_target_name) in step_input.latent_state:
                target = float(step_input.latent_state[str(latent_target_name)])
                context["target_source"] = "latent_state"
            else:
                target = float(context.get("target_value", current))
                context["target_source"] = "context"
            time_constant = max(float(context.get("time_constant_seconds", 2.0)), 1e-6)
            alpha = clip01(float(step_input.dt_seconds) / time_constant)
            current = current + alpha * (target - current)
            noise = float(context.get("noise_value", 0.0))
            yield BehaviorSample(
                parameter_name=parameter_name,
                parameter_value_clean=float(current),
                parameter_value=float(current + noise),
                state=None,
                metadata=dict(context),
            )


class InertialProfiler(BehaviorProfiler):
    def profile(
        self,
        *,
        parameter_name: str,
        features: Mapping[str, float | str | None],
    ) -> BehaviorProfileResult:
        scores = score_behavior_families_from_primitives(
            primitive_evidence=features,
            parameter_datatype_profiled="numeric",
        )
        best_family, confidence = choose_behavior_family(scores)
        return BehaviorProfileResult(
            behavior_family_profiled=best_family,
            behavior_profile_confidence=confidence,
            score_by_family=scores,
            profiled_features=dict(features),
        )


class InertialViolator(BehaviorViolator):
    @staticmethod
    def _coerce_float(value: object | None) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except Exception:
            return None

    def violate_stream(
        self,
        *,
        parameter_name: str,
        generated_stream: Iterable[BehaviorSample],
        context: Mapping[str, Any],
    ) -> Iterator[BehaviorSample]:
        violation_type = str(context.get("violation_type") or "timing_lag")
        lag_steps = max(int(context.get("lag_steps", 1)), 1)
        anomaly_rate = clip01(float(context.get("anomaly_rate", 0.0)))
        observed_buffer: list[float | str | None] = []
        slowed_value: float | None = None
        stuck_value = self._coerce_float(context.get("stuck_value"))
        previous_observed: float | None = None
        distorted_value: float | None = None
        for sample in generated_stream:
            observed_buffer.append(sample.parameter_value)
            apply_violation = anomaly_rate >= 1.0 or (anomaly_rate > 0.0 and len(observed_buffer) % max(int(round(1.0 / anomaly_rate)), 1) == 0)
            observed = self._coerce_float(sample.parameter_value)
            if apply_violation and violation_type == "timing_lag" and len(observed_buffer) > lag_steps:
                perturbed = observed_buffer[-(lag_steps + 1)]
            elif apply_violation and violation_type == "increased_time_constant" and observed is not None:
                slowdown = max(float(context.get("slowdown_factor", 3.0)), 1.0)
                alpha = 1.0 / slowdown
                slowed_value = observed if slowed_value is None else (slowed_value + alpha * (observed - slowed_value))
                perturbed = slowed_value
            elif apply_violation and violation_type == "stuck_value":
                if stuck_value is None:
                    stuck_value = observed
                perturbed = stuck_value if stuck_value is not None else sample.parameter_value
            elif apply_violation and violation_type == "ramp_distortion" and observed is not None:
                slope_scale = float(context.get("slope_scale", 0.5))
                if previous_observed is None or distorted_value is None:
                    distorted_value = observed
                else:
                    distorted_value = distorted_value + ((observed - previous_observed) * slope_scale)
                perturbed = distorted_value
            else:
                perturbed = sample.parameter_value
            previous_observed = observed
            metadata = dict(sample.metadata)
            metadata["misbehavior_applied"] = apply_violation
            metadata["misbehavior_family_label"] = violation_type if apply_violation else None
            yield BehaviorSample(
                parameter_name=parameter_name,
                parameter_value_clean=sample.parameter_value_clean,
                parameter_value=perturbed,
                state=sample.state,
                metadata=metadata,
            )


class InertialExpectation(BehaviorExpectation):
    def evaluate(
        self,
        *,
        generated_rows: pd.DataFrame,
        profile_result: BehaviorProfileResult,
    ) -> dict[str, float | bool | str]:
        return {
            "behavior_expected": "inertial",
            "self_classified": profile_result.behavior_family_profiled == "inertial",
            "confidence_at_least_half": float(profile_result.behavior_profile_confidence) >= 0.5,
        }


class InertialBehavior(Behavior):
    def __init__(self) -> None:
        self.contract = InertialContract()
        self.feature_extractor = InertialFeatureExtractor()
        self.generator = InertialGenerator()
        self.profiler = InertialProfiler()
        self.validator = FamilyValidator(expected_family="inertial")
        self.violator = InertialViolator()
        self.expectation = InertialExpectation()
