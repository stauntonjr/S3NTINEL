"""Tracking behavior bundle: generator, profiler, validator, and violator."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any, Mapping

import pandas as pd

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
    build_numeric_primitive_evidence,
    choose_behavior_family,
    score_behavior_families_from_primitives,
)
from libs.behavior.utils import clip01
from libs.behavior.validation import FamilyValidator


@dataclass(frozen=True)
class TrackingContract(BehaviorContract):
    behavior_family: str = "tracking"
    defining_primitives: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS["tracking"].defining_primitives
    expected_traits: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS["tracking"].expected_traits
    supported_datatypes: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS["tracking"].supported_datatypes
    allowed_fault_families: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS["tracking"].allowed_fault_families


class TrackingFeatureExtractor(BehaviorFeatureExtractor):
    def compute_features(
        self,
        *,
        parameter_name: str,
        telemetry_pdf: pd.DataFrame,
    ) -> dict[str, float | str | None]:
        return build_numeric_primitive_evidence(parameter_name=parameter_name, telemetry_pdf=telemetry_pdf)


class TrackingGenerator(BehaviorGenerator):
    def generate_stream(
        self,
        *,
        parameter_name: str,
        step_inputs: Iterable[BehaviorStepInput],
        initial_state: Any = None,
    ) -> Iterator[BehaviorSample]:
        current = float(initial_state if initial_state is not None else 0.0)
        previous_target = current
        for step_input in step_inputs:
            context = dict(step_input.context)
            latent_target_name = context.get("latent_target_name")
            if latent_target_name is not None and str(latent_target_name) in step_input.latent_state:
                target = float(step_input.latent_state[str(latent_target_name)])
                context["target_source"] = "latent_state"
            else:
                target = float(context.get("target_value", current))
                context["target_source"] = "context"
            dt_seconds = max(float(step_input.dt_seconds), 1e-6)
            response_rate = max(float(context.get("response_rate", 2.1)), 1e-6)
            damping = clip01(float(context.get("tracking_damping", 0.35)))
            error_band_fraction = max(float(context.get("tracking_error_band_fraction", 0.04)), 0.0)
            error_band = max(float(context.get("tracking_error_band_abs", 0.1)), max(abs(target), 1.0) * error_band_fraction)
            alpha = clip01(response_rate * dt_seconds)
            commanded_step = alpha * (target - current)
            current = current + commanded_step
            if abs(target - current) <= error_band:
                current = target + ((current - target) * damping)
            bound_min = context.get("bound_min")
            bound_max = context.get("bound_max")
            if bound_min is not None:
                current = max(current, float(bound_min))
            if bound_max is not None:
                current = min(current, float(bound_max))
            noise = float(context.get("noise_value", 0.0))
            context["tracking_target_delta"] = target - previous_target
            previous_target = target
            yield BehaviorSample(
                parameter_name=parameter_name,
                parameter_value_clean=float(current),
                parameter_value=float(current + noise),
                state=None,
                metadata=dict(context),
            )


class TrackingProfiler(BehaviorProfiler):
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


class TrackingViolator(BehaviorViolator):
    def violate_stream(
        self,
        *,
        parameter_name: str,
        generated_stream: Iterable[BehaviorSample],
        context: Mapping[str, Any],
    ) -> Iterator[BehaviorSample]:
        violation_type = str(context.get("violation_type") or "tracking_degradation")
        anomaly_rate = clip01(float(context.get("anomaly_rate", 0.0)))
        degradation_scale = float(context.get("tracking_scale", 0.65))
        offset = float(context.get("tracking_offset", context.get("bias", 0.0)))
        for step_index, sample in enumerate(generated_stream):
            apply_violation = anomaly_rate >= 1.0 or (anomaly_rate > 0.0 and step_index % max(int(round(1.0 / anomaly_rate)), 1) == 0)
            value = sample.parameter_value
            try:
                observed = float(value) if value is not None else None
            except Exception:
                observed = None
            perturbed: object | None = value
            if apply_violation and observed is not None:
                if violation_type == "tracking_degradation":
                    reference = float(sample.parameter_value_clean) if sample.parameter_value_clean is not None else observed
                    perturbed = (reference * degradation_scale) + offset
                elif violation_type == "saturation":
                    lower = context.get("saturation_min", context.get("bound_min"))
                    upper = context.get("saturation_max", context.get("bound_max"))
                    bounded = observed
                    if lower is not None:
                        bounded = max(bounded, float(lower))
                    if upper is not None:
                        bounded = min(bounded, float(upper))
                    perturbed = bounded
                elif violation_type == "offset":
                    perturbed = observed + offset
                elif violation_type == "oscillation":
                    period_steps = max(int(context.get("oscillation_period_steps", 4)), 1)
                    amplitude = float(context.get("oscillation_amplitude", 1.0))
                    from math import sin, pi

                    perturbed = observed + amplitude * sin((2.0 * pi * step_index) / period_steps)
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


class TrackingExpectation(BehaviorExpectation):
    def evaluate(
        self,
        *,
        generated_rows: pd.DataFrame,
        profile_result: BehaviorProfileResult,
    ) -> dict[str, float | bool | str]:
        return {
            "behavior_expected": "tracking",
            "self_classified": profile_result.behavior_family_profiled == "tracking",
            "confidence_at_least_half": float(profile_result.behavior_profile_confidence) >= 0.5,
        }


class TrackingBehavior(Behavior):
    def __init__(self) -> None:
        self.contract = TrackingContract()
        self.feature_extractor = TrackingFeatureExtractor()
        self.generator = TrackingGenerator()
        self.profiler = TrackingProfiler()
        self.validator = FamilyValidator(expected_family="tracking")
        self.violator = TrackingViolator()
        self.expectation = TrackingExpectation()
