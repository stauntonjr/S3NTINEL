"""Regulated behavior bundle: generator, profiler, validator, and violator."""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Iterable, Iterator
from typing import Any, Mapping

import pandas as pd
import numpy as np

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
class RegulatedContract(BehaviorContract):
    behavior_family: str = "regulated"
    defining_primitives: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS["regulated"].defining_primitives
    expected_traits: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS["regulated"].expected_traits
    supported_datatypes: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS["regulated"].supported_datatypes
    allowed_fault_families: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS["regulated"].allowed_fault_families


class RegulatedFeatureExtractor(BehaviorFeatureExtractor):
    def compute_features(
        self,
        *,
        parameter_name: str,
        telemetry_pdf: pd.DataFrame,
    ) -> dict[str, float | str | None]:
        primitive = build_numeric_primitive_evidence(parameter_name=parameter_name, telemetry_pdf=telemetry_pdf)
        series = numeric_series(telemetry_pdf)
        diffs = series.diff().dropna()
        return {
            **primitive,
            "mean_reversion_score_profiled": clip01(float(primitive.get("reversal_rate_profiled") or 0.0) * 2.0),
            "boundedness_score_profiled": float(primitive.get("bound_occupancy_profiled") or 0.0),
        }


class RegulatedGenerator(BehaviorGenerator):
    def generate_stream(
        self,
        *,
        parameter_name: str,
        step_inputs: Iterable[BehaviorStepInput],
        initial_state: Any = None,
    ) -> Iterator[BehaviorSample]:
        current = float(initial_state if initial_state is not None else 0.0)
        trim_phase = 0.0
        settled_steps = 0
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
            reversion_rate = max(float(context.get("reversion_rate", 1.5)), 1e-6)
            trim_hz = max(float(context.get("trim_oscillation_hz", 0.18)), 0.0)
            trim_fraction = max(float(context.get("control_trim_fraction", 0.006)), 0.0)
            control_band_fraction = max(float(context.get("control_band_fraction", 0.025)), 0.0)

            error = target - current
            control_band = max(float(context.get("control_band_abs", 0.05)), max(abs(target), 1.0) * control_band_fraction)
            trim_amplitude = max(float(context.get("control_trim_abs", 0.02)), control_band * trim_fraction * 10.0)

            # Closed-loop regulated channels should converge quickly, then continue
            # making small bounded corrections around the setpoint instead of behaving
            # like a plain first-order inertial response.
            alpha = clip01(dt_seconds * reversion_rate)
            current = current + (alpha * error)

            if abs(target - current) <= control_band:
                settled_steps += 1
                if settled_steps >= 2:
                    trim_phase = trim_phase + (2.0 * np.pi * trim_hz * dt_seconds)
                    trim_value = trim_amplitude * float(np.sin(trim_phase))
                    current = target + ((current - target) * 0.20) + trim_value
                    context["control_mode"] = "trim"
                else:
                    current = target + ((current - target) * 0.35)
                    context["control_mode"] = "track"
            else:
                settled_steps = 0
                trim_phase = 0.0
                context["control_mode"] = "track"

            noise = float(context.get("noise_value", 0.0))
            yield BehaviorSample(
                parameter_name=parameter_name,
                parameter_value_clean=float(current),
                parameter_value=float(current + noise),
                state=None,
                metadata=dict(context),
            )


class RegulatedProfiler(BehaviorProfiler):
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


class RegulatedViolator(BehaviorViolator):
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
        violation_type = str(context.get("violation_type") or ("offset" if "bias" in context else "offset"))
        bias = float(context.get("bias", context.get("offset_value", 0.0)))
        anomaly_rate = clip01(float(context.get("anomaly_rate", 0.0)))
        rng = np.random.default_rng(int(context.get("rng_seed", 0)))
        for step_index, sample in enumerate(generated_stream):
            apply_bias = bool(rng.random() < anomaly_rate)
            observed = self._coerce_float(sample.parameter_value)
            clean_value = self._coerce_float(sample.parameter_value_clean)
            perturbed: object | None = sample.parameter_value
            if apply_bias and observed is not None:
                if violation_type == "offset":
                    perturbed = observed + bias
                elif violation_type == "saturation":
                    lower = context.get("saturation_min", context.get("clamp_min"))
                    upper = context.get("saturation_max", context.get("clamp_max"))
                    bounded = observed
                    if lower is not None:
                        bounded = max(bounded, float(lower))
                    if upper is not None:
                        bounded = min(bounded, float(upper))
                    perturbed = bounded
                elif violation_type == "tracking_degradation":
                    scale = float(context.get("tracking_scale", 0.6))
                    reference = clean_value if clean_value is not None else observed
                    perturbed = (reference * scale) + float(context.get("tracking_offset", 0.0))
                elif violation_type == "oscillation":
                    amplitude = float(context.get("oscillation_amplitude", 1.0))
                    period_steps = max(int(context.get("oscillation_period_steps", 4)), 1)
                    perturbed = observed + amplitude * float(np.sin((2.0 * np.pi * step_index) / period_steps))
                else:
                    perturbed = observed + bias
            metadata = dict(sample.metadata)
            metadata["misbehavior_applied"] = apply_bias
            metadata["misbehavior_family_label"] = violation_type if apply_bias else None
            yield BehaviorSample(
                parameter_name=parameter_name,
                parameter_value_clean=sample.parameter_value_clean,
                parameter_value=perturbed,
                state=sample.state,
                metadata=metadata,
            )


class RegulatedExpectation(BehaviorExpectation):
    def evaluate(
        self,
        *,
        generated_rows: pd.DataFrame,
        profile_result: BehaviorProfileResult,
    ) -> dict[str, float | bool | str]:
        return {
            "behavior_expected": "regulated",
            "self_classified": profile_result.behavior_family_profiled == "regulated",
            "confidence_at_least_half": float(profile_result.behavior_profile_confidence) >= 0.5,
        }


class RegulatedBehavior(Behavior):
    def __init__(self) -> None:
        self.contract = RegulatedContract()
        self.feature_extractor = RegulatedFeatureExtractor()
        self.generator = RegulatedGenerator()
        self.profiler = RegulatedProfiler()
        self.validator = FamilyValidator(expected_family="regulated")
        self.violator = RegulatedViolator()
        self.expectation = RegulatedExpectation()
