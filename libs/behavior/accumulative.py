"""Accumulative behavior bundle: generator, profiler, validator, and violator."""

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
    build_numeric_primitive_evidence,
    choose_behavior_family,
    score_behavior_families_from_primitives,
)
from libs.behavior.utils import clip01
from libs.behavior.validation import FamilyValidator


@dataclass(frozen=True)
class AccumulativeContract(BehaviorContract):
    behavior_family: str = "accumulative"
    defining_primitives: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS["accumulative"].defining_primitives
    expected_traits: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS["accumulative"].expected_traits
    supported_datatypes: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS["accumulative"].supported_datatypes
    allowed_fault_families: tuple[str, ...] = BEHAVIOR_FAMILY_DEFINITIONS["accumulative"].allowed_fault_families


class AccumulativeFeatureExtractor(BehaviorFeatureExtractor):
    def compute_features(
        self,
        *,
        parameter_name: str,
        telemetry_pdf: pd.DataFrame,
    ) -> dict[str, float | str | None]:
        return build_numeric_primitive_evidence(parameter_name=parameter_name, telemetry_pdf=telemetry_pdf)


class AccumulativeGenerator(BehaviorGenerator):
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
            latent_rate_name = context.get("latent_target_name")
            if latent_rate_name is not None and str(latent_rate_name) in step_input.latent_state:
                rate = float(step_input.latent_state[str(latent_rate_name)])
                context["target_source"] = "latent_state"
            else:
                rate = float(context.get("rate_value", context.get("target_value", 0.0)))
                context["target_source"] = "context"
            current = current + float(step_input.dt_seconds) * rate
            noise = float(context.get("noise_value", 0.0))
            yield BehaviorSample(
                parameter_name=parameter_name,
                parameter_value_clean=float(current),
                parameter_value=float(current + noise),
                state=None,
                metadata=dict(context),
            )


class AccumulativeProfiler(BehaviorProfiler):
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


class AccumulativeViolator(BehaviorViolator):
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
        violation_type = str(context.get("violation_type") or ("reset_drop" if "drop_value" in context else "bias"))
        drop_value = float(context.get("drop_value", 0.0))
        anomaly_rate = clip01(float(context.get("anomaly_rate", 0.0)))
        rng = np.random.default_rng(int(context.get("rng_seed", 0)))
        persistent_offset = 0.0
        for step_index, sample in enumerate(generated_stream):
            apply_drop = bool(rng.random() < anomaly_rate)
            observed = self._coerce_float(sample.parameter_value)
            perturbed: object | None = sample.parameter_value
            if apply_drop and observed is not None:
                if violation_type == "reset_drop":
                    if step_index == 0 or bool(context.get("persistent_drop", True)):
                        persistent_offset -= drop_value
                    perturbed = observed + persistent_offset
                elif violation_type == "leak_rate":
                    leak_rate = float(context.get("leak_rate", 0.05))
                    persistent_offset -= leak_rate
                    perturbed = observed + persistent_offset
                elif violation_type == "drift":
                    drift_rate = float(context.get("drift_rate", 0.05))
                    perturbed = observed + (drift_rate * step_index)
                elif violation_type == "bias":
                    perturbed = observed + float(context.get("bias", 0.0))
                else:
                    perturbed = observed - drop_value
            metadata = dict(sample.metadata)
            metadata["misbehavior_applied"] = apply_drop
            metadata["misbehavior_family_label"] = violation_type if apply_drop else None
            yield BehaviorSample(
                parameter_name=parameter_name,
                parameter_value_clean=sample.parameter_value_clean,
                parameter_value=perturbed,
                state=sample.state,
                metadata=metadata,
            )


class AccumulativeExpectation(BehaviorExpectation):
    def evaluate(
        self,
        *,
        generated_rows: pd.DataFrame,
        profile_result: BehaviorProfileResult,
    ) -> dict[str, float | bool | str]:
        return {
            "behavior_expected": "accumulative",
            "self_classified": profile_result.behavior_family_profiled == "accumulative",
            "confidence_at_least_half": float(profile_result.behavior_profile_confidence) >= 0.5,
        }


class AccumulativeBehavior(Behavior):
    def __init__(self) -> None:
        self.contract = AccumulativeContract()
        self.feature_extractor = AccumulativeFeatureExtractor()
        self.generator = AccumulativeGenerator()
        self.profiler = AccumulativeProfiler()
        self.validator = FamilyValidator(expected_family="accumulative")
        self.violator = AccumulativeViolator()
        self.expectation = AccumulativeExpectation()
