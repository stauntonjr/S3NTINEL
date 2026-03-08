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
from libs.behavior.utils import clip01, numeric_series
from libs.behavior.validation import FamilyValidator


@dataclass(frozen=True)
class RegulatedContract(BehaviorContract):
    behavior_family: str = "regulated"
    expected_traits: tuple[str, ...] = ("bounded", "central_band_occupancy", "mean_reverting")
    supported_datatypes: tuple[str, ...] = ("numeric",)
    allowed_fault_families: tuple[str, ...] = ("offset", "noise_increase", "stuck", "dropout")


class RegulatedFeatureExtractor(BehaviorFeatureExtractor):
    def compute_features(
        self,
        *,
        parameter_name: str,
        telemetry_pdf: pd.DataFrame,
    ) -> dict[str, float | str | None]:
        series = numeric_series(telemetry_pdf)
        if len(series) < 3:
            return {
                "sample_count_profiled": float(len(series)),
                "central_band_occupancy_profiled": None,
                "excursion_rate_profiled": None,
                "mean_reversion_score_profiled": None,
                "boundedness_score_profiled": None,
            }
        median = float(series.median())
        iqr = float(series.quantile(0.75) - series.quantile(0.25))
        band_radius = max(1e-6, 1.5 * iqr)
        central_band_occupancy = float(((series - median).abs() <= band_radius).mean())
        diffs = series.diff().dropna()
        sign_flips = float((diffs.mul(diffs.shift(1)).lt(0)).mean()) if len(diffs) > 1 else 0.0
        excursion_rate = 1.0 - central_band_occupancy
        total_range = float(series.max() - series.min())
        boundedness = 1.0 / (1.0 + max(total_range, 0.0))
        mean_reversion = clip01(sign_flips * 2.0)
        return {
            "sample_count_profiled": float(len(series)),
            "central_band_occupancy_profiled": central_band_occupancy,
            "excursion_rate_profiled": excursion_rate,
            "mean_reversion_score_profiled": mean_reversion,
            "boundedness_score_profiled": boundedness,
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
        for step_input in step_inputs:
            context = dict(step_input.context)
            latent_target_name = context.get("latent_target_name")
            if latent_target_name is not None and str(latent_target_name) in step_input.latent_state:
                target = float(step_input.latent_state[str(latent_target_name)])
                context["target_source"] = "latent_state"
            else:
                target = float(context.get("target_value", current))
                context["target_source"] = "context"
            reversion_rate = float(context.get("reversion_rate", 1.5))
            current = current + (target - current) * min(max(float(step_input.dt_seconds) * reversion_rate, 0.0), 1.0)
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
        central_band = float(features.get("central_band_occupancy_profiled") or 0.0)
        mean_reversion = float(features.get("mean_reversion_score_profiled") or 0.0)
        boundedness = float(features.get("boundedness_score_profiled") or 0.0)
        regulated_score = clip01((central_band + mean_reversion + boundedness) / 3.0)
        inertial_score = clip01((1.0 - float(features.get("excursion_rate_profiled") or 0.0)) * 0.35)
        mixed_unknown = clip01(1.0 - max(regulated_score, inertial_score))
        scores = {
            "regulated": regulated_score,
            "inertial": inertial_score,
            "accumulative": 0.0,
            "discrete_state": 0.0,
            "mixed_unknown": mixed_unknown,
        }
        best_family = max(scores, key=scores.get)
        return BehaviorProfileResult(
            behavior_family_profiled=best_family,
            behavior_profile_confidence=float(scores[best_family]),
            score_by_family=scores,
            profiled_features=dict(features),
        )


class RegulatedViolator(BehaviorViolator):
    def violate_stream(
        self,
        *,
        parameter_name: str,
        generated_stream: Iterable[BehaviorSample],
        context: Mapping[str, Any],
    ) -> Iterator[BehaviorSample]:
        bias = float(context.get("bias", 0.0))
        anomaly_rate = clip01(float(context.get("anomaly_rate", 0.0)))
        rng = np.random.default_rng(int(context.get("rng_seed", 0)))
        for sample in generated_stream:
            apply_bias = bool(rng.random() < anomaly_rate)
            try:
                observed = float(sample.parameter_value) if sample.parameter_value is not None else None
            except Exception:
                observed = None
            perturbed = observed + bias if apply_bias and observed is not None else sample.parameter_value
            metadata = dict(sample.metadata)
            metadata["misbehavior_applied"] = apply_bias
            metadata["misbehavior_family_label"] = "offset" if apply_bias else None
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
