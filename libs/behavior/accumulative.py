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
from libs.behavior.utils import clip01, lag1_autocorrelation, numeric_series
from libs.behavior.validation import FamilyValidator


@dataclass(frozen=True)
class AccumulativeContract(BehaviorContract):
    behavior_family: str = "accumulative"
    expected_traits: tuple[str, ...] = ("persistent", "monotone", "integrative")
    supported_datatypes: tuple[str, ...] = ("numeric",)
    allowed_fault_families: tuple[str, ...] = ("reset_drop", "drift", "leakage", "noise_increase")


class AccumulativeFeatureExtractor(BehaviorFeatureExtractor):
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
                "lag1_autocorr_profiled": None,
                "monotonicity_score_profiled": None,
                "sign_flip_rate_profiled": None,
                "net_change_ratio_profiled": None,
            }
        diff = series.diff().dropna()
        if diff.empty:
            return {
                "sample_count_profiled": float(len(series)),
                "lag1_autocorr_profiled": 1.0,
                "monotonicity_score_profiled": 1.0,
                "sign_flip_rate_profiled": 0.0,
                "net_change_ratio_profiled": 1.0,
            }
        pos_ratio = float((diff > 0).mean())
        neg_ratio = float((diff < 0).mean())
        monotonicity = max(pos_ratio, neg_ratio)
        sign_flips = ((diff > 0) != (diff.shift(1) > 0)).dropna().mean() if len(diff) > 1 else 0.0
        sign_flip_rate = float(sign_flips) if sign_flips == sign_flips else 0.0
        lag1 = lag1_autocorrelation(series)
        lag1_autocorr = float(lag1) if lag1 is not None and lag1 == lag1 else 0.0
        gross_change = float(diff.abs().sum())
        net_change = float(abs(series.iloc[-1] - series.iloc[0]))
        net_change_ratio = net_change / max(gross_change, 1e-6)
        return {
            "sample_count_profiled": float(len(series)),
            "lag1_autocorr_profiled": lag1_autocorr,
            "monotonicity_score_profiled": monotonicity,
            "sign_flip_rate_profiled": sign_flip_rate,
            "net_change_ratio_profiled": net_change_ratio,
        }


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
        lag1 = clip01((float(features.get("lag1_autocorr_profiled") or 0.0) + 1.0) / 2.0)
        monotonicity = float(features.get("monotonicity_score_profiled") or 0.0)
        net_change_ratio = float(features.get("net_change_ratio_profiled") or 0.0)
        low_flip = clip01(1.0 - float(features.get("sign_flip_rate_profiled") or 0.0))
        accumulative_score = clip01((0.3 * lag1) + (0.35 * monotonicity) + (0.2 * net_change_ratio) + (0.15 * low_flip))
        inertial_score = clip01(0.4 * lag1 + 0.2 * low_flip)
        regulated_score = clip01(0.15 * low_flip)
        mixed_unknown = clip01(1.0 - max(accumulative_score, inertial_score, regulated_score))
        scores = {
            "regulated": regulated_score,
            "inertial": inertial_score,
            "accumulative": accumulative_score,
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


class AccumulativeViolator(BehaviorViolator):
    def violate_stream(
        self,
        *,
        parameter_name: str,
        generated_stream: Iterable[BehaviorSample],
        context: Mapping[str, Any],
    ) -> Iterator[BehaviorSample]:
        drop_value = float(context.get("drop_value", 0.0))
        anomaly_rate = clip01(float(context.get("anomaly_rate", 0.0)))
        rng = np.random.default_rng(int(context.get("rng_seed", 0)))
        for sample in generated_stream:
            apply_drop = bool(rng.random() < anomaly_rate)
            try:
                observed = float(sample.parameter_value) if sample.parameter_value is not None else None
            except Exception:
                observed = None
            perturbed = observed - drop_value if apply_drop and observed is not None else sample.parameter_value
            metadata = dict(sample.metadata)
            metadata["misbehavior_applied"] = apply_drop
            metadata["misbehavior_family_label"] = "reset_drop" if apply_drop else None
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
