"""Shared validator helpers for behavior-family contracts."""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Iterable

from libs.behavior.base import BehaviorProfileResult, BehaviorSample, BehaviorValidator
from libs.behavior.tick import materialize_behavior_samples


@dataclass(frozen=True)
class FamilyValidator(BehaviorValidator):
    expected_family: str

    def validate_stream(
        self,
        *,
        parameter_name: str,
        generated_stream: Iterable[BehaviorSample],
        profile_result: BehaviorProfileResult,
    ) -> dict[str, float | bool | str]:
        samples = materialize_behavior_samples(generated_stream)
        return {
            "behavior_family_expected": self.expected_family,
            "sample_count": float(len(samples)),
            "self_classified": profile_result.behavior_family_profiled == self.expected_family,
            "confidence": float(profile_result.behavior_profile_confidence),
        }
