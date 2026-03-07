# Behavior Family Skeletons

This note gives concrete skeletons for the first two behavior-family files:

- `regulated.py`
- `inertial.py`

These are design skeletons, not implemented code.

## 1. `libs/behavior/regulated.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import pandas as pd

from libs.behavior.base import (
    Behavior,
    BehaviorContract,
    BehaviorProfileResult,
    BehaviorExpectation,
    BehaviorFeatureExtractor,
    BehaviorProfiler,
    BehaviorGenerator,
    BehaviorValidator,
    BehaviorViolator,
)


@dataclass(frozen=True)
class RegulatedBehaviorContract(BehaviorContract):
    behavior_family: str = "regulated"
    expected_traits: tuple[str, ...] = (
        "bounded",
        "central_band_occupancy",
        "mean_reverting",
    )
    supported_datatypes: tuple[str, ...] = ("numeric",)
    allowed_fault_families: tuple[str, ...] = (
        "bias",
        "noise",
        "clipping",
        "dropout",
        "timing_fault",
    )


class RegulatedFeatureExtractor(BehaviorFeatureExtractor):
    def compute_features(
        self,
        *,
        parameter_name: str,
        telemetry_pdf: pd.DataFrame,
    ) -> dict[str, float | str | None]:
        return {
            "central_band_occupancy_profiled": None,
            "excursion_rate_profiled": None,
            "mean_reversion_score_profiled": None,
            "boundedness_score_profiled": None,
        }


class RegulatedGenerator(BehaviorGenerator):
    def step(
        self,
        *,
        dt_seconds: float,
        latent_state: Mapping[str, float],
        parameter_state: Any,
        context: Mapping[str, Any],
    ) -> Any:
        return parameter_state

    def observe(
        self,
        *,
        parameter_state: Any,
        context: Mapping[str, Any],
    ) -> object:
        return None


class RegulatedProfiler(BehaviorProfiler):
    def profile(
        self,
        *,
        parameter_name: str,
        features: Mapping[str, float | str | None],
    ) -> BehaviorProfileResult:
        return BehaviorProfileResult(
            behavior_family_profiled="regulated",
            behavior_profile_confidence=0.0,
            score_by_family={
                "regulated": 0.0,
                "inertial": 0.0,
                "accumulative": 0.0,
                "discrete_state": 0.0,
                "mixed_unknown": 1.0,
            },
            profiled_features=dict(features),
        )


class RegulatedValidator(BehaviorValidator):
    def validate(
        self,
        *,
        generated_rows: pd.DataFrame,
        profile_result: BehaviorProfileResult,
    ) -> dict[str, float | bool | str]:
        return {
            "behavior_family_expected": "regulated",
            "self_classified": profile_result.behavior_family_profiled == "regulated",
            "confidence": profile_result.behavior_profile_confidence,
        }


class RegulatedViolator(BehaviorViolator):
    def violate(
        self,
        *,
        parameter_name: str,
        generated_rows: pd.DataFrame,
        context: Mapping[str, Any],
    ) -> pd.DataFrame:
        return generated_rows


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
            "confidence": profile_result.behavior_profile_confidence,
        }


class RegulatedBehavior(Behavior):
    def __init__(self) -> None:
        self.contract = RegulatedBehaviorContract()
        self.feature_extractor = RegulatedFeatureExtractor()
        self.generator = RegulatedGenerator()
        self.profiler = RegulatedProfiler()
        self.validator = RegulatedValidator()
        self.violator = RegulatedViolator()
        self.expectation = RegulatedExpectation()
```

## 2. `libs/behavior/inertial.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import pandas as pd

from libs.behavior.base import (
    Behavior,
    BehaviorContract,
    BehaviorProfileResult,
    BehaviorExpectation,
    BehaviorFeatureExtractor,
    BehaviorProfiler,
    BehaviorGenerator,
    BehaviorValidator,
    BehaviorViolator,
)


@dataclass(frozen=True)
class InertialBehaviorContract(BehaviorContract):
    behavior_family: str = "inertial"
    expected_traits: tuple[str, ...] = (
        "persistent",
        "smooth",
        "lagged_response",
    )
    supported_datatypes: tuple[str, ...] = ("numeric",)
    allowed_fault_families: tuple[str, ...] = (
        "lag_increase",
        "bias",
        "drift",
        "noise",
        "timing_fault",
    )


class InertialFeatureExtractor(BehaviorFeatureExtractor):
    def compute_features(
        self,
        *,
        parameter_name: str,
        telemetry_pdf: pd.DataFrame,
    ) -> dict[str, float | str | None]:
        return {
            "lag1_autocorr_profiled": None,
            "diff_energy_profiled": None,
            "sign_flip_rate_profiled": None,
            "smoothness_score_profiled": None,
        }


class InertialGenerator(BehaviorGenerator):
    def step(
        self,
        *,
        dt_seconds: float,
        latent_state: Mapping[str, float],
        parameter_state: Any,
        context: Mapping[str, Any],
    ) -> Any:
        return parameter_state

    def observe(
        self,
        *,
        parameter_state: Any,
        context: Mapping[str, Any],
    ) -> object:
        return None


class InertialProfiler(BehaviorProfiler):
    def profile(
        self,
        *,
        parameter_name: str,
        features: Mapping[str, float | str | None],
    ) -> BehaviorProfileResult:
        return BehaviorProfileResult(
            behavior_family_profiled="inertial",
            behavior_profile_confidence=0.0,
            score_by_family={
                "regulated": 0.0,
                "inertial": 0.0,
                "accumulative": 0.0,
                "discrete_state": 0.0,
                "mixed_unknown": 1.0,
            },
            profiled_features=dict(features),
        )


class InertialValidator(BehaviorValidator):
    def validate(
        self,
        *,
        generated_rows: pd.DataFrame,
        profile_result: BehaviorProfileResult,
    ) -> dict[str, float | bool | str]:
        return {
            "behavior_family_expected": "inertial",
            "self_classified": profile_result.behavior_family_profiled == "inertial",
            "confidence": profile_result.behavior_profile_confidence,
        }


class InertialViolator(BehaviorViolator):
    def violate(
        self,
        *,
        parameter_name: str,
        generated_rows: pd.DataFrame,
        context: Mapping[str, Any],
    ) -> pd.DataFrame:
        return generated_rows


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
            "confidence": profile_result.behavior_profile_confidence,
        }


class InertialBehavior(Behavior):
    def __init__(self) -> None:
        self.contract = InertialBehaviorContract()
        self.feature_extractor = InertialFeatureExtractor()
        self.generator = InertialGenerator()
        self.profiler = InertialProfiler()
        self.validator = InertialValidator()
        self.violator = InertialViolator()
        self.expectation = InertialExpectation()
```

## 3. Registry example

```python
from libs.behavior.accumulative import AccumulativeBehavior
from libs.behavior.discrete_state import DiscreteStateBehavior
from libs.behavior.inertial import InertialBehavior
from libs.behavior.regulated import RegulatedBehavior
from libs.behavior.registry import BehaviorRegistry


behavior_registry = BehaviorRegistry(
    behaviors=[
        RegulatedBehavior(),
        InertialBehavior(),
        AccumulativeBehavior(),
        DiscreteStateBehavior(),
    ]
)
```

## 4. Implementation guidance

When these files are implemented for real:

- keep family objects stateless
- keep extracted features flat
- keep profile results flat
- do not let the profiler read privileged simulator internals
- use the same family registry from:
  - simulation
  - profiling
  - tests
