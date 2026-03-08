"""Behavior-local generator/profiler/validator/violator bundles."""

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
    BehaviorValidator,
    BehaviorViolator,
    behavior_samples_to_frame,
)
from libs.behavior.accumulative import AccumulativeBehavior
from libs.behavior.discrete_state import DiscreteStateBehavior
from libs.behavior.inertial import InertialBehavior
from libs.behavior.registry import BehaviorRegistry, build_default_behavior_registry
from libs.behavior.regulated import RegulatedBehavior
from libs.behavior.tick import iter_tick_samples, materialize_behavior_samples, single_step_input_stream
from libs.behavior.utils import clip01, numeric_series
from libs.behavior.validation import FamilyValidator

__all__ = [
    "Behavior",
    "BehaviorContract",
    "BehaviorExpectation",
    "BehaviorFeatureExtractor",
    "BehaviorGenerator",
    "BehaviorSample",
    "BehaviorStepInput",
    "BehaviorProfileResult",
    "BehaviorProfiler",
    "BehaviorRegistry",
    "BehaviorValidator",
    "BehaviorViolator",
    "AccumulativeBehavior",
    "DiscreteStateBehavior",
    "InertialBehavior",
    "FamilyValidator",
    "RegulatedBehavior",
    "behavior_samples_to_frame",
    "clip01",
    "iter_tick_samples",
    "materialize_behavior_samples",
    "numeric_series",
    "single_step_input_stream",
    "build_default_behavior_registry",
]
