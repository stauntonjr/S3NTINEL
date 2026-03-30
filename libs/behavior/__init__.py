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
from libs.behavior.primitives import (
    BEHAVIOR_FAMILY_DEFINITIONS,
    DISCRETE_PRIMITIVE_FEATURE_COLUMNS,
    NUMERIC_PRIMITIVE_FEATURE_COLUMNS,
    PRIMITIVE_PROFILE_COLUMNS,
    PROFILED_BEHAVIOR_FAMILIES,
    SIMULATED_BEHAVIOR_FAMILIES,
    BehaviorFamilyDefinition,
    BehaviorPrimitiveSpec,
    build_discrete_primitive_evidence,
    build_numeric_primitive_evidence,
    choose_behavior_family,
    score_behavior_families_from_primitives,
)
from libs.behavior.registry import BehaviorRegistry, build_default_behavior_registry
from libs.behavior.regulated import RegulatedBehavior
from libs.behavior.tracking import TrackingBehavior
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
    "BEHAVIOR_FAMILY_DEFINITIONS",
    "DiscreteStateBehavior",
    "DISCRETE_PRIMITIVE_FEATURE_COLUMNS",
    "InertialBehavior",
    "NUMERIC_PRIMITIVE_FEATURE_COLUMNS",
    "PRIMITIVE_PROFILE_COLUMNS",
    "PROFILED_BEHAVIOR_FAMILIES",
    "FamilyValidator",
    "RegulatedBehavior",
    "SIMULATED_BEHAVIOR_FAMILIES",
    "TrackingBehavior",
    "BehaviorFamilyDefinition",
    "BehaviorPrimitiveSpec",
    "behavior_samples_to_frame",
    "build_discrete_primitive_evidence",
    "build_numeric_primitive_evidence",
    "choose_behavior_family",
    "clip01",
    "iter_tick_samples",
    "materialize_behavior_samples",
    "numeric_series",
    "score_behavior_families_from_primitives",
    "single_step_input_stream",
    "build_default_behavior_registry",
]
