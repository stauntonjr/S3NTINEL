"""Registry for behavior-local generator/profiler/validator/violator bundles."""

from __future__ import annotations

from dataclasses import dataclass, field

from libs.behavior.base import Behavior


@dataclass
class BehaviorRegistry:
    _behaviors: dict[str, Behavior] = field(default_factory=dict)

    def register(self, behavior: Behavior) -> None:
        self._behaviors[str(behavior.contract.behavior_family)] = behavior

    def get(self, behavior_family: str) -> Behavior:
        return self._behaviors[str(behavior_family)]

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._behaviors))

    def items(self) -> tuple[tuple[str, Behavior], ...]:
        return tuple((name, self._behaviors[name]) for name in self.names())


def build_default_behavior_registry() -> BehaviorRegistry:
    from libs.behavior.accumulative import AccumulativeBehavior
    from libs.behavior.discrete_state import DiscreteStateBehavior
    from libs.behavior.inertial import InertialBehavior
    from libs.behavior.regulated import RegulatedBehavior
    from libs.behavior.tracking import TrackingBehavior

    registry = BehaviorRegistry()
    registry.register(AccumulativeBehavior())
    registry.register(DiscreteStateBehavior())
    registry.register(InertialBehavior())
    registry.register(RegulatedBehavior())
    registry.register(TrackingBehavior())
    return registry
