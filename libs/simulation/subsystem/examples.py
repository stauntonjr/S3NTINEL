"""Example subsystem specs."""

from __future__ import annotations

from collections.abc import Iterable

from libs.simulation.module.spec import ModuleSpec
from libs.simulation.subsystem.spec import SubsystemSpec


def build_subsystem_spec(
    *,
    subsystem_id: str,
    system_id: str,
    modules: Iterable[ModuleSpec],
) -> SubsystemSpec:
    return SubsystemSpec(
        subsystem_id=subsystem_id,
        system_id=system_id,
        modules=tuple(modules),
    )
