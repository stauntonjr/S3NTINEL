"""Example system specs."""

from __future__ import annotations

from collections.abc import Iterable

from libs.simulation.subsystem.spec import SubsystemSpec
from libs.simulation.system.spec import SystemSpec


def build_system_spec(
    *,
    system_id: str,
    subsystems: Iterable[SubsystemSpec],
) -> SystemSpec:
    return SystemSpec(
        system_id=system_id,
        subsystems=tuple(subsystems),
    )
