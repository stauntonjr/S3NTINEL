"""Scenario-layer builders for realistic simulation presets."""

from __future__ import annotations

from typing import Any


_POWER_PRESSURIZATION_EXPORTS = {
    "MissionProfileSpec",
    "PowerPressurizationScenarioSpec",
    "ScenarioStochasticSpec",
    "StructuralRoleSpec",
    "build_power_pressurization_aircraft_spec",
    "build_power_pressurization_flight_spec",
    "build_power_pressurization_localization_focus_flight_spec",
    "build_power_pressurization_scenario_spec",
}


def __getattr__(name: str) -> Any:
    if name in _POWER_PRESSURIZATION_EXPORTS:
        from libs.simulation.scenarios import power_pressurization

        return getattr(power_pressurization, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = sorted(_POWER_PRESSURIZATION_EXPORTS)
