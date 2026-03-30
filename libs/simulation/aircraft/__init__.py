from __future__ import annotations

from libs.simulation.aircraft.runtime import Aircraft
from libs.simulation.aircraft.spec import AircraftSpec


def build_coupled_module_aircraft_spec() -> AircraftSpec:
    from libs.simulation.aircraft.examples import build_coupled_module_aircraft_spec as _builder

    return _builder()


def build_power_chain_aircraft_spec() -> AircraftSpec:
    from libs.simulation.aircraft.examples import build_power_chain_aircraft_spec as _builder

    return _builder()


def build_pressurization_aircraft_spec() -> AircraftSpec:
    from libs.simulation.aircraft.examples import build_pressurization_aircraft_spec as _builder

    return _builder()


__all__ = [
    "Aircraft",
    "AircraftSpec",
    "build_coupled_module_aircraft_spec",
    "build_power_chain_aircraft_spec",
    "build_pressurization_aircraft_spec",
]
