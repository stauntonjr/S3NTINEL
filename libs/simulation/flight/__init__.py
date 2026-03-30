from __future__ import annotations

from libs.simulation.flight.runtime import Flight, FlightTick
from libs.simulation.flight.spec import (
    FlightSpec,
    InitialStateSpec,
    InputProgramSpec,
    StepInputSpec,
)


def build_coupled_module_flight_spec() -> FlightSpec:
    from libs.simulation.flight.examples import build_coupled_module_flight_spec as _builder

    return _builder()


def build_named_flight_spec(flight_name: str, *, seed: int | None = None) -> FlightSpec:
    from libs.simulation.flight.examples import build_named_flight_spec as _builder

    return _builder(flight_name, seed=seed)


def build_power_chain_flight_spec() -> FlightSpec:
    from libs.simulation.flight.examples import build_power_chain_flight_spec as _builder

    return _builder()


def build_pressurization_flight_spec() -> FlightSpec:
    from libs.simulation.flight.examples import build_pressurization_flight_spec as _builder

    return _builder()


def list_flight_names() -> tuple[str, ...]:
    from libs.simulation.flight.examples import list_flight_names as _builder

    return _builder()


__all__ = [
    "Flight",
    "FlightSpec",
    "FlightTick",
    "InitialStateSpec",
    "InputProgramSpec",
    "StepInputSpec",
    "build_coupled_module_flight_spec",
    "build_power_chain_flight_spec",
    "build_pressurization_flight_spec",
    "build_named_flight_spec",
    "list_flight_names",
]
