"""Tail example builders."""

from __future__ import annotations

from libs.simulation.aircraft.examples import (
    build_coupled_module_aircraft_spec,
    build_power_chain_aircraft_spec,
    build_pressurization_aircraft_spec,
)
from libs.simulation.tail.runtime import Tail


def build_coupled_module_tail(*, tail_id: str = "TCOUPLED") -> Tail:
    return Tail.from_spec(build_coupled_module_aircraft_spec(), tail_id=tail_id)


def build_power_chain_tail(*, tail_id: str = "TPOWER") -> Tail:
    return Tail.from_spec(build_power_chain_aircraft_spec(), tail_id=tail_id)


def build_pressurization_tail(*, tail_id: str = "TPRESS") -> Tail:
    return Tail.from_spec(build_pressurization_aircraft_spec(), tail_id=tail_id)
