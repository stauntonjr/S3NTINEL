"""Fleet example builders."""

from __future__ import annotations

from libs.simulation.fleet.runtime import Fleet
from libs.simulation.tail.examples import build_power_chain_tail, build_pressurization_tail


def build_example_fleet(*, fleet_id: str = "FLEET_EXAMPLE") -> Fleet:
    return Fleet.from_tails(
        [
            build_power_chain_tail(tail_id="TPOWER_001"),
            build_pressurization_tail(tail_id="TPRESS_001"),
        ],
        fleet_id=fleet_id,
    )
