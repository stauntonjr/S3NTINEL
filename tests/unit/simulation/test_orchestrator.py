from __future__ import annotations

from libs.simulation.flight.examples import build_power_chain_flight_spec
from libs.simulation.flight.runtime import Flight


def test_flight_iter_ticks_runs_modules_in_order_and_advances_clock():
    flight = Flight.from_spec(build_power_chain_flight_spec(), tail_id="T001", flight_id="F001")

    ticks = list(flight.iter_ticks(n_steps=3, dt_seconds=1.0))

    assert [tick.step_index for tick in ticks] == [0, 1, 2]
    assert all(tick.timestamp_utc is not None for tick in ticks)
    assert flight.step_index == 3
    assert ticks[-1].phase_label == "takeoff_climb"


def test_flight_iter_ticks_applies_initial_state_only_once():
    flight = Flight.from_spec(build_power_chain_flight_spec())

    first_tick = flight.step(dt_seconds=1.0)
    second_tick = flight.step(dt_seconds=1.0)

    assert first_tick.samples_by_module_id
    assert second_tick.samples_by_module_id
    assert flight.step_index == 2
