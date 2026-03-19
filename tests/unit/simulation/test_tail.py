from __future__ import annotations

from libs.simulation.fleet.runtime import Fleet
from libs.simulation.flight.examples import build_power_chain_flight_spec
from libs.simulation.tail.runtime import Tail


def test_tail_start_flight_owns_aircraft_instance_and_sets_active_flight():
    tail = Tail.from_spec(build_power_chain_flight_spec().aircraft_spec, tail_id="TTAIL")

    flight = tail.start_flight(build_power_chain_flight_spec(), flight_id="F001")

    assert flight.tail is tail
    assert flight.aircraft is tail.aircraft
    assert flight.tail_id == "TTAIL"
    assert tail.active_flight is flight


def test_tail_record_flight_moves_flight_into_history():
    tail = Tail.from_spec(build_power_chain_flight_spec().aircraft_spec, tail_id="TTAIL")
    flight = tail.start_flight(build_power_chain_flight_spec(), flight_id="F001")

    tail.record_flight(flight)

    assert tail.active_flight is None
    assert tail.flight_history == (flight,)


def test_fleet_routes_flights_to_the_requested_tail():
    left_tail = Tail.from_spec(build_power_chain_flight_spec().aircraft_spec, tail_id="T001")
    right_tail = Tail.from_spec(build_power_chain_flight_spec().aircraft_spec, tail_id="T002")
    fleet = Fleet.from_tails([left_tail, right_tail], fleet_id="FLEET_001")

    flight = fleet.start_flight(
        tail_id="T002",
        spec=build_power_chain_flight_spec(),
        flight_id="F002",
    )

    assert fleet.tail_ids == ("T001", "T002")
    assert flight.tail is right_tail
    assert right_tail.active_flight is flight
