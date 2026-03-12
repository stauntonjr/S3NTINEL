from libs.simulation.aircraft.examples import build_coupled_module_aircraft_spec
from libs.simulation.flight.examples import build_pressurization_flight_spec
from libs.simulation.flight.runtime import Flight


def test_aircraft_runtime_builds_modules_and_coupling_index():
    aircraft = Flight.from_spec(build_pressurization_flight_spec()).aircraft

    assert set(aircraft.module_ids) == {"MOD_PRESS_MODE", "MOD_AIRCRAFT_ALT", "MOD_PRESS_CTRL", "MOD_CABIN"}
    assert set(aircraft.coupling_source_module_ids) == {"MOD_PRESS_MODE", "MOD_AIRCRAFT_ALT", "MOD_PRESS_CTRL"}


def test_flight_runtime_owns_phase_and_time_state():
    flight = Flight.from_spec(build_pressurization_flight_spec())

    tick = flight.step(dt_seconds=1.0)

    assert flight.current_phase_label == "gate_turnaround"
    assert flight.current_timestamp_utc == tick.timestamp_utc
    assert not hasattr(flight.aircraft, "current_phase_label")
    assert not hasattr(flight.aircraft, "current_timestamp_utc")
