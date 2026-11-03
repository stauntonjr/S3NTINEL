from __future__ import annotations

from copy import deepcopy

import pytest

from libs.simulation.event_truth import annotate_event_type_labels
from libs.simulation.flight.examples import build_named_flight_spec, build_power_chain_flight_spec
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


def test_flight_audit_is_opt_in_tick_local_and_in_execution_order():
    flight = Flight.from_spec(build_power_chain_flight_spec(), tail_id="T001", flight_id="F001")
    disabled = flight.step(dt_seconds=1.0)
    assert disabled.coupling_executions == ()
    assert disabled.coupling_audit_rows() == []

    first, second = tuple(flight.iter_ticks(n_steps=2, dt_seconds=1.0, collect_coupling_audit=True))
    assert isinstance(first.coupling_executions, tuple)
    assert len(first.coupling_executions) == len(flight.spec.aircraft_spec.couplings)
    assert [r.source_module_id for r in first.coupling_executions] == ["MOD_SWITCH", "MOD_SOURCE", "MOD_SOURCE"]
    assert all(r.timestamp_utc == first.timestamp_utc for r in first.coupling_executions)
    assert all(r.timestamp_utc == second.timestamp_utc for r in second.coupling_executions)
    rows = first.coupling_audit_rows()
    assert len(rows) == 3
    assert all((r["tail_id"], r["flight_id"], r["step_index"]) == ("T001", "F001", 1) for r in rows)
    assert all(r["delivered_effective_timestamp_utc"] is None for r in rows)
    assert all(r["delivered_misbehavior_window_id"] is None for r in rows)
    rows[0]["coupling_id"] = "caller-mutated"
    assert first.coupling_audit_rows()[0]["coupling_id"] != "caller-mutated"
    assert flight.step(dt_seconds=1.0).coupling_executions == ()
    assert len(first.coupling_executions) == len(second.coupling_executions) == 3


def _runtime_state(flight):
    return deepcopy(
        {
            module_id: (module.input_ports, module.output_ports, module.delayed_input_transfers_by_key)
            for module_id, module in flight.aircraft.modules.items()
        }
    )


@pytest.mark.parametrize("apply_faults", [False, True])
def test_flight_audit_preserves_seeded_telemetry_phase_context_and_port_queues(apply_faults):
    plain = Flight.from_spec(
        build_named_flight_spec("power_pressurization_hierarchy_smoke", seed=17), tail_id="T001", flight_id="F001"
    )
    audited = Flight.from_spec(
        build_named_flight_spec("power_pressurization_hierarchy_smoke", seed=17), tail_id="T001", flight_id="F001"
    )
    receipts = 0
    windows = set()
    for left, right in zip(
        plain.iter_ticks(n_steps=600, dt_seconds=0.5, apply_faults=apply_faults),
        audited.iter_ticks(n_steps=600, dt_seconds=0.5, apply_faults=apply_faults, collect_coupling_audit=True),
    ):
        assert left.telemetry_rows() == right.telemetry_rows()
        assert left.phase_row() == right.phase_row()
        assert left.step_misbehavior_context == right.step_misbehavior_context
        assert left.coupling_executions == ()
        receipts += len(right.coupling_executions)
        windows.update(r.misbehavior_window_id for r in right.coupling_executions if r.misbehavior_window_id)
    assert receipts > 0
    assert bool(windows) == apply_faults
    assert _runtime_state(plain) == _runtime_state(audited)


def test_simulate_rows_keeps_two_outputs_without_audit_collection():
    spec = build_power_chain_flight_spec()
    result = Flight.from_spec(spec).simulate_rows(n_steps=3, dt_seconds=1.0)
    assert len(result) == 2
    raw, phases = result
    ticks = tuple(Flight.from_spec(spec).iter_ticks(n_steps=3, dt_seconds=1.0))
    assert raw == annotate_event_type_labels([row for tick in ticks for row in tick.telemetry_rows()])
    assert phases == [tick.phase_row() for tick in ticks]
    assert all(tick.coupling_executions == () for tick in ticks)
