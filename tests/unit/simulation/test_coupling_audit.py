from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone

import pytest

from libs.simulation import Coupling, Module, ModuleSpec, PortSpec
from libs.simulation.flight.examples import build_power_chain_flight_spec
from libs.simulation.flight.runtime import Flight

T0 = datetime(2025, 1, 1, tzinfo=timezone.utc)


def _modules(*, value=28.0, timestamp=T0):
    source = Module.from_spec(
        ModuleSpec(
            module_id="SRC",
            subsystem_id="SUB",
            system_id="SYS",
            output_ports=(PortSpec.output(port_name="out", value_datatype_label="numeric"),),
        )
    )
    target = Module.from_spec(
        ModuleSpec(
            module_id="DST",
            subsystem_id="SUB",
            system_id="SYS",
            input_ports=(PortSpec.input(port_name="in", value_datatype_label="numeric"),),
        )
    )
    source.output_port("out").current_value = value
    source.output_port("out").timestamp_utc = timestamp
    return source, target


def _coupling(**overrides):
    return Coupling(
        **{
            "source_module_id": "SRC",
            "source_port_name": "out",
            "target_module_id": "DST",
            "target_port_name": "in",
            "relation_type": "drive",
            "metadata": {"coupling_id": "C1"},
            **overrides,
        }
    )


def _context(detail, window):
    return {
        "misbehavior_family_label": "coupling",
        "misbehavior_detail_label": detail,
        "misbehavior_window_id": window,
        "fault_family_label": "coupling",
        "fault_type": detail,
        "fault_window_id": f"fault-{window}",
    }


def test_audit_nominal_write_preserves_apply_return_and_port_provenance():
    source, target = _modules()
    audit = []

    assert _coupling().apply(source, target, audit_sink=audit) is None

    port = target.input_port("in")
    assert (port.current_value, port.timestamp_utc) == (28.0, T0)
    assert port.metadata["coupling_id"] == "C1"
    assert "misbehavior_window_id" not in port.metadata
    assert len(audit) == 1
    receipt = audit[0]
    assert (receipt.coupling_id, receipt.source_module_id, receipt.target_module_id) == ("C1", "SRC", "DST")
    assert (receipt.source_port_name, receipt.target_port_name, receipt.relation_type) == ("out", "in", "drive")
    assert (receipt.action, receipt.gate_block_reason, receipt.delivered_transfer) == ("written", None, None)
    assert (receipt.timestamp_utc, receipt.source_timestamp_utc) == (None, T0)
    assert (receipt.gain, receipt.sign, receipt.lag_seconds) == (1.0, 1, 0.0)
    assert not receipt.context_present
    assert receipt.misbehavior_window_id == receipt.fault_window_id == ""
    assert receipt.scheduled_for_timestamp_utc is None
    with pytest.raises(FrozenInstanceError):
        receipt.action = "blocked"


def test_audit_admitted_break_holds_value_without_relabeling_the_input():
    source, target = _modules()
    coupling = _coupling()
    coupling.apply(source, target)
    port = target.input_port("in")
    original_metadata = dict(port.metadata)
    source.output_port("out").current_value = 30.0
    audit = []
    context = _context("coupling_break", "break-1")

    assert (
        coupling.apply(
            source, target, timestamp_utc=T0 + timedelta(seconds=1), misbehavior_context=context, audit_sink=audit
        )
        is None
    )

    assert (port.current_value, port.timestamp_utc, port.metadata) == (28.0, T0, original_metadata)
    assert len(audit) == 1
    assert audit[0].action == "suppressed_break"
    assert audit[0].misbehavior_window_id == "break-1"
    assert audit[0].fault_window_id == "fault-break-1"
    assert audit[0].context_present
    assert audit[0].delivered_transfer is None
    context["misbehavior_window_id"] = "changed-later"
    assert audit[0].misbehavior_window_id == "break-1"


@pytest.mark.parametrize(
    "options,source_modes,target_modes,phase,context,reason",
    [
        (
            {"phase_gate": ("cruise",), "source_mode_name": "pack", "source_mode_gate": ("HIGH",)},
            {"pack": "OFF"},
            {},
            "taxi",
            {},
            "phase",
        ),
        ({"source_mode_name": "pack", "source_mode_gate": ("HIGH",)}, {"pack": "OFF"}, {}, None, {}, "source_mode"),
        ({"source_mode_name": "pack", "source_mode_gate": ("HIGH",)}, {}, {}, None, {}, "source_mode"),
        (
            {"target_mode_name": "cabin", "target_mode_gate": ("OPEN",)},
            {},
            {"cabin": "CLOSED"},
            None,
            {},
            "target_mode",
        ),
        ({"mode_gate": ("HIGH",)}, {"pack": "LOW"}, {"cabin": "OFF"}, None, {}, "mode"),
        ({"mode_gate": ("HIGH",)}, {}, {"cabin": "HIGH"}, None, {}, None),
        (
            {"mode_gate": ("EMERGENCY",), "source_mode_name": "pack", "source_mode_gate": ("HIGH",)},
            {"pack": "HIGH"},
            {},
            None,
            {},
            None,
        ),
        (
            {
                "source_mode_name": "pack",
                "source_mode_gate": ("HIGH",),
                "target_mode_name": "cabin",
                "target_mode_gate": ("OPEN",),
            },
            {"pack": "HIGH"},
            {"cabin": "OFF"},
            None,
            {},
            "target_mode",
        ),
        (
            {"phase_gate": ("cruise",), "source_mode_name": "pack", "source_mode_gate": ("HIGH",)},
            {"pack": "OFF"},
            {},
            "taxi",
            _context("phase_context_violation", "phase-1"),
            "source_mode",
        ),
        (
            {"phase_gate": ("cruise",), "source_mode_name": "pack", "source_mode_gate": ("HIGH",)},
            {"pack": "HIGH"},
            {},
            "taxi",
            _context("phase_context_violation", "phase-1"),
            None,
        ),
    ],
)
def test_audit_reports_first_gate_without_changing_admission(
    options, source_modes, target_modes, phase, context, reason
):
    source, target = _modules()
    source.mode_state_by_name.update(source_modes)
    target.mode_state_by_name.update(target_modes)
    port = target.input_port("in")
    port.current_value = 99.0
    port.metadata = {"held": True}
    coupling = _coupling(**options)
    audit = []

    coupling.apply(source, target, current_phase_label=phase, misbehavior_context=context, audit_sink=audit)

    assert len(audit) == 1
    assert audit[0].gate_block_reason == reason
    assert audit[0].action == ("blocked" if reason else "written")
    assert coupling.is_active(source, target, current_phase_label=phase, ignore_phase_gate=bool(context)) == (
        reason is None
    )
    if reason:
        assert (port.current_value, port.timestamp_utc, port.metadata) == (99.0, None, {"held": True})
    else:
        assert (port.current_value, port.timestamp_utc) == (28.0, T0)
    if context:
        assert audit[0].ignore_phase_gate
        assert audit[0].misbehavior_window_id == "phase-1"


def test_audit_gate_block_takes_precedence_over_break():
    source, target = _modules()
    audit = []
    _coupling(phase_gate=("cruise",)).apply(
        source,
        target,
        current_phase_label="taxi",
        misbehavior_context=_context("coupling_break", "break-1"),
        audit_sink=audit,
    )
    assert len(audit) == 1
    assert (audit[0].action, audit[0].gate_block_reason) == ("blocked", "phase")
    assert audit[0].misbehavior_window_id == "break-1"


def test_audit_enqueue_records_deadline_without_claiming_delivery():
    source, target = _modules()
    audit = []
    _coupling(lag_seconds=1.0).apply(
        source, target, timestamp_utc=T0, misbehavior_context=_context("timing_lag", "prior-1"), audit_sink=audit
    )
    assert target.input_port("in").current_value is None
    assert len(audit) == 1
    assert audit[0].action == "enqueued"
    assert audit[0].scheduled_for_timestamp_utc == T0 + timedelta(seconds=1)
    assert audit[0].delivered_transfer is None
    assert audit[0].misbehavior_window_id == "prior-1"
    queue = target.delayed_input_transfers_by_key[_coupling(lag_seconds=1.0).key()]
    assert len(queue.transfers) == 1
    assert queue.transfers[0].value == 28.0


@pytest.mark.parametrize("block", [False, True])
def test_audit_previous_delivery_is_independent_of_suppressed_current_attempt(block):
    source, target = _modules()
    coupling = _coupling(lag_seconds=1.0, phase_gate=("cruise",))
    coupling.apply(
        source,
        target,
        timestamp_utc=T0,
        current_phase_label="cruise",
        misbehavior_context=_context("timing_lag", "prior-1"),
    )
    source.output_port("out").current_value = 30.0
    audit = []
    coupling.apply(
        source,
        target,
        timestamp_utc=T0 + timedelta(seconds=1),
        current_phase_label="taxi" if block else "cruise",
        misbehavior_context=_context("coupling_break", "break-1"),
        audit_sink=audit,
    )
    port = target.input_port("in")
    assert (port.current_value, port.timestamp_utc) == (28.0, T0 + timedelta(seconds=1))
    assert port.metadata["misbehavior_window_id"] == "prior-1"
    assert len(audit) == 1
    assert audit[0].action == ("blocked" if block else "suppressed_break")
    assert audit[0].gate_block_reason == ("phase" if block else None)
    assert audit[0].misbehavior_window_id == "break-1"
    delivery = audit[0].delivered_transfer
    assert delivery is not None
    assert delivery.effective_timestamp_utc == T0 + timedelta(seconds=1)
    assert (delivery.misbehavior_window_id, delivery.fault_window_id) == ("prior-1", "fault-prior-1")
    assert (delivery.misbehavior_detail_label, delivery.fault_type) == ("timing_lag", "timing_lag")
    with pytest.raises(FrozenInstanceError):
        delivery.misbehavior_window_id = "changed"
    assert all(not queue.transfers for queue in target.delayed_input_transfers_by_key.values())


def test_audit_only_reports_the_last_due_entry_in_existing_queue_order():
    source, target = _modules()
    coupling = _coupling(lag_seconds=1.0)
    # A later deadline is deliberately enqueued first. Do not silently sort the queue.
    coupling.apply(
        source,
        target,
        timestamp_utc=T0 + timedelta(seconds=0.5),
        misbehavior_context=_context("timing_lag", "later-deadline"),
    )
    source.output_port("out").current_value = 29.0
    coupling.apply(source, target, timestamp_utc=T0, misbehavior_context=_context("timing_lag", "last-in-queue"))
    audit = []
    coupling.apply(
        source,
        target,
        timestamp_utc=T0 + timedelta(seconds=2),
        misbehavior_context=_context("coupling_break", "break-1"),
        audit_sink=audit,
    )
    assert len(audit) == 1
    delivery = audit[0].delivered_transfer
    assert delivery is not None
    assert delivery.misbehavior_window_id == "last-in-queue"
    assert delivery.effective_timestamp_utc == T0 + timedelta(seconds=1)
    assert target.input_port("in").current_value == 29.0
    assert target.input_port("in").metadata["misbehavior_window_id"] == "last-in-queue"
    assert all(not queue.transfers for queue in target.delayed_input_transfers_by_key.values())


@pytest.mark.parametrize("override", [{"gain_override": 2.0}, {"sign_override": -1}, {"lag_seconds_override": 2.0}])
def test_audit_does_not_drain_a_different_effective_transfer_key(override):
    source, target = _modules()
    coupling = _coupling(lag_seconds=1.0, phase_gate=("cruise",))
    coupling.apply(source, target, timestamp_utc=T0, current_phase_label="cruise")
    audit = []
    coupling.apply(
        source,
        target,
        timestamp_utc=T0 + timedelta(seconds=2),
        current_phase_label="taxi",
        misbehavior_context=override,
        audit_sink=audit,
    )
    assert len(audit) == 1
    assert audit[0].action == "blocked"
    assert audit[0].delivered_transfer is None
    assert target.input_port("in").current_value is None
    assert len(target.delayed_input_transfers_by_key[coupling.key()].transfers) == 1
    coupling.apply(
        source,
        target,
        timestamp_utc=T0 + timedelta(seconds=2),
        current_phase_label="cruise",
        misbehavior_context=_context("coupling_break", "break-1"),
        audit_sink=audit,
    )
    assert audit[-1].delivered_transfer is not None
    assert target.input_port("in").current_value == 28.0


@pytest.mark.parametrize(
    "relation,value,context,action,want",
    [
        ("enable", 0.0, {}, "written", None),
        ("inhibit", 0.0, {}, "no_write", 99.0),
        ("inhibit", 1.0, {}, "written", None),
        ("inhibit", 0.0, _context("coupling_break", "break-1"), "suppressed_break", 99.0),
    ],
)
def test_audit_distinguishes_none_writes_from_inactive_inhibit_and_break(relation, value, context, action, want):
    source, target = _modules(value=value)
    port = target.input_port("in")
    port.current_value = 99.0
    audit = []
    _coupling(relation_type=relation).apply(source, target, misbehavior_context=context, audit_sink=audit)
    assert len(audit) == 1
    assert audit[0].action == action
    assert port.current_value == want
    assert port.timestamp_utc == (T0 if action == "written" else None)


def test_audit_positive_lag_without_timestamp_preserves_immediate_write_fallback():
    source, target = _modules(timestamp=None)
    audit = []
    _coupling(lag_seconds=1.0).apply(source, target, audit_sink=audit)
    assert len(audit) == 1
    assert audit[0].action == "written"
    assert (audit[0].source_timestamp_utc, audit[0].scheduled_for_timestamp_utc) == (None, None)
    assert audit[0].delivered_transfer is None
    assert target.input_port("in").current_value == 28.0
    assert not target.delayed_input_transfers_by_key


def test_audit_exception_does_not_fabricate_a_completed_invocation():
    source, target = _modules()
    audit = []
    with pytest.raises(KeyError):
        _coupling(target_port_name="missing").apply(source, target, audit_sink=audit)
    assert audit == []


def test_tick_rows_keep_current_attempt_and_delivered_window_identities_separate():
    source, target = _modules()
    coupling = _coupling(lag_seconds=1.0)
    coupling.apply(source, target, misbehavior_context=_context("timing_lag", "prior-1"))
    audit = []
    coupling.apply(
        source,
        target,
        timestamp_utc=T0 + timedelta(seconds=1),
        misbehavior_context=_context("coupling_break", "break-1"),
        audit_sink=audit,
    )
    base_tick = Flight.from_spec(build_power_chain_flight_spec(), tail_id="T1", flight_id="F1").step(dt_seconds=1.0)
    tick = replace(base_tick, coupling_executions=tuple(audit))
    (row,) = tick.coupling_audit_rows()
    assert (row["tail_id"], row["flight_id"], row["step_index"]) == ("T1", "F1", 0)
    assert row["misbehavior_window_id"] == "break-1"
    assert row["delivered_misbehavior_window_id"] == "prior-1"
    assert row["delivered_fault_window_id"] == "fault-prior-1"
    assert row["delivered_effective_timestamp_utc"] == T0 + timedelta(seconds=1)
    assert row["action"] == "suppressed_break"
    assert "delivered_transfer" not in row


def test_audit_shared_queue_retains_the_delivered_transfers_originating_coupling_id():
    source, target = _modules()
    source.mode_state_by_name["mode"] = "SECOND"
    first = _coupling(lag_seconds=1.0, mode_gate=("FIRST",), metadata={"coupling_id": "C-FIRST"})
    second = _coupling(lag_seconds=1.0, mode_gate=("SECOND",), metadata={"coupling_id": "C-SECOND"})
    assert first.key() == second.key()
    second.apply(source, target, timestamp_utc=T0)
    audit = []

    first.apply(source, target, timestamp_utc=T0 + timedelta(seconds=1), audit_sink=audit)

    assert target.input_port("in").current_value == 28.0
    assert target.input_port("in").metadata["coupling_id"] == "C-SECOND"
    (receipt,) = audit
    assert (receipt.coupling_id, receipt.action, receipt.gate_block_reason) == ("C-FIRST", "blocked", "mode")
    assert receipt.delivered_transfer is not None
    assert getattr(receipt.delivered_transfer, "coupling_id", None) == "C-SECOND"
    base_tick = Flight.from_spec(build_power_chain_flight_spec()).step(dt_seconds=1.0)
    (row,) = replace(base_tick, coupling_executions=tuple(audit)).coupling_audit_rows()
    assert row["coupling_id"] == "C-FIRST"
    assert row.get("delivered_coupling_id") == "C-SECOND"
