import libs.simulation as simulation
from libs.simulation.phase.runtime import index_phase_envelopes_by_label, resolve_phase_label_for_step, validate_phase_label
from libs.simulation.phase.spec import PhaseEnvelopeSpec, PhaseScheduleSpec, PhaseSegmentSpec


def test_phase_specs_are_importable_from_phase_spec_module():
    segment = PhaseSegmentSpec("takeoff_climb", 2)
    schedule = PhaseScheduleSpec((segment,), repeat=True)
    envelope = PhaseEnvelopeSpec(phase_label="takeoff_climb")

    assert schedule.segments[0].phase_label == "takeoff_climb"
    assert envelope.phase_label == "takeoff_climb"


def test_phase_helper_functions_remain_in_phase_runtime_module():
    schedule = PhaseScheduleSpec((PhaseSegmentSpec("takeoff_climb", 2),), repeat=True)
    indexed = index_phase_envelopes_by_label((PhaseEnvelopeSpec("takeoff_climb"),))

    assert validate_phase_label("takeoff_climb") == "takeoff_climb"
    assert resolve_phase_label_for_step(schedule, 3) == "takeoff_climb"
    assert indexed["takeoff_climb"].phase_label == "takeoff_climb"


def test_root_surface_exports_phase_symbols_from_new_modules():
    assert simulation.PhaseSegmentSpec is PhaseSegmentSpec
    assert simulation.PhaseScheduleSpec is PhaseScheduleSpec
    assert simulation.PhaseEnvelopeSpec is PhaseEnvelopeSpec


def test_phase_helper_functions_validate_phase_spec_inputs():
    schedule = PhaseScheduleSpec((PhaseSegmentSpec("gate_turnaround", 1), PhaseSegmentSpec("takeoff_climb", 2)))

    assert resolve_phase_label_for_step(schedule, 0) == "gate_turnaround"
    assert resolve_phase_label_for_step(schedule, 1) == "takeoff_climb"
