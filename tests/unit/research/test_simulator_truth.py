import json
from dataclasses import replace

import pytest

from libs.research.simulator_protocol import AuthoredInterval, Threshold, WindowScore
from libs.research.simulator_truth import authored_intervals, coverage_inventory, evaluate_truth
from libs.simulation.cli import resolve_flight


def interval(identity, start, end, detail="drift"):
    return AuthoredInterval(301, identity, start, end, "accumulative", detail, {}, {})


def test_half_open_truth_overlap_shared_alerts_and_zero_support_denominators(tmp_path):
    intervals = [interval("left", 0, 40), interval("touch", 40, 80), interval("inside", 10, 20)]
    scores = [
        WindowScore("faulted_test", 301, "lexical", "conditional", 0, 40, 20, 3.0),
        WindowScore("faulted_test", 301, "lexical", "conditional", 40, 80, 20, 0.0),
    ]
    truth = tmp_path / "truth.jsonl"
    truth.write_text(
        json.dumps({"step_index": 1, "misbehavior_window_id": "left", "misbehavior_applied": True})
        + "\n"
        + json.dumps({"step_index": 4, "misbehavior_window_id": "", "misbehavior_applied": True})
        + "\n"
    )
    result = evaluate_truth(
        intervals, scores, {"lexical/conditional": Threshold(1, 336, 334, 0, 2, True)}, truth, [0, 4, 8]
    )
    rows = {r["window_id"]: r for r in result["faults"]}
    assert len(rows) == 3
    assert rows["left"]["detected"] and not rows["touch"]["detected"]
    assert rows["inside"]["detected"] and rows["inside"]["applied_rows"] == 0
    assert rows["left"]["off_grid_only"] is True
    assert rows["left"]["applied_rows"] == 1
    assert result["alerts"][0]["overlapping_fault_ids"] == ["left", "inside"]
    assert rows["inside"]["overlap_steps"] == 10


def test_authored_inventory_preserves_realized_metadata_and_missing_family_coverage():
    flight = resolve_flight("power_pressurization_hierarchy_composite", 301)
    intervals = authored_intervals(flight, 301)
    assert intervals and all(i.window_id and i.realized_parameters for i in intervals)
    coverage = coverage_inventory(intervals)
    assert coverage["stuck_at"]["status"] == "not_covered"
    assert coverage["drift"]["status"] == "covered"
    windows = flight.misbehavior_program_spec.windows
    bad_window = replace(windows[0], metadata={})
    # Explicitly empty authored identity may not be synthesized from observed flags.
    bad = replace(
        flight,
        misbehavior_program_spec=replace(flight.misbehavior_program_spec, windows=(bad_window,)),
        fault_program_spec=None,
    )
    with pytest.raises(ValueError, match="identity"):
        authored_intervals(bad, 301)


def test_duplicate_interval_identity_is_rejected(tmp_path):
    path = tmp_path / "raw.jsonl"
    path.write_text("")
    with pytest.raises(ValueError):
        evaluate_truth([interval("same", 0, 10), interval("same", 20, 30)], [], {}, path, [])
