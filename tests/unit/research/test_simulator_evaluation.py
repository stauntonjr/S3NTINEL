import json
from dataclasses import replace

import pytest

from libs.research.conditional_evaluation import calibrate, fit_arms, model_payload, score_mission, select_parents
from libs.research.simulator_protocol import EncodedMission, PilotProtocol


def encoded(states, role="fit", steps=None):
    return EncodedMission(role, 101, tuple(range(len(states))) if steps is None else tuple(steps), tuple(states))


def test_parent_selection_uses_fit_only_predictive_association_and_lexical_ties():
    fit = [encoded([{"a": t % 2, "b": t % 2, "c": t % 2, "d": 0} for t in range(8)])]
    assert select_parents(fit, ("a", "b", "c", "d")) == {"a": (), "b": ("a",), "c": ("a",), "d": ()}
    with pytest.raises(ValueError, match="fit"):
        select_parents([replace(fit[0], role="faulted_test")], ("a", "b", "c", "d"))
    with pytest.raises(ValueError):
        fit_arms(fit, {"wrong": [0, 1]})


def test_calibration_order_statistic_strict_ties_and_resolution():
    result = calibrate(list(range(336)))
    assert (result.count, result.requested_rank, result.value, result.exceedances) == (336, 334, 333, 2)
    assert result.resolution_sufficient
    tied = calibrate([1.0] * 336)
    assert tied.exceedances == 0 and tied.ties == 336
    assert not calibrate([1.0, 2.0]).resolution_sufficient
    for scores in ([], [float("nan")], [float("inf")]):
        with pytest.raises(ValueError):
            calibrate(scores)


def test_window_alignment_partial_exclusion_order_invariance_and_immutable_scoring():
    fit = [encoded([{"a": 1, "b": 1}, {"a": 1, "b": 1}])]
    arms = fit_arms(fit, {"a": (0, 1), "b": (0, 1)})
    protocol = replace(PilotProtocol(), n_steps=84)
    mission = encoded([{"a": 1, "b": 1}] * 21, role="nominal_test", steps=range(0, 84, 4))
    before = json.dumps({k: model_payload(v) for k, v in arms.items()}, sort_keys=True)
    results = {}
    for arm, model in arms.items():
        scores, details = score_mission(model, mission, arm=arm, protocol=protocol)
        assert len(scores) == 6
        assert {(s.start_step, s.end_step_exclusive, s.observations) for s in scores} == {(0, 40, 20), (40, 80, 20)}
        assert details["partial_window_states"] == 1
        assert details["sensors"]["a"]["marginal"]["mean_bits"] == pytest.approx(0.4150374992788438)
        temporal = [s.mean_bits for s in scores if s.model == "temporal"]
        assert temporal[0] == pytest.approx((0.4150374992788438 + 9 * 0.5849625007211563) / 10)
        results[arm] = {(s.model, s.start_step): s.mean_bits for s in scores}
    assert results["lexical"] == results["reverse_lexical"]
    assert json.dumps({k: model_payload(v) for k, v in arms.items()}, sort_keys=True) == before


def test_no_cross_mission_transition_and_unseen_fallback_are_counted():
    arms = fit_arms([encoded([{"a": 0}]), encoded([{"a": 1}])], {"a": (0, 1)})
    assert not arms["lexical"].temporal["a"]
    p = replace(PilotProtocol(), n_steps=4, dt_seconds=1.0, window_seconds=2.0)
    scores, details = score_mission(
        arms["lexical"], encoded([{"a": 0}, {"a": 1}, {"a": 0}, {"a": 1}], "nominal_test"), arm="lexical", protocol=p
    )
    assert all(s.mean_bits == 1 for s in scores)
    assert details["fallback"]["a"]["temporal"] == 3
    assert details["fallback"]["a"]["conditional"] == 3
    with pytest.raises(ValueError):
        score_mission(arms["lexical"], encoded([{"a": 0}] * 3, steps=[0, 1, 3]), arm="lexical", protocol=p)
