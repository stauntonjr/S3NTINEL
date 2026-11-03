import pytest

from libs.research.conditional_information import CategoricalPredictor, window_scores


def test_cross_sensor_coherence_break_has_excess_conditional_bits():
    model = CategoricalPredictor({"pump": [0, 1], "pressure": [0, 1]}, {"pressure": ["pump"]})
    nominal = [
        {"pump": ((i * 73 + i * i * 19) % 101) % 2, "pressure": ((i * 73 + i * i * 19) % 101) % 2} for i in range(200)
    ]
    model.fit([nominal])
    good = model.score([{"pump": 0, "pressure": 0}, {"pump": 1, "pressure": 1}])
    bad = model.score([{"pump": 0, "pressure": 0}, {"pump": 1, "pressure": 0}])
    assert bad[-1].conditional_bits > good[-1].conditional_bits


def test_window_aggregation_preserves_sum():
    model = CategoricalPredictor({"a": [0, 1]}).fit([[{"a": 0}, {"a": 1}]])
    scored = model.score([{"a": 0}, {"a": 1}, {"a": 0}])
    windows = window_scores(scored, 2)
    assert len(windows) == 2
    assert sum(w["conditional_total_bits"] for w in windows) == pytest.approx(sum(r.conditional_bits for r in scored))


def test_rejects_noncausal_parents_and_unknown_symbols():
    with pytest.raises(ValueError):
        CategoricalPredictor({"a": [0, 1], "b": [0, 1]}, {"a": ["b"]})
    model = CategoricalPredictor({"a": [0, 1]}).fit([[{"a": 0}]])
    with pytest.raises(ValueError):
        model.score([{"a": 2}])


def test_no_refit_and_sequence_boundaries():
    model = CategoricalPredictor({"a": [0, 1]}).fit([[{"a": 0}], [{"a": 1}]])
    with pytest.raises(ValueError):
        model.fit([[{"a": 0}]])
    assert model.score([{"a": 1}])[0].temporal_bits == model.score([{"a": 1}])[0].marginal_bits


@pytest.mark.parametrize(
    "invalid_state",
    [{"a": 2}, {"wrong_sensor": 1}],
    ids=["outside_alphabet", "wrong_sensor_set"],
)
def test_failed_fit_does_not_contaminate_retry(invalid_state):
    model = CategoricalPredictor({"a": [0, 1]})
    with pytest.raises(ValueError):
        model.fit([[{"a": 1}, {"a": 0}], [invalid_state]])
    with pytest.raises(ValueError, match="before scoring"):
        model.score([{"a": 1}])

    model.fit([[{"a": 1}, {"a": 1}]])
    scored = model.score([{"a": 1}, {"a": 1}])
    # Only the successful reference contributes: marginal P(1)=3/4;
    # the sole temporal/conditional transition has P(1|1)=2/3.
    assert scored[0].marginal_bits == pytest.approx(0.4150374992788438)
    assert scored[1].marginal_bits == pytest.approx(0.4150374992788438)
    assert scored[1].temporal_bits == pytest.approx(0.5849625007211563)
    assert scored[1].conditional_bits == pytest.approx(0.5849625007211563)


@pytest.mark.parametrize("alpha", [float("nan"), float("inf"), float("-inf")])
def test_rejects_nonfinite_smoothing(alpha):
    with pytest.raises(ValueError, match="alpha"):
        CategoricalPredictor({"a": [0, 1]}, alpha=alpha)
