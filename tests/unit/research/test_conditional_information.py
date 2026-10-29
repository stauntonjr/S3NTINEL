import pytest

from libs.research.conditional_information import CategoricalPredictor, window_scores


def test_cross_sensor_coherence_break_has_excess_conditional_bits():
    model = CategoricalPredictor({"pump": [0, 1], "pressure": [0, 1]}, {"pressure": ["pump"]})
    nominal = [{"pump": ((i * 73 + i * i * 19) % 101) % 2, "pressure": ((i * 73 + i * i * 19) % 101) % 2} for i in range(200)]
    model.fit([nominal])
    good = model.score([{"pump": 0, "pressure": 0}, {"pump": 1, "pressure": 1}])
    bad = model.score([{"pump": 0, "pressure": 0}, {"pump": 1, "pressure": 0}])
    assert bad[-1].conditional_bits > good[-1].conditional_bits
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
