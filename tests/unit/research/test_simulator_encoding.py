import json
from dataclasses import replace

import pytest

from libs.research.simulator_protocol import Mission, SensorSpec
from libs.research.symbol_encoding import fit_encoder


def mission(values, kind="numeric", role="fit"):
    return Mission(
        role,
        101,
        len(values),
        1.0,
        (SensorSpec("a", kind, 1.0, 1),),
        tuple(range(len(values))),
        tuple({"a": str(v)} for v in values),
    )


def test_numeric_octiles_ties_and_fixed_under_overflow_support():
    encoder = fit_encoder([mission(range(9))])
    encoded = encoder.transform(mission([-1, 0, 1, 8, 9], role="nominal_test"))
    assert [s["a"] for s in encoded.states] == [0, 1, 2, 8, 9]
    assert encoder.alphabets == {"a": tuple(range(10))}
    assert encoded.diagnostics["a"]["underflow"] == 1
    assert encoded.diagnostics["a"]["overflow"] == 1


def test_constant_numeric_channel_has_defined_symbol_and_diagnostics():
    encoder = fit_encoder([mission([5, 5])])
    result = encoder.transform(mission([4, 5, 6]))
    assert [r["a"] for r in result.states] == [0, 8, 9]
    assert encoder.to_payload()["a"]["constant"] is True


def test_categorical_vocabulary_is_fit_only_sorted_and_exact():
    encoder = fit_encoder([mission(["b", "a"], "categorical")])
    before = json.dumps(encoder.to_payload(), sort_keys=True)
    result = encoder.transform(mission(["a", "b", "novel"], "categorical", "nominal_test"))
    assert [r["a"] for r in result.states] == [1, 2, 0]
    assert result.diagnostics["a"]["unknown"] == 1
    assert json.dumps(encoder.to_payload(), sort_keys=True) == before
    exact = fit_encoder([mission(["1", "1.0"], "categorical")])
    assert exact.to_payload()["a"]["vocabulary"] == ["1", "1.0"]


def test_encoder_rejects_wrong_roles_schema_and_excess_cardinality():
    with pytest.raises(ValueError, match="fit"):
        fit_encoder([mission([1], role="calibration")])
    with pytest.raises(ValueError):
        fit_encoder([mission(range(17), "categorical")])
    encoder = fit_encoder([mission([1, 2])])
    with pytest.raises(ValueError):
        encoder.transform(mission([1], "categorical"))
    with pytest.raises(ValueError):
        encoder.transform(replace(mission([1]), values=({"wrong": "1"},)))
    with pytest.raises(ValueError):
        encoder.transform(mission(["nan"]))
    before = json.dumps(encoder.to_payload(), sort_keys=True)
    encoder.transform(mission([-1e30, 1e30], role="faulted_test"))
    assert json.dumps(encoder.to_payload(), sort_keys=True) == before
