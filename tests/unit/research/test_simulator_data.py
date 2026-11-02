import json
from dataclasses import replace

import pytest

from libs.research.simulator_data import generate_mission, observed_fingerprint, sensor_specs, synchronize_rows
from libs.research.simulator_protocol import PilotProtocol, SensorSpec
from libs.simulation.cli import resolve_flight


def fixture_rows():
    sensors = (
        SensorSpec("a", "numeric", 2.0, 1),
        SensorSpec("b", "numeric", 1.0, 2),
        SensorSpec("c", "categorical", 0.5, 4),
    )
    rows = [
        {"step_index": t, "parameter_name": s.name, "parameter_value": str(t), "parameter_value_clean": "SECRET"}
        for t in range(12)
        for s in sensors
        if t % s.stride == 0
    ]
    return sensors, rows


def sync(rows, sensors):
    return synchronize_rows(rows, sensors, role="fit", seed=101, n_steps=12, dt_seconds=0.5)


def test_common_native_grid_and_observed_only_projection():
    sensors, rows = fixture_rows()
    mission = sync(rows, sensors)
    assert mission.steps == (0, 4, 8)
    assert mission.values[1] == {"a": "4", "b": "4", "c": "4"}
    assert mission.diagnostics["retained_rows"] == 9
    assert mission.diagnostics["original_rows"] == 21
    assert mission.diagnostics["off_grid_rows"] == 12


@pytest.mark.parametrize("breakage", ["missing", "duplicate", "unknown", "nan", "inf", "empty", "off_stride"])
def test_invalid_rows_fail_instead_of_changing_denominators(breakage):
    sensors, rows = fixture_rows()
    if breakage == "missing":
        rows = [r for r in rows if not (r["step_index"] == 4 and r["parameter_name"] == "a")]
    elif breakage == "duplicate":
        rows.append(dict(rows[0]))
    elif breakage == "unknown":
        rows.append({"step_index": 4, "parameter_name": "unexpected", "parameter_value": "0"})
    elif breakage in {"nan", "inf"}:
        rows[0]["parameter_value"] = breakage
    elif breakage == "empty":
        rows[0]["parameter_value"] = None
    else:
        rows.append({"step_index": 3, "parameter_name": "b", "parameter_value": "0"})
    with pytest.raises(ValueError):
        sync(rows, sensors)


def test_protocol_prevents_partition_leakage_and_invalid_bounds():
    protocol = PilotProtocol()
    assert protocol.roles() == {
        "fit": (101, 102, 103, 104),
        "calibration": (201, 202, 203, 204),
        "nominal_test": (301, 302, 303, 304),
        "faulted_test": (301, 302, 303, 304),
    }
    for changes in (
        {"calibration_seeds": (101,)},
        {"test_seeds": (201,)},
        {"fit_seeds": (101, 101)},
        {"max_sensors": 0},
        {"n_steps": 0},
        {"dt_seconds": float("nan")},
    ):
        with pytest.raises(ValueError):
            replace(protocol, **changes)


def test_declared_schema_and_rates_are_validated():
    flight = resolve_flight("power_pressurization_hierarchy_composite", 101)
    sensors = sensor_specs(flight, 0.5)
    assert 0 < len(sensors) <= 64
    assert {s.kind for s in sensors} == {"numeric", "categorical"}
    with pytest.raises(ValueError, match="stride"):
        sensor_specs(flight, 0.3)
    with pytest.raises(ValueError):
        sync(fixture_rows()[1], (sensors[0], sensors[0]))


def test_real_seed_reproducibility_fresh_runtime_and_raw_evidence(tmp_path):
    protocol = replace(PilotProtocol(), n_steps=12, fit_seeds=(101, 102), calibration_seeds=(201,), test_seeds=(301,))
    first = generate_mission(protocol, "fit", 101, tmp_path / "first")
    second = generate_mission(protocol, "fit", 101, tmp_path / "second")
    other = generate_mission(protocol, "fit", 102, tmp_path / "other")
    assert observed_fingerprint(first) == observed_fingerprint(second)
    assert observed_fingerprint(first) != observed_fingerprint(other)
    raw = json.loads((tmp_path / "first" / "raw.jsonl").read_text().splitlines()[0])
    assert "parameter_value_clean" in raw
    assert set(first.values[0]) == {s.name for s in first.sensors}
    with pytest.raises(FileExistsError):
        generate_mission(protocol, "fit", 101, tmp_path / "first")
    with pytest.raises(ValueError, match="seed"):
        generate_mission(protocol, "fit", 201, tmp_path / "invalid")
    with pytest.raises(ValueError, match="bound"):
        generate_mission(replace(protocol, max_raw_rows=1), "fit", 101, tmp_path / "bound")
