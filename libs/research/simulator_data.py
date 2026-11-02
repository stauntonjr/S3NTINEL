"""Observed-only native simulator adapter with strict common-rate synchronization."""

import json
from collections import Counter
from dataclasses import asdict
from hashlib import sha256
from math import isclose, isfinite, lcm
from pathlib import Path

from libs.research.simulator_protocol import Mission, PilotProtocol, SensorSpec
from libs.simulation.cli import DEFAULT_START_TIMESTAMP_UTC, resolve_flight
from libs.simulation.flight.runtime import Flight
from libs.simulation.reference_inference import nominal_companion_flight


def sensor_specs(flight, dt_seconds: float) -> tuple[SensorSpec, ...]:
    sensors = []
    for module in flight.aircraft_spec.iter_module_specs():
        for parameter in module.parameters:
            rate = parameter.sampling_rate_hz
            if rate is None or not isfinite(rate) or rate <= 0 or not isfinite(dt_seconds) or dt_seconds <= 0:
                raise ValueError(f"invalid declared rate/cadence: {parameter.parameter_name}")
            stride = 1 / (rate * dt_seconds)
            if stride < 1 or not isclose(stride, round(stride), abs_tol=1e-9):
                raise ValueError(f"incompatible native stride: {parameter.parameter_name}")
            if parameter.parameter_datatype_label not in {"numeric", "categorical"}:
                raise ValueError(f"unknown datatype: {parameter.parameter_datatype_label}")
            sensors.append(
                SensorSpec(parameter.parameter_name, parameter.parameter_datatype_label, rate, round(stride))
            )
    if not sensors or len({s.name for s in sensors}) != len(sensors):
        raise ValueError("empty or duplicate sensor identities")
    return tuple(sorted(sensors, key=lambda s: s.name))


def synchronize_rows(rows, sensors, *, role, seed, n_steps, dt_seconds) -> Mission:
    by_name = {s.name: s for s in sensors}
    if not sensors or len(by_name) != len(sensors):
        raise ValueError("empty or duplicate sensor schema")
    for s in sensors:
        if (
            s.kind not in {"numeric", "categorical"}
            or s.stride < 1
            or not isclose(s.rate_hz * dt_seconds * s.stride, 1.0)
        ):
            raise ValueError(f"invalid sensor kind/rate/stride: {s.name}")
    stride = lcm(*(s.stride for s in sensors))
    steps = tuple(range(0, n_steps, stride))
    grid = {t: {} for t in steps}
    seen = set()
    counts, retained = Counter(), Counter()
    for row in rows:
        step, name = row["step_index"], row["parameter_name"]
        if name not in by_name or type(step) is not int or not 0 <= step < n_steps:
            raise ValueError(f"unexpected sensor/step: {name}@{step}")
        s = by_name[name]
        if step % s.stride or (step, name) in seen:
            raise ValueError(f"duplicate or off-stride row: {name}@{step}")
        seen.add((step, name))
        value = row["parameter_value"]
        if not isinstance(value, str) or not value:
            raise ValueError(f"missing observed value: {name}@{step}")
        if s.kind == "numeric":
            try:
                valid = isfinite(float(value))
            except ValueError:
                valid = False
            if not valid:
                raise ValueError(f"non-finite numeric observation: {name}@{step}")
        if "timestamp_utc" in row:
            from datetime import datetime

            timestamp = row["timestamp_utc"]
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp)
            if (timestamp - DEFAULT_START_TIMESTAMP_UTC).total_seconds() != step * dt_seconds:
                raise ValueError(f"timestamp misalignment: {name}@{step}")
        counts[str(s.rate_hz)] += 1
        if step in grid:
            grid[step][name] = value
            retained[str(s.rate_hz)] += 1
    for t, state in grid.items():
        if set(state) != set(by_name):
            raise ValueError(f"missing common-grid observations at step {t}: {sorted(set(by_name) - set(state))}")
    kept = sum(retained.values())
    return Mission(
        role,
        seed,
        n_steps,
        dt_seconds,
        tuple(sensors),
        steps,
        tuple(grid[t] for t in steps),
        {
            "original_rows": len(rows),
            "retained_rows": kept,
            "off_grid_rows": len(rows) - kept,
            "synchronized_states": len(steps),
            "stride": stride,
            "rate_retention": {r: {"original": n, "retained": retained[r]} for r, n in counts.items()},
        },
    )


def observed_fingerprint(mission: Mission) -> str:
    data = [[t, sorted(v.items())] for t, v in zip(mission.steps, mission.values)]
    return sha256(json.dumps(data, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def generate_mission(protocol: PilotProtocol, role: str, seed: int, output_dir: Path) -> Mission:
    if role not in protocol.roles() or seed not in protocol.roles()[role]:
        raise ValueError("role/seed not in protocol")
    output_dir.mkdir(parents=True, exist_ok=False)
    spec = resolve_flight(protocol.flight_name, seed)
    if role != "faulted_test":
        spec = nominal_companion_flight(spec)
    sensors = sensor_specs(spec, protocol.dt_seconds)
    predicted_rows = sum((protocol.n_steps - 1) // s.stride + 1 for s in sensors)
    if len(sensors) > protocol.max_sensors or predicted_rows > protocol.max_raw_rows:
        raise ValueError("mission exceeds sensor/raw row bound")
    flight = Flight.from_spec(
        spec, tail_id="T_RESEARCH", flight_id=f"{role}_{seed}", start_timestamp_utc=DEFAULT_START_TIMESTAMP_UTC
    )
    rows, _ = flight.simulate_rows(n_steps=protocol.n_steps, dt_seconds=protocol.dt_seconds, apply_faults=True)
    with (output_dir / "raw.jsonl").open("x") as stream:
        for row in rows:
            stream.write(json.dumps(row, default=str, allow_nan=False) + "\n")
    if len(rows) > protocol.max_raw_rows:
        raise ValueError("raw row bound exceeded")
    mission = synchronize_rows(
        rows, sensors, role=role, seed=seed, n_steps=protocol.n_steps, dt_seconds=protocol.dt_seconds
    )
    (output_dir / "mission.json").write_text(json.dumps(asdict(mission), sort_keys=True, allow_nan=False))
    return mission
