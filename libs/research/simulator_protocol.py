"""Frozen contracts for the bounded research pilot; not production configuration."""

from dataclasses import dataclass, field
from math import isfinite


@dataclass(frozen=True)
class PilotProtocol:
    flight_name: str = "power_pressurization_hierarchy_composite"
    fit_seeds: tuple[int, ...] = (101, 102, 103, 104)
    calibration_seeds: tuple[int, ...] = (201, 202, 203, 204)
    test_seeds: tuple[int, ...] = (301, 302, 303, 304)
    n_steps: int = 3360
    dt_seconds: float = 0.5
    max_sensors: int = 64
    max_raw_rows: int = 250000
    window_seconds: float = 20.0
    alpha: float = 1.0
    false_alarm_target: float = 0.01

    def __post_init__(self):
        groups = (self.fit_seeds, self.calibration_seeds, self.test_seeds)
        if any(not g or len(g) != len(set(g)) or any(type(s) is not int for s in g) for g in groups):
            raise ValueError("seed partitions must be nonempty and unique")
        if any(set(a) & set(b) for i, a in enumerate(groups) for b in groups[i + 1 :]):
            raise ValueError("seed partitions must be disjoint")
        if type(self.n_steps) is not int or not 0 < self.n_steps <= 3360:
            raise ValueError("n_steps outside bounded mission")
        if not 0 < self.max_sensors <= 64 or not 0 < self.max_raw_rows <= 250000:
            raise ValueError("resource bound outside approved ceiling")
        if any(not isfinite(v) or v <= 0 for v in (self.dt_seconds, self.window_seconds)):
            raise ValueError("cadence/window must be finite and positive")
        if self.alpha != 1 or self.false_alarm_target != 0.01:
            raise ValueError("pilot smoothing and false-alarm target are fixed")

    def roles(self):
        return {
            "fit": self.fit_seeds,
            "calibration": self.calibration_seeds,
            "nominal_test": self.test_seeds,
            "faulted_test": self.test_seeds,
        }


@dataclass(frozen=True)
class SensorSpec:
    name: str
    kind: str
    rate_hz: float
    stride: int


@dataclass(frozen=True)
class Mission:
    role: str
    seed: int
    n_steps: int
    dt_seconds: float
    sensors: tuple[SensorSpec, ...]
    steps: tuple[int, ...]
    values: tuple[dict[str, str], ...]
    diagnostics: dict = field(default_factory=dict)


@dataclass(frozen=True)
class EncodedMission:
    role: str
    seed: int
    steps: tuple[int, ...]
    states: tuple[dict[str, int], ...]
    diagnostics: dict = field(default_factory=dict)


@dataclass(frozen=True)
class WindowScore:
    role: str
    seed: int
    arm: str
    model: str
    start_step: int
    end_step_exclusive: int
    observations: int
    mean_bits: float


@dataclass(frozen=True)
class Threshold:
    value: float
    count: int
    requested_rank: int
    ties: int
    exceedances: int
    resolution_sufficient: bool


@dataclass(frozen=True)
class AuthoredInterval:
    seed: int
    window_id: str
    start_step: int
    end_step_exclusive: int
    family: str
    detail: str
    subject: dict
    realized_parameters: dict
