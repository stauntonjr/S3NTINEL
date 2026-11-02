"""Finite encoding fitted only on nominal fit-role observations."""

from bisect import bisect_right
from collections import Counter
from copy import deepcopy
from dataclasses import dataclass
from math import isfinite

import numpy as np

from libs.research.simulator_protocol import EncodedMission


@dataclass(frozen=True)
class SymbolEncoder:
    sensors: tuple
    _rules: dict

    @property
    def alphabets(self):
        return {
            s.name: tuple(range(10 if s.kind == "numeric" else len(self._rules[s.name]["vocabulary"]) + 1))
            for s in self.sensors
        }

    def to_payload(self):
        return deepcopy(self._rules)

    def transform(self, mission):
        if mission.sensors != self.sensors or len(mission.steps) != len(mission.values):
            raise ValueError("encoder schema/length mismatch")
        states = []
        occupancy = {s.name: Counter() for s in self.sensors}
        for row in mission.values:
            if set(row) != set(self._rules):
                raise ValueError("encoder observed sensor set mismatch")
            state = {}
            for s in self.sensors:
                rule = self._rules[s.name]
                if s.kind == "numeric":
                    value = float(row[s.name])
                    if not isfinite(value):
                        raise ValueError("non-finite encoding input")
                    symbol = (
                        0
                        if value < rule["min"]
                        else 9
                        if value > rule["max"]
                        else 1 + bisect_right(rule["cuts"], value)
                    )
                else:
                    raw = row[s.name]
                    if not isinstance(raw, str) or not raw:
                        raise ValueError("invalid categorical observation")
                    vocabulary = rule["vocabulary"]
                    symbol = vocabulary.index(raw) + 1 if raw in vocabulary else 0
                state[s.name] = symbol
                occupancy[s.name][symbol] += 1
            states.append(state)
        diagnostics = {}
        for s in self.sensors:
            counts = occupancy[s.name]
            diagnostics[s.name] = {
                "observations": sum(counts.values()),
                "occupancy": dict(counts),
                "underflow": counts[0] if s.kind == "numeric" else 0,
                "overflow": counts[9] if s.kind == "numeric" else 0,
                "unknown": counts[0] if s.kind == "categorical" else 0,
            }
        return EncodedMission(mission.role, mission.seed, mission.steps, tuple(states), diagnostics)


def fit_encoder(fit_missions):
    if not fit_missions or any(m.role != "fit" for m in fit_missions):
        raise ValueError("encoder needs nominal fit-role missions only")
    sensors = fit_missions[0].sensors
    if any(m.sensors != sensors or not m.values for m in fit_missions):
        raise ValueError("empty/mismatched fit schema")
    names = {s.name for s in sensors}
    if any(set(row) != names for m in fit_missions for row in m.values):
        raise ValueError("fit sensor set mismatch")
    rules = {}
    for s in sensors:
        values = [row[s.name] for m in fit_missions for row in m.values]
        if s.kind == "numeric":
            data = np.asarray(values, dtype=float)
            if not np.isfinite(data).all():
                raise ValueError("non-finite fit input")
            rules[s.name] = {
                "kind": s.kind,
                "min": float(data.min()),
                "max": float(data.max()),
                "cuts": np.quantile(data, np.arange(1, 8) / 8, method="linear").tolist(),
                "constant": bool(data.min() == data.max()),
            }
        elif s.kind == "categorical":
            if any(not isinstance(v, str) or not v for v in values):
                raise ValueError("invalid fit category")
            vocabulary = sorted(set(values))
            if len(vocabulary) > 16:
                raise ValueError("categorical fit cardinality exceeds 16")
            rules[s.name] = {"kind": s.kind, "vocabulary": vocabulary, "constant": len(vocabulary) == 1}
        else:
            raise ValueError("unknown fit datatype")
    return SymbolEncoder(sensors, rules)
