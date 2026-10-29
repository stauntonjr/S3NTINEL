"""Research-only conditional information baselines for discrete sensor streams.

No production scoring dependency. Inputs are ordered observations (time, sensor values)
encoded as finite nonnegative integer symbols. Training and evaluation must use
separate sequences; never fit on faulted evaluation data.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from math import log2
from typing import Mapping, Sequence

State = Mapping[str, int]


@dataclass(frozen=True)
class Surprise:
    time_index: int
    sensor: str
    symbol: int
    marginal_bits: float
    temporal_bits: float
    conditional_bits: float

    @property
    def spatial_gain_bits(self) -> float:
        """Positive means the conditional model predicts better than temporal-only."""
        return self.temporal_bits - self.conditional_bits


class CategoricalPredictor:
    """Additive-smoothed finite-alphabet conditional model.

    The alphabet must be declared before fitting, including every possible
    evaluation symbol. A context unseen in training backs off to the global
    marginal distribution. The caller supplies a causal sensor order.
    """

    def __init__(self, alphabet: Mapping[str, Sequence[int]], parents: Mapping[str, Sequence[str]] | None = None, alpha: float = 1.0):
        if alpha <= 0:
            raise ValueError("alpha must be positive")
        self.alphabet = {s: tuple(sorted(set(v))) for s, v in alphabet.items()}
        if not self.alphabet or any(not v for v in self.alphabet.values()):
            raise ValueError("each sensor needs a nonempty alphabet")
        self.order = tuple(self.alphabet)
        self.parents = {s: tuple((parents or {}).get(s, ())) for s in self.order}
        for i, s in enumerate(self.order):
            if any(p not in self.order[:i] for p in self.parents[s]):
                raise ValueError("parents must precede the sensor in decoding order")
        self.alpha = alpha
        self.marginal = {s: Counter() for s in self.order}
        self.temporal = {s: defaultdict(Counter) for s in self.order}
        self.conditional = {s: defaultdict(Counter) for s in self.order}
        self.fitted = False

    def _validate(self, states: Sequence[State]) -> None:
        for row in states:
            if set(row) != set(self.order):
                raise ValueError("every state must contain exactly the declared sensors")
            for s in self.order:
                if row[s] not in self.alphabet[s]:
                    raise ValueError(f"symbol outside alphabet: {s}={row[s]}")

    def fit(self, sequences: Sequence[Sequence[State]]) -> "CategoricalPredictor":
        if self.fitted:
            raise ValueError("predictor already fitted; create a new instance")
        if not sequences or not any(sequences):
            raise ValueError("at least one nonempty training sequence required")
        for states in sequences:
            self._validate(states)
            for t, row in enumerate(states):
                for s in self.order:
                    value = row[s]
                    self.marginal[s][value] += 1
                    if t:
                        previous = states[t - 1][s]
                        self.temporal[s][(previous,)][value] += 1
                        context = (previous,) + tuple(row[p] for p in self.parents[s])
                        self.conditional[s][context][value] += 1
        self.fitted = True
        return self

    def _bits(self, sensor: str, symbol: int, counts: Counter) -> float:
        k = len(self.alphabet[sensor])
        probability = (counts[symbol] + self.alpha) / (sum(counts.values()) + self.alpha * k)
        return -log2(probability)

    def score(self, states: Sequence[State]) -> list[Surprise]:
        if not self.fitted:
            raise ValueError("fit on nominal reference sequences before scoring")
        self._validate(states)
        result = []
        for t, row in enumerate(states):
            for s in self.order:
                value = row[s]
                base = self.marginal[s]
                marginal = self._bits(s, value, base)
                if t == 0:
                    temporal = conditional = marginal
                else:
                    prev = (states[t - 1][s],)
                    context = prev + tuple(row[p] for p in self.parents[s])
                    temporal = self._bits(s, value, self.temporal[s].get(prev, base))
                    conditional = self._bits(s, value, self.conditional[s].get(context, base))
                result.append(Surprise(t, s, value, marginal, temporal, conditional))
        return result


def window_scores(rows: Sequence[Surprise], window_size: int) -> list[dict[str, float | int]]:
    """Nonoverlapping windows; reports total and per-observation ideal bits."""
    if window_size < 1:
        raise ValueError("window_size must be positive")
    buckets: dict[int, list[Surprise]] = defaultdict(list)
    for row in rows:
        buckets[row.time_index // window_size].append(row)
    result = []
    for w, samples in sorted(buckets.items()):
        n = len(samples)
        result.append({
            "window": w,
            "observations": n,
            "marginal_total_bits": sum(r.marginal_bits for r in samples),
            "temporal_total_bits": sum(r.temporal_bits for r in samples),
            "conditional_total_bits": sum(r.conditional_bits for r in samples),
            "conditional_bits_per_observation": sum(r.conditional_bits for r in samples) / n,
            "spatial_gain_bits": sum(r.spatial_gain_bits for r in samples),
        })
    return result
