"""Fit-only parent selection and fixed-window likelihood evaluation."""

from collections import Counter, defaultdict
from math import ceil, isclose, isfinite, log2

from libs.research.conditional_information import CategoricalPredictor
from libs.research.simulator_protocol import Threshold, WindowScore

MODELS = ("marginal", "temporal", "conditional")


def select_parents(fit, order):
    if not fit or any(m.role != "fit" or not m.states for m in fit):
        raise ValueError("parent selection requires fit-role observations")
    rows = [row for m in fit for row in m.states]
    if len(set(order)) != len(order) or any(set(row) != set(order) for row in rows):
        raise ValueError("parent sensor schema mismatch")
    marginals = {s: Counter(row[s] for row in rows) for s in order}
    n = len(rows)
    parents = {}
    for i, sensor in enumerate(order):
        candidates = []
        for parent in order[:i]:
            joint = Counter((row[sensor], row[parent]) for row in rows)
            mi = max(
                0.0,
                sum(
                    count / n * log2(count * n / (marginals[sensor][a] * marginals[parent][b]))
                    for (a, b), count in joint.items()
                ),
            )
            candidates.append((mi, parent))
        ranked = sorted(candidates, key=lambda item: (-item[0], item[1]))
        parents[sensor] = (ranked[0][1],) if ranked and ranked[0][0] > 0 else ()
    return parents


def fit_arms(fit, alphabets):
    arms = {}
    for name, reverse in (("lexical", False), ("reverse_lexical", True)):
        order = tuple(sorted(alphabets, reverse=reverse))
        parents = select_parents(fit, order)
        arms[name] = CategoricalPredictor({s: alphabets[s] for s in order}, parents, alpha=1.0).fit(
            [m.states for m in fit]
        )
    return arms


def model_payload(model):
    def contexts(table):
        return [
            {"context": list(context), "counts": sorted(counts.items()), "support": sum(counts.values())}
            for context, counts in sorted(table.items())
        ]

    return {
        "order": list(model.order),
        "alphabet": {s: list(model.alphabet[s]) for s in model.order},
        "parents": {s: list(model.parents[s]) for s in model.order},
        "alpha": model.alpha,
        "fitted": model.fitted,
        "marginal": {s: sorted(model.marginal[s].items()) for s in model.order},
        "temporal": {s: contexts(model.temporal[s]) for s in model.order},
        "conditional": {s: contexts(model.conditional[s]) for s in model.order},
    }


def calibrate(scores, false_alarm_target=0.01):
    if not scores or any(not isfinite(s) for s in scores) or not 0 < false_alarm_target < 1:
        raise ValueError("invalid calibration observations/budget")
    ordered = sorted(scores)
    rank = ceil((len(scores) + 1) * (1 - false_alarm_target))
    threshold = ordered[min(rank, len(scores)) - 1]
    return Threshold(
        threshold, len(scores), rank, ordered.count(threshold), sum(s > threshold for s in ordered), rank <= len(scores)
    )


def score_mission(model, mission, *, arm, protocol):
    steps = mission.steps
    stride = steps[1] - steps[0] if len(steps) > 1 else protocol.n_steps
    if (
        not steps
        or stride < 1
        or steps != tuple(range(0, protocol.n_steps, stride))
        or len(steps) != len(mission.states)
    ):
        raise ValueError("incomplete/irregular synchronized states")
    window_steps = round(protocol.window_seconds / protocol.dt_seconds)
    if not isclose(window_steps * protocol.dt_seconds, protocol.window_seconds) or window_steps % stride:
        raise ValueError("window incompatible with native grid")
    full_end = protocol.n_steps // window_steps * window_steps
    bits = model.score(mission.states)
    windows = defaultdict(lambda: defaultdict(list))
    sensor_sums = {s: {name: [] for name in MODELS} for s in model.order}
    fallback = {s: {"eligible": 0, "temporal": 0, "conditional": 0} for s in model.order}
    contributions = []
    for row in bits:
        step = steps[row.time_index]
        if step >= full_end:
            continue
        window_start = step // window_steps * window_steps
        values = {name: getattr(row, name + "_bits") for name in MODELS}
        if not all(isfinite(value) for value in values.values()):
            raise ValueError("non-finite likelihood")
        for name, value in values.items():
            windows[window_start][name].append(value)
            sensor_sums[row.sensor][name].append(value)
        if row.time_index:
            previous = (mission.states[row.time_index - 1][row.sensor],)
            context = previous + tuple(mission.states[row.time_index][p] for p in model.parents[row.sensor])
            fallback[row.sensor]["eligible"] += 1
            fallback[row.sensor]["temporal"] += previous not in model.temporal[row.sensor]
            fallback[row.sensor]["conditional"] += context not in model.conditional[row.sensor]
        contributions.append({"step": step, "sensor": row.sensor, "symbol": row.symbol, **values})
    scores = [
        WindowScore(
            mission.role, mission.seed, arm, name, start, start + window_steps, len(values), sum(values) / len(values)
        )
        for start, models in sorted(windows.items())
        for name, values in models.items()
    ]
    details = {
        "partial_window_states": sum(t >= full_end for t in steps),
        "fallback": fallback,
        "sensors": {
            s: {
                name: {
                    "total_bits": sum(values),
                    "observations": len(values),
                    "mean_bits": sum(values) / len(values) if values else None,
                }
                for name, values in models.items()
            }
            for s, models in sensor_sums.items()
        },
        "contributions": contributions,
    }
    return scores, details
