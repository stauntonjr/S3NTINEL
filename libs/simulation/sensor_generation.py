"""Generate a single parameter observation (no anomaly labeling).

Returns a dict with keys:
- parameter_value: str (observed downstream signal, after generator-side noise/effects)
- parameter_value_clean: str | None (generator clean value before noise/effects)

Downstream profilers, detectors, and structure builders should consume
`parameter_value` as the observed signal. `parameter_value_clean` is retained for
simulation labels, truth-oriented validation, and debug.
"""
from __future__ import annotations

import math
from typing import Dict

import numpy as np

from libs.common import SensorDataType
from libs.simulation.phase_engine import state_from_probs


def generate_parameter_observation(
    *,
    datatype: str,
    parameter_name: str,
    spec: dict,
    modifier: dict,
    latent: float,
    phase_name: str,
    t: int,
    system_id: str,
    tail_profile: dict,
    phase_corr_scale: float,
    phase_noise_scale: float,
    flight_noise_scale: float,
    rng_local: np.random.Generator,
    binary_state_cache: dict[str, str],
    categorical_state_cache: dict[str, str],
) -> dict[str, object]:
    parameter_value: str | None = None
    parameter_value_clean: str | None = None
    state: str | None = None

    if datatype == SensorDataType.NUMERIC.value:
        baseline = float(spec.get("baseline", 0.0))
        tail_bias = float(tail_profile.get("global_bias", 0.0)) + float(tail_profile.get("system_bias", {}).get(system_id, 0.0))
        aging = float(tail_profile.get("aging_drift_per_hour", 0.0)) * (t / 3600.0)
        trend = float(spec.get("trend_per_sec", 0.0) + modifier.get("trend_add", 0.0)) * t
        osc = float(spec.get("osc_amp", 0.0)) * math.sin(2.0 * math.pi * (t / float(spec.get("osc_period_sec", 1.0))))
        corr = float(spec.get("corr_scale", 1.0)) * phase_corr_scale * float(tail_profile.get("system_corr_scale", {}).get(system_id, 1.0)) * latent
        noise_sigma = float(spec.get("noise_sigma", 1.0)) * phase_noise_scale * flight_noise_scale * float(tail_profile.get("global_noise_scale", 1.0))
        noise = float(rng_local.normal(0.0, max(noise_sigma, 1e-6)))
        phase_add = float(modifier.get("add", 0.0))

        val_clean = baseline + tail_bias + aging + trend + osc + corr + phase_add
        min_val = spec.get("min_val")
        max_val = spec.get("max_val")
        if min_val is not None:
            val_clean = max(float(min_val), val_clean)
        if max_val is not None:
            val_clean = min(float(max_val), val_clean)

        val = val_clean + noise
        if min_val is not None:
            val = max(float(min_val), val)
        if max_val is not None:
            val = min(float(max_val), val)

        parameter_value_clean = f"{val_clean:.6f}"
        parameter_value = f"{val:.6f}"

    elif datatype == SensorDataType.BINARY.value:
        p_on = float(modifier.get("binary_on_prob", spec.get("base_on_prob", 0.5)))
        p_on = p_on + 0.08 * float(spec.get("latent_gain", 0.8)) * float(np.tanh(latent))
        p_on = float(np.clip(p_on, 0.01, 0.99))
        persistence = float(np.clip(spec.get("persistence", 0.985), 0.0, 0.9999))
        prev_state = binary_state_cache.get(parameter_name)
        if prev_state is None:
            state = "1" if rng_local.random() < p_on else "0"
        elif prev_state == "1":
            p_stay_on = persistence + (1.0 - persistence) * p_on
            state = "1" if rng_local.random() < p_stay_on else "0"
        else:
            p_turn_on = (1.0 - persistence) * p_on
            state = "1" if rng_local.random() < p_turn_on else "0"
        binary_state_cache[parameter_name] = state
        parameter_value = str(state)
        parameter_value_clean = str(state)

    elif datatype == SensorDataType.CATEGORICAL.value:
        states = list(spec.get("states", ["STATE_A", "STATE_B", "STATE_C"]))
        base_probs = spec.get("base_probs")
        base_probs = [1.0 / len(states)] * len(states) if base_probs is None else list(base_probs)
        probs = np.array(modifier.get("state_weights", base_probs), dtype=float)
        if len(states) >= 2:
            probs[0] = max(0.01, probs[0] - 0.04 * np.tanh(latent))
            probs[1] = max(0.01, probs[1] + 0.04 * np.tanh(latent))
        probs = probs / probs.sum()
        persistence = float(np.clip(spec.get("persistence", 0.97), 0.0, 0.999))
        prev_state = categorical_state_cache.get(parameter_name)
        if prev_state in states and rng_local.random() < persistence:
            state = str(prev_state)
        else:
            state = state_from_probs(states, probs.tolist(), rng_local)
        categorical_state_cache[parameter_name] = state
        parameter_value = str(state)
        parameter_value_clean = str(state)

    else:
        p_emit = float(spec.get("base_prob", 0.01))
        if rng_local.random() < p_emit:
            state = str(rng_local.choice(spec.get("codes", ["CODE_000"])))
        else:
            state = "NONE"
        parameter_value = state
        parameter_value_clean = state

    return {
        "parameter_value": parameter_value,
        "parameter_value_clean": parameter_value_clean,
    }
