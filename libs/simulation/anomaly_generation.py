"""Generate multi-parameter anomaly descriptors and per-parameter modifiers.

This module produces anomaly directives (modifiers + event labels) that the
`flight_simulator` can apply when emitting telemetry. It does NOT emit
telemetry itself; it returns per-parameter modifier/event mappings.
"""
from __future__ import annotations

import math
from typing import Any, Dict

import numpy as np

from libs.common.event_types import TruthAnomalyType, EventType


def generate_anomalies_for_t(
    *,
    flight_setup: dict,
    rng_local: np.random.Generator,
    phase_name: str,
    t: int,
    parameter_names_in_order: list[str],
    parameter_behavior: dict[str, dict],
) -> dict[str, dict[str, Any]]:
    """Return mapping parameter_name -> label metadata for parameters affected by anomalies at time t.

    Simple policy: if the current phase is in burst_phases and a random draw
    triggers a burst, select a subset of primary_targets and assign each a
    shock drawn from a normal distribution scaled by the parameter noise.
    """
    out: dict[str, dict[str, Any]] = {}
    anomaly_plan = flight_setup.get("anomaly_plan", {}) or {}
    anomaly_rate_base = float(anomaly_plan.get("base_event_rate_per_min", 0.0) / 60.0)
    burst_multiplier = float(anomaly_plan.get("burst_multiplier", 1.0))

    # Quick check: only consider bursts in configured phases
    if phase_name not in anomaly_plan.get("burst_phases", []):
        return out

    # Probability to start a burst at this second
    p_burst = max(0.0, anomaly_rate_base * burst_multiplier)
    if rng_local.random() >= p_burst:
        return out

    primary = list(anomaly_plan.get("primary_targets", []))
    if not primary:
        # fallback: use a small random subset of all parameters
        primary = list(parameter_names_in_order)

    # choose how many sensors to affect: at least 1 up to min(len(primary), 4)
    k = int(max(1, min(len(primary), int(round(rng_local.exponential(1.0)) + 1))))
    chosen = list(rng_local.choice(primary, size=min(k, len(primary)), replace=False))

    for parameter_name in chosen:
        spec = parameter_behavior.get(parameter_name, {})
        noise_sigma = float(spec.get("noise_sigma", 1.0))
        # scale shock by noise; follow prior behavior (mean ~4*noise_sigma)
        shock = float(rng_local.normal(4.0 * max(noise_sigma, 0.1), 1.5 * max(noise_sigma, 0.1)))
        score = abs(shock)
        # Use 'add' modifier to nudge the observation generation
        modifier = {"add": shock}
        out[parameter_name] = {
            "modifier": modifier,
            "event_type_label": EventType.THRESHOLD,
            "anomaly_type_label": TruthAnomalyType.BURST_NUMERIC_SHIFT,
            "anomaly_score_label": score,
        }

    return out
