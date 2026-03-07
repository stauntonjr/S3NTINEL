from __future__ import annotations

import math

import numpy as np


def build_timeline(
    *,
    phase_sequence: list[str],
    phase_map: dict[str, dict],
    rng_local: np.random.Generator,
) -> list[dict]:
    timeline: list[dict] = []
    cursor = 0
    for phase_name in phase_sequence:
        spec = phase_map[phase_name]
        duration = int(max(20, round(spec["duration_sec"]["mean"] + rng_local.normal(0.0, spec["duration_sec"]["jitter"]))))
        timeline.append(
            {
                "phase_id": int(spec["phase_id"]),
                "phase_name": phase_name,
                "t_start": int(cursor),
                "t_end": int(cursor + duration),
                "noise_scale": float(spec["noise_scale"]),
                "corr_scale": float(spec["corr_scale"]),
                "modifiers": spec.get("modifiers", {}),
                "transition": spec.get("transition", {}),
            }
        )
        cursor += duration
    return timeline


def _blend_alpha(offset_into_transition: int, transition_sec: int, sharpness: float) -> float:
    if transition_sec <= 0:
        return 0.0
    x = (float(offset_into_transition) / float(transition_sec) - 0.5) * 8.0 * max(float(sharpness), 0.05)
    return float(1.0 / (1.0 + math.exp(-x)))


def phase_state_for_t(timeline: list[dict], t: int) -> tuple[dict, dict, float]:
    for index, segment in enumerate(timeline):
        if segment["t_start"] <= t < segment["t_end"]:
            if index >= len(timeline) - 1:
                return segment, segment, 0.0
            transition = segment.get("transition", {}) or {}
            transition_sec = int(max(0, transition.get("transition_sec", 0)))
            if transition_sec <= 0:
                return segment, segment, 0.0

            seg_len = int(segment["t_end"] - segment["t_start"])
            transition_start = max(0, seg_len - transition_sec)
            t_into_seg = int(t - segment["t_start"])
            if t_into_seg < transition_start:
                return segment, segment, 0.0

            alpha = _blend_alpha(
                offset_into_transition=t_into_seg - transition_start,
                transition_sec=transition_sec,
                sharpness=float(transition.get("sharpness", 1.0)),
            )
            return segment, timeline[index + 1], float(np.clip(alpha, 0.0, 1.0))
    return timeline[-1], timeline[-1], 0.0


def blend_modifiers(primary_modifier: dict, secondary_modifier: dict, alpha: float) -> dict:
    if alpha <= 0.0 or not secondary_modifier:
        return dict(primary_modifier)

    merged: dict = {}
    for key in set(primary_modifier) | set(secondary_modifier):
        a = primary_modifier.get(key)
        b = secondary_modifier.get(key)

        if key == "state_weights" and isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)) and len(a) == len(b):
            merged[key] = [(1.0 - alpha) * float(x) + alpha * float(y) for x, y in zip(a, b)]
            continue

        if isinstance(a, (int, float)) and isinstance(b, (int, float)):
            merged[key] = (1.0 - alpha) * float(a) + alpha * float(b)
            continue

        if a is None:
            merged[key] = b
        elif b is None:
            merged[key] = a
        else:
            merged[key] = b if alpha >= 0.5 else a

    return merged


def state_from_probs(states: list[str], probs: list[float], rng_local: np.random.Generator) -> str:
    probs_array = np.array(probs, dtype=float)
    probs_array = probs_array / probs_array.sum()
    return str(rng_local.choice(states, p=probs_array))
