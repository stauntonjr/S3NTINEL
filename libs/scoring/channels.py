"""Canonical anomaly score channel names and shared helpers."""

from __future__ import annotations

from typing import Mapping

REGIME_DEVIATION_CHANNEL = "regime_deviation"
RECONSTRUCTION_ERROR_CHANNEL = "reconstruction_error"
EVENT_DISCORDANCE_CHANNEL = "event_discordance"
BOUND_VIOLATION_CHANNEL = "bound_violation"
RESPONSE_VIOLATION_CHANNEL = "response_violation"
STATE_VIOLATION_CHANNEL = "state_violation"
COHERENCE_BREAK_CHANNEL = "coherence_break"

SCORE_COMPONENT_NAMES: tuple[str, ...] = (
    REGIME_DEVIATION_CHANNEL,
    RECONSTRUCTION_ERROR_CHANNEL,
    EVENT_DISCORDANCE_CHANNEL,
    BOUND_VIOLATION_CHANNEL,
    RESPONSE_VIOLATION_CHANNEL,
    STATE_VIOLATION_CHANNEL,
    COHERENCE_BREAK_CHANNEL,
)


def zero_score_component_scores() -> dict[str, float]:
    return {name: 0.0 for name in SCORE_COMPONENT_NAMES}


def score_component_scores_with_updates(
    updates: Mapping[str, float | int | None] | None = None,
) -> dict[str, float]:
    component_scores = zero_score_component_scores()
    if not updates:
        return component_scores
    for name in SCORE_COMPONENT_NAMES:
        component_scores[name] = float(updates.get(name, 0.0) or 0.0)
    return component_scores


def dominant_score_component(scores: Mapping[str, float | int | None] | None) -> str:
    component_scores = score_component_scores_with_updates(scores)
    best_name = SCORE_COMPONENT_NAMES[0]
    best_score = float(component_scores[best_name])
    for name in SCORE_COMPONENT_NAMES[1:]:
        score = float(component_scores[name])
        if score > best_score:
            best_name = name
            best_score = score
    return best_name


def active_channel_mean(scores: Mapping[str, float | int | None] | None) -> float:
    component_scores = score_component_scores_with_updates(scores)
    active_values = [float(score) for score in component_scores.values() if float(score) > 0.0]
    if not active_values:
        return 0.0
    return float(sum(active_values) / len(active_values))


def score_component_map_expr(component_expr_by_name: Mapping[str, object]) -> object:
    from pyspark.sql import functions as F

    args: list[object] = []
    for name in SCORE_COMPONENT_NAMES:
        args.extend(
            [
                F.lit(name),
                F.coalesce(component_expr_by_name.get(name), F.lit(0.0)).cast("double"),
            ]
        )
    return F.create_map(*args)


def dominant_score_component_expr(component_expr_by_name: Mapping[str, object]) -> object:
    from pyspark.sql import functions as F

    first_name = SCORE_COMPONENT_NAMES[0]
    best_name = F.lit(first_name)
    best_score = F.coalesce(component_expr_by_name.get(first_name), F.lit(0.0)).cast("double")
    for name in SCORE_COMPONENT_NAMES[1:]:
        score = F.coalesce(component_expr_by_name.get(name), F.lit(0.0)).cast("double")
        best_name = F.when(score > best_score, F.lit(name)).otherwise(best_name)
        best_score = F.greatest(best_score, score)
    return best_name


def active_channel_mean_expr(component_expr_by_name: Mapping[str, object]) -> object:
    from pyspark.sql import functions as F

    total_score = F.lit(0.0)
    active_count = F.lit(0)
    for name in SCORE_COMPONENT_NAMES:
        score = F.coalesce(component_expr_by_name.get(name), F.lit(0.0)).cast("double")
        total_score = total_score + score
        active_count = active_count + F.when(score > F.lit(0.0), F.lit(1)).otherwise(F.lit(0))
    return F.when(active_count > F.lit(0), total_score / active_count.cast("double")).otherwise(F.lit(0.0))
