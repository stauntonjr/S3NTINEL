"""Shared primitive vocabulary and family scoring for behavior semantics."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Mapping

import pandas as pd

from libs.behavior.utils import clip01, lag1_autocorrelation, numeric_series

if TYPE_CHECKING:
    from pyspark.sql import Column

PERSISTENT_SIGNED_RUN = "persistent_signed_run"
RUN_REINFORCEMENT = "run_reinforcement"
REVERSAL = "reversal"
CENTER_OCCUPANCY = "center_occupancy"
EXCURSION = "excursion"
EXCURSION_RETURN = "excursion_return"
BOUND_OCCUPANCY = "bound_occupancy"
SATURATION = "saturation"
MONOTONE_ACCUMULATION = "monotone_accumulation"
RESET_DROP = "reset_drop"
STATE_TRANSITION = "state_transition"
STATE_DWELL = "state_dwell"
STATE_CHATTER = "state_chatter"
OSCILLATION = "oscillation"
TRACKING_ERROR = "tracking_error"
TRACKING_RECOVERY = "tracking_recovery"
LAGGED_RESPONSE = "lagged_response"

NUMERIC_PRIMITIVE_FEATURE_COLUMNS = (
    "persistent_run_strength_profiled",
    "run_reinforcement_score_profiled",
    "reversal_rate_profiled",
    "sign_flip_rate_profiled",
    "center_occupancy_profiled",
    "excursion_rate_profiled",
    "excursion_return_ratio_profiled",
    "bound_occupancy_profiled",
    "saturation_rate_profiled",
    "monotone_accumulation_score_profiled",
    "reset_drop_rate_profiled",
    "oscillation_score_profiled",
    "tracking_error_score_profiled",
    "tracking_recovery_score_profiled",
    "lagged_response_score_profiled",
)

DISCRETE_PRIMITIVE_FEATURE_COLUMNS = (
    "transition_rate_profiled",
    "mean_dwell_profiled",
    "state_chatter_rate_profiled",
    "dominant_state_ratio_profiled",
)

PRIMITIVE_PROFILE_COLUMNS = (
    "parameter_name",
    "parameter_datatype_profiled",
    "sample_count",
    "profile_window_start_utc",
    "profile_window_end_utc",
    *NUMERIC_PRIMITIVE_FEATURE_COLUMNS,
    *DISCRETE_PRIMITIVE_FEATURE_COLUMNS,
)


@dataclass(frozen=True)
class BehaviorPrimitiveSpec:
    name: str
    description: str
    supported_datatypes: tuple[str, ...] = ()


@dataclass(frozen=True)
class BehaviorFamilyDefinition:
    family: str
    defining_primitives: tuple[str, ...]
    positive_weights: Mapping[str, float]
    negative_weights: Mapping[str, float] = field(default_factory=dict)
    supported_datatypes: tuple[str, ...] = ()
    expected_traits: tuple[str, ...] = ()
    allowed_fault_families: tuple[str, ...] = ()


PRIMITIVE_SPECS: tuple[BehaviorPrimitiveSpec, ...] = (
    BehaviorPrimitiveSpec(PERSISTENT_SIGNED_RUN, "Sustained same-sign normalized motion.", ("numeric", "constant")),
    BehaviorPrimitiveSpec(RUN_REINFORCEMENT, "Strengthening same-sign motion within a run.", ("numeric", "constant")),
    BehaviorPrimitiveSpec(REVERSAL, "Sign reversals in first-difference motion.", ("numeric", "constant")),
    BehaviorPrimitiveSpec(CENTER_OCCUPANCY, "Time spent near the normalized center band.", ("numeric", "constant")),
    BehaviorPrimitiveSpec(EXCURSION, "Time spent away from the center band.", ("numeric", "constant")),
    BehaviorPrimitiveSpec(EXCURSION_RETURN, "Rate of returning from an excursion back toward center.", ("numeric", "constant")),
    BehaviorPrimitiveSpec(BOUND_OCCUPANCY, "Time spent within normalized soft bounds.", ("numeric", "constant")),
    BehaviorPrimitiveSpec(SATURATION, "Time spent near normalized hard bounds.", ("numeric", "constant")),
    BehaviorPrimitiveSpec(MONOTONE_ACCUMULATION, "Same-sign net accumulation over time.", ("numeric", "constant")),
    BehaviorPrimitiveSpec(RESET_DROP, "Large counter-run resets against the dominant direction.", ("numeric", "constant")),
    BehaviorPrimitiveSpec(STATE_TRANSITION, "Abrupt low-cardinality state transitions.", ("binary", "categorical", "high_cardinality")),
    BehaviorPrimitiveSpec(STATE_DWELL, "Persistence within a state before transition.", ("binary", "categorical", "high_cardinality")),
    BehaviorPrimitiveSpec(STATE_CHATTER, "Rapid alternation between neighboring states.", ("binary", "categorical", "high_cardinality")),
    BehaviorPrimitiveSpec(OSCILLATION, "Alternating bounded numeric motion.", ("numeric", "constant")),
    BehaviorPrimitiveSpec(TRACKING_ERROR, "Bounded movement with low implied tracking error.", ("numeric", "constant")),
    BehaviorPrimitiveSpec(TRACKING_RECOVERY, "Return toward target band after movement.", ("numeric", "constant")),
    BehaviorPrimitiveSpec(LAGGED_RESPONSE, "Persistent smooth lagged response.", ("numeric", "constant")),
)

BEHAVIOR_FAMILY_DEFINITIONS: dict[str, BehaviorFamilyDefinition] = {
    "regulated": BehaviorFamilyDefinition(
        family="regulated",
        defining_primitives=(CENTER_OCCUPANCY, BOUND_OCCUPANCY, EXCURSION_RETURN, SATURATION),
        positive_weights={
            "center_occupancy_profiled": 0.28,
            "bound_occupancy_profiled": 0.20,
            "excursion_return_ratio_profiled": 0.26,
            "tracking_recovery_score_profiled": 0.10,
            "reversal_rate_profiled": 0.04,
        },
        negative_weights={
            "monotone_accumulation_score_profiled": 0.28,
            "saturation_rate_profiled": 0.12,
            "excursion_rate_profiled": 0.18,
            "lagged_response_score_profiled": 0.08,
        },
        supported_datatypes=("numeric", "constant"),
        expected_traits=("bounded", "central_band_occupancy", "mean_reverting"),
        allowed_fault_families=("offset", "saturation", "tracking_degradation", "oscillation"),
    ),
    "tracking": BehaviorFamilyDefinition(
        family="tracking",
        defining_primitives=(PERSISTENT_SIGNED_RUN, EXCURSION, TRACKING_ERROR, TRACKING_RECOVERY, LAGGED_RESPONSE),
        positive_weights={
            "persistent_run_strength_profiled": 0.24,
            "excursion_rate_profiled": 0.20,
            "tracking_error_score_profiled": 0.24,
            "tracking_recovery_score_profiled": 0.08,
            "lagged_response_score_profiled": 0.34,
        },
        negative_weights={
            "center_occupancy_profiled": 0.08,
            "monotone_accumulation_score_profiled": 0.12,
            "saturation_rate_profiled": 0.06,
        },
        supported_datatypes=("numeric", "constant"),
        expected_traits=("target_following", "bounded_error", "recovery_after_target_change"),
        allowed_fault_families=("tracking_degradation", "saturation", "offset", "oscillation"),
    ),
    "inertial": BehaviorFamilyDefinition(
        family="inertial",
        defining_primitives=(PERSISTENT_SIGNED_RUN, REVERSAL, LAGGED_RESPONSE),
        positive_weights={
            "persistent_run_strength_profiled": 0.20,
            "lagged_response_score_profiled": 0.52,
            "bound_occupancy_profiled": 0.02,
            "run_reinforcement_score_profiled": 0.12,
            "tracking_error_score_profiled": 0.02,
            "excursion_rate_profiled": 0.16,
        },
        negative_weights={
            "reversal_rate_profiled": 0.08,
            "tracking_recovery_score_profiled": 0.10,
            "monotone_accumulation_score_profiled": 0.10,
            "center_occupancy_profiled": 0.06,
            "oscillation_score_profiled": 0.08,
        },
        supported_datatypes=("numeric", "constant"),
        expected_traits=("persistent", "smooth", "lagged_response"),
        allowed_fault_families=("timing_lag", "increased_time_constant", "stuck_value", "ramp_distortion"),
    ),
    "accumulative": BehaviorFamilyDefinition(
        family="accumulative",
        defining_primitives=(PERSISTENT_SIGNED_RUN, MONOTONE_ACCUMULATION, RESET_DROP),
        positive_weights={
            "monotone_accumulation_score_profiled": 0.62,
            "persistent_run_strength_profiled": 0.18,
            "reset_drop_rate_profiled": 0.16,
            "run_reinforcement_score_profiled": 0.08,
            "lagged_response_score_profiled": 0.06,
        },
        negative_weights={
            "reversal_rate_profiled": 0.12,
            "excursion_return_ratio_profiled": 0.24,
            "center_occupancy_profiled": 0.14,
            "tracking_recovery_score_profiled": 0.08,
        },
        supported_datatypes=("numeric", "constant"),
        expected_traits=("persistent", "monotone", "integrative"),
        allowed_fault_families=("reset_drop", "leak_rate", "drift", "bias"),
    ),
    "discrete_state": BehaviorFamilyDefinition(
        family="discrete_state",
        defining_primitives=(STATE_TRANSITION, STATE_DWELL, STATE_CHATTER),
        positive_weights={
            "discrete_low_cardinality_score_profiled": 0.25,
            "discrete_low_transition_score_profiled": 0.20,
            "discrete_dwell_score_profiled": 0.25,
            "dominant_state_ratio_profiled": 0.15,
            "transition_balance_score_profiled": 0.15,
        },
        negative_weights={"state_chatter_rate_profiled": 0.20},
        supported_datatypes=("binary", "categorical", "high_cardinality"),
        expected_traits=("finite_alphabet", "state_dwell", "abrupt_transitions"),
        allowed_fault_families=("illegal_transition", "dwell_violation", "state_chatter", "stuck_state"),
    ),
}

PROFILED_BEHAVIOR_FAMILIES: tuple[str, ...] = ("regulated", "tracking", "inertial", "accumulative", "discrete_state", "mixed_unknown")
SIMULATED_BEHAVIOR_FAMILIES: tuple[str, ...] = ("regulated", "tracking", "inertial", "accumulative", "discrete_state")
NUMERIC_BEHAVIOR_DATATYPES: tuple[str, ...] = ("numeric", "constant")
DISCRETE_BEHAVIOR_DATATYPES: tuple[str, ...] = ("binary", "categorical", "high_cardinality")
MIXED_UNKNOWN_LOW_SCORE_THRESHOLD = 0.38
MIXED_UNKNOWN_AMBIGUOUS_SCORE_THRESHOLD = 0.55
MIXED_UNKNOWN_AMBIGUOUS_MARGIN_THRESHOLD = 0.03
MIXED_UNKNOWN_BASE_SCORE = 0.85
MIXED_UNKNOWN_BASE_MARGIN = 0.18
MIXED_UNKNOWN_LOW_SCORE_FLOOR = 0.55
MIXED_UNKNOWN_AMBIGUOUS_FLOOR = 0.52


@dataclass(frozen=True)
class BehaviorChoiceColumns:
    family: "Column"
    confidence: "Column"
    mixed_unknown_score: "Column"


@dataclass(frozen=True)
class BehaviorChoiceThresholds:
    low_score_threshold: float = 0.38
    ambiguous_score_threshold: float = 0.55
    ambiguous_margin_threshold: float = 0.03
    base_score: float = 0.85
    base_margin: float = 0.18
    low_score_floor: float = 0.55
    ambiguous_floor: float = 0.52


def _definition_score_from_values(
    definition: BehaviorFamilyDefinition,
    *,
    value_for: Callable[[str], float],
) -> float:
    positive = sum(float(weight) * float(value_for(name)) for name, weight in definition.positive_weights.items())
    negative = sum(float(weight) * float(value_for(name)) for name, weight in definition.negative_weights.items())
    return clip01(positive - negative)


def _mixed_unknown_score(
    top_score: float,
    second_score: float,
    *,
    thresholds: BehaviorChoiceThresholds = BehaviorChoiceThresholds(),
) -> float:
    margin = max(float(top_score) - float(second_score), 0.0)
    mixed_unknown = clip01(max(float(thresholds.base_score) - float(top_score), float(thresholds.base_margin) - margin))
    if float(top_score) < float(thresholds.low_score_threshold):
        return max(mixed_unknown, float(thresholds.low_score_floor))
    if float(top_score) < float(thresholds.ambiguous_score_threshold) and margin < float(thresholds.ambiguous_margin_threshold):
        return max(mixed_unknown, float(thresholds.ambiguous_floor))
    return mixed_unknown


def _should_use_mixed_unknown(
    top_score: float,
    second_score: float,
    *,
    thresholds: BehaviorChoiceThresholds = BehaviorChoiceThresholds(),
) -> bool:
    margin = max(float(top_score) - float(second_score), 0.0)
    return (float(top_score) < float(thresholds.low_score_threshold)) or (
        float(top_score) < float(thresholds.ambiguous_score_threshold) and margin < float(thresholds.ambiguous_margin_threshold)
    )


def build_behavior_family_score_columns(
    *,
    parameter_datatype_column: "Column",
    value_for: Callable[[str], "Column"],
) -> dict[str, "Column"]:
    from pyspark.sql import functions as F

    family_scores: dict[str, "Column"] = {}
    numeric_mask = parameter_datatype_column.isin(*NUMERIC_BEHAVIOR_DATATYPES)
    discrete_mask = parameter_datatype_column.isin(*DISCRETE_BEHAVIOR_DATATYPES)
    for family_name, definition in BEHAVIOR_FAMILY_DEFINITIONS.items():
        positive = F.lit(0.0)
        for column_name, weight in definition.positive_weights.items():
            positive = positive + (F.lit(float(weight)) * value_for(column_name))
        negative = F.lit(0.0)
        for column_name, weight in definition.negative_weights.items():
            negative = negative + (F.lit(float(weight)) * value_for(column_name))
        score = F.least(F.lit(1.0), F.greatest(F.lit(0.0), positive - negative))
        supported_types = tuple(definition.supported_datatypes)
        if supported_types == DISCRETE_BEHAVIOR_DATATYPES:
            family_scores[family_name] = F.when(discrete_mask, score).otherwise(F.lit(0.0))
        elif supported_types == NUMERIC_BEHAVIOR_DATATYPES:
            family_scores[family_name] = F.when(numeric_mask, score).otherwise(F.lit(0.0))
        else:
            family_scores[family_name] = score
    return family_scores


def build_behavior_choice_columns(
    family_scores: Mapping[str, "Column"],
    *,
    thresholds: BehaviorChoiceThresholds = BehaviorChoiceThresholds(),
) -> BehaviorChoiceColumns:
    from pyspark.sql import functions as F

    ranked_scores = F.array_sort(
        F.array(
            *[
                F.struct(score.alias("score"), F.lit(family_name).alias("family"))
                for family_name, score in family_scores.items()
            ]
        ),
        lambda left, right: (
            F.when(left["score"] < right["score"], F.lit(1))
            .when(left["score"] > right["score"], F.lit(-1))
            .when(left["family"] < right["family"], F.lit(1))
            .when(left["family"] > right["family"], F.lit(-1))
            .otherwise(F.lit(0))
        ),
    )
    top_score = ranked_scores.getItem(0)["score"]
    top_family = ranked_scores.getItem(0)["family"]
    second_score = ranked_scores.getItem(1)["score"]
    margin = F.greatest(top_score - second_score, F.lit(0.0))
    mixed_unknown_score = F.least(
        F.lit(1.0),
        F.greatest(
            F.lit(0.0),
            F.greatest(F.lit(float(thresholds.base_score)) - top_score, F.lit(float(thresholds.base_margin)) - margin),
        ),
    )
    mixed_unknown_score = (
        F.when(
            top_score < F.lit(float(thresholds.low_score_threshold)),
            F.greatest(mixed_unknown_score, F.lit(float(thresholds.low_score_floor))),
        )
        .when(
            (top_score < F.lit(float(thresholds.ambiguous_score_threshold)))
            & (margin < F.lit(float(thresholds.ambiguous_margin_threshold))),
            F.greatest(mixed_unknown_score, F.lit(float(thresholds.ambiguous_floor))),
        )
        .otherwise(mixed_unknown_score)
    )
    use_mixed_unknown = (top_score < F.lit(float(thresholds.low_score_threshold))) | (
        (top_score < F.lit(float(thresholds.ambiguous_score_threshold)))
        & (margin < F.lit(float(thresholds.ambiguous_margin_threshold)))
    )
    effective_top_family = F.when(use_mixed_unknown, F.lit("mixed_unknown")).otherwise(top_family)
    effective_top_score = F.when(use_mixed_unknown, F.greatest(mixed_unknown_score, top_score)).otherwise(top_score)
    confidence = F.greatest(F.lit(0.0), F.least(F.lit(1.0), effective_top_score))
    confidence = F.when(effective_top_family == F.lit("mixed_unknown"), F.greatest(confidence, mixed_unknown_score)).otherwise(confidence)
    return BehaviorChoiceColumns(
        family=effective_top_family,
        confidence=confidence,
        mixed_unknown_score=mixed_unknown_score,
    )


def _bounded_series_from_pdf(telemetry_pdf: pd.DataFrame) -> pd.Series:
    return numeric_series(telemetry_pdf).astype("float64")


def build_numeric_primitive_evidence(
    *,
    parameter_name: str,
    telemetry_pdf: pd.DataFrame,
) -> dict[str, float | str | None]:
    series = _bounded_series_from_pdf(telemetry_pdf)
    if len(series) < 3:
        return {
            "sample_count_profiled": float(len(series)),
            **{name: None for name in NUMERIC_PRIMITIVE_FEATURE_COLUMNS},
        }
    q25 = float(series.quantile(0.25))
    q50 = float(series.quantile(0.5))
    q75 = float(series.quantile(0.75))
    iqr = max(q75 - q25, 1e-6)
    scaled = (series - q50) / iqr
    diff = scaled.diff().dropna()
    if diff.empty:
        return {
            "sample_count_profiled": float(len(series)),
            "persistent_run_strength_profiled": 1.0,
            "run_reinforcement_score_profiled": 0.0,
            "reversal_rate_profiled": 0.0,
            "center_occupancy_profiled": 1.0,
            "excursion_rate_profiled": 0.0,
            "excursion_return_ratio_profiled": 0.0,
            "bound_occupancy_profiled": 1.0,
            "saturation_rate_profiled": 0.0,
            "monotone_accumulation_score_profiled": 0.0,
            "reset_drop_rate_profiled": 0.0,
            "oscillation_score_profiled": 0.0,
            "tracking_error_score_profiled": 1.0,
            "tracking_recovery_score_profiled": 0.0,
            "lagged_response_score_profiled": 1.0,
        }

    sign_series = diff.apply(lambda value: 1 if value > 0 else (-1 if value < 0 else 0))
    significant_sign = sign_series.where(diff.abs() >= 0.05, 0)
    run_lengths: list[int] = []
    run_peaks: list[float] = []
    current_sign = 0
    current_length = 0
    current_peak = 0.0
    reinforcement_count = 0
    last_peak = 0.0
    for sign, abs_delta in zip(significant_sign.tolist(), diff.abs().tolist(), strict=False):
        if sign == 0:
            if current_length > 0:
                run_lengths.append(current_length)
                run_peaks.append(current_peak)
            current_sign = 0
            current_length = 0
            current_peak = 0.0
            last_peak = 0.0
            continue
        if sign != current_sign:
            if current_length > 0:
                run_lengths.append(current_length)
                run_peaks.append(current_peak)
            current_sign = sign
            current_length = 1
            current_peak = abs_delta
            last_peak = abs_delta
            continue
        current_length += 1
        current_peak = max(current_peak, abs_delta)
        if current_length >= 2 and last_peak > 0.0 and current_peak >= (last_peak * 1.5):
            reinforcement_count += 1
            last_peak = current_peak
    if current_length > 0:
        run_lengths.append(current_length)
        run_peaks.append(current_peak)

    diff_count = float(len(diff))
    sign_flips = float((((diff > 0) != (diff.shift(1) > 0)).dropna()).mean()) if len(diff) > 1 else 0.0
    gross_change = float(diff.abs().sum())
    net_change_ratio = float(abs(series.iloc[-1] - series.iloc[0]) / max(series.diff().abs().sum(), 1e-6))
    dominant_sign_ratio = max(float((diff > 0).mean()), float((diff < 0).mean()))
    dominant_positive = float((diff > 0).mean()) >= float((diff < 0).mean())
    reset_drop_rate = float(
        (((diff < -1.5) if dominant_positive else (diff > 1.5)).mean())
    )
    excursion_prev = scaled.shift(1).abs() > 1.0
    excursion_curr = scaled.abs() <= 1.0
    excursion_return = float(((excursion_prev & excursion_curr).mean()) if len(scaled) > 1 else 0.0)
    lag1 = lag1_autocorrelation(series)
    lag1_unit = clip01(((float(lag1) if lag1 is not None else 0.0) + 1.0) / 2.0)
    level_energy = float((scaled**2).mean()) if len(scaled) else 1.0
    diff_energy_ratio = float((diff.pow(2).mean()) / max(level_energy, 1e-6))
    smoothness = clip01(1.0 - min(diff_energy_ratio, 1.0))
    persistent_run_strength = clip01((max(run_lengths) if run_lengths else 0.0) / max(diff_count, 1.0))
    run_reinforcement = clip01(float(reinforcement_count) / max(float(len(run_lengths)), 1.0))
    center_occupancy = float((scaled.abs() <= 1.0).mean())
    excursion_rate = float((scaled.abs() > 1.0).mean())
    bound_occupancy = float((scaled.abs() <= 2.5).mean())
    saturation_rate = float((scaled.abs() >= 2.0).mean())
    monotone_accumulation = clip01((0.5 * dominant_sign_ratio) + (0.35 * net_change_ratio) + (0.15 * (1.0 - sign_flips)))
    oscillation_score = clip01((0.55 * sign_flips) + (0.25 * bound_occupancy) + (0.20 * (1.0 - net_change_ratio)))
    tracking_error_score = clip01(
        (0.10 * bound_occupancy)
        + (0.05 * (1.0 - saturation_rate))
        + (0.35 * min(excursion_rate * 2.0, 1.0))
        + (0.20 * persistent_run_strength)
        + (0.30 * (1.0 - center_occupancy))
    )
    tracking_recovery = clip01((0.65 * excursion_return) + (0.35 * excursion_rate))
    lagged_response = clip01(
        (0.35 * lag1_unit)
        + (0.25 * smoothness)
        + (0.20 * min(excursion_rate * 1.5, 1.0))
        + (0.10 * persistent_run_strength)
        + (0.10 * (1.0 - center_occupancy))
    )

    return {
        "sample_count_profiled": float(len(series)),
        "persistent_run_strength_profiled": persistent_run_strength,
        "run_reinforcement_score_profiled": run_reinforcement,
        "reversal_rate_profiled": sign_flips,
        "sign_flip_rate_profiled": sign_flips,
        "center_occupancy_profiled": center_occupancy,
        "excursion_rate_profiled": excursion_rate,
        "excursion_return_ratio_profiled": excursion_return,
        "bound_occupancy_profiled": bound_occupancy,
        "saturation_rate_profiled": saturation_rate,
        "monotone_accumulation_score_profiled": monotone_accumulation,
        "reset_drop_rate_profiled": reset_drop_rate,
        "oscillation_score_profiled": oscillation_score,
        "tracking_error_score_profiled": tracking_error_score,
        "tracking_recovery_score_profiled": tracking_recovery,
        "lagged_response_score_profiled": lagged_response,
    }


def build_discrete_primitive_evidence(
    *,
    parameter_name: str,
    telemetry_pdf: pd.DataFrame,
) -> dict[str, float | str | None]:
    values = telemetry_pdf.get("parameter_value", pd.Series(dtype="object")).fillna("").astype(str)
    nonempty = values[values != ""]
    if len(values) < 2:
        return {
            "sample_count_profiled": float(len(values)),
            "transition_rate_profiled": None,
            "mean_dwell_profiled": None,
            "state_chatter_rate_profiled": None,
            "dominant_state_ratio_profiled": None,
            "discrete_low_cardinality_score_profiled": None,
            "discrete_low_transition_score_profiled": None,
            "discrete_dwell_score_profiled": None,
            "transition_balance_score_profiled": None,
        }
    transition_mask = values != values.shift(1)
    transition_count = max(int(transition_mask.sum()) - 1, 0)
    transition_rate = float(transition_count / max(len(values) - 1, 1))
    run_lengths: list[int] = []
    current_run = 0
    previous = None
    for value in values.tolist():
        if value == previous:
            current_run += 1
        else:
            if current_run:
                run_lengths.append(current_run)
            current_run = 1
            previous = value
    if current_run:
        run_lengths.append(current_run)
    mean_dwell = float(sum(run_lengths) / max(len(run_lengths), 1))
    dominant_state_ratio = float(nonempty.value_counts(normalize=True, dropna=False).iloc[0]) if len(nonempty) else 0.0
    chatter_count = 0
    for idx in range(2, len(values)):
        if values.iloc[idx] == values.iloc[idx - 2] and values.iloc[idx - 1] != values.iloc[idx]:
            chatter_count += 1
    distinct_count = float(nonempty.nunique())
    low_cardinality = clip01(1.0 - min(max(distinct_count - 1.0, 0.0), 9.0) / 9.0)
    low_transition = clip01(1.0 - transition_rate)
    dwell_score = clip01(min(mean_dwell, 10.0) / 10.0)
    transition_balance = clip01(1.0 - min(abs(transition_rate - 0.18) / 0.18, 1.0))
    return {
        "sample_count_profiled": float(len(values)),
        "transition_rate_profiled": transition_rate,
        "mean_dwell_profiled": mean_dwell,
        "state_chatter_rate_profiled": float(chatter_count / max(len(values) - 2, 1)),
        "dominant_state_ratio_profiled": dominant_state_ratio,
        "discrete_low_cardinality_score_profiled": low_cardinality,
        "discrete_low_transition_score_profiled": low_transition,
        "discrete_dwell_score_profiled": dwell_score,
        "transition_balance_score_profiled": transition_balance,
    }


def score_behavior_families_from_primitives(
    *,
    primitive_evidence: Mapping[str, float | str | None],
    parameter_datatype_profiled: str,
) -> dict[str, float]:
    datatype = str(parameter_datatype_profiled or "")
    scores: dict[str, float] = {}
    for family in PROFILED_BEHAVIOR_FAMILIES:
        if family == "mixed_unknown":
            continue
        definition = BEHAVIOR_FAMILY_DEFINITIONS[family]
        if definition.supported_datatypes and datatype not in definition.supported_datatypes:
            scores[family] = 0.0
            continue
        scores[family] = _definition_score_from_values(
            definition,
            value_for=lambda name: float(primitive_evidence.get(name) or 0.0),
        )
    ranked = sorted(scores.items(), key=lambda item: (item[1], item[0]), reverse=True)
    top_score = ranked[0][1] if ranked else 0.0
    second_score = ranked[1][1] if len(ranked) > 1 else 0.0
    scores["mixed_unknown"] = _mixed_unknown_score(top_score, second_score)
    return scores


def choose_behavior_family(scores: Mapping[str, float]) -> tuple[str, float]:
    ranked = sorted(((str(name), float(score)) for name, score in scores.items()), key=lambda item: (item[1], item[0]), reverse=True)
    top_name, top_score = ranked[0] if ranked else ("mixed_unknown", 0.0)
    second_score = ranked[1][1] if len(ranked) > 1 else 0.0
    if top_name != "mixed_unknown" and _should_use_mixed_unknown(top_score, second_score):
        top_name = "mixed_unknown"
        top_score = max(float(scores.get("mixed_unknown", 0.0)), top_score)
    confidence = clip01(top_score if top_name != "mixed_unknown" else max(top_score, float(scores.get("mixed_unknown", 0.0))))
    if top_name == "mixed_unknown":
        confidence = max(confidence, float(scores.get("mixed_unknown", 0.0)))
    return top_name, confidence
