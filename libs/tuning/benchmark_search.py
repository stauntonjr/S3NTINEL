"""Stage-local benchmark search spaces and variant generation."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import product
import random
from typing import Any, Literal

from libs.tuning.benchmark_variants import BenchmarkVariant


@dataclass(frozen=True)
class BenchmarkSearchDimension:
    name: str
    values: tuple[Any, ...]
    kind: Literal["arg", "env"] = "arg"


@dataclass(frozen=True)
class BenchmarkSearchSpec:
    stage: str
    mode: str
    description: str
    dimensions: tuple[BenchmarkSearchDimension, ...]


KNOWN_SEARCH_STRATEGIES = ("grid", "random")

SEARCH_SPEC_BY_STAGE = {
    "profile": BenchmarkSearchSpec(
        stage="profile",
        mode="profile",
        description="Early-stage profiling search over behavior-primitive thresholds and mixed-unknown gating.",
        dimensions=(
            BenchmarkSearchDimension(
                name="profile_behavior_significant_diff_threshold",
                values=(0.03, 0.05, 0.08),
            ),
            BenchmarkSearchDimension(
                name="profile_behavior_center_band_width",
                values=(0.8, 1.0, 1.2),
            ),
            BenchmarkSearchDimension(
                name="profile_behavior_soft_bound_width",
                values=(2.0, 2.5, 3.0),
            ),
            BenchmarkSearchDimension(
                name="profile_behavior_hard_bound_width",
                values=(1.6, 2.0, 2.4),
            ),
            BenchmarkSearchDimension(
                name="profile_behavior_mixed_unknown_low_score_threshold",
                values=(0.30, 0.38, 0.46),
            ),
            BenchmarkSearchDimension(
                name="profile_behavior_mixed_unknown_ambiguous_margin_threshold",
                values=(0.02, 0.03, 0.05),
            ),
        ),
    ),
    "event": BenchmarkSearchSpec(
        stage="event",
        mode="event",
        description="Event-stage search over generic morphology-policy gains and a narrow precision-safe detector neighborhood.",
        dimensions=(
            BenchmarkSearchDimension(
                name="slope_threshold_scale",
                values=(0.3, 0.35, 0.4),
            ),
            BenchmarkSearchDimension(
                name="slope_abs_threshold",
                values=(1.5, 2.0, 2.5),
            ),
            BenchmarkSearchDimension(
                name="slope_min_persistence_samples",
                values=(2, 3),
            ),
            BenchmarkSearchDimension(
                name="slope_reemit_ratio",
                values=(1.5, 1.75),
            ),
            BenchmarkSearchDimension(
                name="event_warmup_points",
                values=(3, 4, 5),
            ),
            BenchmarkSearchDimension(
                name="event_low_scale_responsiveness",
                values=(0.9, 1.0, 1.1),
            ),
            BenchmarkSearchDimension(
                name="event_repeatability_aggressiveness",
                values=(0.9, 1.0, 1.1),
            ),
            BenchmarkSearchDimension(
                name="event_drift_conservatism",
                values=(0.9, 1.0, 1.1),
            ),
            BenchmarkSearchDimension(
                name="event_chatter_suppression",
                values=(1.0, 1.15, 1.3),
            ),
        ),
    ),
    "windowing": BenchmarkSearchSpec(
        stage="windowing",
        mode="structural",
        description="Windowing-stage search over a narrow neighborhood around the promoted 5s/10-event adaptive policy.",
        dimensions=(
            BenchmarkSearchDimension(
                name="window_max_ms",
                values=(5000, 7500, 10000),
            ),
            BenchmarkSearchDimension(
                name="window_event_threshold",
                values=(8, 10, 12),
            ),
            BenchmarkSearchDimension(
                name="window_min_ms",
                values=(25, 50),
            ),
            BenchmarkSearchDimension(
                name="window_inactivity_timeout_ms",
                values=(0, 500),
            ),
        ),
    ),
    "structure": BenchmarkSearchSpec(
        stage="structure",
        mode="structural",
        description="Structure-stage search over backbone, graph, and hierarchy controls.",
        dimensions=(
            BenchmarkSearchDimension(
                name="backbone_parameter_count",
                values=(6, 12),
            ),
            BenchmarkSearchDimension(
                name="backbone_ridge_lambda",
                values=(0.5, 2.0),
            ),
            BenchmarkSearchDimension(
                name="backbone_event_prior_alpha",
                values=(0.2, 0.6),
            ),
            BenchmarkSearchDimension(
                name="S3NTINEL_V2_MIN_ABS_PARTIAL_CORR",
                values=("0.03", "0.08"),
                kind="env",
            ),
            BenchmarkSearchDimension(
                name="S3NTINEL_V2_GRAPH_MIN_FUSED_EDGE_WEIGHT",
                values=("0.03", "0.08"),
                kind="env",
            ),
            BenchmarkSearchDimension(
                name="S3NTINEL_V2_HIERARCHY_TOP_K_PER_SENSOR",
                values=("2", "4"),
                kind="env",
            ),
        ),
    ),
    "phase": BenchmarkSearchSpec(
        stage="phase",
        mode="full",
        description="Phase-stage search over phase count, detection feature budgets, and temporal smoothing controls.",
        dimensions=(
            BenchmarkSearchDimension(
                name="phase_count",
                values=(3, 4, 5),
            ),
            BenchmarkSearchDimension(
                name="S3NTINEL_PHASE_DETECT_SENSOR_COUNT",
                values=("6", "10"),
                kind="env",
            ),
            BenchmarkSearchDimension(
                name="S3NTINEL_PHASE_DETECT_EVENT_TYPE_COUNT",
                values=("4", "8"),
                kind="env",
            ),
            BenchmarkSearchDimension(
                name="S3NTINEL_PHASE_DETECT_CATEGORICAL_STATE_COUNT",
                values=("4", "8"),
                kind="env",
            ),
            BenchmarkSearchDimension(
                name="S3NTINEL_PHASE_STABLE_DRIFT_QUANTILE",
                values=("0.25", "0.45"),
                kind="env",
            ),
            BenchmarkSearchDimension(
                name="S3NTINEL_PHASE_SMOOTHING_RADIUS",
                values=("1", "3"),
                kind="env",
            ),
            BenchmarkSearchDimension(
                name="S3NTINEL_PHASE_TRANSITION_PENALTY",
                values=("1.0", "2.0"),
                kind="env",
            ),
            BenchmarkSearchDimension(
                name="S3NTINEL_PHASE_MIN_DWELL_WINDOWS",
                values=("4", "12"),
                kind="env",
            ),
        ),
    ),
    "anomaly": BenchmarkSearchSpec(
        stage="anomaly",
        mode="full",
        description="Late-stage anomaly search over score calibration warmth and attribution breadth controls.",
        dimensions=(
            BenchmarkSearchDimension(
                name="min_warm",
                values=(1, 4, 8),
            ),
            BenchmarkSearchDimension(
                name="S3NTINEL_MAX_BRIDGE_REFERENCE_ROWS",
                values=("5000", "20000"),
                kind="env",
            ),
            BenchmarkSearchDimension(
                name="S3NTINEL_SUBSYSTEM_TOP_SENSORS_K",
                values=("3", "7"),
                kind="env",
            ),
        ),
    ),
}
KNOWN_SEARCH_STAGES = tuple(SEARCH_SPEC_BY_STAGE)


def _normalize_variant_value(value: Any) -> str:
    return str(value).replace("-", "neg_").replace(".", "p").replace("/", "_")


def resolve_search_spec(search_stage: str) -> BenchmarkSearchSpec:
    try:
        return SEARCH_SPEC_BY_STAGE[str(search_stage)]
    except KeyError as exc:
        raise RuntimeError(f"unknown benchmark search stage {search_stage!r}") from exc


def build_search_variants(
    *,
    search_stage: str,
    search_strategy: str,
    search_budget: int | None = None,
    search_seed: int = 0,
) -> tuple[BenchmarkVariant, ...]:
    spec = resolve_search_spec(search_stage)
    combinations = [
        tuple(zip(spec.dimensions, candidate_values, strict=True))
        for candidate_values in product(*(dimension.values for dimension in spec.dimensions))
    ]
    if str(search_strategy) == "random":
        rng = random.Random(int(search_seed))
        rng.shuffle(combinations)
    elif str(search_strategy) != "grid":
        raise RuntimeError(f"unknown benchmark search strategy {search_strategy!r}")
    if search_budget is not None:
        combinations = combinations[: max(int(search_budget), 0)]
    variants = [
        BenchmarkVariant(
            name="baseline",
            description="Canonical settings with no extra tuning overrides.",
            env_overrides={},
            arg_overrides={},
        )
    ]
    for combination_index, combination in enumerate(combinations, start=1):
        arg_overrides = {
            str(dimension.name): value
            for dimension, value in combination
            if dimension.kind == "arg"
        }
        env_overrides = {
            str(dimension.name): str(value)
            for dimension, value in combination
            if dimension.kind == "env"
        }
        variants.append(
            BenchmarkVariant(
                name=f"{search_stage}_search_{combination_index:03d}",
                description=f"{spec.description} Overrides: args={arg_overrides!r} env={env_overrides!r}",
                env_overrides=env_overrides,
                arg_overrides=arg_overrides,
            )
        )
    return tuple(variants)
