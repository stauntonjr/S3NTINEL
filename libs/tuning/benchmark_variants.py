"""Benchmark variant policy for pipeline performance profiling."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from libs.tuning.presets import OBJECTIVE_PRESET_BY_NAME, ObjectivePreset


@dataclass(frozen=True)
class BenchmarkVariant:
    name: str
    description: str
    env_overrides: dict[str, str]
    arg_overrides: dict[str, Any] | None = None
    objective_preset: ObjectivePreset | None = None
    objective_overrides: tuple[tuple[str, Any], ...] = ()


SAFE_SMALL_SEGMENT_ENV = {
    "S3NTINEL_EVENT_SEGMENT_MAX_ROWS": "25000",
    "S3NTINEL_EVENT_SEGMENT_MAX_SPAN_MS": "600000",
    "S3NTINEL_WINDOW_SEGMENT_MAX_ROWS": "25000",
    "S3NTINEL_WINDOW_SEGMENT_MAX_SPAN_MS": "600000",
    "S3NTINEL_PHASE_SEGMENT_MAX_ROWS": "2500",
    "S3NTINEL_PHASE_SEGMENT_MAX_SPAN_MS": "1200000",
}

FULL_SWEEP_ARG_VALUE_BY_NAME = {
    "min_warm": (2, 4),
    "delta_threshold": (0.1, 0.5),
    "slope_source": ("ema",),
    "ema_alpha": (0.1, 0.35),
    "slope_threshold_mode": ("fixed",),
    "slope_threshold_quantile": (0.6, 0.9),
    "slope_threshold_scale": (0.35, 0.75),
    "slope_threshold_min": (1e-7, 1e-4),
    "slope_abs_threshold": (0.5, 2.0),
    "slope_min_persistence_samples": (3, 4),
    "slope_reemit_ratio": (1.25, 2.0),
    "event_warmup_points": (2, 4),
    "window_max_ms": (5000, 20000),
    "window_event_threshold": (10, 30),
    "window_min_ms": (25, 100),
    "window_inactivity_timeout_ms": (500, 2000),
    "phase_count": (4, 5),
    "backbone_parameter_count": (6, 12),
    "backbone_ridge_lambda": (0.5, 2.0),
    "backbone_event_prior_alpha": (0.2, 0.6),
}


def _normalize_variant_value(value: Any) -> str:
    return str(value).replace("-", "neg_").replace(".", "p").replace("/", "_")


def _full_parameter_sweep_variants() -> tuple[BenchmarkVariant, ...]:
    variants: list[BenchmarkVariant] = [
        BenchmarkVariant(
            name="baseline",
            description="Canonical settings with no extra tuning overrides.",
            env_overrides={},
            arg_overrides={},
        )
    ]
    for arg_name, candidate_values in FULL_SWEEP_ARG_VALUE_BY_NAME.items():
        display_name = str(arg_name).replace("_", "-")
        for value in candidate_values:
            variants.append(
                BenchmarkVariant(
                    name=f"{arg_name}_{_normalize_variant_value(value)}",
                    description=f"One-at-a-time full-simulation sweep for `{display_name}` set to `{value}`.",
                    env_overrides={},
                    arg_overrides={str(arg_name): value},
                )
            )
    return tuple(variants)

VARIANT_SET_BY_NAME = {
    "quick": (
        BenchmarkVariant(
            name="baseline",
            description="Canonical settings with no extra tuning overrides.",
            env_overrides={},
        ),
        BenchmarkVariant(
            name="all_small_segments",
            description="Moderately smaller event/window/phase segments that stay in a safer range for the window stage.",
            env_overrides=dict(SAFE_SMALL_SEGMENT_ENV),
        ),
        BenchmarkVariant(
            name="all_large_segments",
            description="Larger event/window/phase segments to reduce carry-over overhead and shuffle fan-out.",
            env_overrides={
                "S3NTINEL_EVENT_SEGMENT_MAX_ROWS": "100000",
                "S3NTINEL_EVENT_SEGMENT_MAX_SPAN_MS": "1800000",
                "S3NTINEL_WINDOW_SEGMENT_MAX_ROWS": "100000",
                "S3NTINEL_WINDOW_SEGMENT_MAX_SPAN_MS": "1800000",
                "S3NTINEL_PHASE_SEGMENT_MAX_ROWS": "10000",
                "S3NTINEL_PHASE_SEGMENT_MAX_SPAN_MS": "3600000",
            },
        ),
    ),
    "detailed": (
        BenchmarkVariant(
            name="baseline",
            description="Canonical settings with no extra tuning overrides.",
            env_overrides={},
        ),
        BenchmarkVariant(
            name="event_small_segments",
            description="Moderately smaller per-parameter event segments only.",
            env_overrides={
                "S3NTINEL_EVENT_SEGMENT_MAX_ROWS": "25000",
                "S3NTINEL_EVENT_SEGMENT_MAX_SPAN_MS": "600000",
            },
        ),
        BenchmarkVariant(
            name="event_small_segments_recall_heavy",
            description="Smaller event segments plus a recall-heavy event objective preset.",
            env_overrides={
                "S3NTINEL_EVENT_SEGMENT_MAX_ROWS": "25000",
                "S3NTINEL_EVENT_SEGMENT_MAX_SPAN_MS": "600000",
            },
            objective_preset=OBJECTIVE_PRESET_BY_NAME["event_recall_heavy"],
        ),
        BenchmarkVariant(
            name="window_small_segments",
            description="Moderately smaller per-flight window segments only.",
            env_overrides={
                "S3NTINEL_WINDOW_SEGMENT_MAX_ROWS": "25000",
                "S3NTINEL_WINDOW_SEGMENT_MAX_SPAN_MS": "600000",
            },
        ),
        BenchmarkVariant(
            name="phase_small_segments",
            description="Moderately smaller per-flight phase segments only.",
            env_overrides={
                "S3NTINEL_PHASE_SEGMENT_MAX_ROWS": "2500",
                "S3NTINEL_PHASE_SEGMENT_MAX_SPAN_MS": "1200000",
            },
        ),
        BenchmarkVariant(
            name="all_small_segments",
            description="Moderately smaller event/window/phase segments together.",
            env_overrides=dict(SAFE_SMALL_SEGMENT_ENV),
        ),
        BenchmarkVariant(
            name="all_large_segments",
            description="Larger event/window/phase segments together.",
            env_overrides={
                "S3NTINEL_EVENT_SEGMENT_MAX_ROWS": "100000",
                "S3NTINEL_EVENT_SEGMENT_MAX_SPAN_MS": "1800000",
                "S3NTINEL_WINDOW_SEGMENT_MAX_ROWS": "100000",
                "S3NTINEL_WINDOW_SEGMENT_MAX_SPAN_MS": "1800000",
                "S3NTINEL_PHASE_SEGMENT_MAX_ROWS": "10000",
                "S3NTINEL_PHASE_SEGMENT_MAX_SPAN_MS": "3600000",
            },
            objective_preset=OBJECTIVE_PRESET_BY_NAME["structural_latency_biased"],
        ),
    ),
    "full_parameter_sweep": _full_parameter_sweep_variants(),
}

VARIANT_BY_NAME = {
    variant.name: variant
    for variant_group in VARIANT_SET_BY_NAME.values()
    for variant in variant_group
}
