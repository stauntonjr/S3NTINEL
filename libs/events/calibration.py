from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from libs.events.continuous import ContinuousDetectorConfig, ContinuousEventDetector
from libs.events.pipeline import EventDetectionPlan
from libs.windows import WindowProfileRowsFrame
from libs.windows.policy_profile import compute_window_policy_penalty

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def _spark_functions():
    from pyspark.sql import functions as F

    return F


@dataclass(frozen=True)
class ContinuousEventCalibrationSpec:
    slope_sources: tuple[str, ...] = ("ema", "raw")
    ema_alphas: tuple[float, ...] = (0.2, 0.35, 0.5)
    slope_abs_thresholds: tuple[float, ...] = (0.0, 0.5, 1.0)
    delta_threshold: float = 0.0
    window_max_ms: int = 5000
    window_event_threshold: int = 10
    window_min_ms: int = 25
    window_inactivity_timeout_ms: int = 0
    window_strategy: str = "segmented"

    def candidate_grid(self) -> tuple[dict[str, Any], ...]:
        candidates: list[dict[str, Any]] = []
        for slope_source in self.slope_sources:
            normalized_source = str(slope_source).strip().lower()
            if normalized_source == "ema":
                for ema_alpha in self.ema_alphas:
                    for slope_abs_threshold in self.slope_abs_thresholds:
                        candidates.append(
                            {
                                "slope_source": normalized_source,
                                "ema_alpha": float(ema_alpha),
                                "slope_abs_threshold": float(slope_abs_threshold),
                                "delta_threshold": float(self.delta_threshold),
                            }
                        )
            else:
                for slope_abs_threshold in self.slope_abs_thresholds:
                    candidates.append(
                        {
                            "slope_source": normalized_source,
                            "ema_alpha": 1.0,
                            "slope_abs_threshold": float(slope_abs_threshold),
                            "delta_threshold": float(self.delta_threshold),
                        }
                    )
        return tuple(candidates)


def _event_type_counts(events_df: "DataFrame") -> dict[str, int]:
    from pyspark.sql import functions as F

    counts_row = (
        events_df.groupBy("event_type_detected")
        .agg(F.count(F.lit(1)).cast("long").alias("event_count"))
        .agg(
            F.map_from_entries(
                F.collect_list(
                    F.struct(
                        F.col("event_type_detected").cast("string"),
                        F.col("event_count").cast("long"),
                    )
                )
            ).alias("event_type_counts")
        )
        .first()
    )
    return {} if counts_row is None else {str(key): int(value) for key, value in dict(counts_row["event_type_counts"] or {}).items()}


def _window_summary(profile_windows_df: "DataFrame") -> dict[str, Any]:
    F = _spark_functions()
    row = (
        profile_windows_df.agg(
            F.count(F.lit(1)).cast("long").alias("window_count"),
            F.avg(F.when(F.col("close_reason") == F.lit("event_threshold"), F.lit(1.0)).otherwise(F.lit(0.0))).alias(
                "event_threshold_rate"
            ),
            F.avg(F.when(F.col("close_reason") == F.lit("budget_threshold"), F.lit(1.0)).otherwise(F.lit(0.0))).alias(
                "budget_threshold_rate"
            ),
            F.avg(F.when(F.col("close_reason") == F.lit("end_of_stream"), F.lit(1.0)).otherwise(F.lit(0.0))).alias(
                "end_of_stream_rate"
            ),
            F.avg(F.col("event_count").cast("double")).alias("mean_event_count"),
            F.percentile_approx(F.col("event_count").cast("double"), F.lit(0.95), 1000).alias("p95_event_count"),
            F.avg(F.col("sensor_count").cast("double")).alias("mean_sensor_count"),
            F.percentile_approx(F.col("sensor_count").cast("double"), F.lit(0.95), 1000).alias("p95_sensor_count"),
            F.sum(F.pow(F.col("event_count").cast("double"), F.lit(2.0))).alias("pair_cost_proxy"),
            F.sum(F.pow(F.col("sensor_count").cast("double"), F.lit(2.0))).alias("same_window_pair_expansion_proxy"),
        ).first()
    )
    if row is None:
        return {
            "window_count": 0,
            "closure_mix": {},
            "mean_event_count": 0.0,
            "p95_event_count": 0.0,
            "mean_sensor_count": 0.0,
            "p95_sensor_count": 0.0,
            "pair_cost_proxy": 0.0,
            "same_window_pair_expansion_proxy": 0.0,
        }
    return {
        "window_count": int(row["window_count"] or 0),
        "closure_mix": {
            "event_threshold": float(row["event_threshold_rate"] or 0.0),
            "budget_threshold": float(row["budget_threshold_rate"] or 0.0),
            "end_of_stream": float(row["end_of_stream_rate"] or 0.0),
        },
        "mean_event_count": float(row["mean_event_count"] or 0.0),
        "p95_event_count": float(row["p95_event_count"] or 0.0),
        "mean_sensor_count": float(row["mean_sensor_count"] or 0.0),
        "p95_sensor_count": float(row["p95_sensor_count"] or 0.0),
        "pair_cost_proxy": float(row["pair_cost_proxy"] or 0.0),
        "same_window_pair_expansion_proxy": float(row["same_window_pair_expansion_proxy"] or 0.0),
    }


def _candidate_score(summary: dict[str, Any]) -> float:
    closure_mix = dict(summary.get("window_summary", {}).get("closure_mix") or {})
    slope_share = float(summary.get("slope_event_share") or 0.0)
    window_summary = dict(summary.get("window_summary") or {})
    return float(
        abs(slope_share - 0.75)
        + compute_window_policy_penalty(
            pair_cost_proxy=float(window_summary.get("pair_cost_proxy") or 0.0),
            same_window_pair_expansion_proxy=float(window_summary.get("same_window_pair_expansion_proxy") or 0.0),
            sampled_event_count=max(int(summary.get("total_event_count") or 0), 1),
            p95_event_count=float(window_summary.get("p95_event_count") or 0.0),
            end_of_stream_rate=float(closure_mix.get("end_of_stream") or 0.0),
        )
    )


def build_continuous_event_calibration_report_spark(
    raw_df: "DataFrame",
    *,
    datatype_profile_df: "DataFrame | None" = None,
    spec: ContinuousEventCalibrationSpec,
) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for candidate in spec.candidate_grid():
        events_df = (
            EventDetectionPlan(
                continuous_detector=ContinuousEventDetector(
                    config=ContinuousDetectorConfig(
                        delta_threshold=float(candidate["delta_threshold"]),
                        slope_source=str(candidate["slope_source"]),
                        ema_alpha=float(candidate["ema_alpha"]),
                        slope_abs_threshold=float(candidate["slope_abs_threshold"]),
                    )
                )
            )
            .build(raw_df, datatype_profile_df=datatype_profile_df)
            .events.to_dataframe()
        )
        event_type_counts = _event_type_counts(events_df)
        total_event_count = int(sum(event_type_counts.values()))
        slope_event_count = int(event_type_counts.get("slope_pos", 0) + event_type_counts.get("slope_neg", 0))
        profile_windows_df = WindowProfileRowsFrame.from_events(
            events_df,
            max_ms=int(spec.window_max_ms),
            event_threshold=int(spec.window_event_threshold),
            min_ms=int(spec.window_min_ms),
            inactivity_timeout_ms=int(spec.window_inactivity_timeout_ms),
            strategy=str(spec.window_strategy),
        ).to_dataframe()
        window_summary = _window_summary(profile_windows_df)
        summary = {
            "slope_source": str(candidate["slope_source"]),
            "ema_alpha": float(candidate["ema_alpha"]),
            "slope_abs_threshold": float(candidate["slope_abs_threshold"]),
            "delta_threshold": float(candidate["delta_threshold"]),
            "total_event_count": total_event_count,
            "event_type_counts": event_type_counts,
            "slope_event_count": slope_event_count,
            "slope_event_share": (
                float(slope_event_count / float(total_event_count))
                if total_event_count > 0
                else None
            ),
            "window_summary": window_summary,
        }
        summary["heuristic_score"] = _candidate_score(summary)
        candidates.append(summary)

    ranked = sorted(
        candidates,
        key=lambda item: (
            float(item.get("heuristic_score") or 0.0),
            float(item.get("window_summary", {}).get("pair_cost_proxy") or 0.0),
            int(item.get("total_event_count") or 0),
        ),
    )
    return {
        "status": "ok",
        "candidate_count": len(ranked),
        "window_policy": {
            "max_ms": int(spec.window_max_ms),
            "event_threshold": int(spec.window_event_threshold),
            "min_ms": int(spec.window_min_ms),
            "inactivity_timeout_ms": int(spec.window_inactivity_timeout_ms),
            "strategy": str(spec.window_strategy),
        },
        "candidates": ranked,
        "recommended_candidate": ranked[0] if ranked else None,
    }
