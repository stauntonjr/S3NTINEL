from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pandas as pd

from libs.io.schemas import WINDOW_POLICY_PROFILE_SCHEMA
from libs.perf import get_logger
from libs.windows.tables import WindowPolicyProfileTable, WindowProfileRowsFrame, WindowsTable
from libs.windows.window import DEFAULT_MIN_SAMPLING_RATE_HZ, WindowPolicy

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


LOGGER = get_logger("libs.windows.policy_profile")


def _spark_functions():
    from pyspark.sql import functions as F

    return F


def _safe_unpersist(df: "DataFrame | None", *, label: str) -> None:
    if df is None:
        return
    try:
        df.unpersist()
    except Exception as exc:  # pragma: no cover - defensive cleanup after Spark JVM failure
        LOGGER.warning("window_policy_profile unpersist skipped for %s: %s", label, exc)


def _normalize_timestamp_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce")


def _majority_phase_label(labels: pd.Series) -> str | None:
    values = [str(item) for item in labels.fillna("").astype(str).tolist() if str(item)]
    if not values:
        return None
    ranked = sorted(((label, values.count(label)) for label in sorted(set(values))), key=lambda item: (-item[1], item[0]))
    return str(ranked[0][0]) if ranked else None


def compute_window_policy_penalty(
    *,
    pair_cost_proxy: float,
    same_window_pair_expansion_proxy: float,
    sampled_event_count: int,
    p95_event_count: float,
    end_of_stream_rate: float,
) -> float:
    normalized_event_count = max(float(sampled_event_count), 1.0)
    event_overload = max(float(p95_event_count) - 1.0, 0.0)
    return float(
        math.log1p(max(float(pair_cost_proxy), 0.0) / normalized_event_count)
        + (0.5 * math.log1p(max(float(same_window_pair_expansion_proxy), 0.0) / normalized_event_count))
        + math.log1p(event_overload)
        + max(float(end_of_stream_rate), 0.0)
    )


def _window_policy_penalty_expr(
    *,
    pair_cost_proxy_col: Any,
    same_window_pair_expansion_proxy_col: Any,
    sampled_event_count_col: Any,
    p95_event_count_col: Any,
    end_of_stream_close_rate_col: Any,
) -> Any:
    F = _spark_functions()
    normalized_event_count = F.greatest(sampled_event_count_col.cast("double"), F.lit(1.0))
    event_overload = F.greatest(p95_event_count_col.cast("double") - F.lit(1.0), F.lit(0.0))
    return (
        F.log1p(F.greatest(pair_cost_proxy_col.cast("double"), F.lit(0.0)) / normalized_event_count)
        + (F.lit(0.5) * F.log1p(F.greatest(same_window_pair_expansion_proxy_col.cast("double"), F.lit(0.0)) / normalized_event_count))
        + F.log1p(event_overload)
        + F.greatest(end_of_stream_close_rate_col.cast("double"), F.lit(0.0))
    )


def _window_policy_objective_score_expr(
    *,
    mean_event_type_count_col: Any,
    pair_cost_proxy_col: Any,
    same_window_pair_expansion_proxy_col: Any,
    sampled_event_count_col: Any,
    p95_event_count_col: Any,
    end_of_stream_close_rate_col: Any,
) -> Any:
    return mean_event_type_count_col.cast("double") - _window_policy_penalty_expr(
        pair_cost_proxy_col=pair_cost_proxy_col,
        same_window_pair_expansion_proxy_col=same_window_pair_expansion_proxy_col,
        sampled_event_count_col=sampled_event_count_col,
        p95_event_count_col=p95_event_count_col,
        end_of_stream_close_rate_col=end_of_stream_close_rate_col,
    )


def build_window_truth_phase_coverage_summary(
    *,
    windows_df: pd.DataFrame,
    phase_labels_df: pd.DataFrame,
    target_phase_label: str = "cruise",
) -> dict[str, Any]:
    if windows_df is None or phase_labels_df is None or windows_df.empty or phase_labels_df.empty:
        return {
            "status": "skipped",
            "reason": "missing windows or phase labels",
        }

    windows = windows_df.copy()
    labels = phase_labels_df.copy()
    windows["t_start"] = _normalize_timestamp_series(windows["t_start"])
    windows["t_end"] = _normalize_timestamp_series(windows["t_end"])
    labels["timestamp_utc"] = _normalize_timestamp_series(labels["timestamp_utc"])

    assignments: list[dict[str, Any]] = []
    for (tail_id, flight_id), flight_windows in windows.groupby(["tail_id", "flight_id"], dropna=False):
        flight_labels = labels[
            (labels["tail_id"].astype(str) == str(tail_id))
            & (labels["flight_id"].astype(str) == str(flight_id))
        ].copy()
        if flight_labels.empty:
            continue
        for row in flight_windows.to_dict(orient="records"):
            t_start = row.get("t_start")
            t_end = row.get("t_end")
            if pd.isna(t_start) or pd.isna(t_end):
                continue
            overlapping = flight_labels[
                (flight_labels["timestamp_utc"] >= t_start)
                & (flight_labels["timestamp_utc"] <= t_end)
            ]
            phase_label = _majority_phase_label(overlapping.get("phase_label", pd.Series(dtype="object")))
            if phase_label is None:
                continue
            assignments.append(
                {
                    "tail_id": str(row.get("tail_id", "")),
                    "flight_id": str(row.get("flight_id", "")),
                    "win_id": int(row.get("win_id", 0) or 0),
                    "phase_label": str(phase_label),
                    "close_reason": str(row.get("close_reason", "") or ""),
                    "duration_ms": float(row.get("duration_ms", 0.0) or 0.0),
                    "real_event_count": float(
                        row.get("real_event_count", row.get("event_count", 0.0)) or 0.0
                    ),
                }
            )

    if not assignments:
        return {
            "status": "skipped",
            "reason": "no overlapping windows and phase labels",
        }

    assigned_df = pd.DataFrame.from_records(assignments)
    majority_counts = {
        str(label): int(count)
        for label, count in assigned_df["phase_label"].value_counts(dropna=False).sort_index().items()
    }
    assigned_window_count = int(len(assigned_df))
    target_phase = str(target_phase_label)
    target_df = assigned_df[assigned_df["phase_label"] == target_phase].copy()
    target_close_counts = {
        str(reason): int(count)
        for reason, count in target_df["close_reason"].fillna("").astype(str).value_counts(dropna=False).sort_index().items()
        if str(reason)
    }
    target_window_count = int(len(target_df))
    target_duration_series = pd.to_numeric(target_df.get("duration_ms"), errors="coerce")
    target_real_event_series = pd.to_numeric(target_df.get("real_event_count"), errors="coerce")
    return {
        "status": "ok",
        "assigned_window_count": assigned_window_count,
        "majority_window_counts_by_phase_label": majority_counts,
        "majority_window_fraction_by_phase_label": {
            label: float(count / assigned_window_count)
            for label, count in majority_counts.items()
        },
        "target_phase_label": target_phase,
        "target_phase_window_count": target_window_count,
        "target_phase_close_reason_counts": target_close_counts,
        "target_phase_close_reason_rates": {
            reason: float(count / target_window_count)
            for reason, count in target_close_counts.items()
            if target_window_count > 0
        },
        "target_phase_duration_ms": {
            "mean": (
                None
                if target_duration_series.empty or target_duration_series.notna().sum() <= 0
                else float(target_duration_series.mean())
            ),
            "p95": (
                None
                if target_duration_series.empty or target_duration_series.notna().sum() <= 0
                else float(target_duration_series.quantile(0.95))
            ),
        },
        "target_phase_real_event_count": {
            "mean": (
                None
                if target_real_event_series.empty or target_real_event_series.notna().sum() <= 0
                else float(target_real_event_series.mean())
            ),
            "p95": (
                None
                if target_real_event_series.empty or target_real_event_series.notna().sum() <= 0
                else float(target_real_event_series.quantile(0.95))
            ),
        },
        "cruise_majority_window_count": target_window_count if target_phase == "cruise" else int(majority_counts.get("cruise", 0)),
    }


@dataclass(frozen=True)
class WindowPolicyProfileSpec:
    min_sampling_rate_hz: float
    configured_max_ms: int
    configured_event_threshold: int
    min_ms: int
    inactivity_timeout_ms: int
    strategy: str = "segmented"
    gap_quantiles: tuple[float, ...] = (0.5, 0.75, 0.9)
    event_threshold_multipliers: tuple[float, ...] = (0.75, 1.0, 1.25, 1.5, 2.0)
    max_profile_flights: int = 64

    @property
    def resolved_max_ms(self) -> int:
        configured = int(self.configured_max_ms)
        if configured > 0:
            return configured
        return WindowPolicy.max_ms_from_min_sampling_rate(max(float(self.min_sampling_rate_hz), DEFAULT_MIN_SAMPLING_RATE_HZ))

    @property
    def fallback_policy(self) -> WindowPolicy:
        return WindowPolicy(
            max_ms=int(self.resolved_max_ms),
            event_threshold=max(int(self.configured_event_threshold), 1),
            min_ms=int(self.min_ms),
            inactivity_timeout_ms=int(self.inactivity_timeout_ms),
        )

    def candidate_policies(self, *, median_gap_ms: float, upper_gap_ms: tuple[float, ...]) -> tuple[WindowPolicy, ...]:
        thresholds = sorted(
            {
                max(int(round(float(self.configured_event_threshold) * float(multiplier))), 2)
                for multiplier in self.event_threshold_multipliers
            }
            | {max(int(self.configured_event_threshold), 2)}
        )
        max_candidates: set[int] = {int(self.resolved_max_ms)}
        for threshold in thresholds:
            for gap_ms in upper_gap_ms:
                candidate = int(round(max(float(median_gap_ms), float(gap_ms), 1.0) * float(threshold)))
                max_candidates.add(max(int(self.min_ms), min(int(self.resolved_max_ms), candidate)))
        policies = {
            WindowPolicy(
                max_ms=max(int(max_ms), int(self.min_ms)),
                event_threshold=max(int(threshold), 2),
                min_ms=int(self.min_ms),
                inactivity_timeout_ms=int(self.inactivity_timeout_ms),
            )
            for threshold in thresholds
            for max_ms in max_candidates
        }
        return tuple(sorted(policies, key=lambda item: (item.max_ms, item.event_threshold)))


@dataclass(frozen=True)
class WindowPolicyEvaluationSpec:
    candidate_frontier_size: int = 5
    stability_sample_fraction: float = 0.8
    stability_sample_count: int = 2
    max_stability_flights: int = 64
    warning_policy_penalty_ratio: float = 1.25
    warning_min_boundary_jaccard: float = 0.5


@dataclass(frozen=True)
class WindowMetricsSummary:
    window_count: int = 0
    event_threshold_rate: float = 0.0
    budget_threshold_rate: float = 0.0
    end_of_stream_rate: float = 0.0
    mean_duration_ms: float = 0.0
    p95_duration_ms: float = 0.0
    mean_event_count: float = 0.0
    p95_event_count: float = 0.0
    mean_quiet_credit_end: float = 0.0
    p95_quiet_credit_end: float = 0.0
    mean_closure_budget_end: float = 0.0
    p95_closure_budget_end: float = 0.0
    mean_sensor_count: float = 0.0
    p95_sensor_count: float = 0.0
    mean_event_type_count: float = 0.0
    p95_event_type_count: float = 0.0
    pair_cost_proxy: float = 0.0
    same_window_pair_expansion_proxy: float = 0.0

    @classmethod
    def from_profile_row(cls, profile_row: dict[str, Any] | None) -> "WindowMetricsSummary":
        if not profile_row:
            return cls()
        return cls(
            window_count=int(profile_row.get("predicted_window_count") or 0),
            event_threshold_rate=float(profile_row.get("event_threshold_close_rate") or 0.0),
            budget_threshold_rate=float(profile_row.get("budget_threshold_close_rate") or 0.0),
            end_of_stream_rate=float(profile_row.get("end_of_stream_close_rate") or 0.0),
            mean_duration_ms=float(profile_row.get("mean_duration_ms") or 0.0),
            p95_duration_ms=float(profile_row.get("p95_duration_ms") or 0.0),
            mean_event_count=float(profile_row.get("mean_event_count") or 0.0),
            p95_event_count=float(profile_row.get("p95_event_count") or 0.0),
            mean_quiet_credit_end=float(profile_row.get("mean_quiet_credit_end") or 0.0),
            p95_quiet_credit_end=float(profile_row.get("p95_quiet_credit_end") or 0.0),
            mean_closure_budget_end=float(profile_row.get("mean_closure_budget_end") or 0.0),
            p95_closure_budget_end=float(profile_row.get("p95_closure_budget_end") or 0.0),
            mean_sensor_count=float(profile_row.get("mean_sensor_count") or 0.0),
            p95_sensor_count=float(profile_row.get("p95_sensor_count") or 0.0),
            mean_event_type_count=float(profile_row.get("mean_event_type_count") or 0.0),
            p95_event_type_count=float(profile_row.get("p95_event_type_count") or 0.0),
            pair_cost_proxy=float(profile_row.get("pair_cost_proxy") or 0.0),
            same_window_pair_expansion_proxy=float(profile_row.get("same_window_pair_expansion_proxy") or 0.0),
        )

    @classmethod
    def from_window_row(cls, metrics_row: Any) -> "WindowMetricsSummary":
        if metrics_row is None:
            return cls()

        def _rate(count_name: str) -> float:
            if int(metrics_row["window_count"] or 0) <= 0:
                return 0.0
            return float(float(metrics_row[count_name] or 0.0) / float(metrics_row["window_count"] or 1))

        return cls(
            window_count=int(metrics_row["window_count"] or 0),
            event_threshold_rate=_rate("event_threshold_count"),
            budget_threshold_rate=_rate("budget_threshold_count"),
            end_of_stream_rate=_rate("end_of_stream_count"),
            mean_duration_ms=float(metrics_row["mean_duration_ms"] or 0.0),
            p95_duration_ms=float(metrics_row["p95_duration_ms"] or 0.0),
            mean_event_count=float(metrics_row["mean_event_count"] or 0.0),
            p95_event_count=float(metrics_row["p95_event_count"] or 0.0),
            mean_quiet_credit_end=float(metrics_row["mean_quiet_credit_end"] or 0.0),
            p95_quiet_credit_end=float(metrics_row["p95_quiet_credit_end"] or 0.0),
            mean_closure_budget_end=float(metrics_row["mean_closure_budget_end"] or 0.0),
            p95_closure_budget_end=float(metrics_row["p95_closure_budget_end"] or 0.0),
            mean_sensor_count=float(metrics_row["mean_sensor_count"] or 0.0),
            p95_sensor_count=float(metrics_row["p95_sensor_count"] or 0.0),
            mean_event_type_count=float(metrics_row["mean_event_type_count"] or 0.0),
            p95_event_type_count=float(metrics_row["p95_event_type_count"] or 0.0),
            pair_cost_proxy=float(metrics_row["pair_cost_proxy"] or 0.0),
            same_window_pair_expansion_proxy=float(metrics_row["same_window_pair_expansion_proxy"] or 0.0),
        )

    def _close_reason_count(self, rate: float) -> int:
        return int(round(float(rate) * float(self.window_count)))

    def to_report(self) -> dict[str, Any]:
        return {
            "window_count": int(self.window_count),
            "closure_mix": {
                "rates": {
                    "event_threshold": float(self.event_threshold_rate),
                    "budget_threshold": float(self.budget_threshold_rate),
                    "end_of_stream": float(self.end_of_stream_rate),
                },
                "counts": {
                    "event_threshold": self._close_reason_count(self.event_threshold_rate),
                    "budget_threshold": self._close_reason_count(self.budget_threshold_rate),
                    "end_of_stream": self._close_reason_count(self.end_of_stream_rate),
                },
                "mean_duration_ms": float(self.mean_duration_ms),
                "p95_duration_ms": float(self.p95_duration_ms),
                "mean_event_count": float(self.mean_event_count),
                "p95_event_count": float(self.p95_event_count),
                "mean_quiet_credit_end": float(self.mean_quiet_credit_end),
                "p95_quiet_credit_end": float(self.p95_quiet_credit_end),
                "mean_closure_budget_end": float(self.mean_closure_budget_end),
                "p95_closure_budget_end": float(self.p95_closure_budget_end),
                "mean_sensor_count": float(self.mean_sensor_count),
                "p95_sensor_count": float(self.p95_sensor_count),
                "mean_event_type_count": float(self.mean_event_type_count),
                "p95_event_type_count": float(self.p95_event_type_count),
            },
            "downstream_cost_proxy": {
                "window_count": int(self.window_count),
                "pair_cost_proxy": float(self.pair_cost_proxy),
                "same_window_pair_expansion_proxy": float(self.same_window_pair_expansion_proxy),
                "mean_event_count": float(self.mean_event_count),
                "p95_event_count": float(self.p95_event_count),
                "mean_sensor_count": float(self.mean_sensor_count),
                "p95_sensor_count": float(self.p95_sensor_count),
            },
        }


@dataclass(frozen=True)
class WindowPolicyProfile:
    spec: WindowPolicyProfileSpec

    def _fallback_profile_row(
        self,
        *,
        sampled_event_count: int,
        sampled_flight_count: int,
    ) -> dict[str, Any]:
        fallback_policy = self.spec.fallback_policy
        return {
            "profile_id": "WINDOW_POLICY_PROFILE_V1",
            "profile_scope": "global",
            "candidate_rank": 1,
            "is_selected": True,
            "max_ms": int(fallback_policy.max_ms),
            "event_threshold": int(fallback_policy.event_threshold),
            "min_ms": int(fallback_policy.min_ms),
            "inactivity_timeout_ms": int(fallback_policy.inactivity_timeout_ms),
            "objective_score": 0.0,
            "balance_penalty": 0.0,
            "predicted_window_count": 0,
            "mean_duration_ms": 0.0,
            "p95_duration_ms": 0.0,
            "mean_event_count": 0.0,
            "p95_event_count": 0.0,
            "mean_sensor_count": 0.0,
            "p95_sensor_count": 0.0,
            "mean_event_type_count": 0.0,
            "p95_event_type_count": 0.0,
            "event_threshold_close_rate": 0.0,
            "budget_threshold_close_rate": 0.0,
            "max_ms_close_rate": 0.0,
            "event_threshold_plus_max_ms_close_rate": 0.0,
            "end_of_stream_close_rate": 0.0,
            "mean_quiet_credit_end": 0.0,
            "p95_quiet_credit_end": 0.0,
            "mean_closure_budget_end": 0.0,
            "p95_closure_budget_end": 0.0,
            "pair_cost_proxy": 0.0,
            "same_window_pair_expansion_proxy": 0.0,
            "sampled_event_count": int(sampled_event_count),
            "sampled_flight_count": int(sampled_flight_count),
        }

    def _fallback_profile_dataframe(
        self,
        spark: Any,
        *,
        sampled_event_count: int,
        sampled_flight_count: int,
    ) -> "DataFrame":
        return spark.createDataFrame(
            [self._fallback_profile_row(sampled_event_count=sampled_event_count, sampled_flight_count=sampled_flight_count)],
            schema=WINDOW_POLICY_PROFILE_SCHEMA(),
        )

    def _skipped_evaluation_report(
        self,
        *,
        reason: str,
        profile_df: "DataFrame | None",
    ) -> dict[str, Any]:
        configured_policy = self.spec.fallback_policy
        selected_policy, policy_source = self.resolve_selected_policy(profile_df, fallback_policy=configured_policy)
        selected_profile_row = self._selected_profile_row(profile_df)
        return {
            "status": "skipped",
            "reason": str(reason),
            "selected_policy": {
                "policy_source": policy_source,
                "resolved_policy": {
                    "max_ms": int(selected_policy.max_ms),
                    "event_threshold": int(selected_policy.event_threshold),
                    "min_ms": int(selected_policy.min_ms),
                    "inactivity_timeout_ms": int(selected_policy.inactivity_timeout_ms),
                },
                "configured_policy": {
                    "max_ms": int(configured_policy.max_ms),
                    "event_threshold": int(configured_policy.event_threshold),
                    "min_ms": int(configured_policy.min_ms),
                    "inactivity_timeout_ms": int(configured_policy.inactivity_timeout_ms),
                },
                "profile_row": selected_profile_row,
            },
            "candidate_frontier": [],
            "selection_delta_vs_configured": {},
            "edge_stability": {"status": "skipped", "reason": str(reason), "samples": [], "mean_boundary_jaccard": None},
            "closure_mix": {"status": "skipped", "reason": str(reason)},
            "downstream_cost_proxy": {"status": "skipped", "reason": str(reason)},
        }

    def _sample_event_flights(self, events_df: "DataFrame") -> tuple["DataFrame", int]:
        F = _spark_functions()
        flight_keys = events_df.select("tail_id", "flight_id").distinct()
        flight_count = int(flight_keys.count())
        if flight_count <= int(self.spec.max_profile_flights):
            return events_df, flight_count
        sampled_keys = flight_keys.orderBy(F.xxhash64("tail_id", "flight_id")).limit(int(self.spec.max_profile_flights))
        return events_df.join(sampled_keys, on=["tail_id", "flight_id"], how="inner"), flight_count

    def _gap_statistics(self, events_df: "DataFrame") -> tuple[float, tuple[float, ...]]:
        from pyspark.sql import Window

        F = _spark_functions()
        event_window = Window.partitionBy("tail_id", "flight_id").orderBy("event_seq_id")
        gap_row = (
            events_df.select("tail_id", "flight_id", "event_seq_id", "timestamp_utc")
            .withColumn("_prev_ts", F.lag("timestamp_utc").over(event_window))
            .withColumn("_gap_ms", (F.unix_millis("timestamp_utc") - F.unix_millis("_prev_ts")).cast("double"))
            .where(F.col("_gap_ms") > F.lit(0.0))
            .agg(
                F.percentile_approx(
                    "_gap_ms",
                    F.array(*[F.lit(float(item)) for item in self.spec.gap_quantiles]),
                    1000,
                ).alias("gap_quantiles"),
            )
            .first()
        )
        quantiles = tuple(float(item) for item in ((gap_row["gap_quantiles"] if gap_row is not None else None) or []) if item is not None)
        if not quantiles:
            fallback = float(max(self.spec.min_ms, 1))
            quantiles = (fallback, fallback, fallback)
        median_gap = float(quantiles[0])
        return median_gap, tuple(float(item) for item in quantiles)

    def _evaluate_candidate(self, events_df: "DataFrame", *, policy: WindowPolicy, sampled_event_count: int, sampled_flight_count: int) -> "DataFrame":
        return self._evaluate_candidate_with_coverage(
            events_df,
            policy=policy,
            sampled_event_count=sampled_event_count,
            sampled_flight_count=sampled_flight_count,
            coverage_timestamps_df=None,
        )

    def _evaluate_candidate_with_coverage(
        self,
        events_df: "DataFrame",
        *,
        policy: WindowPolicy,
        sampled_event_count: int,
        sampled_flight_count: int,
        coverage_timestamps_df: "DataFrame | None",
    ) -> "DataFrame":
        from pyspark.sql import functions as F

        profile_windows_df = WindowProfileRowsFrame.from_events(
            events_df,
            max_ms=int(policy.max_ms),
            event_threshold=int(policy.event_threshold),
            min_ms=int(policy.min_ms),
            inactivity_timeout_ms=int(policy.inactivity_timeout_ms),
            strategy=self.spec.strategy,
            coverage_timestamps_df=coverage_timestamps_df,
        ).to_dataframe()
        return profile_windows_df.agg(
            F.count(F.lit(1)).cast("long").alias("predicted_window_count"),
            F.avg(F.col("duration_ms").cast("double")).alias("mean_duration_ms"),
            F.percentile_approx(F.col("duration_ms").cast("double"), F.lit(0.95), 1000).alias("p95_duration_ms"),
            F.avg(F.col("event_count").cast("double")).alias("mean_event_count"),
            F.percentile_approx(F.col("event_count").cast("double"), F.lit(0.95), 1000).alias("p95_event_count"),
            F.avg(F.col("quiet_credit_end").cast("double")).alias("mean_quiet_credit_end"),
            F.percentile_approx(F.col("quiet_credit_end").cast("double"), F.lit(0.95), 1000).alias(
                "p95_quiet_credit_end"
            ),
            F.avg(F.col("closure_budget_end").cast("double")).alias("mean_closure_budget_end"),
            F.percentile_approx(F.col("closure_budget_end").cast("double"), F.lit(0.95), 1000).alias(
                "p95_closure_budget_end"
            ),
            F.avg(F.col("sensor_count").cast("double")).alias("mean_sensor_count"),
            F.percentile_approx(F.col("sensor_count").cast("double"), F.lit(0.95), 1000).alias("p95_sensor_count"),
            F.avg(F.col("event_type_count").cast("double")).alias("mean_event_type_count"),
            F.percentile_approx(F.col("event_type_count").cast("double"), F.lit(0.95), 1000).alias("p95_event_type_count"),
            F.avg(
                F.when(F.col("close_reason") == F.lit("event_threshold"), F.lit(1.0)).otherwise(F.lit(0.0))
            ).alias("event_threshold_close_rate"),
            F.avg(
                F.when(F.col("close_reason") == F.lit("budget_threshold"), F.lit(1.0)).otherwise(F.lit(0.0))
            ).alias("budget_threshold_close_rate"),
            F.avg(
                F.when(F.col("close_reason") == F.lit("budget_threshold"), F.lit(1.0)).otherwise(F.lit(0.0))
            ).alias("max_ms_close_rate"),
            F.avg(
                F.when(F.col("close_reason") == F.lit("event_threshold+max_ms"), F.lit(1.0)).otherwise(F.lit(0.0))
            ).alias("event_threshold_plus_max_ms_close_rate"),
            F.avg(
                F.when(F.col("close_reason") == F.lit("end_of_stream"), F.lit(1.0)).otherwise(F.lit(0.0))
            ).alias("end_of_stream_close_rate"),
            F.sum((F.col("event_count").cast("double") * F.col("event_count").cast("double"))).alias("pair_cost_proxy"),
            F.sum((F.col("sensor_count").cast("double") * F.col("sensor_count").cast("double"))).alias(
                "same_window_pair_expansion_proxy"
            ),
        ).select(
            F.lit("WINDOW_POLICY_PROFILE_V1").alias("profile_id"),
            F.lit("global").alias("profile_scope"),
            F.lit(int(policy.max_ms)).cast("int").alias("max_ms"),
            F.lit(int(policy.event_threshold)).cast("int").alias("event_threshold"),
            F.lit(int(policy.min_ms)).cast("int").alias("min_ms"),
            F.lit(int(policy.inactivity_timeout_ms)).cast("int").alias("inactivity_timeout_ms"),
            F.lit(float(sampled_event_count)).cast("long").alias("sampled_event_count"),
            F.lit(int(sampled_flight_count)).cast("int").alias("sampled_flight_count"),
            "predicted_window_count",
            F.coalesce(F.col("mean_duration_ms"), F.lit(0.0)).cast("double").alias("mean_duration_ms"),
            F.coalesce(F.col("p95_duration_ms"), F.lit(0.0)).cast("double").alias("p95_duration_ms"),
            F.coalesce(F.col("mean_event_count"), F.lit(0.0)).cast("double").alias("mean_event_count"),
            F.coalesce(F.col("p95_event_count"), F.lit(0.0)).cast("double").alias("p95_event_count"),
            F.coalesce(F.col("mean_quiet_credit_end"), F.lit(0.0)).cast("double").alias("mean_quiet_credit_end"),
            F.coalesce(F.col("p95_quiet_credit_end"), F.lit(0.0)).cast("double").alias("p95_quiet_credit_end"),
            F.coalesce(F.col("mean_closure_budget_end"), F.lit(0.0))
            .cast("double")
            .alias("mean_closure_budget_end"),
            F.coalesce(F.col("p95_closure_budget_end"), F.lit(0.0))
            .cast("double")
            .alias("p95_closure_budget_end"),
            F.coalesce(F.col("mean_sensor_count"), F.lit(0.0)).cast("double").alias("mean_sensor_count"),
            F.coalesce(F.col("p95_sensor_count"), F.lit(0.0)).cast("double").alias("p95_sensor_count"),
            F.coalesce(F.col("mean_event_type_count"), F.lit(0.0)).cast("double").alias("mean_event_type_count"),
            F.coalesce(F.col("p95_event_type_count"), F.lit(0.0)).cast("double").alias("p95_event_type_count"),
            F.coalesce(F.col("event_threshold_close_rate"), F.lit(0.0)).cast("double").alias("event_threshold_close_rate"),
            F.coalesce(F.col("budget_threshold_close_rate"), F.lit(0.0))
            .cast("double")
            .alias("budget_threshold_close_rate"),
            F.coalesce(F.col("max_ms_close_rate"), F.lit(0.0)).cast("double").alias("max_ms_close_rate"),
            F.coalesce(F.col("event_threshold_plus_max_ms_close_rate"), F.lit(0.0))
            .cast("double")
            .alias("event_threshold_plus_max_ms_close_rate"),
            F.coalesce(F.col("end_of_stream_close_rate"), F.lit(0.0)).cast("double").alias("end_of_stream_close_rate"),
            F.coalesce(F.col("pair_cost_proxy"), F.lit(0.0)).cast("double").alias("pair_cost_proxy"),
            F.coalesce(F.col("same_window_pair_expansion_proxy"), F.lit(0.0))
            .cast("double")
            .alias("same_window_pair_expansion_proxy"),
        )

    def _selected_profile_row(self, profile_df: "DataFrame | None") -> dict[str, Any] | None:
        if profile_df is None:
            return None
        F = _spark_functions()
        selected_row = profile_df.where(F.col("is_selected") == F.lit(True)).orderBy(F.col("candidate_rank").asc()).limit(1).first()
        return None if selected_row is None else dict(selected_row.asDict())

    def _candidate_frontier(self, profile_df: "DataFrame", *, limit: int) -> list[dict[str, Any]]:
        from pyspark.sql import functions as F

        frontier_row = (
            profile_df.orderBy(F.col("candidate_rank").asc())
            .limit(int(max(limit, 1)))
            .agg(
                F.collect_list(
                    F.struct(
                        F.col("candidate_rank").cast("int").alias("candidate_rank"),
                        F.col("max_ms").cast("int").alias("max_ms"),
                        F.col("event_threshold").cast("int").alias("event_threshold"),
                        F.col("objective_score").cast("double").alias("objective_score"),
                        F.col("balance_penalty").cast("double").alias("balance_penalty"),
                        F.col("predicted_window_count").cast("long").alias("predicted_window_count"),
                        F.col("p95_event_count").cast("double").alias("p95_event_count"),
                        F.col("event_threshold_close_rate").cast("double").alias("event_threshold_close_rate"),
                        F.col("pair_cost_proxy").cast("double").alias("pair_cost_proxy"),
                    )
                ).alias("frontier"),
            )
            .first()
        )
        frontier = [] if frontier_row is None else list(frontier_row["frontier"] or [])
        return [dict(item.asDict()) for item in frontier]

    @staticmethod
    def _public_profile_row(profile_row: dict[str, Any] | None) -> dict[str, Any] | None:
        if profile_row is None:
            return None
        hidden_keys = {"max_ms_close_rate", "event_threshold_plus_max_ms_close_rate"}
        return {str(key): value for key, value in profile_row.items() if str(key) not in hidden_keys}

    def _profile_row_for_policy(self, profile_df: "DataFrame | None", *, policy: WindowPolicy) -> dict[str, Any] | None:
        if profile_df is None:
            return None
        F = _spark_functions()
        row = (
            profile_df.where(F.col("max_ms") == F.lit(int(policy.max_ms)))
            .where(F.col("event_threshold") == F.lit(int(policy.event_threshold)))
            .where(F.col("min_ms") == F.lit(int(policy.min_ms)))
            .where(F.col("inactivity_timeout_ms") == F.lit(int(policy.inactivity_timeout_ms)))
            .orderBy(F.col("candidate_rank").asc())
            .limit(1)
            .first()
        )
        return None if row is None else dict(row.asDict())

    @staticmethod
    def _metrics_from_profile_row(profile_row: dict[str, Any] | None) -> dict[str, Any]:
        return WindowMetricsSummary.from_profile_row(profile_row).to_report()

    def _flight_subset_events(
        self,
        events_df: "DataFrame",
        *,
        salt: int,
        sample_fraction: float,
        max_flights: int,
    ) -> tuple["DataFrame", int]:
        F = _spark_functions()
        flight_keys = events_df.select("tail_id", "flight_id").distinct()
        flight_count = int(flight_keys.count())
        if flight_count <= 0:
            return events_df.limit(0), 0
        target_count = max(1, int(round(float(flight_count) * float(sample_fraction))))
        limit_count = min(flight_count, max(int(max_flights), 1), target_count)
        sampled_keys = flight_keys.orderBy(F.xxhash64("tail_id", "flight_id", F.lit(int(salt)))).limit(int(limit_count))
        return events_df.join(sampled_keys, on=["tail_id", "flight_id"], how="inner"), int(limit_count)

    def _window_metrics(self, windows_df: "DataFrame") -> dict[str, Any]:
        from pyspark.sql import functions as F

        metrics_row = (
            windows_df.agg(
                F.count(F.lit(1)).cast("long").alias("window_count"),
                F.sum(F.when(F.col("close_reason") == F.lit("event_threshold"), F.lit(1)).otherwise(F.lit(0)))
                .cast("long")
                .alias("event_threshold_count"),
                F.sum(F.when(F.col("close_reason") == F.lit("budget_threshold"), F.lit(1)).otherwise(F.lit(0)))
                .cast("long")
                .alias("budget_threshold_count"),
                F.sum(F.when(F.col("close_reason") == F.lit("event_threshold+max_ms"), F.lit(1)).otherwise(F.lit(0)))
                .cast("long")
                .alias("event_threshold_plus_max_ms_count"),
                F.sum(F.when(F.col("close_reason") == F.lit("end_of_stream"), F.lit(1)).otherwise(F.lit(0)))
                .cast("long")
                .alias("end_of_stream_count"),
                F.avg(F.col("duration_ms").cast("double")).alias("mean_duration_ms"),
                F.percentile_approx(F.col("duration_ms").cast("double"), F.lit(0.95), 1000).alias("p95_duration_ms"),
                F.avg(F.col("event_count").cast("double")).alias("mean_event_count"),
                F.percentile_approx(F.col("event_count").cast("double"), F.lit(0.95), 1000).alias("p95_event_count"),
                F.avg(F.col("quiet_credit_end").cast("double")).alias("mean_quiet_credit_end"),
                F.percentile_approx(F.col("quiet_credit_end").cast("double"), F.lit(0.95), 1000).alias(
                    "p95_quiet_credit_end"
                ),
                F.avg(F.col("closure_budget_end").cast("double")).alias("mean_closure_budget_end"),
                F.percentile_approx(F.col("closure_budget_end").cast("double"), F.lit(0.95), 1000).alias(
                    "p95_closure_budget_end"
                ),
                F.avg(F.col("sensor_count").cast("double")).alias("mean_sensor_count"),
                F.percentile_approx(F.col("sensor_count").cast("double"), F.lit(0.95), 1000).alias("p95_sensor_count"),
                F.avg(F.size(F.map_keys("event_type_counts")).cast("double")).alias("mean_event_type_count"),
                F.percentile_approx(F.size(F.map_keys("event_type_counts")).cast("double"), F.lit(0.95), 1000).alias("p95_event_type_count"),
                F.sum(F.pow(F.col("event_count").cast("double"), F.lit(2.0))).alias("pair_cost_proxy"),
                F.sum(F.pow(F.col("sensor_count").cast("double"), F.lit(2.0))).alias("same_window_pair_expansion_proxy"),
            ).first()
        )
        return WindowMetricsSummary.from_window_row(metrics_row).to_report()

    def _selection_delta_vs_configured(
        self,
        *,
        selected_policy: WindowPolicy,
        configured_policy: WindowPolicy,
        selected_metrics: dict[str, Any],
        configured_metrics: dict[str, Any],
    ) -> dict[str, Any]:
        selected_cost = dict(selected_metrics.get("downstream_cost_proxy") or {})
        configured_cost = dict(configured_metrics.get("downstream_cost_proxy") or {})

        def _delta(selected_value: float | int, configured_value: float | int) -> dict[str, float | int | None]:
            delta_value = float(selected_value) - float(configured_value)
            relative = None
            if float(configured_value) != 0.0:
                relative = float(delta_value / float(configured_value))
            return {
                "selected": selected_value,
                "configured": configured_value,
                "absolute_delta": delta_value,
                "relative_delta": relative,
            }

        return {
            "max_ms": _delta(int(selected_policy.max_ms), int(configured_policy.max_ms)),
            "event_threshold": _delta(int(selected_policy.event_threshold), int(configured_policy.event_threshold)),
            "predicted_window_count": _delta(
                int(selected_cost.get("window_count") or 0),
                int(configured_cost.get("window_count") or 0),
            ),
            "pair_cost_proxy": _delta(
                float(selected_cost.get("pair_cost_proxy") or 0.0),
                float(configured_cost.get("pair_cost_proxy") or 0.0),
            ),
        }

    @staticmethod
    def _warning_reasons(
        *,
        selected_profile_row: dict[str, Any] | None,
        configured_profile_row: dict[str, Any] | None,
        edge_stability: dict[str, Any],
        evaluation_spec: WindowPolicyEvaluationSpec,
    ) -> list[str]:
        warning_reasons: list[str] = []
        selected_policy_penalty = float(((selected_profile_row or {}).get("balance_penalty")) or 0.0)
        configured_policy_penalty = float(((configured_profile_row or {}).get("balance_penalty")) or 0.0)
        if configured_policy_penalty > 0.0:
            policy_penalty_ratio = float(selected_policy_penalty / configured_policy_penalty)
            if policy_penalty_ratio > float(evaluation_spec.warning_policy_penalty_ratio):
                warning_reasons.append("policy_penalty_exceeds_configured_baseline")
        mean_boundary_jaccard = edge_stability.get("mean_boundary_jaccard")
        if mean_boundary_jaccard is not None and float(mean_boundary_jaccard) < float(evaluation_spec.warning_min_boundary_jaccard):
            warning_reasons.append("boundary_stability_below_threshold")
        return warning_reasons

    def _edge_stability(
        self,
        events_df: "DataFrame",
        *,
        selected_policy: WindowPolicy,
        full_windows_df: "DataFrame",
        evaluation_spec: WindowPolicyEvaluationSpec,
    ) -> dict[str, Any]:
        key_columns = ["tail_id", "flight_id", "t_start", "t_end"]
        flight_count = int(events_df.select("tail_id", "flight_id").distinct().count())
        if flight_count <= 0:
            return {"status": "skipped", "reason": "events are empty", "samples": [], "mean_boundary_jaccard": None}
        if flight_count < 2:
            return {"status": "skipped", "reason": "need at least two flights for subset stability", "samples": [], "mean_boundary_jaccard": None}

        full_windows_df = full_windows_df.persist()
        samples: list[dict[str, Any]] = []
        try:
            for sample_index in range(int(max(evaluation_spec.stability_sample_count, 0))):
                subset_events_df, subset_flight_count = self._flight_subset_events(
                    events_df,
                    salt=sample_index + 1,
                    sample_fraction=float(evaluation_spec.stability_sample_fraction),
                    max_flights=int(evaluation_spec.max_stability_flights),
                )
                if subset_flight_count <= 0:
                    samples.append(
                        {
                            "sample_index": sample_index,
                            "status": "skipped",
                            "reason": "subset contains no flights",
                        }
                    )
                    continue
                subset_keys_df = subset_events_df.select("tail_id", "flight_id").distinct().persist()
                subset_windows_df = WindowsTable.from_events(
                    subset_events_df,
                    max_ms=int(selected_policy.max_ms),
                    event_threshold=int(selected_policy.event_threshold),
                    min_ms=int(selected_policy.min_ms),
                    inactivity_timeout_ms=int(selected_policy.inactivity_timeout_ms),
                    strategy=self.spec.strategy,
                ).to_dataframe().persist()
                baseline_subset_df = (
                    full_windows_df.join(subset_keys_df, on=["tail_id", "flight_id"], how="inner").select(*key_columns).distinct().persist()
                )
                subset_boundary_df = subset_windows_df.select(*key_columns).distinct().persist()
                try:
                    baseline_count = int(baseline_subset_df.count())
                    subset_count = int(subset_boundary_df.count())
                    intersection_count = int(
                        subset_boundary_df.join(baseline_subset_df, on=key_columns, how="inner").count()
                    )
                    union_count = int(baseline_count + subset_count - intersection_count)
                    subset_metrics = self._window_metrics(subset_windows_df)
                    samples.append(
                        {
                            "sample_index": sample_index,
                            "status": "ok",
                            "sample_flight_count": subset_flight_count,
                            "baseline_window_count": baseline_count,
                            "subset_window_count": subset_count,
                            "boundary_jaccard": (
                                float(intersection_count / union_count)
                                if union_count > 0
                                else None
                            ),
                            "p95_event_count": float(
                                ((subset_metrics.get("closure_mix") or {}).get("p95_event_count")) or 0.0
                            ),
                        }
                    )
                finally:
                    _safe_unpersist(subset_boundary_df, label="subset_boundary_df")
                    _safe_unpersist(baseline_subset_df, label="baseline_subset_df")
                    _safe_unpersist(subset_windows_df, label="subset_windows_df")
                    _safe_unpersist(subset_keys_df, label="subset_keys_df")
        finally:
            _safe_unpersist(full_windows_df, label="full_windows_df")

        usable_jaccards = [
            float(sample["boundary_jaccard"])
            for sample in samples
            if sample.get("status") == "ok" and sample.get("boundary_jaccard") is not None
        ]
        return {
            "status": "ok" if usable_jaccards else "skipped",
            "sample_count": len(samples),
            "full_flight_count": flight_count,
            "mean_boundary_jaccard": (
                float(sum(usable_jaccards) / float(len(usable_jaccards)))
                if usable_jaccards
                else None
            ),
            "samples": samples,
        }

    def build_evaluation_report(
        self,
        events_df: "DataFrame",
        *,
        profile_df: "DataFrame | None",
        evaluation_spec: WindowPolicyEvaluationSpec | None = None,
        coverage_timestamps_df: "DataFrame | None" = None,
    ) -> dict[str, Any]:
        evaluation_spec = evaluation_spec or WindowPolicyEvaluationSpec(
            max_stability_flights=int(self.spec.max_profile_flights),
        )
        managed_profile_df = profile_df.persist() if profile_df is not None else None
        if managed_profile_df is not None:
            managed_profile_df.count()
        configured_policy = self.spec.fallback_policy
        try:
            selected_policy, policy_source = self.resolve_selected_policy(
                managed_profile_df,
                fallback_policy=configured_policy,
            )
            selected_profile_row = self._public_profile_row(self._selected_profile_row(managed_profile_df))
            event_count = int(events_df.count())
            flight_count = int(events_df.select("tail_id", "flight_id").distinct().count()) if event_count > 0 else 0
            if event_count <= 0:
                return {
                    "status": "skipped",
                    "reason": "events are empty",
                    "selected_policy": {
                        "policy_source": policy_source,
                        "resolved_policy": {
                            "max_ms": int(selected_policy.max_ms),
                            "event_threshold": int(selected_policy.event_threshold),
                            "min_ms": int(selected_policy.min_ms),
                            "inactivity_timeout_ms": int(selected_policy.inactivity_timeout_ms),
                        },
                        "configured_policy": {
                            "max_ms": int(configured_policy.max_ms),
                            "event_threshold": int(configured_policy.event_threshold),
                            "min_ms": int(configured_policy.min_ms),
                            "inactivity_timeout_ms": int(configured_policy.inactivity_timeout_ms),
                        },
                        "profile_row": selected_profile_row,
                    },
                    "candidate_frontier": [],
                    "selection_delta_vs_configured": {},
                    "edge_stability": {"status": "skipped", "reason": "events are empty", "samples": []},
                    "closure_mix": {"status": "skipped", "reason": "events are empty"},
                    "downstream_cost_proxy": {"status": "skipped", "reason": "events are empty"},
                }

            selected_profile_metrics = self._metrics_from_profile_row(selected_profile_row)
            configured_profile_row = self._profile_row_for_policy(managed_profile_df, policy=configured_policy)
            configured_metrics = self._metrics_from_profile_row(configured_profile_row)
            selected_metrics = selected_profile_metrics
            if flight_count < 2:
                edge_stability = {"status": "skipped", "reason": "need at least two flights for subset stability", "samples": [], "mean_boundary_jaccard": None}
            else:
                selected_windows_df = WindowsTable.from_events(
                    events_df,
                    max_ms=int(selected_policy.max_ms),
                    event_threshold=int(selected_policy.event_threshold),
                    min_ms=int(selected_policy.min_ms),
                    inactivity_timeout_ms=int(selected_policy.inactivity_timeout_ms),
                    strategy=self.spec.strategy,
                    coverage_timestamps_df=coverage_timestamps_df,
                ).to_dataframe().persist()
                try:
                    edge_stability = self._edge_stability(
                        events_df,
                        selected_policy=selected_policy,
                        full_windows_df=selected_windows_df,
                        evaluation_spec=evaluation_spec,
                    )
                finally:
                    _safe_unpersist(selected_windows_df, label="selected_windows_df")

            selection_delta = self._selection_delta_vs_configured(
                selected_policy=selected_policy,
                configured_policy=configured_policy,
                selected_metrics=selected_metrics,
                configured_metrics=configured_metrics,
            )
            warning_reasons = self._warning_reasons(
                selected_profile_row=selected_profile_row,
                configured_profile_row=configured_profile_row,
                edge_stability=edge_stability,
                evaluation_spec=evaluation_spec,
            )
            status = "warning" if warning_reasons else "ok"
            return {
                "status": status,
                "selected_policy": {
                    "policy_source": policy_source,
                    "resolved_policy": {
                        "max_ms": int(selected_policy.max_ms),
                        "event_threshold": int(selected_policy.event_threshold),
                        "min_ms": int(selected_policy.min_ms),
                        "inactivity_timeout_ms": int(selected_policy.inactivity_timeout_ms),
                    },
                    "configured_policy": {
                        "max_ms": int(configured_policy.max_ms),
                        "event_threshold": int(configured_policy.event_threshold),
                        "min_ms": int(configured_policy.min_ms),
                        "inactivity_timeout_ms": int(configured_policy.inactivity_timeout_ms),
                    },
                    "profile_row": selected_profile_row,
                },
                "candidate_frontier": [] if managed_profile_df is None else self._candidate_frontier(
                    managed_profile_df,
                    limit=int(evaluation_spec.candidate_frontier_size),
                ),
                "selection_delta_vs_configured": selection_delta,
                "edge_stability": edge_stability,
                "closure_mix": dict(selected_metrics.get("closure_mix") or {}),
                "downstream_cost_proxy": dict(selected_metrics.get("downstream_cost_proxy") or {}),
                "warnings": warning_reasons,
            }
        finally:
            if managed_profile_df is not None:
                _safe_unpersist(managed_profile_df, label="managed_profile_df")

    def build_dataframe(self, events_df: "DataFrame", *, coverage_timestamps_df: "DataFrame | None" = None) -> "DataFrame":
        spark = events_df.sparkSession
        F = _spark_functions()
        from pyspark.sql import Window as SparkWindow

        sampled_events_df, _ = self._sample_event_flights(events_df)
        sampled_events_df = sampled_events_df.persist()
        sampled_event_count = 0
        sampled_flight_count = 0
        try:
            try:
                sampled_event_count = int(sampled_events_df.count())
                sampled_flight_count = int(sampled_events_df.select("tail_id", "flight_id").distinct().count())
                median_gap_ms, gap_quantiles = self._gap_statistics(sampled_events_df)
                candidates = self.spec.candidate_policies(median_gap_ms=median_gap_ms, upper_gap_ms=gap_quantiles)
                candidate_rows: list[dict[str, Any]] = []
                for policy in candidates:
                    candidate_row = (
                        self._evaluate_candidate_with_coverage(
                            sampled_events_df,
                            policy=policy,
                            sampled_event_count=sampled_event_count,
                            sampled_flight_count=sampled_flight_count,
                            coverage_timestamps_df=coverage_timestamps_df,
                        )
                        .first()
                    )
                    if candidate_row is not None:
                        candidate_rows.append(dict(candidate_row.asDict(recursive=True)))
            except Exception as exc:  # pragma: no cover - exercised in Spark integration
                LOGGER.warning("window policy profile falling back to configured policy after profiling failure: %s", exc)
                return self._fallback_profile_dataframe(
                    spark,
                    sampled_event_count=sampled_event_count,
                    sampled_flight_count=sampled_flight_count,
                )
            if not candidate_rows:
                return self._fallback_profile_dataframe(
                    spark,
                    sampled_event_count=sampled_event_count,
                    sampled_flight_count=sampled_flight_count,
                )
            fallback_policy = self.spec.fallback_policy
            candidate_df = spark.createDataFrame(candidate_rows)
            ranked_df = (
                candidate_df.withColumn(
                    "balance_penalty",
                    _window_policy_penalty_expr(
                        pair_cost_proxy_col=F.col("pair_cost_proxy"),
                        same_window_pair_expansion_proxy_col=F.col("same_window_pair_expansion_proxy"),
                        sampled_event_count_col=F.col("sampled_event_count"),
                        p95_event_count_col=F.col("p95_event_count"),
                        end_of_stream_close_rate_col=F.col("end_of_stream_close_rate"),
                    ),
                )
                .withColumn(
                    "objective_score",
                    _window_policy_objective_score_expr(
                        mean_event_type_count_col=F.col("mean_event_type_count"),
                        pair_cost_proxy_col=F.col("pair_cost_proxy"),
                        same_window_pair_expansion_proxy_col=F.col("same_window_pair_expansion_proxy"),
                        sampled_event_count_col=F.col("sampled_event_count"),
                        p95_event_count_col=F.col("p95_event_count"),
                        end_of_stream_close_rate_col=F.col("end_of_stream_close_rate"),
                    ),
                )
                .withColumn("_max_ms_distance", F.abs(F.col("max_ms") - F.lit(int(fallback_policy.max_ms))))
                .withColumn(
                    "_event_threshold_distance",
                    F.abs(F.col("event_threshold") - F.lit(int(fallback_policy.event_threshold))),
                )
            )
            ranking_window = SparkWindow.orderBy(
                F.col("objective_score").desc(),
                F.col("balance_penalty").asc(),
                F.col("pair_cost_proxy").asc(),
                F.col("same_window_pair_expansion_proxy").asc(),
                F.col("p95_event_count").asc(),
                F.col("_max_ms_distance").asc(),
                F.col("_event_threshold_distance").asc(),
            )
            return ranked_df.withColumn("candidate_rank", F.row_number().over(ranking_window)).withColumn(
                "is_selected",
                F.col("candidate_rank") == F.lit(1),
            ).select(*WINDOW_POLICY_PROFILE_SCHEMA().fieldNames())
        finally:
            _safe_unpersist(sampled_events_df, label="sampled_events_df")

    @classmethod
    def resolve_selected_policy(cls, profile_df: "DataFrame | None", *, fallback_policy: WindowPolicy) -> tuple[WindowPolicy, str]:
        if profile_df is None:
            return fallback_policy, "configured"
        selected_row = (
            profile_df.where(_spark_functions().col("is_selected") == _spark_functions().lit(True))
            .orderBy(_spark_functions().col("candidate_rank").asc())
            .limit(1)
            .first()
        )
        if selected_row is None:
            return fallback_policy, "configured"
        return (
            WindowPolicy(
                max_ms=int(selected_row["max_ms"]),
                event_threshold=int(selected_row["event_threshold"]),
                min_ms=int(selected_row["min_ms"]),
                inactivity_timeout_ms=int(selected_row["inactivity_timeout_ms"]),
            ),
            "profile",
        )

def build_window_policy_profile_evaluation_report_spark(
    events_df: "DataFrame",
    *,
    profile_df: "DataFrame | None",
    profile_spec: WindowPolicyProfileSpec,
    evaluation_spec: WindowPolicyEvaluationSpec | None = None,
    coverage_timestamps_df: "DataFrame | None" = None,
) -> dict[str, Any]:
    profile = WindowPolicyProfile(spec=profile_spec)
    try:
        return profile.build_evaluation_report(
            events_df,
            profile_df=profile_df,
            evaluation_spec=evaluation_spec,
            coverage_timestamps_df=coverage_timestamps_df,
        )
    except Exception as exc:  # pragma: no cover - exercised in Spark integration
        LOGGER.warning("window policy evaluation skipped after profiling/evaluation failure: %s", exc)
        return profile._skipped_evaluation_report(
            reason=f"evaluation_failed: {exc!r}",
            profile_df=profile_df,
        )
