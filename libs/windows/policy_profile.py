from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from libs.io.schemas import WINDOW_POLICY_PROFILE_SCHEMA
from libs.windows.pipeline import build_windows_table
from libs.windows.window import DEFAULT_MIN_SAMPLING_RATE_HZ, WindowPolicy

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def _spark_functions():
    from pyspark.sql import functions as F

    return F


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
    target_event_threshold_close_rate: float = 0.75
    target_max_ms_close_rate: float = 0.25

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
class WindowPolicyProfile:
    spec: WindowPolicyProfileSpec

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
        from pyspark.sql import functions as F

        windows_df = build_windows_table(
            events_df,
            max_ms=int(policy.max_ms),
            event_threshold=int(policy.event_threshold),
            min_ms=int(policy.min_ms),
            inactivity_timeout_ms=int(policy.inactivity_timeout_ms),
            strategy=self.spec.strategy,
        )
        return windows_df.agg(
            F.count(F.lit(1)).cast("long").alias("predicted_window_count"),
            F.avg(F.col("duration_ms").cast("double")).alias("mean_duration_ms"),
            F.percentile_approx(F.col("duration_ms").cast("double"), F.lit(0.95), 1000).alias("p95_duration_ms"),
            F.avg(F.col("event_count").cast("double")).alias("mean_event_count"),
            F.percentile_approx(F.col("event_count").cast("double"), F.lit(0.95), 1000).alias("p95_event_count"),
            F.avg(F.col("sensor_count").cast("double")).alias("mean_sensor_count"),
            F.avg(F.size(F.map_keys("event_type_counts")).cast("double")).alias("mean_event_type_count"),
            F.avg(
                F.when(F.instr(F.col("close_reason"), "event_threshold") > F.lit(0), F.lit(1.0)).otherwise(F.lit(0.0))
            ).alias("event_threshold_close_rate"),
            F.avg(
                F.when(F.instr(F.col("close_reason"), "max_ms") > F.lit(0), F.lit(1.0)).otherwise(F.lit(0.0))
            ).alias("max_ms_close_rate"),
            F.sum((F.col("event_count").cast("double") * F.col("event_count").cast("double"))).alias("pair_cost_proxy"),
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
            F.coalesce(F.col("mean_sensor_count"), F.lit(0.0)).cast("double").alias("mean_sensor_count"),
            F.coalesce(F.col("mean_event_type_count"), F.lit(0.0)).cast("double").alias("mean_event_type_count"),
            F.coalesce(F.col("event_threshold_close_rate"), F.lit(0.0)).cast("double").alias("event_threshold_close_rate"),
            F.coalesce(F.col("max_ms_close_rate"), F.lit(0.0)).cast("double").alias("max_ms_close_rate"),
            F.coalesce(F.col("pair_cost_proxy"), F.lit(0.0)).cast("double").alias("pair_cost_proxy"),
        )

    def build_dataframe(self, events_df: "DataFrame") -> "DataFrame":
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        sampled_events_df, source_flight_count = self._sample_event_flights(events_df)
        sampled_events_df = sampled_events_df.persist()
        try:
            sampled_event_count = int(sampled_events_df.count())
            sampled_flight_count = int(sampled_events_df.select("tail_id", "flight_id").distinct().count())
            median_gap_ms, gap_quantiles = self._gap_statistics(sampled_events_df)
            candidates = self.spec.candidate_policies(median_gap_ms=median_gap_ms, upper_gap_ms=gap_quantiles)
            candidate_df = None
            for policy in candidates:
                stats_df = self._evaluate_candidate(
                    sampled_events_df,
                    policy=policy,
                    sampled_event_count=sampled_event_count,
                    sampled_flight_count=sampled_flight_count,
                )
                candidate_df = stats_df if candidate_df is None else candidate_df.unionByName(stats_df)
            if candidate_df is None:
                spark = events_df.sparkSession
                candidate_df = spark.createDataFrame([], schema=WINDOW_POLICY_PROFILE_SCHEMA())
            ranked = (
                candidate_df.withColumn(
                    "balance_penalty",
                    F.abs(F.col("event_threshold_close_rate") - F.lit(float(self.spec.target_event_threshold_close_rate)))
                    + F.abs(F.col("max_ms_close_rate") - F.lit(float(self.spec.target_max_ms_close_rate))),
                )
                .withColumn(
                    "objective_score",
                    (
                        F.col("mean_event_type_count")
                        - F.col("balance_penalty")
                        - F.log1p(F.col("pair_cost_proxy") / F.greatest(F.col("sampled_event_count").cast("double"), F.lit(1.0)))
                    ).cast("double"),
                )
            )
            rank_window = Window.orderBy(
                F.col("balance_penalty").asc(),
                F.col("pair_cost_proxy").asc(),
                F.col("mean_event_type_count").desc(),
                F.abs(F.col("max_ms") - F.lit(int(self.spec.fallback_policy.max_ms))).asc(),
                F.abs(F.col("event_threshold") - F.lit(int(self.spec.fallback_policy.event_threshold))).asc(),
            )
            return (
                ranked.withColumn("candidate_rank", F.row_number().over(rank_window))
                .withColumn("is_selected", F.col("candidate_rank") == F.lit(1))
                .select(
                    "profile_id",
                    "profile_scope",
                    "candidate_rank",
                    "is_selected",
                    "max_ms",
                    "event_threshold",
                    "min_ms",
                    "inactivity_timeout_ms",
                    "objective_score",
                    "balance_penalty",
                    "predicted_window_count",
                    "mean_duration_ms",
                    "p95_duration_ms",
                    "mean_event_count",
                    "p95_event_count",
                    "mean_sensor_count",
                    "mean_event_type_count",
                    "event_threshold_close_rate",
                    "max_ms_close_rate",
                    "pair_cost_proxy",
                    "sampled_event_count",
                    "sampled_flight_count",
                )
            )
        finally:
            sampled_events_df.unpersist()

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


def build_window_policy_profile_table(events_df: "DataFrame", *, spec: WindowPolicyProfileSpec) -> "DataFrame":
    return WindowPolicyProfile(spec=spec).build_dataframe(events_df)
