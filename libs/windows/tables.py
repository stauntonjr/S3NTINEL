"""Typed Spark windows artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from libs.io.schemas.windows import WINDOWS_SCHEMA, WINDOW_POLICY_PROFILE_SCHEMA, WINDOW_X_SCHEMA
from libs.pyspark import Frame, Table


@dataclass(frozen=True)
class WindowProfileRowsFrame(Frame):
    @classmethod
    def from_events(
        cls,
        events_df: "DataFrame",
        *,
        max_ms: int,
        event_threshold: int,
        min_ms: int,
        inactivity_timeout_ms: int = 0,
        strategy: str = "segmented",
        coverage_timestamps_df: "DataFrame | None" = None,
    ) -> "WindowProfileRowsFrame":
        from libs.windows.pipeline import AdaptiveWindowPlan, AdaptiveWindowPolicy
        from pyspark.sql import functions as F

        resolved_strategy = str(strategy).strip().lower()
        if resolved_strategy != "segmented":
            raise ValueError("WindowProfileRowsFrame.from_events supports only the canonical Spark strategy: segmented")
        plan = AdaptiveWindowPlan(
            policy=AdaptiveWindowPolicy(
                max_ms=int(max_ms),
                event_threshold=int(event_threshold),
                min_ms=int(min_ms),
                inactivity_timeout_ms=int(inactivity_timeout_ms),
            )
        )
        sequence_frame = plan._build_segment_frame(events_df, coverage_timestamps_df=coverage_timestamps_df)
        assignment_events_df = plan._build_assignment_events(events_df)
        window_summaries_df = plan._build_window_summaries(sequence_frame=sequence_frame)
        duration_ms_col = plan.policy.to_window_policy().duration_ms_expr(t_start=F.col("t_start"), t_end=F.col("t_end"))
        base_windows_df = window_summaries_df.select(
            "tail_id",
            "flight_id",
            "win_id",
            F.greatest(duration_ms_col, F.lit(int(plan.policy.min_ms))).cast("int").alias("duration_ms"),
            F.col("event_count").cast("int").alias("event_count"),
            F.col("real_event_count").cast("int").alias("real_event_count"),
            F.col("quiet_credit_end").cast("double").alias("quiet_credit_end"),
            F.col("closure_budget_end").cast("double").alias("closure_budget_end"),
            F.col("close_reason").cast("string").alias("close_reason"),
            "start_event_seq_id",
            "end_event_seq_id",
            "date_utc",
        )
        assignments_df = plan._build_assignments(
            event_rows_df=assignment_events_df,
            window_summaries_df=window_summaries_df,
        )
        profile_counts_df = assignments_df.groupBy("tail_id", "flight_id", "win_id").agg(
            F.countDistinct("parameter_name").cast("int").alias("sensor_count"),
            F.countDistinct("event_type_detected").cast("int").alias("event_type_count"),
        )
        return cls(
            dataframe=base_windows_df.join(profile_counts_df, on=["tail_id", "flight_id", "win_id"], how="left").select(
                "tail_id",
                "flight_id",
                "win_id",
                "duration_ms",
                "event_count",
                "real_event_count",
                "quiet_credit_end",
                "closure_budget_end",
                F.coalesce(F.col("sensor_count"), F.lit(0).cast("int")).alias("sensor_count"),
                F.coalesce(F.col("event_type_count"), F.lit(0).cast("int")).alias("event_type_count"),
                "close_reason",
                "start_event_seq_id",
                "end_event_seq_id",
                "date_utc",
            )
        )


@dataclass(frozen=True)
class WindowsTable(Table):
    @classmethod
    def spark_schema(cls):
        return WINDOWS_SCHEMA()

    @classmethod
    def from_events(
        cls,
        events_df: "DataFrame",
        *,
        max_ms: int,
        event_threshold: int,
        min_ms: int,
        inactivity_timeout_ms: int = 0,
        strategy: str = "segmented",
        coverage_timestamps_df: "DataFrame | None" = None,
    ) -> "WindowsTable":
        from libs.windows.pipeline import AdaptiveWindowPlan, AdaptiveWindowPolicy

        resolved_strategy = str(strategy).strip().lower()
        if resolved_strategy != "segmented":
            raise ValueError("WindowsTable.from_events supports only the canonical Spark strategy: segmented")
        artifact_set = AdaptiveWindowPlan(
            policy=AdaptiveWindowPolicy(
                max_ms=int(max_ms),
                event_threshold=int(event_threshold),
                min_ms=int(min_ms),
                inactivity_timeout_ms=int(inactivity_timeout_ms),
            )
        ).build_with_coverage(
            events_df,
            coverage_timestamps_df=coverage_timestamps_df,
        )
        return cls(dataframe=artifact_set.windows_df)


@dataclass(frozen=True)
class WindowFeaturesTable(Table):
    @classmethod
    def spark_schema(cls):
        return WINDOW_X_SCHEMA()

    @classmethod
    def from_raw_events_and_windows(
        cls,
        raw_df: "DataFrame",
        events_df: "DataFrame",
        windows_df: "DataFrame",
        scaling_profile_df: "DataFrame | None" = None,
    ) -> "WindowFeaturesTable":
        from libs.windows.features import WindowFeaturesPlan

        return cls(
            dataframe=WindowFeaturesPlan().build(
                raw_df,
                events_df,
                windows_df,
                scaling_profile_df=scaling_profile_df,
            )
        )

    @classmethod
    def from_raw_events_windows_with_diagnostics(
        cls,
        raw_df: "DataFrame",
        events_df: "DataFrame",
        windows_df: "DataFrame",
        scaling_profile_df: "DataFrame | None" = None,
    ) -> tuple["WindowFeaturesTable", "WindowFeaturesDiagnostics"]:
        from libs.windows.features import WindowFeaturesPlan

        dataframe, diagnostics = WindowFeaturesPlan().build_with_diagnostics(
            raw_df,
            events_df,
            windows_df,
            scaling_profile_df=scaling_profile_df,
        )
        return cls(dataframe=dataframe), diagnostics


@dataclass(frozen=True)
class WindowPolicyProfileTable(Table):
    @classmethod
    def spark_schema(cls):
        return WINDOW_POLICY_PROFILE_SCHEMA()

    @classmethod
    def from_events(
        cls,
        events_df: "DataFrame",
        *,
        spec: "WindowPolicyProfileSpec",
        coverage_timestamps_df: "DataFrame | None" = None,
    ) -> "WindowPolicyProfileTable":
        from libs.windows.policy_profile import WindowPolicyProfile

        return cls(
            dataframe=WindowPolicyProfile(spec=spec).build_dataframe(
                events_df,
                coverage_timestamps_df=coverage_timestamps_df,
            )
        )


if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from libs.windows.features import WindowFeaturesDiagnostics
    from libs.windows.policy_profile import WindowPolicyProfileSpec
