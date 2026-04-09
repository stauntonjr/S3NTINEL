"""Class-oriented canonical windows-table builder."""

from __future__ import annotations

from dataclasses import dataclass, field

from libs.common import empty_map
from libs.perf.annotations import hot_path
from libs.spark_sequence import (
    SegmentedSequencePlan,
    SequenceOrderingPolicy,
    SequenceSegmentPolicy,
    segment_policy_from_env,
)
from libs.windows.buffer import WindowSensorBuffer
from libs.windows.window import WindowClosureBudgetPolicy, WindowPolicy


def _default_window_segment_policy() -> SequenceSegmentPolicy:
    return segment_policy_from_env(
        "WINDOW",
        default_max_rows_per_segment=50_000,
        default_max_span_ms=900_000,
    )


def build_window_coverage_timestamps(raw_df: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return (
        raw_df.select("tail_id", "flight_id", "timestamp_utc", "date_utc")
        .where(F.col("timestamp_utc").isNotNull())
        .distinct()
    )


def build_budget_threshold_steps(
    events_df: "DataFrame",
    *,
    policy: WindowPolicy,
) -> "DataFrame":
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    budget_policy = WindowClosureBudgetPolicy.from_window_policy(policy)
    threshold = max(int(budget_policy.event_threshold), 1)
    event_window = Window.partitionBy("tail_id", "flight_id").orderBy("event_seq_id")
    flight_window = Window.partitionBy("tail_id", "flight_id")
    boundary_index_col = F.sequence(F.col("_boundary_start").cast("long"), F.col("_boundary_end").cast("long"))
    return (
        events_df.select("tail_id", "flight_id", "event_seq_id", "timestamp_utc", "date_utc")
        .withColumn("_event_ordinal", F.row_number().over(event_window).cast("long"))
        .withColumn("_flight_start_ts", F.min("timestamp_utc").over(flight_window))
        .withColumn("_elapsed_ms", (F.unix_millis("timestamp_utc") - F.unix_millis("_flight_start_ts")).cast("long"))
        .withColumn("_next_ts", F.lead("timestamp_utc").over(event_window))
        .withColumn("_next_elapsed_ms", (F.unix_millis("_next_ts") - F.unix_millis("_flight_start_ts")).cast("long"))
        .withColumn(
            "_credit_after_event",
            budget_policy.closure_budget_expr(
                duration_ms=F.col("_elapsed_ms"),
                real_event_count=F.col("_event_ordinal"),
            ),
        )
        .withColumn(
            "_credit_before_next_event",
            budget_policy.closure_budget_expr(
                duration_ms=F.col("_next_elapsed_ms"),
                real_event_count=F.col("_event_ordinal"),
            ),
        )
        .withColumn("_boundary_start", (F.floor(F.col("_credit_after_event") / F.lit(float(threshold))) + F.lit(1)).cast("long"))
        .withColumn("_boundary_end", F.floor(F.col("_credit_before_next_event") / F.lit(float(threshold))).cast("long"))
        .where(F.col("_next_ts").isNotNull() & (F.col("_boundary_end") >= F.col("_boundary_start")))
        .withColumn("_boundary_index", F.explode(boundary_index_col))
        .withColumn(
            "_boundary_elapsed_ms",
            budget_policy.elapsed_ms_for_budget_units_expr(
                budget_units=(
                    (F.col("_boundary_index").cast("double") * F.lit(float(threshold)))
                    - F.col("_event_ordinal").cast("double")
                )
            ),
        )
        .select(
            "tail_id",
            "flight_id",
            F.lit(None).cast("long").alias("event_seq_id"),
            F.timestamp_millis(F.unix_millis("_flight_start_ts") + F.col("_boundary_elapsed_ms")).alias("timestamp_utc"),
            F.to_date(F.timestamp_millis(F.unix_millis("_flight_start_ts") + F.col("_boundary_elapsed_ms"))).alias("date_utc"),
            F.lit(None).cast("string").alias("parameter_name"),
            F.lit(None).cast("string").alias("event_type_detected"),
            F.lit(True).alias("is_budget_step"),
        )
    )


@dataclass(frozen=True)
class OpenWindowState:
    win_id: str = "open_win_id"
    t_start: str = "open_t_start"
    t_end: str = "open_t_end"
    start_event_seq_id: str = "open_start_event_seq_id"
    end_event_seq_id: str = "open_end_event_seq_id"
    event_count: str = "open_event_count"


@dataclass(frozen=True)
class AdaptiveWindowSegmentState:
    next_win_id: str = "next_win_id"
    has_open_window: str = "has_open_window"
    open_state: OpenWindowState = field(default_factory=OpenWindowState)
    closed_windows: str = "closed_windows"

    def window_summary_array_type(self) -> str:
        return (
            "array<struct<"
            "tail_id:string,"
            "flight_id:string,"
            "win_id:bigint,"
            "t_start:timestamp,"
            "t_end:timestamp,"
            "start_event_seq_id:bigint,"
            "end_event_seq_id:bigint,"
            "event_count:int,"
            "real_event_count:int,"
            "quiet_credit_end:double,"
            "closure_budget_end:double,"
            "close_reason:string,"
            "date_utc:date"
            ">>"
        )

    def empty_windows_column(self) -> "Column":
        from pyspark.sql import functions as F

        return F.array().cast(self.window_summary_array_type())

    def window_summary_column(
        self,
        *,
        tail_id: "Column",
        flight_id: "Column",
        win_id: "Column",
        t_start: "Column",
        t_end: "Column",
        start_event_seq_id: "Column",
        end_event_seq_id: "Column",
        event_count: "Column",
        real_event_count: "Column",
        quiet_credit_end: "Column",
        closure_budget_end: "Column",
        close_reason: "Column",
        date_utc: "Column",
    ) -> "Column":
        from pyspark.sql import functions as F

        return F.struct(
            tail_id.cast("string").alias("tail_id"),
            flight_id.cast("string").alias("flight_id"),
            win_id.cast("long").alias("win_id"),
            t_start.cast("timestamp").alias("t_start"),
            t_end.cast("timestamp").alias("t_end"),
            start_event_seq_id.cast("long").alias("start_event_seq_id"),
            end_event_seq_id.cast("long").alias("end_event_seq_id"),
            event_count.cast("int").alias("event_count"),
            real_event_count.cast("int").alias("real_event_count"),
            quiet_credit_end.cast("double").alias("quiet_credit_end"),
            closure_budget_end.cast("double").alias("closure_budget_end"),
            close_reason.cast("string").alias("close_reason"),
            date_utc.cast("date").alias("date_utc"),
        )

    def initial_state_column(self) -> "Column":
        from pyspark.sql import functions as F

        open_state = self.open_state
        return F.struct(
            F.lit(1).cast("long").alias(self.next_win_id),
            F.lit(False).alias(self.has_open_window),
            F.lit(None).cast("long").alias(open_state.win_id),
            F.lit(None).cast("timestamp").alias(open_state.t_start),
            F.lit(None).cast("timestamp").alias(open_state.t_end),
            F.lit(None).cast("long").alias(open_state.start_event_seq_id),
            F.lit(None).cast("long").alias(open_state.end_event_seq_id),
            F.lit(0).cast("int").alias(open_state.event_count),
            self.empty_windows_column().alias(self.closed_windows),
        )

    def carry_state_column(self, *, state: "Column") -> "Column":
        from pyspark.sql import functions as F

        open_state = self.open_state
        return F.struct(
            state[self.next_win_id].alias(self.next_win_id),
            state[self.has_open_window].alias(self.has_open_window),
            state[open_state.win_id].alias(open_state.win_id),
            state[open_state.t_start].alias(open_state.t_start),
            state[open_state.t_end].alias(open_state.t_end),
            state[open_state.start_event_seq_id].alias(open_state.start_event_seq_id),
            state[open_state.end_event_seq_id].alias(open_state.end_event_seq_id),
            state[open_state.event_count].alias(open_state.event_count),
            self.empty_windows_column().alias(self.closed_windows),
        )


@dataclass(frozen=True)
class AdaptiveWindowPolicy:
    max_ms: int
    event_threshold: int
    min_ms: int
    inactivity_timeout_ms: int = 0
    segment_policy: SequenceSegmentPolicy = field(default_factory=_default_window_segment_policy)

    def to_window_policy(self) -> WindowPolicy:
        return WindowPolicy(
            max_ms=int(self.max_ms),
            event_threshold=int(self.event_threshold),
            min_ms=int(self.min_ms),
            inactivity_timeout_ms=int(self.inactivity_timeout_ms),
        )


@dataclass(frozen=True)
class AdaptiveWindowTransition:
    policy: WindowPolicy
    state: AdaptiveWindowSegmentState = field(default_factory=AdaptiveWindowSegmentState)

    def _budget_policy(self) -> WindowClosureBudgetPolicy:
        return WindowClosureBudgetPolicy.from_window_policy(self.policy)

    def _elapsed_ms_expr(self, *, t_start: "Column", t_end: "Column") -> "Column":
        from pyspark.sql import functions as F

        return (F.unix_millis(t_end) - F.unix_millis(t_start)).cast("long")

    def _window_summary_from_ms_column(
        self,
        *,
        tail_id: "Column",
        flight_id: "Column",
        win_id: "Column",
        t_start_ms: "Column",
        t_end_ms: "Column",
        start_event_seq_id: "Column",
        end_event_seq_id: "Column",
        real_event_count: "Column",
        close_reason: "Column",
    ) -> "Column":
        from pyspark.sql import functions as F

        t_start = F.timestamp_millis(t_start_ms.cast("long"))
        t_end = F.timestamp_millis(t_end_ms.cast("long"))
        duration_ms = (t_end_ms.cast("long") - t_start_ms.cast("long")).cast("long")
        budget_policy = self._budget_policy()
        quiet_credit_end = budget_policy.quiet_credit_expr(duration_ms=duration_ms)
        closure_budget_end = budget_policy.closure_budget_expr(
            duration_ms=duration_ms,
            real_event_count=real_event_count,
        )
        return self.state.window_summary_column(
            tail_id=tail_id,
            flight_id=flight_id,
            win_id=win_id,
            t_start=t_start,
            t_end=t_end,
            start_event_seq_id=start_event_seq_id,
            end_event_seq_id=end_event_seq_id,
            event_count=real_event_count,
            real_event_count=real_event_count,
            quiet_credit_end=quiet_credit_end,
            closure_budget_end=closure_budget_end,
            close_reason=close_reason,
            date_utc=F.to_date(t_start),
        )

    def state_after_step_column(
        self,
        *,
        acc: "Column",
        step: "Column",
        tail_id: "Column",
        flight_id: "Column",
    ) -> "Column":
        from pyspark.sql import functions as F

        open_state = self.state.open_state
        empty_windows = self.state.empty_windows_column()
        is_budget_step = step["is_budget_step"]
        has_event = step["event_type_detected"].isNotNull() & (F.trim(step["event_type_detected"]) != F.lit(""))
        threshold = F.lit(int(self.policy.event_threshold)).cast("int")
        inactivity_timeout_ms = int(self.policy.inactivity_timeout_ms)
        inactivity_condition = (
            F.lit(inactivity_timeout_ms > 0)
            & acc[self.state.has_open_window]
            & (acc[open_state.event_count] > F.lit(0))
            & (
                (F.unix_millis(step["timestamp_utc"]) - F.unix_millis(acc[open_state.t_end])).cast("long")
                >= F.lit(int(inactivity_timeout_ms)).cast("long")
            )
        )
        budget_boundary_condition = acc[self.state.has_open_window] & is_budget_step
        preclose_reason = (
            F.when(inactivity_condition, F.lit("inactivity_timeout"))
            .when(budget_boundary_condition, F.lit("budget_threshold"))
            .otherwise(F.lit(None).cast("string"))
        )
        preclose_t_end_ms = (
            F.when(inactivity_condition, F.unix_millis(acc[open_state.t_end]).cast("long"))
            .when(budget_boundary_condition, F.unix_millis(step["timestamp_utc"]).cast("long"))
            .otherwise(F.lit(None).cast("long"))
        )
        preclose_window = self._window_summary_from_ms_column(
            tail_id=tail_id,
            flight_id=flight_id,
            win_id=acc[open_state.win_id],
            t_start_ms=F.unix_millis(acc[open_state.t_start]).cast("long"),
            t_end_ms=preclose_t_end_ms,
            start_event_seq_id=acc[open_state.start_event_seq_id],
            end_event_seq_id=acc[open_state.end_event_seq_id],
            real_event_count=acc[open_state.event_count],
            close_reason=preclose_reason,
        )
        next_win_after_preclose = acc[self.state.next_win_id] + F.when(preclose_reason.isNotNull(), F.lit(1)).otherwise(F.lit(0))
        keep_existing_open = acc[self.state.has_open_window] & preclose_reason.isNull()
        working_win_id = F.when(keep_existing_open, acc[open_state.win_id]).otherwise(next_win_after_preclose)
        working_t_start = F.when(keep_existing_open, acc[open_state.t_start]).otherwise(step["timestamp_utc"])
        working_t_end = (
            F.when(has_event, step["timestamp_utc"])
            .when(keep_existing_open, acc[open_state.t_end])
            .otherwise(step["timestamp_utc"])
        )
        working_start_event_seq_id = (
            F.when(
                has_event
                & keep_existing_open
                & ((acc[open_state.event_count] <= F.lit(0)) | acc[open_state.start_event_seq_id].isNull()),
                step["event_seq_id"],
            )
            .when(keep_existing_open, acc[open_state.start_event_seq_id])
            .otherwise(step["event_seq_id"])
        )
        working_end_event_seq_id = (
            F.when(has_event, step["event_seq_id"])
            .when(keep_existing_open, acc[open_state.end_event_seq_id])
            .otherwise(F.lit(None).cast("long"))
        )
        working_event_count = (
            F.when(
                has_event,
                F.when(keep_existing_open, acc[open_state.event_count] + F.lit(1)).otherwise(F.lit(1)),
            )
            .otherwise(F.when(keep_existing_open, acc[open_state.event_count]).otherwise(F.lit(0)))
            .cast("int")
        )
        working_duration_ms = self._elapsed_ms_expr(t_start=working_t_start, t_end=working_t_end)
        budget_policy = self._budget_policy()
        working_closure_budget = budget_policy.closure_budget_expr(
            duration_ms=working_duration_ms,
            real_event_count=working_event_count,
        )
        postclose_condition = has_event & (working_closure_budget >= threshold.cast("double"))
        postclose_window = self._window_summary_from_ms_column(
            tail_id=tail_id,
            flight_id=flight_id,
            win_id=working_win_id,
            t_start_ms=F.unix_millis(working_t_start).cast("long"),
            t_end_ms=F.unix_millis(working_t_end).cast("long"),
            start_event_seq_id=working_start_event_seq_id,
            end_event_seq_id=working_end_event_seq_id,
            real_event_count=working_event_count,
            close_reason=budget_policy.close_reason_expr(real_event_count=working_event_count),
        )
        closed_windows = F.concat(
            F.coalesce(acc[self.state.closed_windows], empty_windows),
            F.when(preclose_reason.isNotNull(), F.array(preclose_window)).otherwise(empty_windows),
            F.when(postclose_condition, F.array(postclose_window)).otherwise(empty_windows),
        )
        final_next_win_id = next_win_after_preclose + F.when(postclose_condition, F.lit(1)).otherwise(F.lit(0))
        return F.struct(
            final_next_win_id.alias(self.state.next_win_id),
            F.when(postclose_condition, F.lit(False)).otherwise(F.lit(True)).alias(self.state.has_open_window),
            F.when(postclose_condition, F.lit(None).cast("long")).otherwise(working_win_id).alias(open_state.win_id),
            F.when(postclose_condition, F.lit(None).cast("timestamp")).otherwise(working_t_start).alias(open_state.t_start),
            F.when(postclose_condition, F.lit(None).cast("timestamp")).otherwise(working_t_end).alias(open_state.t_end),
            F.when(postclose_condition, F.lit(None).cast("long"))
            .otherwise(working_start_event_seq_id)
            .alias(open_state.start_event_seq_id),
            F.when(postclose_condition, F.lit(None).cast("long"))
            .otherwise(working_end_event_seq_id)
            .alias(open_state.end_event_seq_id),
            F.when(postclose_condition, F.lit(0)).otherwise(working_event_count).cast("int").alias(open_state.event_count),
            closed_windows.alias(self.state.closed_windows),
        )

    def end_of_stream_windows_column(
        self,
        *,
        state: "Column",
        tail_id: "Column",
        flight_id: "Column",
        is_last_segment: "Column",
    ) -> "Column":
        from pyspark.sql import functions as F

        open_state = self.state.open_state
        final_window = self._window_summary_from_ms_column(
            tail_id=tail_id,
            flight_id=flight_id,
            win_id=state[open_state.win_id],
            t_start_ms=F.unix_millis(state[open_state.t_start]).cast("long"),
            t_end_ms=F.unix_millis(state[open_state.t_end]).cast("long"),
            start_event_seq_id=state[open_state.start_event_seq_id],
            end_event_seq_id=state[open_state.end_event_seq_id],
            real_event_count=state[open_state.event_count],
            close_reason=F.lit("end_of_stream"),
        )
        return F.when(
            is_last_segment & state[self.state.has_open_window] & (state[open_state.event_count] > F.lit(0)),
            F.array(final_window),
        ).otherwise(self.state.empty_windows_column())


@dataclass(frozen=True)
class AdaptiveWindowArtifactSet:
    windows_df: "DataFrame"
    segments_df: "DataFrame"


@dataclass(frozen=True)
class AdaptiveWindowPlan:
    policy: AdaptiveWindowPolicy
    sequence_plan: SegmentedSequencePlan = field(
        default_factory=lambda: SegmentedSequencePlan(
            ordering=SequenceOrderingPolicy(
                key_columns=("tail_id", "flight_id"),
                order_columns=("window_step_order",),
                timestamp_column="timestamp_utc",
            ),
            policy=_default_window_segment_policy(),
        )
    )

    def _active_sequence_plan(self) -> SegmentedSequencePlan:
        return SegmentedSequencePlan(
            ordering=self.sequence_plan.ordering,
            policy=self.policy.segment_policy,
        )

    def _validate_events(self, events_df: "DataFrame") -> None:
        required_columns = {
            "tail_id",
            "flight_id",
            "event_seq_id",
            "timestamp_utc",
            "parameter_name",
            "event_type_detected",
            "payload",
            "date_utc",
        }
        missing_columns = required_columns.difference(events_df.columns)
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(
                "AdaptiveWindowPlan.build expects canonical event rows with event ordering; "
                f"missing: {missing}"
            )

    def _build_step_rows(
        self,
        events_df: "DataFrame",
        *,
        coverage_timestamps_df: "DataFrame | None" = None,
    ) -> "DataFrame":
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        event_rows = events_df.select(
            "tail_id",
            "flight_id",
            "event_seq_id",
            "timestamp_utc",
            "date_utc",
            "parameter_name",
            "event_type_detected",
        ).withColumn("is_budget_step", F.lit(False))
        budget_rows = build_budget_threshold_steps(
            events_df,
            policy=self.policy.to_window_policy(),
        )
        combined_rows = event_rows.withColumn("_step_priority", F.lit(1)).unionByName(
            budget_rows.withColumn("_step_priority", F.lit(0)),
            allowMissingColumns=False,
        )
        return combined_rows.withColumn(
            "window_step_order",
            F.row_number().over(
                Window.partitionBy("tail_id", "flight_id").orderBy(
                    F.col("timestamp_utc").asc(),
                    F.col("_step_priority").asc(),
                    F.col("event_seq_id").asc(),
                )
            ).cast("long"),
        ).drop("_step_priority")

    def _build_segment_frame(
        self,
        events_df: "DataFrame",
        *,
        coverage_timestamps_df: "DataFrame | None" = None,
    ) -> "SegmentedSequenceFrame":
        self._validate_events(events_df)
        sequence_plan = self._active_sequence_plan()
        event_rows = self._build_step_rows(
            events_df,
            coverage_timestamps_df=coverage_timestamps_df,
        )
        segmented = sequence_plan.assign_segments(event_rows)
        segment_steps_df = sequence_plan.build_segment_steps(
            segmented.rows_df,
            step_columns=(
                "tail_id",
                "flight_id",
                "parameter_name",
                "timestamp_utc",
                "date_utc",
                "event_type_detected",
                "event_seq_id",
                "is_budget_step",
            ),
        )
        return segmented.__class__(
            rows_df=segmented.rows_df,
            segments_df=segmented.segments_df,
            segment_steps_df=segment_steps_df,
        )

    def _build_assignment_events(self, events_df: "DataFrame") -> "DataFrame":
        return events_df.select(
            "tail_id",
            "flight_id",
            "event_seq_id",
            "parameter_name",
            "event_type_detected",
            WindowSensorBuffer.spark_event_value_expr().alias("event_value"),
        )

    def _build_window_summaries(
        self,
        *,
        sequence_frame: "SegmentedSequenceFrame",
    ) -> "DataFrame":
        from pyspark.sql import functions as F

        assert sequence_frame.segment_steps_df is not None
        transition = AdaptiveWindowTransition(policy=self.policy.to_window_policy())
        sequence_plan = self._active_sequence_plan()
        key_columns = sequence_plan.ordering.key_columns
        segment_id_column = sequence_plan.ordering.segment_id_column
        segment_steps_df = sequence_frame.segment_steps_df
        last_segment_df = sequence_frame.segments_df.groupBy(*key_columns).agg(
            F.max(F.col(segment_id_column)).alias("_last_segment_id")
        )

        carry_df: "DataFrame | None" = None
        summary_frames: list["DataFrame"] = []
        segment_ids = sequence_plan.collect_segment_ids(sequence_frame.segments_df)
        initial_state = transition.state.initial_state_column()

        for segment_id in segment_ids:
            current_segments_df = (
                segment_steps_df.where(F.col(segment_id_column) == F.lit(int(segment_id)))
                .join(last_segment_df, on=list(key_columns), how="left")
                .withColumn("__tail_id", F.col("tail_id"))
                .withColumn("__flight_id", F.col("flight_id"))
                .withColumn("__is_last_segment", F.col(segment_id_column) == F.col("_last_segment_id"))
            )
            if carry_df is not None:
                current_segments_df = current_segments_df.join(carry_df, on=list(key_columns), how="left")
            else:
                current_segments_df = current_segments_df.withColumn("carry_state", initial_state)
            aggregated_df = current_segments_df.select(
                *list(key_columns),
                F.col(segment_id_column),
                F.col("__tail_id"),
                F.col("__flight_id"),
                F.col("__is_last_segment"),
                F.aggregate(
                    F.col("steps"),
                    F.coalesce(F.col("carry_state"), initial_state),
                    lambda acc, step: transition.state_after_step_column(
                        acc=acc,
                        step=step,
                        tail_id=F.col("__tail_id"),
                        flight_id=F.col("__flight_id"),
                    ),
                ).alias("state_after"),
            )
            segment_windows_df = (
                aggregated_df.select(
                    *list(key_columns),
                    F.col(segment_id_column),
                    F.concat(
                        F.col("state_after.closed_windows"),
                        transition.end_of_stream_windows_column(
                            state=F.col("state_after"),
                            tail_id=F.col("__tail_id"),
                            flight_id=F.col("__flight_id"),
                            is_last_segment=F.col("__is_last_segment"),
                        ),
                    ).alias("windows_out"),
                )
                .select(F.explode_outer(F.col("windows_out")).alias("window"))
                .where(F.col("window").isNotNull())
                .select("window.*")
            )
            summary_frames.append(segment_windows_df)
            carry_df = (
                aggregated_df.where(~F.col("__is_last_segment"))
                .select(
                    *list(key_columns),
                    transition.state.carry_state_column(state=F.col("state_after")).alias("carry_state"),
                )
            )

        if not summary_frames:
            spark = sequence_frame.rows_df.sparkSession
            return spark.createDataFrame(
                [],
                schema=(
                    "tail_id string, flight_id string, win_id long, "
                    "t_start timestamp, t_end timestamp, "
                    "start_event_seq_id long, end_event_seq_id long, "
                    "event_count int, real_event_count int, "
                    "quiet_credit_end double, closure_budget_end double, "
                    "close_reason string, date_utc date"
                ),
            )
        summary_df = summary_frames[0]
        for frame in summary_frames[1:]:
            summary_df = summary_df.unionByName(frame, allowMissingColumns=False)
        return summary_df

    @staticmethod
    def _empty_string_map_expr() -> "Column":
        return empty_map()

    @staticmethod
    def _empty_int_map_expr() -> "Column":
        return empty_map("string", "int")

    def _build_assignments(self, *, event_rows_df: "DataFrame", window_summaries_df: "DataFrame") -> "DataFrame":
        from pyspark.sql import functions as F

        assignable_events_df = event_rows_df.where(
            F.col("event_type_detected").isNotNull() & (F.length(F.trim(F.col("event_type_detected"))) > 0)
        ).alias("events")
        window_ranges_df = window_summaries_df.select(
            "tail_id",
            "flight_id",
            "win_id",
            "start_event_seq_id",
            "end_event_seq_id",
        ).alias("windows")
        join_condition = (
            (F.col("events.tail_id") == F.col("windows.tail_id"))
            & (F.col("events.flight_id") == F.col("windows.flight_id"))
            & (F.col("events.event_seq_id") >= F.col("windows.start_event_seq_id"))
            & (F.col("events.event_seq_id") <= F.col("windows.end_event_seq_id"))
        )
        return assignable_events_df.join(
            window_ranges_df,
            on=join_condition,
            how="inner",
        ).select(
            F.col("events.tail_id").alias("tail_id"),
            F.col("events.flight_id").alias("flight_id"),
            F.col("windows.win_id").alias("win_id"),
            F.col("events.event_seq_id").alias("event_seq_id"),
            F.col("events.parameter_name").alias("parameter_name"),
            F.col("events.event_type_detected").alias("event_type_detected"),
            F.col("events.event_value").alias("event_value"),
        )

    def _build_windows_from_summaries(
        self,
        *,
        event_rows_df: "DataFrame",
        window_summaries_df: "DataFrame",
    ) -> "DataFrame":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        policy = self.policy.to_window_policy()
        duration_ms_col = policy.duration_ms_expr(t_start=F.col("t_start"), t_end=F.col("t_end"))
        base_windows_df = window_summaries_df.select(
            "tail_id",
            "flight_id",
            "win_id",
            "t_start",
            "t_end",
            F.greatest(duration_ms_col, F.lit(int(self.policy.min_ms))).cast("int").alias("duration_ms"),
            F.col("event_count").cast("int").alias("event_count"),
            F.col("real_event_count").cast("int").alias("real_event_count"),
            F.col("quiet_credit_end").cast("double").alias("quiet_credit_end"),
            F.col("closure_budget_end").cast("double").alias("closure_budget_end"),
            "close_reason",
            "date_utc",
        )
        assignments_df = self._build_assignments(
            event_rows_df=event_rows_df,
            window_summaries_df=window_summaries_df,
        )
        event_type_counts_df = (
            assignments_df.groupBy("tail_id", "flight_id", "win_id", "event_type_detected")
            .agg(F.count(F.lit(1)).cast("int").alias("event_type_count"))
            .groupBy("tail_id", "flight_id", "win_id")
            .agg(
                F.map_from_entries(
                    F.collect_list(F.struct(F.col("event_type_detected"), F.col("event_type_count")))
                ).alias("event_type_counts")
            )
        )
        last_event_window = Window.partitionBy("tail_id", "flight_id", "win_id", "parameter_name").orderBy(
            F.col("event_seq_id").desc()
        )
        snapshot_rows_df = (
            assignments_df.withColumn("_snapshot_rank", F.row_number().over(last_event_window))
            .where(F.col("_snapshot_rank") == F.lit(1))
            .drop("_snapshot_rank")
        )
        snapshot_df = snapshot_rows_df.groupBy("tail_id", "flight_id", "win_id").agg(
            F.count(F.lit(1)).cast("int").alias("sensor_count"),
            F.map_from_entries(
                F.collect_list(F.struct(F.col("parameter_name"), F.col("event_value")))
            ).alias("zoh_snapshot"),
        )
        return (
            base_windows_df.join(event_type_counts_df, on=["tail_id", "flight_id", "win_id"], how="left")
            .join(snapshot_df, on=["tail_id", "flight_id", "win_id"], how="left")
            .select(
                "tail_id",
                "flight_id",
                "win_id",
                "t_start",
                "t_end",
                "duration_ms",
                "event_count",
                "real_event_count",
                "quiet_credit_end",
                "closure_budget_end",
                F.coalesce(F.col("sensor_count"), F.lit(0).cast("int")).alias("sensor_count"),
                F.coalesce(F.col("event_type_counts"), self._empty_int_map_expr()).alias("event_type_counts"),
                F.coalesce(F.col("zoh_snapshot"), self._empty_string_map_expr()).alias("zoh_snapshot"),
                "close_reason",
                F.lit(1).cast("int").alias("zoh_version"),
                "date_utc",
            )
        )

    @hot_path
    def build(self, events_df: "DataFrame") -> AdaptiveWindowArtifactSet:
        return self.build_with_coverage(events_df, coverage_timestamps_df=None)

    @hot_path
    def build_with_coverage(
        self,
        events_df: "DataFrame",
        *,
        coverage_timestamps_df: "DataFrame | None" = None,
    ) -> AdaptiveWindowArtifactSet:
        from pyspark import StorageLevel

        sequence_frame = self._build_segment_frame(events_df, coverage_timestamps_df=coverage_timestamps_df)
        assignment_events_df = self._build_assignment_events(events_df)
        window_summaries_df = self._build_window_summaries(sequence_frame=sequence_frame).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        try:
            window_summaries_df.count()
            windows_df = self._build_windows_from_summaries(
                event_rows_df=assignment_events_df,
                window_summaries_df=window_summaries_df,
            )
        finally:
            window_summaries_df.unpersist()
        return AdaptiveWindowArtifactSet(windows_df=windows_df, segments_df=sequence_frame.segments_df)



from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame
    from libs.spark_sequence import SegmentedSequenceFrame
