# File: libs/events/categorical.py
"""Categorical transition and missing/dropped event detection."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from libs.common import ParameterDataType, empty_array, spark_normalized_parameter_datatype_expr
from libs.events.types import (
    CategoricalDwellGuardEvent,
    DroppedEvent,
    DwellBucketEvent,
    DwellViolationEvent,
    IllegalTransitionEvent,
    StateEnterEvent,
    StateExitEvent,
    TransitionEvent,
    append_detected_events,
    empty_detected_event_array,
)
from libs.perf.annotations import hot_path
from libs.spark_sequence import SegmentedSequencePlan, SequenceOrderingPolicy, segment_policy_from_env


def _default_event_segment_policy():
    return segment_policy_from_env(
        "EVENT",
        default_max_rows_per_segment=50_000,
        default_max_span_ms=900_000,
    )


@dataclass(frozen=True)
class CategoricalDetectorConfig:
    min_dwell_seconds: float = 0.0
    max_dwell_seconds: float = 0.0
    emit_state_enter: bool = True
    emit_state_exit: bool = True
    emit_dwell_bucket: bool = True
    illegal_transitions: frozenset[tuple[str, str]] = frozenset()


@dataclass(frozen=True)
class CategoricalSequenceStateLayout:
    last_state: str = "last_state"
    last_state_ts: str = "last_state_ts"
    last_dwell_guard_ts: str = "last_dwell_guard_ts"
    missing: str = "missing"
    emitted_events: str = "emitted_events"

    def initial_state_column(self) -> "Column":
        from pyspark.sql import functions as F

        return F.struct(
            F.lit(None).cast("string").alias(self.last_state),
            F.lit(None).cast("timestamp").alias(self.last_state_ts),
            F.lit(None).cast("timestamp").alias(self.last_dwell_guard_ts),
            F.lit(False).alias(self.missing),
            empty_detected_event_array().alias(self.emitted_events),
        )

    def state_after_step_column(
        self,
        *,
        acc: "Column",
        step: "Column",
        emit_state_enter: "Column",
        emit_state_exit: "Column",
        emit_dwell_bucket: "Column",
        min_dwell_seconds: "Column",
        max_dwell_seconds: "Column",
        illegal_transition_values: "Column",
    ) -> "Column":
        from pyspark.sql import functions as F

        current_missing = step["is_missing"]
        current_state = step["current_state"]
        last_state = acc[self.last_state]
        last_state_ts = acc[self.last_state_ts]
        last_dwell_guard_ts = acc[self.last_dwell_guard_ts]
        was_missing = acc[self.missing]
        not_missing = ~current_missing
        missing_transition = current_missing & ~was_missing
        enter_condition = not_missing & (last_state.isNull() | was_missing)
        transition_condition = not_missing & ~was_missing & last_state.isNotNull() & (current_state != last_state)
        same_state_condition = not_missing & ~was_missing & last_state.isNotNull() & (current_state == last_state)
        dwell_seconds = F.when(
            last_state_ts.isNotNull(),
            (F.unix_millis(step["timestamp_utc"]) - F.unix_millis(last_state_ts)).cast("double") / F.lit(1000.0),
        ).otherwise(F.lit(0.0))
        seconds_since_guard = F.when(
            last_dwell_guard_ts.isNotNull(),
            (F.unix_millis(step["timestamp_utc"]) - F.unix_millis(last_dwell_guard_ts)).cast("double") / F.lit(1000.0),
        ).otherwise(F.lit(None).cast("double"))
        dwell_bucket = (
            F.when(dwell_seconds < F.lit(1.0), F.lit("lt_1s"))
            .when(dwell_seconds < F.lit(5.0), F.lit("1s_to_5s"))
            .when(dwell_seconds < F.lit(30.0), F.lit("5s_to_30s"))
            .otherwise(F.lit("gte_30s"))
        )
        illegal_pair_key = F.concat_ws("\u0001", last_state.cast("string"), current_state.cast("string"))
        illegal_transition_condition = (
            transition_condition
            & (F.size(illegal_transition_values) > F.lit(0))
            & F.array_contains(illegal_transition_values, illegal_pair_key)
        )
        dwell_guard_condition = (
            same_state_condition
            & (max_dwell_seconds > F.lit(0.0))
            & (dwell_seconds >= max_dwell_seconds)
            & (last_dwell_guard_ts.isNull() | (seconds_since_guard >= max_dwell_seconds))
        )

        state_enter_event = StateEnterEvent().optional_from_step(
            condition=enter_condition & emit_state_enter,
            step=step,
            from_state=F.lit("none"),
            to_state=current_state,
        )
        state_exit_missing_event = StateExitEvent().optional_from_step(
            condition=missing_transition & emit_state_exit & last_state.isNotNull(),
            step=step,
            from_state=last_state,
            to_state=F.lit("missing"),
        )
        dropped_event = DroppedEvent().optional_from_step(
            condition=missing_transition,
            step=step,
            from_state=F.coalesce(last_state, F.lit("none")),
            to_state=F.lit("missing"),
        )
        state_exit_transition_event = StateExitEvent().optional_from_step(
            condition=transition_condition & emit_state_exit,
            step=step,
            from_state=last_state,
            to_state=current_state,
            dwell_seconds=dwell_seconds,
        )
        dwell_bucket_event = DwellBucketEvent().optional_from_step(
            condition=transition_condition & emit_dwell_bucket,
            step=step,
            state=last_state,
            dwell_seconds=dwell_seconds,
            bucket=dwell_bucket,
        )
        transition_event = TransitionEvent().optional_from_step(
            condition=transition_condition,
            step=step,
            from_state=last_state,
            to_state=current_state,
            dwell_seconds=dwell_seconds,
        )
        dwell_violation_event = DwellViolationEvent().optional_from_step(
            condition=transition_condition & (min_dwell_seconds > F.lit(0.0)) & (dwell_seconds < min_dwell_seconds),
            step=step,
            from_state=last_state,
            to_state=current_state,
            dwell_seconds=dwell_seconds,
            min_dwell_seconds=min_dwell_seconds,
        )
        illegal_transition_event = IllegalTransitionEvent().optional_from_step(
            condition=illegal_transition_condition,
            step=step,
            from_state=last_state,
            to_state=current_state,
        )
        dwell_guard_event = CategoricalDwellGuardEvent().optional_from_step(
            condition=dwell_guard_condition,
            step=step,
            state=current_state,
            dwell_seconds=dwell_seconds,
            max_dwell_seconds=max_dwell_seconds,
        )
        emitted_events = append_detected_events(
            acc[self.emitted_events],
            state_enter_event,
            state_exit_missing_event,
            dropped_event,
            state_exit_transition_event,
            dwell_bucket_event,
            transition_event,
            dwell_violation_event,
            illegal_transition_event,
            dwell_guard_event,
        )
        return F.struct(
            F.when(current_missing, F.lit(None).cast("string")).otherwise(current_state.cast("string")).alias(self.last_state),
            F.when(current_missing | enter_condition | transition_condition, step["timestamp_utc"]).otherwise(last_state_ts).alias(self.last_state_ts),
            F.when(current_missing, step["timestamp_utc"])
            .when(enter_condition | transition_condition, F.lit(None).cast("timestamp"))
            .when(dwell_guard_condition, step["timestamp_utc"])
            .otherwise(last_dwell_guard_ts)
            .alias(self.last_dwell_guard_ts),
            current_missing.alias(self.missing),
            emitted_events.alias(self.emitted_events),
        )

    def carry_state_column(self, *, state: "Column") -> "Column":
        from pyspark.sql import functions as F

        return F.struct(
            state[self.last_state].alias(self.last_state),
            state[self.last_state_ts].alias(self.last_state_ts),
            state[self.last_dwell_guard_ts].alias(self.last_dwell_guard_ts),
            state[self.missing].alias(self.missing),
            empty_detected_event_array().alias(self.emitted_events),
        )


@dataclass(frozen=True)
class CategoricalEventDetector:
    config: CategoricalDetectorConfig = field(default_factory=CategoricalDetectorConfig)
    state_layout: CategoricalSequenceStateLayout = field(default_factory=CategoricalSequenceStateLayout)
    sequence_plan: SegmentedSequencePlan = field(
        default_factory=lambda: SegmentedSequencePlan(
            ordering=SequenceOrderingPolicy(
                key_columns=("tail_id", "flight_id", "parameter_name"),
                order_columns=("sample_seq_id",),
                timestamp_column="timestamp_utc",
                row_number_column="sample_seq_id",
            ),
            policy=_default_event_segment_policy(),
        )
    )

    def _prepare_source(self, raw_df: "DataFrame") -> "DataFrame":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        parameter_col = "parameter_name" if "parameter_name" in raw_df.columns else "sensor"
        source_df = raw_df.select(
            *[F.col(column_name) for column_name in raw_df.columns if column_name != "parameter_name"],
            F.col(parameter_col).cast("string").alias("parameter_name"),
        )
        if "parameter_datatype_normalized" in raw_df.columns:
            source_df = source_df.where(
                F.col("parameter_datatype_normalized").isin(
                    ParameterDataType.BINARY.value,
                    ParameterDataType.CATEGORICAL.value,
                )
            )
        elif "parameter_datatype_profiled" in raw_df.columns:
            source_df = source_df.where(
                spark_normalized_parameter_datatype_expr(F.col("parameter_datatype_profiled")).isin(
                    ParameterDataType.BINARY.value,
                    ParameterDataType.CATEGORICAL.value,
                )
            )
        elif "parameter_datatype" in raw_df.columns:
            source_df = source_df.where(
                spark_normalized_parameter_datatype_expr(F.col("parameter_datatype")).isin(
                    ParameterDataType.BINARY.value,
                    ParameterDataType.CATEGORICAL.value,
                )
            )

        current_state_raw = F.trim(F.col("parameter_value").cast("string"))
        is_missing = (
            F.col("parameter_value").isNull()
            | current_state_raw.isNull()
            | (current_state_raw == F.lit(""))
            | F.lower(current_state_raw).isin("null", "nan", "none")
        )
        prepared_df = source_df.select(
            "*",
            F.when(is_missing, F.lit(None).cast("string")).otherwise(current_state_raw).alias("current_state"),
            is_missing.alias("is_missing"),
        ).where(
            F.col("tail_id").isNotNull()
            & F.col("flight_id").isNotNull()
            & F.col("timestamp_utc").isNotNull()
            & F.col("parameter_name").isNotNull()
        )
        if "sample_seq_id" not in prepared_df.columns:
            order_window = Window.partitionBy("tail_id", "flight_id", "parameter_name").orderBy("timestamp_utc")
            prepared_df = prepared_df.select(
                "*",
                F.row_number().over(order_window).cast("long").alias("sample_seq_id"),
            )
        return prepared_df

    def _ensure_segmented_source(self, raw_df: "DataFrame") -> "DataFrame":
        prepared_df = self._prepare_source(raw_df)
        ordering = self.sequence_plan.ordering
        required = {
            *ordering.key_columns,
            *ordering.order_columns,
            ordering.segment_id_column,
            ordering.row_in_segment_column,
        }
        if required.issubset(set(prepared_df.columns)):
            return prepared_df
        return self.sequence_plan.assign_segments(prepared_df).rows_df

    def build(self, raw_df: "DataFrame") -> "DataFrame":
        from pyspark.sql import functions as F

        source_df = self._ensure_segmented_source(raw_df)
        key_columns = self.sequence_plan.ordering.key_columns
        segment_id_column = self.sequence_plan.ordering.segment_id_column
        segment_steps_df = self.sequence_plan.build_segment_steps(
            source_df,
            step_columns=(
                "tail_id",
                "flight_id",
                "parameter_name",
                "timestamp_utc",
                "date_utc",
                "current_state",
                "is_missing",
            ),
        )

        active = self.config
        illegal_transition_values = [f"{str(item[0])}\u0001{str(item[1])}" for item in sorted(active.illegal_transitions)]
        illegal_transition_literal = (
            F.array(*[F.lit(item) for item in illegal_transition_values])
            if illegal_transition_values
            else empty_array("string")
        )
        carry_df: "DataFrame | None" = None
        event_frames: list["DataFrame"] = []
        segment_ids = self.sequence_plan.collect_segment_ids(source_df.select(*key_columns, segment_id_column).distinct())
        initial_state = self.state_layout.initial_state_column()

        for segment_id in segment_ids:
            current_segments_df = segment_steps_df.where(F.col(segment_id_column) == F.lit(int(segment_id)))
            if carry_df is not None:
                current_segments_df = current_segments_df.join(carry_df, on=list(key_columns), how="left")
            else:
                current_segments_df = current_segments_df.withColumn("carry_state", initial_state)
            aggregated_df = current_segments_df.select(
                *list(key_columns),
                F.col(segment_id_column),
                F.aggregate(
                    F.col("steps"),
                    F.coalesce(F.col("carry_state"), initial_state),
                    lambda acc, step: self.state_layout.state_after_step_column(
                        acc=acc,
                        step=step,
                        emit_state_enter=F.lit(bool(active.emit_state_enter)),
                        emit_state_exit=F.lit(bool(active.emit_state_exit)),
                        emit_dwell_bucket=F.lit(bool(active.emit_dwell_bucket)),
                        min_dwell_seconds=F.lit(float(active.min_dwell_seconds)),
                        max_dwell_seconds=F.lit(float(active.max_dwell_seconds)),
                        illegal_transition_values=illegal_transition_literal,
                    ),
                ).alias("state_after"),
            )
            segment_events_df = (
                aggregated_df.select(F.explode_outer(F.col("state_after.emitted_events")).alias("event"))
                .where(F.col("event").isNotNull())
                .select("event.*")
            )
            event_frames.append(segment_events_df)
            carry_df = aggregated_df.select(
                *list(key_columns),
                self.state_layout.carry_state_column(state=F.col("state_after")).alias("carry_state"),
            )

        if not event_frames:
            spark = raw_df.sparkSession
            return spark.createDataFrame(
                [],
                schema=(
                    "tail_id string, flight_id string, win_id long, "
                    "timestamp_utc timestamp, parameter_name string, "
                    "event_type_detected string, payload map<string,string>, date_utc date"
                ),
            )
        events_df = event_frames[0]
        for frame in event_frames[1:]:
            events_df = events_df.unionByName(frame, allowMissingColumns=False)
        return events_df


@hot_path
def build_categorical_events(
    raw_df: "DataFrame",
    config: CategoricalDetectorConfig | None = None,
) -> "DataFrame":
    return CategoricalEventDetector(config=config if config else CategoricalDetectorConfig()).build(raw_df)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
