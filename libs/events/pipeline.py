"""Class-oriented Spark event-table builders for canonical normalized raw telemetry."""

from __future__ import annotations

from dataclasses import dataclass, field

from libs.common import (
    ParameterDataType,
    empty_map,
    sorted_map_json,
    spark_normalized_parameter_datatype_expr,
)
from libs.events.categorical import CategoricalEventDetector
from libs.events.continuous import ContinuousDetectorConfig, ContinuousEventDetector
from libs.events.tables import EventsTable
from libs.pyspark import Frame
from libs.spark_sequence import (
    SegmentedSequencePlan,
    SequenceOrderingPolicy,
    segment_policy_from_env,
)


@dataclass(frozen=True)
class EventOrderingPolicy:
    source_ordering: SequenceOrderingPolicy = field(
        default_factory=lambda: SequenceOrderingPolicy(
            key_columns=("tail_id", "flight_id", "parameter_name"),
            order_columns=("timestamp_utc", "parameter_value", "value_num"),
            timestamp_column="timestamp_utc",
            row_number_column="sample_seq_id",
        )
    )
    event_ordering: SequenceOrderingPolicy = field(
        default_factory=lambda: SequenceOrderingPolicy(
            key_columns=("tail_id", "flight_id"),
            order_columns=("timestamp_utc", "parameter_name", "event_type_detected", "payload_json"),
            timestamp_column="timestamp_utc",
            row_number_column="event_seq_id",
        )
    )


@dataclass(frozen=True)
class EventSourceFrame(Frame):
    numeric_df: "DataFrame"
    categorical_df: "DataFrame"
    ordering: EventOrderingPolicy = field(default_factory=EventOrderingPolicy)

    @classmethod
    def from_raw(
        cls,
        raw_df: "DataFrame",
        *,
        datatype_profile_df: "DataFrame | None" = None,
        event_profile_df: "DataFrame | None" = None,
        ordering: EventOrderingPolicy | None = None,
    ) -> "EventSourceFrame":
        from pyspark.sql import functions as F
        active_ordering = ordering if ordering is not None else EventOrderingPolicy()
        source_ordering = active_ordering.source_ordering
        key_columns = source_ordering.normalized_key_columns()
        sequence_plan = SegmentedSequencePlan(
            ordering=source_ordering,
            policy=segment_policy_from_env(
                "EVENT",
                default_max_rows_per_segment=50_000,
                default_max_span_ms=900_000,
            ),
        )

        parameter_col = "parameter_name" if "parameter_name" in raw_df.columns else "sensor"
        range_partition_columns = [F.col(column_name) for column_name in (*key_columns, *source_ordering.normalized_order_columns())]
        base_df = (
            raw_df.select(
                F.col("tail_id").cast("string").alias("tail_id"),
                F.col("flight_id").cast("string").alias("flight_id"),
                F.col("timestamp_utc").cast("timestamp").alias("timestamp_utc"),
                F.col(parameter_col).cast("string").alias("parameter_name"),
                F.trim(F.col("parameter_value").cast("string")).alias("parameter_value"),
                F.col("val").cast("double").alias("value_num"),
                F.col("date_utc").cast("date").alias("date_utc"),
                (
                    F.col("parameter_datatype_profiled").cast("string")
                    if "parameter_datatype_profiled" in raw_df.columns
                    else F.col("parameter_datatype_label").cast("string")
                    if "parameter_datatype_label" in raw_df.columns
                    else F.col("parameter_datatype").cast("string")
                    if "parameter_datatype" in raw_df.columns
                    else F.lit(None).cast("string")
                ).alias("_datatype_inline"),
            )
            .where(
                F.col("tail_id").isNotNull()
                & F.col("flight_id").isNotNull()
                & F.col("timestamp_utc").isNotNull()
                & F.col("parameter_name").isNotNull()
            )
            .repartitionByRange(*range_partition_columns)
        )
        if datatype_profile_df is not None:
            profile_lookup_df = datatype_profile_df.select(
                F.col("parameter_name").cast("string").alias("_profile_parameter_name"),
                F.col("parameter_datatype_profiled").cast("string").alias("_profile_parameter_datatype"),
            )
            base_df = base_df.join(
                F.broadcast(profile_lookup_df),
                on=base_df["parameter_name"] == profile_lookup_df["_profile_parameter_name"],
                how="left",
            ).drop("_profile_parameter_name")
        else:
            base_df = base_df.select("*", F.lit(None).cast("string").alias("_profile_parameter_datatype"))

        if event_profile_df is not None:
            profile_columns = [
                column_name
                for column_name in event_profile_df.columns
                if column_name.startswith("recommended_")
            ]
            event_profile_lookup_df = event_profile_df.select(
                F.col("parameter_name").cast("string").alias("_event_profile_parameter_name"),
                *[F.col(column_name).alias(column_name) for column_name in profile_columns],
            )
            base_df = base_df.join(
                F.broadcast(event_profile_lookup_df),
                on=base_df["parameter_name"] == event_profile_lookup_df["_event_profile_parameter_name"],
                how="left",
            ).drop("_event_profile_parameter_name")

        prepared_df = base_df.select(
            "*",
            F.coalesce(F.col("_datatype_inline"), F.col("_profile_parameter_datatype")).alias("parameter_datatype_profiled"),
            spark_normalized_parameter_datatype_expr(
                F.coalesce(F.col("_datatype_inline"), F.col("_profile_parameter_datatype"))
            ).alias("parameter_datatype_normalized"),
            F.col("value_num").cast("double").alias("val"),
        )
        sequence_frame = sequence_plan.assign_segments(prepared_df)
        prepared_df = sequence_frame.rows_df

        numeric_df = prepared_df.where(
            F.col("parameter_datatype_normalized").isin(ParameterDataType.NUMERIC.value, ParameterDataType.CONSTANT.value)
            | (F.col("parameter_datatype_normalized").isNull() & F.col("value_num").isNotNull())
        )
        categorical_df = prepared_df.where(
            F.col("parameter_datatype_normalized").isin(ParameterDataType.BINARY.value, ParameterDataType.CATEGORICAL.value)
            | (
                F.col("parameter_datatype_normalized").isNull()
                & (F.col("value_num").isNull() | F.col("parameter_value").isNull())
            )
        )
        return cls(
            dataframe=prepared_df,
            numeric_df=numeric_df,
            categorical_df=categorical_df,
            ordering=active_ordering,
        )


@dataclass(frozen=True)
class EventArtifactSet:
    source_frame: EventSourceFrame
    events: EventsTable


@dataclass(frozen=True)
class EventDetectionPlan:
    continuous_detector: ContinuousEventDetector
    categorical_detector: CategoricalEventDetector = field(default_factory=CategoricalEventDetector)
    ordering: EventOrderingPolicy = field(default_factory=EventOrderingPolicy)

    @staticmethod
    def _validate_raw_input(raw_df: "DataFrame") -> None:
        required_columns = {
            "tail_id",
            "flight_id",
            "timestamp_utc",
            "parameter_name",
            "parameter_value",
            "val",
            "date_utc",
        }
        missing_columns = required_columns.difference(raw_df.columns)
        if missing_columns:
            missing_list = ", ".join(sorted(missing_columns))
            raise ValueError(
                "build_events_table expects canonical normalized raw telemetry; "
                f"missing columns: {missing_list}"
            )

    def _canonicalize_events(self, events_df: "DataFrame") -> "DataFrame":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        payload_col = F.col("payload") if "payload" in events_df.columns else empty_map()
        payload_json_expr = sorted_map_json(payload_col) if "payload" in events_df.columns else F.lit("{}")
        passthrough_columns = [
            F.col(column_name)
            for column_name in events_df.columns
            if column_name not in {"win_id", "anomaly_type_detected", "anomaly_score_detected", "payload", "payload_json"}
        ]
        canonical_df = events_df.select(
            *passthrough_columns,
            F.lit(None).cast("int").alias("win_id"),
            F.lit(None).cast("string").alias("anomaly_type_detected"),
            F.lit(None).cast("double").alias("anomaly_score_detected"),
            F.coalesce(payload_col, empty_map()).alias("payload"),
            payload_json_expr.alias("payload_json"),
        )
        event_order = self.ordering.event_ordering
        order_window = Window.partitionBy(*event_order.key_columns).orderBy(
            *[F.col(column_name).asc_nulls_last() for column_name in event_order.order_columns]
        )
        return (
            canonical_df.select(
                "tail_id",
                "flight_id",
                F.row_number().over(order_window).cast("long").alias("event_seq_id"),
                "win_id",
                "timestamp_utc",
                "parameter_name",
                "event_type_detected",
                "anomaly_type_detected",
                "anomaly_score_detected",
                "payload",
                "date_utc",
            )
        )

    def build(
        self,
        raw_df: "DataFrame",
        *,
        datatype_profile_df: "DataFrame | None" = None,
        event_profile_df: "DataFrame | None" = None,
    ) -> EventArtifactSet:
        self._validate_raw_input(raw_df)
        source_frame = EventSourceFrame.from_raw(
            raw_df,
            datatype_profile_df=datatype_profile_df,
            event_profile_df=event_profile_df,
            ordering=self.ordering,
        )
        continuous_events = self.continuous_detector.build(source_frame.numeric_df)
        categorical_events = self.categorical_detector.build(source_frame.categorical_df)
        events_df = self._canonicalize_events(continuous_events.unionByName(categorical_events, allowMissingColumns=True))
        return EventArtifactSet(source_frame=source_frame, events=EventsTable(dataframe=events_df))


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
