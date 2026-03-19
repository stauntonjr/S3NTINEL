from __future__ import annotations

from dataclasses import dataclass, field
import os


_SEGMENT_POLICY_PROFILE_DEFAULTS: dict[str, dict[str, tuple[int, int]]] = {
    "laptop_large_sim_large_segments": {
        "EVENT": (100_000, 1_800_000),
        "WINDOW": (100_000, 1_800_000),
        "PHASE": (10_000, 3_600_000),
    },
}


@dataclass(frozen=True)
class SequenceOrderingPolicy:
    """Deterministic ordering contract for one logical sequence family."""

    key_columns: tuple[str, ...]
    order_columns: tuple[str, ...]
    timestamp_column: str | None = None
    row_number_column: str = "sequence_row_number"
    segment_id_column: str = "flight_segment_id"
    row_in_segment_column: str = "sequence_row_in_segment"

    def normalized_key_columns(self) -> tuple[str, ...]:
        if not self.key_columns:
            raise ValueError("SequenceOrderingPolicy.key_columns must not be empty")
        return tuple(str(column_name) for column_name in self.key_columns)

    def normalized_order_columns(self) -> tuple[str, ...]:
        if not self.order_columns:
            raise ValueError("SequenceOrderingPolicy.order_columns must not be empty")
        return tuple(str(column_name) for column_name in self.order_columns)


@dataclass(frozen=True)
class SequenceSegmentPolicy:
    """Deterministic physical segmentation policy for long ordered streams."""

    max_rows_per_segment: int = 50_000
    max_span_ms: int = 0

    def normalized_max_rows_per_segment(self) -> int:
        return max(int(self.max_rows_per_segment), 1)

    def normalized_max_span_ms(self) -> int:
        return max(int(self.max_span_ms), 0)


def segment_policy_from_env(
    prefix: str,
    *,
    default_max_rows_per_segment: int,
    default_max_span_ms: int,
) -> SequenceSegmentPolicy:
    """Build a segment policy from semantics-preserving environment overrides."""

    normalized_prefix = str(prefix).strip().upper()
    if not normalized_prefix:
        raise ValueError("segment_policy_from_env requires a non-empty prefix")
    env_prefix = f"S3NTINEL_{normalized_prefix}_SEGMENT"
    profile_name = str(os.getenv("S3NTINEL_SPARK_PROFILE", "")).strip().lower()
    profile_defaults = _SEGMENT_POLICY_PROFILE_DEFAULTS.get(profile_name, {})
    profile_segment_defaults = profile_defaults.get(normalized_prefix)
    profile_max_rows = (
        default_max_rows_per_segment if profile_segment_defaults is None else int(profile_segment_defaults[0])
    )
    profile_max_span_ms = (
        default_max_span_ms if profile_segment_defaults is None else int(profile_segment_defaults[1])
    )
    max_rows = int(os.getenv(f"{env_prefix}_MAX_ROWS", str(profile_max_rows)))
    max_span_ms = int(os.getenv(f"{env_prefix}_MAX_SPAN_MS", str(profile_max_span_ms)))
    return SequenceSegmentPolicy(
        max_rows_per_segment=max_rows,
        max_span_ms=max_span_ms,
    )


@dataclass(frozen=True)
class SequenceKey:
    columns: tuple[str, ...]
    values: tuple[object, ...]


@dataclass(frozen=True)
class SequenceSegment:
    key: SequenceKey
    flight_segment_id: int
    segment_row_count: int
    t_start: object | None = None
    t_end: object | None = None


@dataclass(frozen=True)
class SegmentedSequenceFrame:
    rows_df: "DataFrame"
    segments_df: "DataFrame"
    segment_steps_df: "DataFrame | None" = None


@dataclass(frozen=True)
class SequenceCarryFrame:
    dataframe: "DataFrame"
    key_columns: tuple[str, ...]
    segment_id_column: str = "flight_segment_id"


@dataclass(frozen=True)
class SegmentedSequencePlan:
    """Shared segmentation/orchestration utilities for bounded Spark sequence kernels."""

    ordering: SequenceOrderingPolicy = field(
        default_factory=lambda: SequenceOrderingPolicy(
            key_columns=("tail_id", "flight_id"),
            order_columns=("timestamp_utc",),
            timestamp_column="timestamp_utc",
        )
    )
    policy: SequenceSegmentPolicy = field(default_factory=SequenceSegmentPolicy)

    def _validate_columns(self, df: "DataFrame") -> tuple[tuple[str, ...], tuple[str, ...]]:
        key_columns = self.ordering.normalized_key_columns()
        order_columns = self.ordering.normalized_order_columns()
        required_columns = set(key_columns).union(order_columns)
        timestamp_column = self.ordering.timestamp_column
        if timestamp_column:
            required_columns.add(str(timestamp_column))
        missing_columns = required_columns.difference(df.columns)
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(
                "SegmentedSequencePlan requires key/order columns; "
                f"missing: {missing}"
            )
        return key_columns, order_columns

    def assign_segments(self, df: "DataFrame") -> SegmentedSequenceFrame:
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        key_columns, order_columns = self._validate_columns(df)
        row_number_column = str(self.ordering.row_number_column)
        segment_id_column = str(self.ordering.segment_id_column)
        row_in_segment_column = str(self.ordering.row_in_segment_column)
        timestamp_column = self.ordering.timestamp_column

        order_window = Window.partitionBy(*key_columns).orderBy(*order_columns)
        first_ts_window = Window.partitionBy(*key_columns)
        segment_order_window = Window.partitionBy(*key_columns, segment_id_column).orderBy(*order_columns)
        max_rows = self.policy.normalized_max_rows_per_segment()
        max_span_ms = self.policy.normalized_max_span_ms()

        rows_df = (
            df.withColumn(row_number_column, F.row_number().over(order_window).cast("long"))
            .withColumn(
                "_row_segment_id",
                F.floor((F.col(row_number_column) - F.lit(1)) / F.lit(max_rows)).cast("long"),
            )
        )
        if max_span_ms > 0 and timestamp_column:
            rows_df = (
                rows_df.withColumn("_sequence_start_ts", F.min(F.col(timestamp_column)).over(first_ts_window))
                .withColumn(
                    "_span_segment_id",
                    F.floor(
                        (
                            F.unix_millis(F.col(timestamp_column))
                            - F.unix_millis(F.col("_sequence_start_ts"))
                        )
                        / F.lit(max_span_ms)
                    ).cast("long"),
                )
                .withColumn(
                    segment_id_column,
                    F.greatest(F.col("_row_segment_id"), F.col("_span_segment_id")).cast("long"),
                )
                .drop("_sequence_start_ts", "_span_segment_id")
            )
        else:
            rows_df = rows_df.withColumn(segment_id_column, F.col("_row_segment_id").cast("long"))

        rows_df = (
            rows_df.drop("_row_segment_id")
            .withColumn(row_in_segment_column, F.row_number().over(segment_order_window).cast("long"))
        )

        segment_aggs = [
            F.count(F.lit(1)).cast("long").alias("segment_row_count"),
            F.min(F.col(row_number_column)).cast("long").alias("segment_row_number_start"),
            F.max(F.col(row_number_column)).cast("long").alias("segment_row_number_end"),
        ]
        if timestamp_column:
            segment_aggs.extend(
                [
                    F.min(F.col(timestamp_column)).alias("segment_t_start"),
                    F.max(F.col(timestamp_column)).alias("segment_t_end"),
                ]
            )
        segments_df = rows_df.groupBy(*key_columns, segment_id_column).agg(*segment_aggs)
        return SegmentedSequenceFrame(rows_df=rows_df, segments_df=segments_df)

    def build_segment_steps(
        self,
        segmented_rows_df: "DataFrame",
        *,
        step_columns: tuple[str, ...],
    ) -> "DataFrame":
        from pyspark.sql import functions as F

        key_columns = self.ordering.normalized_key_columns()
        row_number_column = str(self.ordering.row_number_column)
        segment_id_column = str(self.ordering.segment_id_column)
        row_in_segment_column = str(self.ordering.row_in_segment_column)
        required_columns = set(key_columns).union(
            {
                row_number_column,
                row_in_segment_column,
                segment_id_column,
            }
        ).union(step_columns)
        missing_columns = required_columns.difference(segmented_rows_df.columns)
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(
                "SegmentedSequencePlan.build_segment_steps requires segmented rows and step columns; "
                f"missing: {missing}"
            )
        step_struct = F.struct(
            F.col(row_number_column).alias(row_number_column),
            F.col(row_in_segment_column).alias(row_in_segment_column),
            *[F.col(column_name).alias(column_name) for column_name in step_columns],
        )
        return (
            segmented_rows_df.groupBy(*key_columns, segment_id_column)
            .agg(F.sort_array(F.collect_list(step_struct)).alias("steps"))
        )

    def collect_segment_ids(self, segments_df: "DataFrame") -> list[int]:
        segment_id_column = str(self.ordering.segment_id_column)
        return [
            int(row[segment_id_column])
            for row in segments_df.select(segment_id_column).distinct().orderBy(segment_id_column).collect()
        ]


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
