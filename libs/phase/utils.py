"""Phase utility helpers for Spark expressions and defaults."""

from __future__ import annotations

from typing import TYPE_CHECKING

from libs.common import empty_array
from libs.spark_sequence import SequenceSegmentPolicy, segment_policy_from_env


def default_phase_segment_policy() -> SequenceSegmentPolicy:
    return segment_policy_from_env(
        "PHASE",
        default_max_rows_per_segment=5_000,
        default_max_span_ms=1_800_000,
    )


def string_array_literal(values: list[str]) -> "Column":
    from pyspark.sql import functions as F

    if not values:
        return empty_array("string")
    return F.array(*[F.lit(str(value)) for value in values])


def double_matrix_literal(values: list[list[float]]) -> "Column":
    from pyspark.sql import functions as F

    if not values:
        return empty_array("array<double>")
    return F.array(*[F.array(*[F.lit(float(item)) for item in row]) for row in values])


def array_distance(left: "Column", right: "Column") -> "Column":
    from pyspark.sql import functions as F

    return F.sqrt(
        F.aggregate(
            F.zip_with(left, right, lambda a, b: (a - b) * (a - b)),
            F.lit(0.0),
            lambda acc, value: acc + value,
        )
    )


def with_progress_mass_coordinates(
    dataframe: "DataFrame",
    *,
    value_column: str = "s_w_scaled",
    order_column: str = "phase_row_number",
    flight_window_count_column: str = "flight_window_count",
) -> "DataFrame":
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    phase_window = Window.partitionBy("tail_id", "flight_id").orderBy(order_column)
    cumulative_window = phase_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    flight_window = Window.partitionBy("tail_id", "flight_id")
    return (
        dataframe.withColumn(
            "flight_progress",
            ((F.col(order_column) - F.lit(1)).cast("double"))
            / F.greatest((F.col(flight_window_count_column) - F.lit(1)).cast("double"), F.lit(1.0)),
        )
        .withColumn("_prev_phase_value", F.lag(value_column).over(phase_window))
        .withColumn(
            "phase_step_distance",
            F.when(F.col("_prev_phase_value").isNull(), F.lit(0.0))
            .otherwise(array_distance(F.col(value_column), F.col("_prev_phase_value")))
            .cast("double"),
        )
        .withColumn(
            "window_progress_mass",
            (F.lit(1.0) + F.log1p(F.coalesce(F.col("phase_step_distance"), F.lit(0.0)))).cast("double"),
        )
        .withColumn(
            "_cumulative_progress_mass",
            F.sum("window_progress_mass").over(cumulative_window).cast("double"),
        )
        .withColumn(
            "_flight_progress_mass_total",
            F.sum("window_progress_mass").over(flight_window).cast("double"),
        )
        .withColumn(
            "progress_mass_position",
            (
                (F.col("_cumulative_progress_mass") - (F.col("window_progress_mass") / F.lit(2.0)))
                / F.greatest(F.col("_flight_progress_mass_total"), F.lit(1.0))
            ).cast("double"),
        )
        .drop("_prev_phase_value", "_cumulative_progress_mass", "_flight_progress_mass_total")
    )


if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame
