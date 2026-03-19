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


if TYPE_CHECKING:
    from pyspark.sql import Column
