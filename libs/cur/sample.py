# File: libs/cur/sample.py
"""Sampling strategies for representative CUR columns and rows."""

from __future__ import annotations

from pyspark.sql import functions as F


def sample_indices(count: int) -> list[int]:
    # HOT PATH: representative index selection must be deterministic and efficient for large candidate sets.
    return list(range(count))


def select_topk_deterministic(
    df: "DataFrame",
    *,
    k: int,
    order_columns: list["Column"],
) -> "DataFrame":
    return df.orderBy(*order_columns).limit(max(int(k), 1))


def select_topk_weighted_without_replacement(
    df: "DataFrame",
    *,
    k: int,
    weight_column: str,
    seed: int,
    tie_break_columns: list["Column"] | None = None,
) -> "DataFrame":
    tie_break_cols = tie_break_columns or []
    safe_weight = F.greatest(F.coalesce(F.col(weight_column).cast("double"), F.lit(0.0)), F.lit(1e-9))
    safe_uniform = F.greatest(F.rand(int(seed)), F.lit(1e-12))
    weighted_key = (-F.log(safe_uniform)) / safe_weight
    return (
        df.withColumn("_weighted_key", weighted_key)
        .orderBy(F.col("_weighted_key").asc(), *tie_break_cols)
        .limit(max(int(k), 1))
        .drop("_weighted_key")
    )


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame
