from __future__ import annotations

import time

from libs.common.event_types import CATEGORICAL_EVENT_TYPES, CONTINUOUS_EVENT_TYPES
from libs.phase.types import PhaseSelectorDiagnostics


def event_counts_from_window_features_spark(window_features_df: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return (
        window_features_df.select(F.explode_outer(F.map_entries("event_type_counts")).alias("entry"))
        .select(
            F.col("entry.key").cast("string").alias("event_type"),
            F.col("entry.value").cast("long").alias("event_count"),
        )
        .where(F.col("event_type").isNotNull())
        .groupBy("event_type")
        .agg(F.sum("event_count").cast("long").alias("event_count"))
    )


def event_type_statistics_from_window_features_spark(window_features_df: "DataFrame") -> "DataFrame":
    from pyspark.sql import functions as F

    return (
        window_features_df.select(
            "tail_id",
            "flight_id",
            "win_id",
            F.explode_outer(F.map_entries("event_type_counts")).alias("entry"),
        )
        .select(
            "tail_id",
            "flight_id",
            "win_id",
            F.col("entry.key").cast("string").alias("event_type"),
            F.col("entry.value").cast("double").alias("event_count"),
        )
        .where(F.col("event_type").isNotNull())
        .groupBy("event_type")
        .agg(
            F.sum("event_count").cast("double").alias("event_count"),
            F.countDistinct(F.struct("tail_id", "flight_id", "win_id")).cast("long").alias("active_window_count"),
        )
        .withColumn(
            "mean_event_count_per_active_window",
            F.col("event_count") / F.greatest(F.col("active_window_count").cast("double"), F.lit(1.0)),
        )
    )


def select_event_types_from_window_features_spark(window_features_df: "DataFrame", *, k: int) -> list[str]:
    from pyspark.sql import functions as F

    limit = max(int(k), 0)
    if limit <= 0:
        return []

    counts_df = event_type_statistics_from_window_features_spark(window_features_df)
    continuous_k = max(limit // 2, 1)
    categorical_k = max(limit - continuous_k, 0)

    selected: list[str] = []
    continuous_rows = (
        counts_df.where(F.col("event_type").isin(list(CONTINUOUS_EVENT_TYPES)))
        .orderBy(
            F.col("mean_event_count_per_active_window").desc(),
            F.col("event_count").desc(),
            F.col("event_type").asc(),
        )
        .limit(continuous_k)
        .collect()
    )
    for row in continuous_rows:
        event_type = str(row["event_type"])
        if event_type not in selected:
            selected.append(event_type)

    if categorical_k > 0:
        categorical_rows = (
            counts_df.where(F.col("event_type").isin(list(CATEGORICAL_EVENT_TYPES)))
            .orderBy(
                F.col("mean_event_count_per_active_window").desc(),
                F.col("event_count").desc(),
                F.col("event_type").asc(),
            )
            .limit(categorical_k)
            .collect()
        )
        for row in categorical_rows:
            event_type = str(row["event_type"])
            if event_type not in selected:
                selected.append(event_type)

    if len(selected) < limit:
        fallback_rows = counts_df.orderBy(F.col("event_count").desc(), F.col("event_type").asc()).limit(limit).collect()
        for row in fallback_rows:
            event_type = str(row["event_type"])
            if event_type in selected:
                continue
            selected.append(event_type)
            if len(selected) >= limit:
                break
    return selected


def select_event_types_with_diagnostics_from_window_features_spark(
    window_features_df: "DataFrame",
    *,
    k: int,
) -> tuple[list[str], PhaseSelectorDiagnostics]:
    from pyspark.sql import functions as F

    start = time.perf_counter()
    limit = max(int(k), 0)
    if limit <= 0:
        return [], PhaseSelectorDiagnostics(
            selector_name="event_types",
            selected_count=0,
            timing_ms=(time.perf_counter() - start) * 1000.0,
            candidate_count=0,
            fallback_used=False,
        )

    counts_df = event_type_statistics_from_window_features_spark(window_features_df)
    candidate_count = int(counts_df.count())
    continuous_k = max(limit // 2, 1)
    categorical_k = max(limit - continuous_k, 0)

    selected: list[str] = []
    fallback_used = False
    continuous_rows = (
        counts_df.where(F.col("event_type").isin(list(CONTINUOUS_EVENT_TYPES)))
        .orderBy(
            F.col("mean_event_count_per_active_window").desc(),
            F.col("event_count").desc(),
            F.col("event_type").asc(),
        )
        .limit(continuous_k)
        .collect()
    )
    for row in continuous_rows:
        event_type = str(row["event_type"])
        if event_type not in selected:
            selected.append(event_type)

    if categorical_k > 0:
        categorical_rows = (
            counts_df.where(F.col("event_type").isin(list(CATEGORICAL_EVENT_TYPES)))
            .orderBy(
                F.col("mean_event_count_per_active_window").desc(),
                F.col("event_count").desc(),
                F.col("event_type").asc(),
            )
            .limit(categorical_k)
            .collect()
        )
        for row in categorical_rows:
            event_type = str(row["event_type"])
            if event_type not in selected:
                selected.append(event_type)

    if len(selected) < limit:
        fallback_used = True
        fallback_rows = counts_df.orderBy(F.col("event_count").desc(), F.col("event_type").asc()).limit(limit).collect()
        for row in fallback_rows:
            event_type = str(row["event_type"])
            if event_type in selected:
                continue
            selected.append(event_type)
            if len(selected) >= limit:
                break

    return selected, PhaseSelectorDiagnostics(
        selector_name="event_types",
        selected_count=len(selected),
        timing_ms=(time.perf_counter() - start) * 1000.0,
        candidate_count=candidate_count,
        fallback_used=fallback_used,
    )


def select_categorical_state_pairs_from_window_features_spark(
    window_features_df: "DataFrame",
    *,
    k: int,
) -> list[tuple[str, str]]:
    from pyspark.sql import functions as F

    limit = max(int(k), 0)
    if limit <= 0:
        return []

    pair_rows = (
        window_features_df.select(
            F.array_union(
                F.map_keys(F.coalesce(F.col("categorical_state_t_start"), F.expr("cast(map() as map<string,string>)"))),
                F.map_keys(F.coalesce(F.col("categorical_state_t_end"), F.expr("cast(map() as map<string,string>)"))),
            ).alias("parameter_names"),
            "categorical_state_t_start",
            "categorical_state_t_end",
        )
        .select(
            F.explode_outer("parameter_names").alias("parameter_name"),
            F.element_at("categorical_state_t_start", F.col("parameter_name")).cast("string").alias("state_start"),
            F.element_at("categorical_state_t_end", F.col("parameter_name")).cast("string").alias("state_end"),
        )
        .withColumn("state_candidates", F.array_distinct(F.array("state_start", "state_end")))
        .select(
            "parameter_name",
            "state_start",
            "state_end",
            F.explode_outer("state_candidates").alias("state"),
        )
        .where(F.col("parameter_name").isNotNull() & F.col("state").isNotNull())
        .groupBy("parameter_name", "state")
        .agg(
            F.count(F.lit(1)).cast("long").alias("state_touch_count"),
            F.sum(
                F.when(
                    (F.coalesce(F.col("state_start"), F.lit("")) != F.coalesce(F.col("state_end"), F.lit("")))
                    & ((F.col("state_start") == F.col("state")) | (F.col("state_end") == F.col("state"))),
                    F.lit(1),
                ).otherwise(F.lit(0)),
            )
            .cast("long")
            .alias("state_change_count"),
        )
        .orderBy(
            F.col("state_change_count").desc(),
            F.col("state_touch_count").desc(),
            F.col("parameter_name").asc(),
            F.col("state").asc(),
        )
        .limit(limit)
        .collect()
    )
    return [(str(row["parameter_name"]), str(row["state"])) for row in pair_rows]


def select_categorical_state_pairs_with_diagnostics_from_window_features_spark(
    window_features_df: "DataFrame",
    *,
    k: int,
) -> tuple[list[tuple[str, str]], PhaseSelectorDiagnostics]:
    from pyspark.sql import functions as F

    start = time.perf_counter()
    limit = max(int(k), 0)
    if limit <= 0:
        return [], PhaseSelectorDiagnostics(
            selector_name="categorical_state_pairs",
            selected_count=0,
            timing_ms=(time.perf_counter() - start) * 1000.0,
            candidate_count=0,
            fallback_used=False,
        )

    pair_counts_df = (
        window_features_df.select(
            F.array_union(
                F.map_keys(F.coalesce(F.col("categorical_state_t_start"), F.expr("cast(map() as map<string,string>)"))),
                F.map_keys(F.coalesce(F.col("categorical_state_t_end"), F.expr("cast(map() as map<string,string>)"))),
            ).alias("parameter_names"),
            "categorical_state_t_start",
            "categorical_state_t_end",
        )
        .select(
            F.explode_outer("parameter_names").alias("parameter_name"),
            F.element_at("categorical_state_t_start", F.col("parameter_name")).cast("string").alias("state_start"),
            F.element_at("categorical_state_t_end", F.col("parameter_name")).cast("string").alias("state_end"),
        )
        .withColumn("state_candidates", F.array_distinct(F.array("state_start", "state_end")))
        .select(
            "parameter_name",
            "state_start",
            "state_end",
            F.explode_outer("state_candidates").alias("state"),
        )
        .where(F.col("parameter_name").isNotNull() & F.col("state").isNotNull())
        .groupBy("parameter_name", "state")
        .agg(
            F.count(F.lit(1)).cast("long").alias("state_touch_count"),
            F.sum(
                F.when(
                    (F.coalesce(F.col("state_start"), F.lit("")) != F.coalesce(F.col("state_end"), F.lit("")))
                    & ((F.col("state_start") == F.col("state")) | (F.col("state_end") == F.col("state"))),
                    F.lit(1),
                ).otherwise(F.lit(0)),
            )
            .cast("long")
            .alias("state_change_count"),
        )
    )
    candidate_count = int(pair_counts_df.count())
    rows = (
        pair_counts_df.orderBy(
            F.col("state_change_count").desc(),
            F.col("state_touch_count").desc(),
            F.col("parameter_name").asc(),
            F.col("state").asc(),
        )
        .limit(limit)
        .collect()
    )
    selected = [(str(row["parameter_name"]), str(row["state"])) for row in rows]
    return selected, PhaseSelectorDiagnostics(
        selector_name="categorical_state_pairs",
        selected_count=len(selected),
        timing_ms=(time.perf_counter() - start) * 1000.0,
        candidate_count=candidate_count,
        fallback_used=False,
    )


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
