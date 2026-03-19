from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pandas as pd

from libs.graph.event import EventGraph


def _spark_functions():
    from pyspark.sql import functions as F

    return F


@dataclass(frozen=True)
class TransitionGraphSpec:
    min_count: int = 1


@dataclass(frozen=True)
class TransitionGraph:
    spec: TransitionGraphSpec
    edges: pd.DataFrame

    @classmethod
    def from_events_spark(cls, events_df: "DataFrame", *, spec: TransitionGraphSpec) -> "DataFrame":
        """Build immediate transition edges as row-normalized transition probabilities."""
        F = _spark_functions()
        from pyspark.sql import Window

        canonical_events_df = EventGraph.normalize_events_spark(events_df)
        ordered_window = Window.partitionBy("tail_id", "flight_id").orderBy("event_seq_id")
        transitions = (
            canonical_events_df.select("tail_id", "flight_id", "event_seq_id", "parameter_name")
            .withColumn("parameter_name_v", F.lead("parameter_name").over(ordered_window))
            .where(F.col("parameter_name_v").isNotNull() & (F.col("parameter_name") != F.col("parameter_name_v")))
            .select(
                F.col("parameter_name").alias("parameter_name_u"),
                "parameter_name_v",
            )
        )
        count_df = transitions.groupBy("parameter_name_u", "parameter_name_v").agg(
            F.count(F.lit(1)).cast("int").alias("precedence_count")
        )
        count_df = count_df.where(F.col("precedence_count") >= F.lit(max(int(spec.min_count), 1)))
        source_totals = count_df.groupBy("parameter_name_u").agg(F.sum("precedence_count").cast("double").alias("source_total"))
        return (
            count_df.join(source_totals, on="parameter_name_u", how="inner")
            .withColumn("precedence_weight", F.col("precedence_count") / F.col("source_total"))
            .withColumn("edge_family", F.lit("transition"))
            .select("parameter_name_u", "parameter_name_v", "precedence_count", "precedence_weight", "edge_family")
        )

    @classmethod
    def from_events(cls, events_df: pd.DataFrame, *, spec: TransitionGraphSpec) -> TransitionGraph:
        event_rows = EventGraph.normalize_events(events_df)
        if event_rows.empty:
            return cls(spec=spec, edges=cls.empty_edges())
        pair_counts: Counter[tuple[str, str]] = Counter()
        outgoing_counts: Counter[str] = Counter()
        for _, group in event_rows.groupby(["tail_id", "flight_id"], sort=True):
            previous_parameter_name: str | None = None
            for row in group.sort_values(["event_seq_id"], kind="mergesort").to_dict(orient="records"):
                parameter_name = str(row["parameter_name"])
                if previous_parameter_name is not None and previous_parameter_name != parameter_name:
                    pair = (previous_parameter_name, parameter_name)
                    pair_counts[pair] += 1
                    outgoing_counts[previous_parameter_name] += 1
                previous_parameter_name = parameter_name
        out: list[dict[str, object]] = []
        for (left, right), count in sorted(pair_counts.items(), key=lambda item: (-item[1], item[0][0], item[0][1])):
            if count < max(int(spec.min_count), 1):
                continue
            out.append(
                {
                    "parameter_name_u": left,
                    "parameter_name_v": right,
                    "precedence_count": int(count),
                    "precedence_weight": float(count) / float(max(outgoing_counts[left], 1)),
                    "edge_family": "transition",
                }
            )
        return cls(spec=spec, edges=pd.DataFrame(out, columns=cls.empty_edges().columns))

    @staticmethod
    def empty_edges() -> pd.DataFrame:
        return pd.DataFrame(columns=["parameter_name_u", "parameter_name_v", "precedence_count", "precedence_weight", "edge_family"])


if TYPE_CHECKING:
    from pyspark.sql import DataFrame
