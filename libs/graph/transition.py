from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

import pandas as pd

from libs.graph.event import EventGraph


@dataclass(frozen=True)
class TransitionGraphSpec:
    min_count: int = 1


@dataclass(frozen=True)
class TransitionGraph:
    spec: TransitionGraphSpec
    edges: pd.DataFrame

    @classmethod
    def from_events(cls, events_df: pd.DataFrame, *, spec: TransitionGraphSpec) -> TransitionGraph:
        event_rows = EventGraph.normalize_events(events_df)
        if event_rows.empty:
            return cls(spec=spec, edges=cls.empty_edges())
        pair_counts: Counter[tuple[str, str]] = Counter()
        outgoing_counts: Counter[str] = Counter()
        for _, group in event_rows.groupby(["tail_id", "flight_id"], sort=True):
            previous_parameter_name: str | None = None
            for row in group.sort_values(["timestamp_utc", "parameter_name"], kind="mergesort").to_dict(orient="records"):
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
