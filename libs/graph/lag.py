from __future__ import annotations

from collections import Counter, defaultdict, deque
from dataclasses import dataclass

import pandas as pd

from libs.graph.event import EventGraph


@dataclass(frozen=True)
class LagGraphSpec:
    tau_max_seconds: float = 30.0
    min_count: int = 1
    max_mean_lag_seconds: float | None = None
    top_k_outgoing: int = 8


@dataclass(frozen=True)
class LagGraph:
    spec: LagGraphSpec
    edges: pd.DataFrame

    @classmethod
    def from_events(cls, events_df: pd.DataFrame, *, spec: LagGraphSpec) -> LagGraph:
        event_rows = EventGraph.normalize_events(events_df)
        if event_rows.empty:
            return cls(spec=spec, edges=cls.empty_edges())
        tau = max(float(spec.tau_max_seconds), 0.0)
        pair_counts: Counter[tuple[str, str]] = Counter()
        lag_sums: defaultdict[tuple[str, str], float] = defaultdict(float)
        outgoing_counts: Counter[str] = Counter()
        for _, group in event_rows.groupby(["tail_id", "flight_id"], sort=True):
            buffer: deque[tuple[pd.Timestamp, str]] = deque()
            for row in group.sort_values(["timestamp_utc", "parameter_name"], kind="mergesort").to_dict(orient="records"):
                timestamp_utc = pd.to_datetime(row["timestamp_utc"], utc=True)
                parameter_name = str(row["parameter_name"])
                lower = timestamp_utc - pd.Timedelta(seconds=tau)
                while buffer and buffer[0][0] < lower:
                    buffer.popleft()
                seen_prev_parameters: set[str] = set()
                for prev_timestamp_utc, prev_parameter_name in reversed(buffer):
                    if prev_parameter_name == parameter_name or prev_parameter_name in seen_prev_parameters:
                        continue
                    pair = (prev_parameter_name, parameter_name)
                    lag = max((timestamp_utc - prev_timestamp_utc).total_seconds(), 0.0)
                    pair_counts[pair] += 1
                    lag_sums[pair] += lag
                    outgoing_counts[prev_parameter_name] += 1
                    seen_prev_parameters.add(prev_parameter_name)
                buffer.append((timestamp_utc, parameter_name))
        out: list[dict[str, object]] = []
        for (left, right), count in sorted(pair_counts.items(), key=lambda item: (-item[1], item[0][0], item[0][1])):
            if count < max(int(spec.min_count), 1):
                continue
            mean_lag_seconds = float(lag_sums[(left, right)] / float(max(count, 1)))
            if spec.max_mean_lag_seconds is not None and mean_lag_seconds > float(spec.max_mean_lag_seconds):
                continue
            shortness = max(0.0, 1.0 - (mean_lag_seconds / float(max(tau, 1e-6))))
            conditional_probability = float(count) / float(max(outgoing_counts[left], 1))
            out.append(
                {
                    "parameter_name_u": left,
                    "parameter_name_v": right,
                    "lag_count": int(count),
                    "lag_weight": conditional_probability * shortness,
                    "mean_lag_seconds": mean_lag_seconds,
                    "edge_family": "lag_directed",
                }
            )
        graph = cls(spec=spec, edges=pd.DataFrame(out, columns=cls.empty_edges().columns))
        return graph.retain_top_k()

    def retain_top_k(self) -> LagGraph:
        if self.spec.top_k_outgoing <= 0 or self.edges.empty:
            return self
        by_source: defaultdict[str, list[dict[str, object]]] = defaultdict(list)
        for row in self.edges.to_dict(orient="records"):
            by_source[str(row["parameter_name_u"])].append(row)
        keep: set[tuple[str, str]] = set()
        for sensor_rows in by_source.values():
            ranked = sorted(
                sensor_rows,
                key=lambda item: (
                    -float(item.get("lag_weight", 0.0) or 0.0),
                    str(item["parameter_name_u"]),
                    str(item["parameter_name_v"]),
                ),
            )[: self.spec.top_k_outgoing]
            for item in ranked:
                keep.add((str(item["parameter_name_u"]), str(item["parameter_name_v"])))
        edges = self.edges[
            self.edges.apply(lambda row: (str(row["parameter_name_u"]), str(row["parameter_name_v"])) in keep, axis=1)
        ].reset_index(drop=True)
        return LagGraph(spec=self.spec, edges=edges)

    @staticmethod
    def empty_edges() -> pd.DataFrame:
        return pd.DataFrame(columns=["parameter_name_u", "parameter_name_v", "lag_count", "lag_weight", "mean_lag_seconds", "edge_family"])
