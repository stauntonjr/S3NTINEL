from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class FusedGraphSpec:
    alpha: float = 1.0
    beta: float = 1.0
    gamma: float = 1.0


@dataclass(frozen=True)
class FusedGraph:
    spec: FusedGraphSpec
    edges: pd.DataFrame

    @classmethod
    def from_components(
        cls,
        precision_df: pd.DataFrame,
        event_df: pd.DataFrame,
        lag_df: pd.DataFrame,
        *,
        spec: FusedGraphSpec,
    ) -> FusedGraph:
        event_map = {
            (str(row["parameter_name_u"]), str(row["parameter_name_v"])): float(row["event_weight"])
            for row in event_df.to_dict(orient="records")
        }
        lag_weight_lists: defaultdict[tuple[str, str], list[float]] = defaultdict(list)
        for row in lag_df.to_dict(orient="records"):
            key = tuple(sorted((str(row["parameter_name_u"]), str(row["parameter_name_v"]))))
            lag_weight_lists[key].append(float(row.get("lag_weight", 0.0) or 0.0))
        lag_weight_map = {key: max(weights, default=0.0) for key, weights in lag_weight_lists.items()}
        precision_map = {
            (str(row["parameter_name_u"]), str(row["parameter_name_v"])): float(row["precision_weight"])
            for row in precision_df.to_dict(orient="records")
        }
        all_pairs = set(event_map.keys()) | set(lag_weight_map.keys()) | set(precision_map.keys())
        out: list[dict[str, object]] = []
        for key in sorted(all_pairs):
            parameter_name_u, parameter_name_v = key
            p = float(precision_map.get(key, 0.0))
            e = float(event_map.get(key, 0.0))
            l = float(lag_weight_map.get(tuple(sorted(key)), 0.0))
            fused = (float(spec.alpha) * p) + (float(spec.beta) * e) + (float(spec.gamma) * l)
            if fused <= 0.0:
                continue
            out.append(
                {
                    "parameter_name_u": parameter_name_u,
                    "parameter_name_v": parameter_name_v,
                    "precision_weight": p,
                    "event_weight": e,
                    "lag_weight": l,
                    "fused_weight": fused,
                    "edge_family": "fused",
                }
            )
        return cls(spec=spec, edges=pd.DataFrame(out, columns=cls.empty_edges().columns))

    @staticmethod
    def empty_edges() -> pd.DataFrame:
        return pd.DataFrame(columns=["parameter_name_u", "parameter_name_v", "precision_weight", "event_weight", "lag_weight", "fused_weight", "edge_family"])
