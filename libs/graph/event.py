from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from math import log

import pandas as pd


@dataclass(frozen=True)
class EventGraphSpec:
    min_count: int = 1
    min_npmi: float = 0.0
    top_k_per_parameter_name: int = 8


@dataclass(frozen=True)
class EventGraph:
    spec: EventGraphSpec
    edges: pd.DataFrame

    @classmethod
    def from_events_and_windows(
        cls,
        events_df: pd.DataFrame,
        windows_df: pd.DataFrame,
        *,
        spec: EventGraphSpec,
    ) -> EventGraph:
        event_rows = cls.normalize_events(events_df)
        window_rows = cls.normalize_windows(windows_df)
        if event_rows.empty or window_rows.empty:
            return cls(spec=spec, edges=cls.empty_edges())

        pair_counts: Counter[tuple[str, str]] = Counter()
        parameter_name_window_counts: Counter[str] = Counter()
        by_events = {
            key: group.sort_values(["timestamp_utc", "parameter_name"], kind="mergesort").reset_index(drop=True)
            for key, group in event_rows.groupby(["tail_id", "flight_id"], sort=True)
        }
        total_windows = int(len(window_rows))
        out: list[dict[str, object]] = []
        for key, window_group in window_rows.groupby(["tail_id", "flight_id"], sort=True):
            event_group = by_events.get(key, pd.DataFrame())
            if event_group.empty:
                continue
            event_idx = 0
            event_len = len(event_group)
            for window in window_group.sort_values(["t_start", "win_id"], kind="mergesort").to_dict(orient="records"):
                t_start = pd.to_datetime(window["t_start"], utc=True)
                t_end = pd.to_datetime(window["t_end"], utc=True)
                parameter_names: set[str] = set()
                idx = event_idx
                while idx < event_len:
                    row = event_group.iloc[idx]
                    timestamp_utc = pd.to_datetime(row["timestamp_utc"], utc=True)
                    if timestamp_utc < t_start:
                        idx += 1
                        event_idx = idx
                        continue
                    if timestamp_utc > t_end:
                        break
                    parameter_names.add(str(row["parameter_name"]))
                    idx += 1
                distinct = sorted(parameter_names)
                for parameter_name in distinct:
                    parameter_name_window_counts[parameter_name] += 1
                for left_idx, left in enumerate(distinct):
                    for right in distinct[left_idx + 1 :]:
                        pair_counts[(left, right)] += 1

        for (left, right), count in sorted(pair_counts.items(), key=lambda item: (-item[1], item[0][0], item[0][1])):
            if count < max(int(spec.min_count), 1) or total_windows <= 0:
                continue
            p_xy = float(count) / float(total_windows)
            p_x = float(parameter_name_window_counts[left]) / float(total_windows)
            p_y = float(parameter_name_window_counts[right]) / float(total_windows)
            if p_xy <= 0.0 or p_x <= 0.0 or p_y <= 0.0:
                continue
            pmi = log(p_xy / max(p_x * p_y, 1e-12))
            npmi = pmi / max(-log(p_xy), 1e-12)
            event_weight = max(float(npmi), 0.0)
            if event_weight < float(spec.min_npmi):
                continue
            out.append(
                {
                    "parameter_name_u": left,
                    "parameter_name_v": right,
                    "cooccur_count": int(count),
                    "event_weight": event_weight,
                    "edge_family": "event",
                }
            )
        graph = cls(spec=spec, edges=pd.DataFrame(out, columns=cls.empty_edges().columns))
        return graph.retain_top_k()

    def retain_top_k(self) -> EventGraph:
        if self.spec.top_k_per_parameter_name <= 0 or self.edges.empty:
            return self
        by_parameter_name: defaultdict[str, list[dict[str, object]]] = defaultdict(list)
        for row in self.edges.to_dict(orient="records"):
            by_parameter_name[str(row["parameter_name_u"])].append(row)
            by_parameter_name[str(row["parameter_name_v"])].append(row)
        keep: set[tuple[str, str]] = set()
        for parameter_name_rows in by_parameter_name.values():
            ranked = sorted(
                parameter_name_rows,
                key=lambda item: (
                    -float(item.get("event_weight", 0.0) or 0.0),
                    str(item["parameter_name_u"]),
                    str(item["parameter_name_v"]),
                ),
            )[: self.spec.top_k_per_parameter_name]
            for item in ranked:
                keep.add(tuple(sorted((str(item["parameter_name_u"]), str(item["parameter_name_v"])))))
        edges = self.edges[
            self.edges.apply(
                lambda row: tuple(sorted((str(row["parameter_name_u"]), str(row["parameter_name_v"])))) in keep,
                axis=1,
            )
        ].reset_index(drop=True)
        return EventGraph(spec=self.spec, edges=edges)

    @staticmethod
    def normalize_events(events_df: pd.DataFrame) -> pd.DataFrame:
        rows = events_df.copy()
        default_text = pd.Series("", index=rows.index, dtype="object")
        rows["tail_id"] = rows.get("tail_id", default_text).astype(str)
        rows["flight_id"] = rows.get("flight_id", default_text).astype(str)
        if "parameter_name" not in rows.columns and "sensor" in rows.columns:
            rows["parameter_name"] = rows["sensor"]
        if "timestamp_utc" not in rows.columns and "ts" in rows.columns:
            rows["timestamp_utc"] = rows["ts"]
        rows["parameter_name"] = rows.get("parameter_name", default_text).astype(str)
        rows["timestamp_utc"] = pd.to_datetime(rows.get("timestamp_utc"), utc=True, errors="coerce")
        rows = rows.dropna(subset=["tail_id", "flight_id", "parameter_name", "timestamp_utc"])
        return rows.sort_values(["tail_id", "flight_id", "timestamp_utc", "parameter_name"], kind="mergesort").reset_index(drop=True)

    @staticmethod
    def normalize_windows(windows_df: pd.DataFrame) -> pd.DataFrame:
        rows = windows_df.copy()
        default_text = pd.Series("", index=rows.index, dtype="object")
        rows["tail_id"] = rows.get("tail_id", default_text).astype(str)
        rows["flight_id"] = rows.get("flight_id", default_text).astype(str)
        rows["win_id"] = pd.to_numeric(rows.get("win_id"), errors="coerce").fillna(0).astype(int)
        rows["t_start"] = pd.to_datetime(rows.get("t_start"), utc=True, errors="coerce")
        rows["t_end"] = pd.to_datetime(rows.get("t_end"), utc=True, errors="coerce")
        rows = rows.dropna(subset=["tail_id", "flight_id", "t_start", "t_end"])
        if "date_utc" not in rows.columns:
            rows["date_utc"] = rows["t_start"].dt.date
        return rows.sort_values(["tail_id", "flight_id", "t_start", "win_id"], kind="mergesort").reset_index(drop=True)

    @staticmethod
    def empty_edges() -> pd.DataFrame:
        return pd.DataFrame(columns=["parameter_name_u", "parameter_name_v", "cooccur_count", "event_weight", "edge_family"])
