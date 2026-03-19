from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from math import log
from typing import TYPE_CHECKING

import pandas as pd


def _spark_functions():
    from pyspark.sql import functions as F

    return F


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
    def from_events_and_windows_spark(
        cls,
        events_df: "DataFrame",
        windows_df: "DataFrame",
        *,
        spec: EventGraphSpec,
    ) -> "DataFrame":
        """Build same-window cooccurrence edges in Spark using positive normalized PMI."""
        F = _spark_functions()

        canonical_events_df = cls.normalize_events_spark(events_df)
        window_rows = cls.normalize_windows_spark(windows_df)
        window_parameter_rows = (
            canonical_events_df.select("tail_id", "flight_id", "timestamp_utc", "parameter_name")
            .join(window_rows.select("tail_id", "flight_id", "win_id", "t_start", "t_end"), on=["tail_id", "flight_id"], how="inner")
            .where((F.col("timestamp_utc") >= F.col("t_start")) & (F.col("timestamp_utc") <= F.col("t_end")))
            .select("tail_id", "flight_id", "win_id", "parameter_name")
            .dropDuplicates(["tail_id", "flight_id", "win_id", "parameter_name"])
        )
        grouped = window_parameter_rows.groupBy("tail_id", "flight_id", "win_id").agg(
            F.sort_array(F.collect_list("parameter_name")).alias("parameter_names")
        )
        total_windows_df = grouped.agg(F.count(F.lit(1)).cast("double").alias("total_windows"))
        pair_rows = (
            grouped.select(
                F.posexplode("parameter_names").alias("left_idx", "parameter_name_u"),
                F.col("parameter_names"),
            )
            .select(
                "parameter_name_u",
                F.expr("slice(parameter_names, left_idx + 2, size(parameter_names))").alias("right_candidates"),
            )
            .select("parameter_name_u", F.explode_outer("right_candidates").alias("parameter_name_v"))
            .where(F.col("parameter_name_v").isNotNull())
        )
        pair_counts = pair_rows.groupBy("parameter_name_u", "parameter_name_v").agg(
            F.count(F.lit(1)).cast("int").alias("cooccur_count")
        )
        parameter_name_window_counts = window_parameter_rows.groupBy("parameter_name").agg(
            F.count(F.lit(1)).cast("int").alias("parameter_name_window_count")
        )
        with_counts = (
            pair_counts.join(
                parameter_name_window_counts.withColumnRenamed("parameter_name", "parameter_name_u"),
                on="parameter_name_u",
            )
            .withColumnRenamed("parameter_name_window_count", "left_window_count")
            .join(
                parameter_name_window_counts.withColumnRenamed("parameter_name", "parameter_name_v"),
                on="parameter_name_v",
            )
            .withColumnRenamed("parameter_name_window_count", "right_window_count")
            .crossJoin(total_windows_df)
            .withColumn("p_xy", F.col("cooccur_count") / F.col("total_windows"))
            .withColumn("p_x", F.col("left_window_count") / F.col("total_windows"))
            .withColumn("p_y", F.col("right_window_count") / F.col("total_windows"))
            .withColumn(
                "event_weight_raw",
                F.log(F.col("p_xy") / (F.col("p_x") * F.col("p_y"))) / (-F.log(F.col("p_xy"))),
            )
            .withColumn("event_weight", F.greatest(F.col("event_weight_raw"), F.lit(0.0)))
            .where(
                (F.col("cooccur_count") >= F.lit(max(int(spec.min_count), 1)))
                & (F.col("event_weight") >= F.lit(float(spec.min_npmi)))
            )
        )
        if int(spec.top_k_per_parameter_name) > 0:
            from pyspark.sql import Window

            undirected = with_counts.select(
                F.col("parameter_name_u"),
                F.col("parameter_name_v"),
                F.col("cooccur_count"),
                F.col("event_weight"),
                F.least("parameter_name_u", "parameter_name_v").alias("parameter_name_min"),
                F.greatest("parameter_name_u", "parameter_name_v").alias("parameter_name_max"),
            )
            exploded = undirected.select(
                F.col("parameter_name_u").alias("parameter_name"),
                F.col("parameter_name_min"),
                F.col("parameter_name_max"),
                F.col("event_weight"),
            ).unionByName(
                undirected.select(
                    F.col("parameter_name_v").alias("parameter_name"),
                    F.col("parameter_name_min"),
                    F.col("parameter_name_max"),
                    F.col("event_weight"),
                )
            )
            rank_window = Window.partitionBy("parameter_name").orderBy(
                F.col("event_weight").desc(), F.col("parameter_name_min"), F.col("parameter_name_max")
            )
            keep = (
                exploded.withColumn("rank", F.row_number().over(rank_window))
                .where(F.col("rank") <= int(spec.top_k_per_parameter_name))
                .select("parameter_name_min", "parameter_name_max")
                .distinct()
            )
            with_counts = with_counts.join(
                keep,
                on=[
                    with_counts.parameter_name_u == keep.parameter_name_min,
                    with_counts.parameter_name_v == keep.parameter_name_max,
                ],
                how="inner",
            ).select(with_counts["*"])
        return with_counts.select(
            "parameter_name_u",
            "parameter_name_v",
            "cooccur_count",
            "event_weight",
            F.lit("event").alias("edge_family"),
        )

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
            key: group.sort_values(["event_seq_id"], kind="mergesort").reset_index(drop=True)
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
        if "event_seq_id" not in rows.columns:
            raise ValueError("graph builders expect canonical events with event_seq_id; missing columns: event_seq_id")
        rows["parameter_name"] = rows.get("parameter_name", default_text).astype(str)
        rows["event_seq_id"] = pd.to_numeric(rows.get("event_seq_id"), errors="coerce")
        rows["timestamp_utc"] = pd.to_datetime(rows.get("timestamp_utc"), utc=True, errors="coerce")
        rows = rows.dropna(subset=["tail_id", "flight_id", "event_seq_id", "parameter_name", "timestamp_utc"])
        rows["event_seq_id"] = rows["event_seq_id"].astype("int64")
        return rows.sort_values(["tail_id", "flight_id", "event_seq_id"], kind="mergesort").reset_index(drop=True)

    @staticmethod
    def normalize_events_spark(events_df: "DataFrame") -> "DataFrame":
        F = _spark_functions()

        required_columns = {"tail_id", "flight_id", "event_seq_id", "timestamp_utc", "parameter_name"}
        missing_columns = required_columns.difference(events_df.columns)
        if missing_columns:
            missing_list = ", ".join(sorted(missing_columns))
            raise ValueError(
                "graph builders expect canonical events with event_seq_id; "
                f"missing columns: {missing_list}"
            )
        return events_df.select(
            F.col("tail_id").cast("string").alias("tail_id"),
            F.col("flight_id").cast("string").alias("flight_id"),
            F.col("event_seq_id").cast("long").alias("event_seq_id"),
            F.col("timestamp_utc").cast("timestamp").alias("timestamp_utc"),
            F.col("parameter_name").cast("string").alias("parameter_name"),
        ).where(
            F.col("tail_id").isNotNull()
            & F.col("flight_id").isNotNull()
            & F.col("event_seq_id").isNotNull()
            & F.col("timestamp_utc").isNotNull()
            & F.col("parameter_name").isNotNull()
        )

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
    def normalize_windows_spark(windows_df: "DataFrame") -> "DataFrame":
        F = _spark_functions()

        return windows_df.select(
            F.col("tail_id").cast("string").alias("tail_id"),
            F.col("flight_id").cast("string").alias("flight_id"),
            F.col("win_id").cast("int").alias("win_id"),
            F.col("t_start").cast("timestamp").alias("t_start"),
            F.col("t_end").cast("timestamp").alias("t_end"),
        ).where(
            F.col("tail_id").isNotNull()
            & F.col("flight_id").isNotNull()
            & F.col("win_id").isNotNull()
            & F.col("t_start").isNotNull()
            & F.col("t_end").isNotNull()
        )

    @staticmethod
    def empty_edges() -> pd.DataFrame:
        return pd.DataFrame(columns=["parameter_name_u", "parameter_name_v", "cooccur_count", "event_weight", "edge_family"])


if TYPE_CHECKING:
    from pyspark.sql import DataFrame
