from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

import pandas as pd

from libs.graph.hierarchy import assign_hierarchy_from_weighted_edges


@dataclass(frozen=True)
class HierarchySpec:
    min_edge_weight: float = 0.05
    top_k_per_parameter_name: int = 3
    subsystem_min_edge_weight: float | None = None
    system_min_edge_weight: float | None = None


@dataclass(frozen=True)
class GraphHierarchy:
    spec: HierarchySpec
    rows: pd.DataFrame

    @classmethod
    def from_fused(cls, fused_df: pd.DataFrame, parameter_names: list[str], *, spec: HierarchySpec) -> GraphHierarchy:
        ranked_neighbors: defaultdict[str, list[tuple[float, str]]] = defaultdict(list)
        filtered_rows: list[dict[str, object]] = []
        parameter_set = {str(item) for item in parameter_names}
        for row in fused_df.to_dict(orient="records"):
            weight = float(row.get("fused_weight", 0.0) or 0.0)
            if weight < float(spec.min_edge_weight):
                continue
            a = str(row.get("parameter_name_u", ""))
            b = str(row.get("parameter_name_v", ""))
            if not a or not b or a not in parameter_set or b not in parameter_set:
                continue
            filtered_rows.append(row)
            ranked_neighbors[a].append((weight, b))
            ranked_neighbors[b].append((weight, a))

        keep_neighbors: dict[str, set[str]] = {}
        for parameter_name, neighbors in ranked_neighbors.items():
            ranked = sorted(neighbors, key=lambda item: (-item[0], item[1]))
            keep_neighbors[parameter_name] = {neighbor for _, neighbor in ranked[: max(int(spec.top_k_per_parameter_name), 1)]}

        retained_edges: list[tuple[str, str, float]] = []
        for row in filtered_rows:
            a = str(row["parameter_name_u"])
            b = str(row["parameter_name_v"])
            if b in keep_neighbors.get(a, set()) and a in keep_neighbors.get(b, set()):
                retained_edges.append((a, b, float(row["fused_weight"])))
        rollup_edges = [
            (str(row["parameter_name_u"]), str(row["parameter_name_v"]), float(row["fused_weight"]))
            for row in filtered_rows
        ]
        hierarchy_rows = assign_hierarchy_from_weighted_edges(
            list(parameter_names),
            retained_edges,
            module_min_edge_weight=float(spec.min_edge_weight),
            subsystem_min_edge_weight=spec.subsystem_min_edge_weight,
            system_min_edge_weight=spec.system_min_edge_weight,
            rollup_edges=rollup_edges,
        )
        out = pd.DataFrame(
            [
                {
                    "parameter_name": str(row["parameter_name"]),
                    "system_id": str(row["system_id"]),
                    "subsystem_id": str(row["subsystem_id"]),
                    "module_id": str(row["module_id"]),
                    "hierarchy_source": "v2_fused_graph_mutual_topk_levels",
                    "hierarchy_profile_id": "HIER_V2",
                }
                for row in hierarchy_rows
            ]
        )
        return cls(spec=spec, rows=out)

    @classmethod
    def from_fused_spark(
        cls,
        fused_df: "DataFrame",
        *,
        parameter_names: list[str],
        spec: HierarchySpec,
    ) -> GraphHierarchy:
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        parameter_set = {str(item) for item in parameter_names if str(item)}
        if not parameter_set:
            return cls(spec=spec, rows=cls.empty_rows())

        filtered = fused_df.where(F.col("fused_weight") >= F.lit(float(spec.min_edge_weight))).select(
            F.col("parameter_name_u").cast("string").alias("parameter_name_u"),
            F.col("parameter_name_v").cast("string").alias("parameter_name_v"),
            F.col("fused_weight").cast("double").alias("fused_weight"),
        )
        if filtered.limit(1).count() == 0:
            hierarchy_rows = assign_hierarchy_from_weighted_edges(
                sorted(parameter_set),
                [],
                module_min_edge_weight=float(spec.min_edge_weight),
                subsystem_min_edge_weight=spec.subsystem_min_edge_weight,
                system_min_edge_weight=spec.system_min_edge_weight,
            )
            return cls(
                spec=spec,
                rows=pd.DataFrame(
                    [
                        {
                            "parameter_name": str(row["parameter_name"]),
                            "system_id": str(row["system_id"]),
                            "subsystem_id": str(row["subsystem_id"]),
                            "module_id": str(row["module_id"]),
                            "hierarchy_source": "v2_fused_graph_mutual_topk_levels",
                            "hierarchy_profile_id": "HIER_V2",
                        }
                        for row in hierarchy_rows
                    ]
                ),
            )

        edges = filtered.select(
            "parameter_name_u",
            "parameter_name_v",
            "fused_weight",
            F.least("parameter_name_u", "parameter_name_v").alias("parameter_name_min"),
            F.greatest("parameter_name_u", "parameter_name_v").alias("parameter_name_max"),
        )
        neighbors = edges.select(
            F.col("parameter_name_u").alias("parameter_name"),
            F.col("parameter_name_v").alias("neighbor"),
            "parameter_name_min",
            "parameter_name_max",
            "fused_weight",
        ).unionByName(
            edges.select(
                F.col("parameter_name_v").alias("parameter_name"),
                F.col("parameter_name_u").alias("neighbor"),
                "parameter_name_min",
                "parameter_name_max",
                "fused_weight",
            )
        )
        rank_window = Window.partitionBy("parameter_name").orderBy(
            F.col("fused_weight").desc(),
            F.col("parameter_name_min"),
            F.col("parameter_name_max"),
        )
        retained_neighbors = neighbors.withColumn("rank", F.row_number().over(rank_window)).where(
            F.col("rank") <= F.lit(max(int(spec.top_k_per_parameter_name), 1))
        )
        mutual_pairs = retained_neighbors.alias("left").join(
            retained_neighbors.alias("right"),
            (F.col("left.parameter_name") == F.col("right.neighbor"))
            & (F.col("left.neighbor") == F.col("right.parameter_name")),
            how="inner",
        ).select(
            F.least(F.col("left.parameter_name"), F.col("left.neighbor")).alias("parameter_name_min"),
            F.greatest(F.col("left.parameter_name"), F.col("left.neighbor")).alias("parameter_name_max"),
        ).distinct()

        retained_pdf = edges.join(mutual_pairs, on=["parameter_name_min", "parameter_name_max"], how="inner").select(
            "parameter_name_u",
            "parameter_name_v",
            "fused_weight",
        ).toPandas()
        rollup_pdf = filtered.select("parameter_name_u", "parameter_name_v", "fused_weight").toPandas()
        retained_edges = [
            (str(row["parameter_name_u"]), str(row["parameter_name_v"]), float(row["fused_weight"]))
            for row in retained_pdf.to_dict(orient="records")
        ]
        rollup_edges = [
            (str(row["parameter_name_u"]), str(row["parameter_name_v"]), float(row["fused_weight"]))
            for row in rollup_pdf.to_dict(orient="records")
        ]
        hierarchy_rows = assign_hierarchy_from_weighted_edges(
            sorted(parameter_set),
            retained_edges,
            module_min_edge_weight=float(spec.min_edge_weight),
            subsystem_min_edge_weight=spec.subsystem_min_edge_weight,
            system_min_edge_weight=spec.system_min_edge_weight,
            rollup_edges=rollup_edges,
        )
        return cls(
            spec=spec,
            rows=pd.DataFrame(
                [
                    {
                        "parameter_name": str(row["parameter_name"]),
                        "system_id": str(row["system_id"]),
                        "subsystem_id": str(row["subsystem_id"]),
                        "module_id": str(row["module_id"]),
                        "hierarchy_source": "v2_fused_graph_mutual_topk_levels",
                        "hierarchy_profile_id": "HIER_V2",
                    }
                    for row in hierarchy_rows
                ]
            ),
        )

    @staticmethod
    def empty_rows() -> pd.DataFrame:
        return pd.DataFrame(
            columns=["parameter_name", "system_id", "subsystem_id", "module_id", "hierarchy_source", "hierarchy_profile_id"]
        )
