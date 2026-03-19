from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import os

import pandas as pd

from libs.graph.hierarchy import _connected_components_from_edges, assign_hierarchy_from_weighted_edges


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

        max_rollup_edge_universe = int(os.getenv("S3NTINEL_MAX_HIERARCHY_ROLLUP_EDGE_UNIVERSE", "250000"))
        parameter_set = {str(item) for item in parameter_names if str(item)}
        if not parameter_set:
            return cls(spec=spec, rows=cls.empty_rows())
        parameter_names_sorted = sorted(parameter_set)
        spark = fused_df.sparkSession

        filtered = fused_df.where(F.col("fused_weight") >= F.lit(float(spec.min_edge_weight))).select(
            F.col("parameter_name_u").cast("string").alias("parameter_name_u"),
            F.col("parameter_name_v").cast("string").alias("parameter_name_v"),
            F.col("fused_weight").cast("double").alias("fused_weight"),
        ).where(
            F.col("parameter_name_u").isin(parameter_names_sorted)
            & F.col("parameter_name_v").isin(parameter_names_sorted)
        )
        if filtered.limit(1).count() == 0:
            hierarchy_rows = assign_hierarchy_from_weighted_edges(
                parameter_names_sorted,
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

        retained_rows = (
            edges.join(mutual_pairs, on=["parameter_name_min", "parameter_name_max"], how="inner")
            .select("parameter_name_u", "parameter_name_v", "fused_weight")
            .limit(max_rollup_edge_universe + 1)
            .collect()
        )
        if len(retained_rows) > max_rollup_edge_universe:
            raise RuntimeError(
                "GraphHierarchy.from_fused_spark performs a bounded local hierarchy rollup over retained fused edges; "
                f"edge count exceeds S3NTINEL_MAX_HIERARCHY_ROLLUP_EDGE_UNIVERSE={max_rollup_edge_universe}."
            )
        retained_edges = [
            (str(row["parameter_name_u"]), str(row["parameter_name_v"]), float(row["fused_weight"]))
            for row in retained_rows
        ]
        module_groups = _connected_components_from_edges(
            parameter_names_sorted,
            retained_edges,
            min_edge_weight=float(spec.min_edge_weight),
        )
        module_by_parameter: dict[str, str] = {}
        for index, members in enumerate(module_groups, start=1):
            module_id = f"MOD_{index:04d}"
            for parameter_name in members:
                module_by_parameter[parameter_name] = module_id

        module_assignment_df = spark.createDataFrame(
            [(parameter_name, module_id) for parameter_name, module_id in sorted(module_by_parameter.items())],
            schema="parameter_name string, module_id string",
        )
        module_rollup_rows = (
            filtered.alias("edge")
            .join(
                module_assignment_df.alias("left_module"),
                F.col("edge.parameter_name_u") == F.col("left_module.parameter_name"),
                how="inner",
            )
            .join(
                module_assignment_df.alias("right_module"),
                F.col("edge.parameter_name_v") == F.col("right_module.parameter_name"),
                how="inner",
            )
            .where(F.col("left_module.module_id") != F.col("right_module.module_id"))
            .groupBy(
                F.least(F.col("left_module.module_id"), F.col("right_module.module_id")).alias("left_group_id"),
                F.greatest(F.col("left_module.module_id"), F.col("right_module.module_id")).alias("right_group_id"),
            )
            .agg(F.avg("edge.fused_weight").cast("double").alias("group_weight"))
            .limit(max_rollup_edge_universe + 1)
            .collect()
        )
        if len(module_rollup_rows) > max_rollup_edge_universe:
            raise RuntimeError(
                "GraphHierarchy.from_fused_spark performs a bounded local hierarchy rollup over module edges; "
                f"edge count exceeds S3NTINEL_MAX_HIERARCHY_ROLLUP_EDGE_UNIVERSE={max_rollup_edge_universe}."
            )
        module_edges = [
            (str(row["left_group_id"]), str(row["right_group_id"]), float(row["group_weight"]))
            for row in module_rollup_rows
        ]
        module_ids = [f"MOD_{index:04d}" for index in range(1, len(module_groups) + 1)]
        subsystem_threshold = (
            float(spec.subsystem_min_edge_weight)
            if spec.subsystem_min_edge_weight is not None
            else max(float(spec.min_edge_weight) * 0.75, 1e-6)
        )
        subsystem_groups = _connected_components_from_edges(
            module_ids,
            module_edges,
            min_edge_weight=subsystem_threshold,
        )
        subsystem_by_module: dict[str, str] = {}
        for index, members in enumerate(subsystem_groups, start=1):
            subsystem_id = f"SUBSYS_{index:04d}"
            for module_id in members:
                subsystem_by_module[module_id] = subsystem_id

        module_edge_df = (
            spark.createDataFrame(module_rollup_rows, schema="left_group_id string, right_group_id string, group_weight double")
            if module_rollup_rows
            else spark.createDataFrame([], schema="left_group_id string, right_group_id string, group_weight double")
        )
        subsystem_assignment_df = spark.createDataFrame(
            [(module_id, subsystem_id) for module_id, subsystem_id in sorted(subsystem_by_module.items())],
            schema="module_id string, subsystem_id string",
        )
        subsystem_rollup_rows = (
            module_edge_df.alias("module_edge")
            .join(
                subsystem_assignment_df.alias("left_subsystem"),
                F.col("module_edge.left_group_id") == F.col("left_subsystem.module_id"),
                how="inner",
            )
            .join(
                subsystem_assignment_df.alias("right_subsystem"),
                F.col("module_edge.right_group_id") == F.col("right_subsystem.module_id"),
                how="inner",
            )
            .where(F.col("left_subsystem.subsystem_id") != F.col("right_subsystem.subsystem_id"))
            .groupBy(
                F.least(F.col("left_subsystem.subsystem_id"), F.col("right_subsystem.subsystem_id")).alias("left_group_id"),
                F.greatest(F.col("left_subsystem.subsystem_id"), F.col("right_subsystem.subsystem_id")).alias("right_group_id"),
            )
            .agg(F.avg("module_edge.group_weight").cast("double").alias("group_weight"))
            .limit(max_rollup_edge_universe + 1)
            .collect()
        )
        if len(subsystem_rollup_rows) > max_rollup_edge_universe:
            raise RuntimeError(
                "GraphHierarchy.from_fused_spark performs a bounded local hierarchy rollup over subsystem edges; "
                f"edge count exceeds S3NTINEL_MAX_HIERARCHY_ROLLUP_EDGE_UNIVERSE={max_rollup_edge_universe}."
            )
        subsystem_edges = [
            (str(row["left_group_id"]), str(row["right_group_id"]), float(row["group_weight"]))
            for row in subsystem_rollup_rows
        ]
        subsystem_ids = [f"SUBSYS_{index:04d}" for index in range(1, len(subsystem_groups) + 1)]
        system_threshold = (
            float(spec.system_min_edge_weight)
            if spec.system_min_edge_weight is not None
            else max(subsystem_threshold * 0.75, 1e-6)
        )
        system_groups = _connected_components_from_edges(
            subsystem_ids,
            subsystem_edges,
            min_edge_weight=system_threshold,
        )
        system_by_subsystem: dict[str, str] = {}
        for index, members in enumerate(system_groups, start=1):
            system_id = f"SYS_{index:04d}"
            for subsystem_id in members:
                system_by_subsystem[subsystem_id] = system_id

        hierarchy_rows = [
            {
                "parameter_name": parameter_name,
                "system_id": system_by_subsystem[subsystem_by_module[module_by_parameter[parameter_name]]],
                "subsystem_id": subsystem_by_module[module_by_parameter[parameter_name]],
                "module_id": module_by_parameter[parameter_name],
            }
            for parameter_name in parameter_names_sorted
        ]
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
