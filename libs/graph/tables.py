"""Typed Spark tables for graph artifact boundaries."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from libs.io.schemas.graph import (
    EVENT_GRAPH_SCHEMA,
    FUSED_GRAPH_SCHEMA,
    GRAPH_PARAMETER_UNIVERSE_SCHEMA,
    HIERARCHY_EDGE_EVIDENCE_SCHEMA,
    HIERARCHY_SENSOR_MAP_SCHEMA,
    LAG_GRAPH_SCHEMA,
    LAG_PROFILE_SCHEMA,
    PRECISION_GRAPH_SCHEMA,
    TRANSITION_GRAPH_SCHEMA,
)
from libs.pyspark import Frame
from libs.pyspark import Table


@dataclass(frozen=True)
class PrecisionGraphTable(Table):
    @classmethod
    def spark_schema(cls):
        return PRECISION_GRAPH_SCHEMA()

    @classmethod
    def from_window_features(
        cls,
        window_features_df: "DataFrame",
        *,
        selected_sensors: list[str],
        ridge_lambda: float,
        min_abs_partial_corr: float,
    ) -> "PrecisionGraphTable":
        from libs.graph.pipeline import build_precision_graph_from_window_features_spark_table

        spark = window_features_df.sparkSession
        precision_pdf = build_precision_graph_from_window_features_spark_table(
            window_features_df,
            selected_sensors=selected_sensors,
            ridge_lambda=ridge_lambda,
            min_abs_partial_corr=min_abs_partial_corr,
        )
        dataframe = (
            spark.createDataFrame(precision_pdf)
            if not precision_pdf.empty
            else spark.createDataFrame([], schema=PRECISION_GRAPH_SCHEMA())
        )
        return cls(dataframe=dataframe)


@dataclass(frozen=True)
class EventGraphTable(Table):
    @classmethod
    def spark_schema(cls):
        return EVENT_GRAPH_SCHEMA()

    @classmethod
    def from_events_and_windows(
        cls,
        events_df: "DataFrame",
        windows_df: "DataFrame",
        *,
        min_count: int,
        min_npmi: float,
        top_k_per_parameter_name: int,
    ) -> "EventGraphTable":
        from libs.graph.pipeline import build_event_graph_spark_table

        return cls(
            dataframe=build_event_graph_spark_table(
                events_df,
                windows_df,
                min_count=min_count,
                min_npmi=min_npmi,
                top_k_per_parameter_name=top_k_per_parameter_name,
            )
        )


@dataclass(frozen=True)
class LagCandidatePairsFrame(Frame):
    @classmethod
    def from_graphs(cls, event_df: "DataFrame", transition_df: "DataFrame") -> "LagCandidatePairsFrame":
        from libs.graph.pipeline import build_lag_candidate_pairs_spark_table

        return cls(dataframe=build_lag_candidate_pairs_spark_table(event_df, transition_df))


@dataclass(frozen=True)
class LagProfileTable(Table):
    @classmethod
    def spark_schema(cls):
        return LAG_PROFILE_SCHEMA()

    @classmethod
    def from_events(
        cls,
        events_df: "DataFrame",
        *,
        tau_max_seconds: float,
        bands: "tuple[LagBandSpec, ...] | None" = None,
        candidate_pairs_df: "DataFrame | None" = None,
    ) -> "LagProfileTable":
        from libs.graph.pipeline import build_lag_profile_spark_table

        return cls(
            dataframe=build_lag_profile_spark_table(
                events_df,
                tau_max_seconds=tau_max_seconds,
                bands=bands,
                candidate_pairs_df=candidate_pairs_df,
            )
        )


@dataclass(frozen=True)
class LagGraphTable(Table):
    @classmethod
    def spark_schema(cls):
        return LAG_GRAPH_SCHEMA()

    @classmethod
    def from_profile(
        cls,
        lag_profile_df: "DataFrame",
        *,
        tau_max_seconds: float,
        bands: "tuple[LagBandSpec, ...] | None",
        min_count: int,
        max_mean_lag_seconds: float | None,
        top_k_outgoing: int,
    ) -> "LagGraphTable":
        from libs.graph.pipeline import collapse_lag_profile_spark_table

        return cls(
            dataframe=collapse_lag_profile_spark_table(
                lag_profile_df,
                tau_max_seconds=tau_max_seconds,
                bands=bands,
                min_count=min_count,
                max_mean_lag_seconds=max_mean_lag_seconds,
                top_k_outgoing=top_k_outgoing,
            )
        )


@dataclass(frozen=True)
class TransitionGraphTable(Table):
    @classmethod
    def spark_schema(cls):
        return TRANSITION_GRAPH_SCHEMA()

    @classmethod
    def from_events(cls, events_df: "DataFrame", *, min_count: int) -> "TransitionGraphTable":
        from libs.graph.pipeline import build_transition_graph_spark_table

        return cls(dataframe=build_transition_graph_spark_table(events_df, min_count=min_count))


@dataclass(frozen=True)
class FusedGraphTable(Table):
    @classmethod
    def spark_schema(cls):
        return FUSED_GRAPH_SCHEMA()

    @classmethod
    def from_component_tables(
        cls,
        precision_df: "DataFrame",
        event_df: "DataFrame",
        lag_df: "DataFrame",
        *,
        alpha: float,
        beta: float,
        gamma: float,
    ) -> "FusedGraphTable":
        from libs.graph.pipeline import build_fused_graph_spark_table

        return cls(
            dataframe=build_fused_graph_spark_table(
                precision_df,
                event_df,
                lag_df,
                alpha=alpha,
                beta=beta,
                gamma=gamma,
            )
        )


@dataclass(frozen=True)
class GraphParameterUniverseTable(Table):
    @classmethod
    def spark_schema(cls):
        return GRAPH_PARAMETER_UNIVERSE_SCHEMA()

    @classmethod
    def from_component_tables(
        cls,
        event_df: "DataFrame",
        lag_df: "DataFrame",
        transition_df: "DataFrame",
        *,
        backbone_all_sensors: list[str],
        max_graph_sensor_universe: int,
    ) -> tuple["GraphParameterUniverseTable", list[str]]:
        from libs.graph.pipeline import build_graph_parameter_universe_spark_table

        dataframe, parameter_names = build_graph_parameter_universe_spark_table(
            event_df,
            lag_df,
            transition_df,
            backbone_all_sensors=backbone_all_sensors,
            max_graph_sensor_universe=max_graph_sensor_universe,
        )
        return cls(dataframe=dataframe), parameter_names


@dataclass(frozen=True)
class HierarchySensorMapTable(Table):
    @classmethod
    def spark_schema(cls):
        return HIERARCHY_SENSOR_MAP_SCHEMA()

    @classmethod
    def from_fused_graph(
        cls,
        fused_df: "DataFrame",
        *,
        parameter_names: list[str],
        min_fused_edge_weight: float,
        hierarchy_top_k_per_parameter_name: int,
        hierarchy_subsystem_min_edge_weight: float | None = None,
        hierarchy_system_min_edge_weight: float | None = None,
        datatype_profile_df: "DataFrame | None" = None,
        behavior_profile_df: "DataFrame | None" = None,
    ) -> "HierarchySensorMapTable":
        from libs.graph.pipeline import build_hierarchy_from_fused_spark_table

        spark = fused_df.sparkSession
        rows = build_hierarchy_from_fused_spark_table(
            fused_df,
            parameter_names=parameter_names,
            min_fused_edge_weight=min_fused_edge_weight,
            hierarchy_top_k_per_parameter_name=hierarchy_top_k_per_parameter_name,
            hierarchy_subsystem_min_edge_weight=hierarchy_subsystem_min_edge_weight,
            hierarchy_system_min_edge_weight=hierarchy_system_min_edge_weight,
            datatype_profile_df=datatype_profile_df,
            behavior_profile_df=behavior_profile_df,
        )
        dataframe = spark.createDataFrame(rows) if not rows.empty else spark.createDataFrame([], schema=HIERARCHY_SENSOR_MAP_SCHEMA())
        return cls(dataframe=dataframe)


@dataclass(frozen=True)
class HierarchyEdgeEvidenceTable(Table):
    @classmethod
    def spark_schema(cls):
        return HIERARCHY_EDGE_EVIDENCE_SCHEMA()

    @classmethod
    def from_graph_hierarchy(
        cls,
        *,
        hierarchy: "GraphHierarchy",
        lag_graph_df: "DataFrame",
        spark: "SparkSession",
    ) -> "HierarchyEdgeEvidenceTable":
        from pyspark.sql import functions as F

        edge_columns = [field.name for field in HIERARCHY_EDGE_EVIDENCE_SCHEMA().fields]
        records = [
            {
                "parameter_name_u": str(row.get("parameter_name_u") or ""),
                "parameter_name_v": str(row.get("parameter_name_v") or ""),
                "rank_parameter_name_u": int(row.get("rank_parameter_name_u") or 0),
                "rank_parameter_name_v": int(row.get("rank_parameter_name_v") or 0),
                "precision_weight": float(row.get("precision_weight") or 0.0),
                "event_weight": float(row.get("event_weight") or 0.0),
                "lag_weight": float(row.get("lag_weight") or 0.0),
                "fused_weight": float(row.get("fused_weight") or 0.0),
                "module_affinity_weight": float(row.get("module_affinity_weight") or 0.0),
                "lag_count_u_to_v": None,
                "lag_weight_u_to_v": None,
                "mean_lag_seconds_u_to_v": None,
                "lag_count_v_to_u": None,
                "lag_weight_v_to_u": None,
                "mean_lag_seconds_v_to_u": None,
                "system_id": str(row.get("system_id") or ""),
                "subsystem_id": str(row.get("subsystem_id") or ""),
                "module_id": str(row.get("module_id") or ""),
                "hierarchy_edge_role": str(row.get("hierarchy_edge_role") or "retained_module_mutual_topk"),
            }
            for row in hierarchy.retained_module_edge_rows.to_dict(orient="records")
        ]
        evidence_df = (
            spark.createDataFrame(records, schema=HIERARCHY_EDGE_EVIDENCE_SCHEMA())
            if records
            else spark.createDataFrame([], schema=HIERARCHY_EDGE_EVIDENCE_SCHEMA())
        )
        forward_lag = lag_graph_df.select(
            F.col("parameter_name_u").alias("lag_parameter_name_u"),
            F.col("parameter_name_v").alias("lag_parameter_name_v"),
            F.col("lag_count").cast("int").alias("lag_count_u_to_v"),
            F.col("lag_weight").cast("double").alias("lag_weight_u_to_v"),
            F.col("mean_lag_seconds").cast("double").alias("mean_lag_seconds_u_to_v"),
        )
        reverse_lag = lag_graph_df.select(
            F.col("parameter_name_u").alias("lag_parameter_name_v"),
            F.col("parameter_name_v").alias("lag_parameter_name_u"),
            F.col("lag_count").cast("int").alias("lag_count_v_to_u"),
            F.col("lag_weight").cast("double").alias("lag_weight_v_to_u"),
            F.col("mean_lag_seconds").cast("double").alias("mean_lag_seconds_v_to_u"),
        )
        return cls(
            dataframe=(
                evidence_df.drop(
                    "lag_count_u_to_v",
                    "lag_weight_u_to_v",
                    "mean_lag_seconds_u_to_v",
                    "lag_count_v_to_u",
                    "lag_weight_v_to_u",
                    "mean_lag_seconds_v_to_u",
                )
                .join(
                    forward_lag,
                    (F.col("parameter_name_u") == F.col("lag_parameter_name_u"))
                    & (F.col("parameter_name_v") == F.col("lag_parameter_name_v")),
                    how="left",
                )
                .drop("lag_parameter_name_u", "lag_parameter_name_v")
                .join(
                    reverse_lag,
                    (F.col("parameter_name_u") == F.col("lag_parameter_name_u"))
                    & (F.col("parameter_name_v") == F.col("lag_parameter_name_v")),
                    how="left",
                )
                .drop("lag_parameter_name_u", "lag_parameter_name_v")
                .select(*edge_columns)
            )
        )


@dataclass(frozen=True)
class HierarchyArtifactSet:
    sensor_map: HierarchySensorMapTable
    edge_evidence: HierarchyEdgeEvidenceTable

    @classmethod
    def from_fused_graph(
        cls,
        fused_df: "DataFrame",
        *,
        lag_graph_df: "DataFrame",
        parameter_names: list[str],
        min_fused_edge_weight: float,
        hierarchy_top_k_per_parameter_name: int,
        hierarchy_subsystem_min_edge_weight: float | None = None,
        hierarchy_system_min_edge_weight: float | None = None,
        datatype_profile_df: "DataFrame | None" = None,
        behavior_profile_df: "DataFrame | None" = None,
    ) -> "HierarchyArtifactSet":
        from libs.graph.hierarchy_artifacts import GraphHierarchy, HierarchySpec

        hierarchy = GraphHierarchy.from_fused_spark(
            fused_df,
            parameter_names=parameter_names,
            spec=HierarchySpec(
                min_edge_weight=min_fused_edge_weight,
                top_k_per_parameter_name=hierarchy_top_k_per_parameter_name,
                subsystem_min_edge_weight=hierarchy_subsystem_min_edge_weight,
                system_min_edge_weight=hierarchy_system_min_edge_weight,
            ),
            datatype_profile_df=datatype_profile_df,
            behavior_profile_df=behavior_profile_df,
        )
        spark = fused_df.sparkSession
        sensor_map_df = (
            spark.createDataFrame(hierarchy.rows, schema=HIERARCHY_SENSOR_MAP_SCHEMA())
            if not hierarchy.rows.empty
            else spark.createDataFrame([], schema=HIERARCHY_SENSOR_MAP_SCHEMA())
        )
        return cls(
            sensor_map=HierarchySensorMapTable(dataframe=sensor_map_df),
            edge_evidence=HierarchyEdgeEvidenceTable.from_graph_hierarchy(
                hierarchy=hierarchy,
                lag_graph_df=lag_graph_df,
                spark=spark,
            ),
        )


if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from libs.graph.hierarchy_artifacts import GraphHierarchy

    from libs.graph.lag import LagBandSpec
