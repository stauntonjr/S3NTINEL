"""Graph artifact builders for Spark fitting stages.

Edge-weight semantics:
- precision_graph: absolute partial correlation
- event_graph: positive normalized PMI over same-window co-occurrence
- lag_graph: row-normalized lagged conditional probability, discounted by mean lag
- transition_graph: row-normalized immediate transition probability
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from libs.graph.data import (
    parameter_name_union_from_component_tables,
    parameter_name_union_from_window_x,
    prepare_events_df,
    prepare_windows_df,
    retain_top_k_directed,
    retain_top_k_undirected,
    selected_backbone_sensors,
)
from libs.graph.event import EventGraph, EventGraphSpec
from libs.graph.fused import FusedGraph, FusedGraphSpec
from libs.graph.hierarchy_model import GraphHierarchy, HierarchySpec
from libs.graph.lag import LagGraph, LagGraphSpec
from libs.graph.precision import PrecisionGraph, PrecisionGraphSpec
from libs.graph.transition import TransitionGraph, TransitionGraphSpec
from libs.io.schemas import (
    EVENT_GRAPH_SCHEMA,
    FUSED_GRAPH_SCHEMA,
    LAG_GRAPH_SCHEMA,
    TRANSITION_GRAPH_SCHEMA,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def _spark_functions():
    from pyspark.sql import functions as F

    return F


def _event_graph_schema():
    return EVENT_GRAPH_SCHEMA()


def _lag_graph_schema():
    return LAG_GRAPH_SCHEMA()


def _transition_graph_schema():
    return TRANSITION_GRAPH_SCHEMA()


def _fused_graph_schema():
    return FUSED_GRAPH_SCHEMA()


def retain_event_graph_top_k(event_df: pd.DataFrame, *, top_k_per_parameter_name: int) -> pd.DataFrame:
    """Retain the top-k undirected event edges per sensor from a precomputed event graph."""
    if event_df.empty:
        return event_df.copy()
    rows = _retain_top_k_undirected(
        event_df.to_dict(orient="records"),
        weight_key="event_weight",
        top_k_per_parameter_name=int(top_k_per_parameter_name),
    )
    if not rows:
        return pd.DataFrame(columns=event_df.columns)
    return pd.DataFrame(rows, columns=event_df.columns)


def retain_lag_graph_top_k(lag_df: pd.DataFrame, *, top_k_outgoing: int) -> pd.DataFrame:
    """Retain the top-k directed lag edges per source from a precomputed lag graph."""
    if lag_df.empty:
        return lag_df.copy()
    rows = _retain_top_k_directed(
        lag_df.to_dict(orient="records"),
        weight_key="lag_weight",
        top_k_outgoing=int(top_k_outgoing),
    )
    if not rows:
        return pd.DataFrame(columns=lag_df.columns)
    return pd.DataFrame(rows, columns=lag_df.columns)


def _build_precision_graph_from_covariance(
    selected_sensors: list[str],
    covariance: np.ndarray,
    *,
    ridge_lambda: float,
    min_abs_partial_corr: float,
) -> pd.DataFrame:
    return PrecisionGraph.from_covariance(
        covariance=covariance,
        spec=PrecisionGraphSpec(
            selected_sensors=tuple(selected_sensors),
            ridge_lambda=ridge_lambda,
            min_abs_partial_corr=min_abs_partial_corr,
        ),
    ).edges


def _build_precision_graph(
    window_x_df: pd.DataFrame,
    selected_sensors: list[str],
    *,
    ridge_lambda: float,
    min_abs_partial_corr: float,
) -> pd.DataFrame:
    return PrecisionGraph.from_window_x(
        window_x_df,
        spec=PrecisionGraphSpec(
            selected_sensors=tuple(selected_sensors),
            ridge_lambda=ridge_lambda,
            min_abs_partial_corr=min_abs_partial_corr,
        ),
    ).edges


def _build_event_graph(
    events_df: pd.DataFrame,
    windows_df: pd.DataFrame,
    *,
    min_count: int,
    min_npmi: float,
    top_k_per_parameter_name: int,
) -> pd.DataFrame:
    return EventGraph.from_events_and_windows(
        events_df,
        windows_df,
        spec=EventGraphSpec(
            min_count=min_count,
            min_npmi=min_npmi,
            top_k_per_parameter_name=top_k_per_parameter_name,
        ),
    ).edges


def _build_lag_graph(
    events_df: pd.DataFrame,
    *,
    tau_max_seconds: float,
    min_count: int,
    max_mean_lag_seconds: float | None,
    top_k_outgoing: int,
) -> pd.DataFrame:
    return LagGraph.from_events(
        events_df,
        spec=LagGraphSpec(
            tau_max_seconds=tau_max_seconds,
            min_count=min_count,
            max_mean_lag_seconds=max_mean_lag_seconds,
            top_k_outgoing=top_k_outgoing,
        ),
    ).edges


def _build_transition_graph(events_df: pd.DataFrame, *, min_count: int) -> pd.DataFrame:
    return TransitionGraph.from_events(
        events_df,
        spec=TransitionGraphSpec(min_count=min_count),
    ).edges


def build_event_graph_spark_table(
    events_df: DataFrame,
    windows_df: DataFrame,
    *,
    min_count: int,
    min_npmi: float,
    top_k_per_parameter_name: int,
) -> DataFrame:
    """Build same-window cooccurrence edges in Spark using positive normalized PMI."""
    F = _spark_functions()
    event_columns = ["tail_id", "flight_id", "timestamp_utc", "parameter_name"]
    window_columns = ["tail_id", "flight_id", "win_id", "t_start", "t_end"]
    joined = (
        events_df.select(*event_columns)
        .join(
            windows_df.select(*window_columns),
            on=["tail_id", "flight_id"],
            how="inner",
        )
        .where((F.col("timestamp_utc") >= F.col("t_start")) & (F.col("timestamp_utc") <= F.col("t_end")))
    )
    grouped = joined.groupBy("tail_id", "flight_id", "win_id").agg(F.sort_array(F.collect_set("parameter_name")).alias("parameter_names"))

    pair_rows = grouped.select(
        F.posexplode("parameter_names").alias("left_idx", "parameter_name_u"),
        F.col("parameter_names"),
    ).select(
        "parameter_name_u",
        F.expr("slice(parameter_names, left_idx + 2, size(parameter_names))").alias("right_candidates"),
    ).select("parameter_name_u", F.explode_outer("right_candidates").alias("parameter_name_v")).where(F.col("parameter_name_v").isNotNull())

    pair_counts = pair_rows.groupBy("parameter_name_u", "parameter_name_v").agg(F.count(F.lit(1)).cast("int").alias("cooccur_count"))
    parameter_name_window_counts = grouped.select(F.explode("parameter_names").alias("parameter_name")).groupBy("parameter_name").agg(
        F.count(F.lit(1)).cast("int").alias("parameter_name_window_count")
    )
    total_windows_df = grouped.agg(F.count(F.lit(1)).cast("double").alias("total_windows"))
    with_counts = (
        pair_counts.join(parameter_name_window_counts.withColumnRenamed("parameter_name", "parameter_name_u"), on="parameter_name_u")
        .withColumnRenamed("parameter_name_window_count", "left_window_count")
        .join(parameter_name_window_counts.withColumnRenamed("parameter_name", "parameter_name_v"), on="parameter_name_v")
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
        .where((F.col("cooccur_count") >= F.lit(max(int(min_count), 1))) & (F.col("event_weight") >= F.lit(float(min_npmi))))
    )
    if int(top_k_per_parameter_name) > 0:
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
        rank_window = Window.partitionBy("parameter_name").orderBy(F.col("event_weight").desc(), F.col("parameter_name_min"), F.col("parameter_name_max"))
        keep = exploded.withColumn("rank", F.row_number().over(rank_window)).where(F.col("rank") <= int(top_k_per_parameter_name)).select(
            "parameter_name_min", "parameter_name_max"
        ).distinct()
        with_counts = with_counts.join(
            keep,
            on=[with_counts.parameter_name_u == keep.parameter_name_min, with_counts.parameter_name_v == keep.parameter_name_max],
            how="inner",
        ).select(with_counts["*"])
    return with_counts.select(
        "parameter_name_u",
        "parameter_name_v",
        "cooccur_count",
        "event_weight",
        F.lit("event").alias("edge_family"),
    )


def build_lag_graph_spark_table(
    events_df: DataFrame,
    *,
    tau_max_seconds: float,
    min_count: int,
    max_mean_lag_seconds: float | None,
    top_k_outgoing: int,
) -> DataFrame:
    """Build directed lag edges as conditional probabilities discounted by mean lag."""
    F = _spark_functions()
    grouped = (
        events_df.select("tail_id", "flight_id", "timestamp_utc", "parameter_name")
        .groupBy("tail_id", "flight_id")
        .applyInPandas(
            lambda pdf: _build_lag_graph(
                prepare_events_df(pdf),
                tau_max_seconds=tau_max_seconds,
                min_count=min_count,
                max_mean_lag_seconds=max_mean_lag_seconds,
                top_k_outgoing=top_k_outgoing,
            ),
            schema=_lag_graph_schema(),
        )
    )
    aggregated = grouped.groupBy("parameter_name_u", "parameter_name_v", "edge_family").agg(
        F.sum("lag_count").cast("int").alias("lag_count"),
        F.sum(F.col("mean_lag_seconds") * F.col("lag_count")).alias("weighted_mean_lag"),
    ).select(
        "parameter_name_u",
        "parameter_name_v",
        "edge_family",
        "lag_count",
        (F.col("weighted_mean_lag") / F.col("lag_count")).alias("mean_lag_seconds"),
    )
    source_totals = aggregated.groupBy("parameter_name_u").agg(F.sum("lag_count").cast("double").alias("source_total"))
    tau = max(float(tau_max_seconds), 1e-6)
    return (
        aggregated.join(source_totals, on="parameter_name_u", how="inner")
        .withColumn("conditional_probability", F.col("lag_count") / F.col("source_total"))
        .withColumn("shortness", F.greatest(F.lit(0.0), F.lit(1.0) - (F.col("mean_lag_seconds") / F.lit(tau))))
        .withColumn("lag_weight", F.col("conditional_probability") * F.col("shortness"))
        .select("parameter_name_u", "parameter_name_v", "edge_family", "lag_count", "lag_weight", "mean_lag_seconds")
    )


def build_transition_graph_spark_table(events_df: DataFrame, *, min_count: int) -> DataFrame:
    """Build immediate transition edges as row-normalized transition probabilities."""
    F = _spark_functions()

    grouped = (
        events_df.select("tail_id", "flight_id", "timestamp_utc", "parameter_name")
        .groupBy("tail_id", "flight_id")
        .applyInPandas(
            lambda pdf: _build_transition_graph(prepare_events_df(pdf), min_count=min_count),
            schema=_transition_graph_schema(),
        )
    )
    count_df = grouped.groupBy("parameter_name_u", "parameter_name_v", "edge_family").agg(
        F.sum("precedence_count").cast("int").alias("precedence_count")
    )
    source_totals = count_df.groupBy("parameter_name_u").agg(F.sum("precedence_count").cast("double").alias("source_total"))
    return count_df.join(source_totals, on="parameter_name_u", how="inner").withColumn(
        "precedence_weight",
        F.col("precedence_count") / F.col("source_total"),
    )


def build_fused_graph_spark_table(
    precision_df: DataFrame,
    event_df: DataFrame,
    lag_df: DataFrame,
    *,
    alpha: float,
    beta: float,
    gamma: float,
) -> DataFrame:
    """Build fused graph edges in Spark from component graph tables."""
    F = _spark_functions()
    precision_edges = precision_df.select(
        "parameter_name_u",
        "parameter_name_v",
        F.col("precision_weight").cast("double").alias("precision_weight"),
    )
    event_edges = event_df.select(
        "parameter_name_u",
        "parameter_name_v",
        F.col("event_weight").cast("double").alias("event_weight"),
    )
    lag_edges = (
        lag_df.select(
            F.least("parameter_name_u", "parameter_name_v").alias("parameter_name_u"),
            F.greatest("parameter_name_u", "parameter_name_v").alias("parameter_name_v"),
            F.col("lag_weight").cast("double").alias("lag_weight"),
        )
        .groupBy("parameter_name_u", "parameter_name_v")
        .agg(F.max("lag_weight").alias("lag_weight"))
    )
    fused = (
        precision_edges.join(event_edges, on=["parameter_name_u", "parameter_name_v"], how="full_outer")
        .join(lag_edges, on=["parameter_name_u", "parameter_name_v"], how="full_outer")
        .na.fill({"precision_weight": 0.0, "event_weight": 0.0, "lag_weight": 0.0})
        .withColumn(
            "fused_weight",
            (F.lit(float(alpha)) * F.col("precision_weight"))
            + (F.lit(float(beta)) * F.col("event_weight"))
            + (F.lit(float(gamma)) * F.col("lag_weight")),
        )
        .where(F.col("fused_weight") > F.lit(0.0))
        .withColumn("edge_family", F.lit("fused"))
    )
    return fused.select(
        "parameter_name_u",
        "parameter_name_v",
        "precision_weight",
        "event_weight",
        "lag_weight",
        "fused_weight",
        "edge_family",
    )


def build_hierarchy_from_fused_spark_table(
    fused_df: DataFrame,
    *,
    parameter_names: list[str],
    min_fused_edge_weight: float,
    hierarchy_top_k_per_parameter_name: int,
    hierarchy_subsystem_min_edge_weight: float | None = None,
    hierarchy_system_min_edge_weight: float | None = None,
) -> pd.DataFrame:
    """Build hierarchy from a Spark fused-edge table using Spark pruning and small driver-side clustering."""
    return GraphHierarchy.from_fused_spark(
        fused_df,
        parameter_names=parameter_names,
        spec=HierarchySpec(
            min_edge_weight=min_fused_edge_weight,
            top_k_per_parameter_name=hierarchy_top_k_per_parameter_name,
            subsystem_min_edge_weight=hierarchy_subsystem_min_edge_weight,
            system_min_edge_weight=hierarchy_system_min_edge_weight,
        ),
    ).rows


def build_precision_graph_from_window_x_spark_table(
    window_x_df: DataFrame,
    *,
    selected_sensors: list[str],
    ridge_lambda: float,
    min_abs_partial_corr: float,
) -> pd.DataFrame:
    """Build precision edges from Spark-aggregated covariance stats."""
    return PrecisionGraph.from_window_x_spark(
        window_x_df,
        spec=PrecisionGraphSpec(
            selected_sensors=tuple(str(item) for item in selected_sensors if str(item)),
            ridge_lambda=ridge_lambda,
            min_abs_partial_corr=min_abs_partial_corr,
        ),
    ).edges


def _fuse_graphs(
    precision_df: pd.DataFrame,
    event_df: pd.DataFrame,
    lag_df: pd.DataFrame,
    *,
    alpha: float,
    beta: float,
    gamma: float,
) -> pd.DataFrame:
    return FusedGraph.from_components(
        precision_df,
        event_df,
        lag_df,
        spec=FusedGraphSpec(alpha=alpha, beta=beta, gamma=gamma),
    ).edges


def _assign_hierarchy(
    fused_df: pd.DataFrame,
    parameter_names: list[str],
    *,
    min_edge_weight: float,
    top_k_per_parameter_name: int = 3,
    subsystem_min_edge_weight: float | None = None,
    system_min_edge_weight: float | None = None,
) -> pd.DataFrame:
    return GraphHierarchy.from_fused(
        fused_df,
        parameter_names,
        spec=HierarchySpec(
            min_edge_weight=min_edge_weight,
            top_k_per_parameter_name=top_k_per_parameter_name,
            subsystem_min_edge_weight=subsystem_min_edge_weight,
            system_min_edge_weight=system_min_edge_weight,
        ),
    ).rows


def build_graph_artifact_tables(
    raw_df: pd.DataFrame,
    events_df: pd.DataFrame,
    windows_df: pd.DataFrame,
    backbone_df: pd.DataFrame,
    *,
    precision_ridge_lambda: float = 1.0,
    min_abs_partial_corr: float = 0.05,
    min_event_count: int = 1,
    min_event_npmi: float = 0.0,
    event_top_k_per_parameter_name: int = 8,
    lag_tau_max_seconds: float = 30.0,
    min_lag_count: int = 1,
    max_mean_lag_seconds: float | None = None,
    lag_top_k_outgoing: int = 8,
    min_transition_count: int = 1,
    alpha: float = 1.0,
    beta: float = 1.0,
    gamma: float = 1.0,
    min_fused_edge_weight: float = 0.05,
    hierarchy_top_k_per_parameter_name: int = 3,
    hierarchy_subsystem_min_edge_weight: float | None = None,
    hierarchy_system_min_edge_weight: float | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    from libs.windows import build_window_features_dataframe

    window_x_df = build_window_features_dataframe(
        raw_df,
        pd.DataFrame(columns=["tail_id", "flight_id", "parameter_name", "timestamp_utc", "event_type_detected", "payload"]),
        windows_df,
    )
    return build_graph_artifacts_from_window_x_table(
        window_x_df,
        events_df,
        windows_df,
        backbone_df,
        precision_ridge_lambda=precision_ridge_lambda,
        min_abs_partial_corr=min_abs_partial_corr,
        min_event_count=min_event_count,
        min_event_npmi=min_event_npmi,
        event_top_k_per_parameter_name=event_top_k_per_parameter_name,
        lag_tau_max_seconds=lag_tau_max_seconds,
        min_lag_count=min_lag_count,
        max_mean_lag_seconds=max_mean_lag_seconds,
        lag_top_k_outgoing=lag_top_k_outgoing,
        min_transition_count=min_transition_count,
        alpha=alpha,
        beta=beta,
        gamma=gamma,
        min_fused_edge_weight=min_fused_edge_weight,
        hierarchy_top_k_per_parameter_name=hierarchy_top_k_per_parameter_name,
        hierarchy_subsystem_min_edge_weight=hierarchy_subsystem_min_edge_weight,
        hierarchy_system_min_edge_weight=hierarchy_system_min_edge_weight,
    )


def build_graph_artifacts_from_window_x_table(
    window_x_df: pd.DataFrame,
    events_df: pd.DataFrame,
    windows_df: pd.DataFrame,
    backbone_df: pd.DataFrame,
    *,
    precision_ridge_lambda: float = 1.0,
    min_abs_partial_corr: float = 0.05,
    min_event_count: int = 1,
    min_event_npmi: float = 0.0,
    event_top_k_per_parameter_name: int = 8,
    lag_tau_max_seconds: float = 30.0,
    min_lag_count: int = 1,
    max_mean_lag_seconds: float | None = None,
    lag_top_k_outgoing: int = 8,
    min_transition_count: int = 1,
    alpha: float = 1.0,
    beta: float = 1.0,
    gamma: float = 1.0,
    min_fused_edge_weight: float = 0.05,
    hierarchy_top_k_per_parameter_name: int = 3,
    hierarchy_subsystem_min_edge_weight: float | None = None,
    hierarchy_system_min_edge_weight: float | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    event_rows = prepare_events_df(events_df)
    window_rows = prepare_windows_df(windows_df)
    selected_sensors = selected_backbone_sensors(backbone_df)

    precision_df = _build_precision_graph(
        window_x_df,
        selected_sensors,
        ridge_lambda=precision_ridge_lambda,
        min_abs_partial_corr=min_abs_partial_corr,
    )
    event_df = _build_event_graph(
        event_rows,
        window_rows,
        min_count=min_event_count,
        min_npmi=min_event_npmi,
        top_k_per_parameter_name=event_top_k_per_parameter_name,
    )
    lag_df = _build_lag_graph(
        event_rows,
        tau_max_seconds=lag_tau_max_seconds,
        min_count=min_lag_count,
        max_mean_lag_seconds=max_mean_lag_seconds,
        top_k_outgoing=lag_top_k_outgoing,
    )
    transition_df = _build_transition_graph(
        event_rows,
        min_count=min_transition_count,
    )
    fused_df = _fuse_graphs(
        precision_df,
        event_df,
        lag_df,
        alpha=float(alpha),
        beta=float(beta),
        gamma=float(gamma),
    )
    parameter_name_union = parameter_name_union_from_window_x(window_x_df, event_rows, selected_sensors)
    hierarchy_df = _assign_hierarchy(
        fused_df,
        parameter_name_union,
        min_edge_weight=min_fused_edge_weight,
        top_k_per_parameter_name=hierarchy_top_k_per_parameter_name,
        subsystem_min_edge_weight=hierarchy_subsystem_min_edge_weight,
        system_min_edge_weight=hierarchy_system_min_edge_weight,
    )
    return precision_df, event_df, lag_df, transition_df, fused_df, hierarchy_df


def build_graph_component_tables_from_window_x_table(
    window_x_df: pd.DataFrame,
    events_df: pd.DataFrame,
    windows_df: pd.DataFrame,
    backbone_df: pd.DataFrame,
    *,
    precision_ridge_lambda: float = 1.0,
    min_abs_partial_corr: float = 0.05,
    min_event_count: int = 1,
    min_event_npmi: float = 0.0,
    lag_tau_max_seconds: float = 30.0,
    min_lag_count: int = 1,
    max_mean_lag_seconds: float | None = None,
    min_transition_count: int = 1,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build graph components without event/lag top-k pruning for cacheable graph sweeps."""
    event_rows = prepare_events_df(events_df)
    window_rows = prepare_windows_df(windows_df)
    selected_sensors = selected_backbone_sensors(backbone_df)

    precision_df = _build_precision_graph(
        window_x_df,
        selected_sensors,
        ridge_lambda=precision_ridge_lambda,
        min_abs_partial_corr=min_abs_partial_corr,
    )
    event_df = _build_event_graph(
        event_rows,
        window_rows,
        min_count=min_event_count,
        min_npmi=min_event_npmi,
        top_k_per_parameter_name=0,
    )
    lag_df = _build_lag_graph(
        event_rows,
        tau_max_seconds=lag_tau_max_seconds,
        min_count=min_lag_count,
        max_mean_lag_seconds=max_mean_lag_seconds,
        top_k_outgoing=0,
    )
    transition_df = _build_transition_graph(
        event_rows,
        min_count=min_transition_count,
    )
    return precision_df, event_df, lag_df, transition_df


def build_graph_fusion_from_tables(
    window_x_df: pd.DataFrame,
    event_df: pd.DataFrame,
    lag_df: pd.DataFrame,
    backbone_df: pd.DataFrame,
    *,
    precision_ridge_lambda: float = 1.0,
    min_abs_partial_corr: float = 0.05,
    alpha: float = 1.0,
    beta: float = 1.0,
    gamma: float = 1.0,
    min_fused_edge_weight: float = 0.05,
    hierarchy_top_k_per_parameter_name: int = 3,
    hierarchy_subsystem_min_edge_weight: float | None = None,
    hierarchy_system_min_edge_weight: float | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build precision, fused graph, and hierarchy from pre-aggregated graph tables."""
    selected_sensors = selected_backbone_sensors(backbone_df)
    precision_df = _build_precision_graph(
        window_x_df,
        selected_sensors,
        ridge_lambda=precision_ridge_lambda,
        min_abs_partial_corr=min_abs_partial_corr,
    )
    fused_df, hierarchy_df = build_graph_fusion_from_component_tables(
        precision_df,
        event_df,
        lag_df,
        backbone_df,
        alpha=alpha,
        beta=beta,
        gamma=gamma,
        min_fused_edge_weight=min_fused_edge_weight,
        hierarchy_top_k_per_parameter_name=hierarchy_top_k_per_parameter_name,
        hierarchy_subsystem_min_edge_weight=hierarchy_subsystem_min_edge_weight,
        hierarchy_system_min_edge_weight=hierarchy_system_min_edge_weight,
    )
    return precision_df, fused_df, hierarchy_df


def build_graph_fusion_from_component_tables(
    precision_df: pd.DataFrame,
    event_df: pd.DataFrame,
    lag_df: pd.DataFrame,
    backbone_df: pd.DataFrame,
    *,
    alpha: float = 1.0,
    beta: float = 1.0,
    gamma: float = 1.0,
    min_fused_edge_weight: float = 0.05,
    hierarchy_top_k_per_parameter_name: int = 3,
    hierarchy_subsystem_min_edge_weight: float | None = None,
    hierarchy_system_min_edge_weight: float | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build fused graph and hierarchy from already-computed component tables."""
    selected_sensors = selected_backbone_sensors(backbone_df)
    fused_df = _fuse_graphs(
        precision_df,
        event_df,
        lag_df,
        alpha=float(alpha),
        beta=float(beta),
        gamma=float(gamma),
    )
    parameter_name_union = parameter_name_union_from_component_tables(backbone_df, event_df, lag_df, selected_sensors)
    hierarchy_df = _assign_hierarchy(
        fused_df,
        parameter_name_union,
        min_edge_weight=min_fused_edge_weight,
        top_k_per_parameter_name=hierarchy_top_k_per_parameter_name,
        subsystem_min_edge_weight=hierarchy_subsystem_min_edge_weight,
        system_min_edge_weight=hierarchy_system_min_edge_weight,
    )
    return fused_df, hierarchy_df
