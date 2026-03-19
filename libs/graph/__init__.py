"""Graph artifact builders for the active V2 fitting pipeline."""

from libs.graph.event import EventGraph, EventGraphSpec
from libs.graph.fused import FusedGraph, FusedGraphSpec
from libs.graph.hierarchy_artifacts import GraphHierarchy, HierarchySpec
from libs.graph.pipeline import (
    build_event_graph_spark_table,
    build_fused_graph_spark_table,
    build_graph_components_with_diagnostics_spark_table,
    build_graph_parameter_universe_spark_table,
    build_hierarchy_from_fused_spark_table,
    build_lag_graph_spark_table,
    build_precision_graph_from_window_features_spark_table,
    build_transition_graph_spark_table,
    retain_event_graph_top_k,
    retain_lag_graph_top_k,
)
from libs.graph.precision import PrecisionGraph, PrecisionGraphSpec
from libs.graph.validator import (
    build_coupling_validation_summary,
    build_graph_validation_summary,
    validate_expected_graph_signatures,
    validate_hierarchy_recovery,
)

__all__ = [
    "EventGraph",
    "EventGraphSpec",
    "FusedGraph",
    "FusedGraphSpec",
    "GraphHierarchy",
    "HierarchySpec",
    "PrecisionGraph",
    "PrecisionGraphSpec",
    "build_graph_validation_summary",
    "build_coupling_validation_summary",
    "build_event_graph_spark_table",
    "build_fused_graph_spark_table",
    "build_graph_components_with_diagnostics_spark_table",
    "build_graph_parameter_universe_spark_table",
    "build_hierarchy_from_fused_spark_table",
    "build_lag_graph_spark_table",
    "build_precision_graph_from_window_features_spark_table",
    "build_transition_graph_spark_table",
    "retain_event_graph_top_k",
    "retain_lag_graph_top_k",
    "validate_expected_graph_signatures",
    "validate_hierarchy_recovery",
]
