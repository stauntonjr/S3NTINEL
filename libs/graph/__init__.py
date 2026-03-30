"""Graph artifact builders for the active V2 fitting pipeline."""

from libs.graph.event import EventGraph, EventGraphSpec
from libs.graph.fused import FusedGraph, FusedGraphSpec
from libs.graph.hierarchy_artifacts import GraphHierarchy, HierarchySpec
from libs.graph.lag import LagBandSpec, LagProfileGraph, resolve_lag_band_specs
from libs.graph.pipeline import (
    build_graph_components_with_diagnostics_spark_table,
    collapse_lag_profile_spark_table,
    retain_event_graph_top_k,
    retain_lag_graph_top_k,
)
from libs.graph.precision import PrecisionGraph, PrecisionGraphSpec
from libs.graph.tables import (
    EventGraphTable,
    FusedGraphTable,
    GraphParameterUniverseTable,
    HierarchySensorMapTable,
    LagCandidatePairsFrame,
    LagGraphTable,
    LagProfileTable,
    PrecisionGraphTable,
    TransitionGraphTable,
)
from libs.graph.transition import TransitionGraph, TransitionGraphSpec
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
    "LagBandSpec",
    "LagProfileGraph",
    "PrecisionGraph",
    "PrecisionGraphSpec",
    "TransitionGraph",
    "TransitionGraphSpec",
    "build_graph_validation_summary",
    "build_coupling_validation_summary",
    "build_graph_components_with_diagnostics_spark_table",
    "collapse_lag_profile_spark_table",
    "PrecisionGraphTable",
    "EventGraphTable",
    "LagCandidatePairsFrame",
    "LagProfileTable",
    "LagGraphTable",
    "TransitionGraphTable",
    "FusedGraphTable",
    "GraphParameterUniverseTable",
    "HierarchySensorMapTable",
    "retain_event_graph_top_k",
    "retain_lag_graph_top_k",
    "resolve_lag_band_specs",
    "validate_expected_graph_signatures",
    "validate_hierarchy_recovery",
]
