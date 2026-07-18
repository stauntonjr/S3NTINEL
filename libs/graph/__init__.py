"""Spark graph artifact builders and validation helpers for the active V2 fitting pipeline."""

from libs.graph.lag import LagBandSpec, LagProfileGraph, resolve_lag_band_specs
from libs.graph.pipeline import (
    build_graph_components_with_diagnostics_spark_table,
    collapse_lag_profile_spark_table,
    retain_event_graph_top_k,
    retain_lag_graph_top_k,
)
from libs.graph.tables import (
    EventGraphTable,
    FusedGraphTable,
    GraphParameterUniverseTable,
    HierarchyArtifactSet,
    HierarchyEdgeEvidenceTable,
    HierarchySensorMapTable,
    LagCandidatePairsFrame,
    LagGraphTable,
    LagProfileTable,
    PrecisionGraphTable,
    TransitionGraphTable,
)
from libs.graph.validator import (
    build_coupling_validation_summary,
    build_graph_validation_summary,
    validate_expected_graph_signatures,
    validate_hierarchy_recovery,
)

__all__ = [
    "LagBandSpec",
    "LagProfileGraph",
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
    "HierarchyArtifactSet",
    "HierarchyEdgeEvidenceTable",
    "HierarchySensorMapTable",
    "retain_event_graph_top_k",
    "retain_lag_graph_top_k",
    "resolve_lag_band_specs",
    "validate_expected_graph_signatures",
    "validate_hierarchy_recovery",
]
