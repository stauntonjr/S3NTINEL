# Graph And Hierarchy Design

This document explains how S3NTINEL turns fitted telemetry and event structure
into graph artifacts and a parameter hierarchy. Current implementation ownership
is [libs/graph/README.md](../../libs/graph/README.md); active artifact contracts
are in [V2 architecture](../current/v2_architecture.md).

## Purpose

Graph construction retains several distinct forms of relationship evidence rather
than treating all correlation as one signal. Hierarchy fitting then uses a bounded,
auditable undirected view of that evidence to assign parameters to modules,
subsystems, and systems.

## Inputs And Outputs

Stage `50_build_graph.py` consumes fitted backbone metadata, `window_features`,
and detected `events`. It persists:

- `precision_graph` for continuous conditional dependence;
- `event_graph` for same-window event co-occurrence;
- `lag_profile` for directed, band-aware nearest-prior event relationships;
- `lag_graph` for the collapsed directed lag representation consumed by fusion;
- `transition_graph` for directed adjacent-event precedence;
- `fused_graph` for weighted structural evidence; and
- `graph_parameter_universe` for the bounded parameter set passed to hierarchy fitting.

Stage `60_fit_hierarchy.py` consumes the fused graph and parameter universe. It
persists `hierarchy_sensor_map` and `hierarchy_edge_evidence`.

## Component Semantics

`precision_graph` represents symmetric conditional association among selected
continuous parameters. `event_graph` captures repeated co-presence in the same
window. `lag_profile` retains directed source-to-target delay evidence per lag
band, including support. `transition_graph` represents immediate event sequence
evidence. These are not interchangeable: a coupling can be contemporaneous,
delayed, conditional, or sequential.

`lag_graph` is the current collapsed representation of the retained lag-profile
bands. It preserves downstream compatibility while `lag_profile` remains the
authoritative diagnostic artifact for lag-band and support interpretation.

## Fusion And Hierarchy

Fusion combines compatible component weights into `fused_graph`. The hierarchy
path converts this relationship evidence into an undirected structural decision:
directed lag and transition signals contribute evidence, but do not impose an
ancestor direction. This distinction prevents temporal precedence from being
mistaken for a containment relationship.

The hierarchy builder ranks fused neighbors in Spark, retains mutual top-k edges,
and performs the bounded clustering/rollup step only on the pruned result. The
resulting IDs in `hierarchy_sensor_map` are the current parameter-to-module,
subsystem, and system assignment used by downstream phase, scoring, and
attribution work.

## Evidence And Replay Invariants

`hierarchy_edge_evidence` records the retained mutual-top-k edge set that formed
the module-level decision. It carries endpoint ranks, fused component weights,
assigned hierarchy IDs, and directed lag evidence in both directions.

The stage also emits `hierarchy_edge_evidence_summary.json`, which compares
configured simulation coupling signatures with retained canonical edges. The
report is diagnostic: an absent signature is evidence for review, not a simulator
failure by itself.

For replay consistency:

1. graph artifacts and `graph_parameter_universe` must come from the same fitting run;
2. hierarchy fitting must use the persisted fused edge set, not recompute graph components;
3. edge evidence must describe the exact retained set used by the hierarchy builder; and
4. directed relationship artifacts must remain available after undirected hierarchy rollup.

## Validation

Graph and hierarchy validation evaluates exact hierarchy labels where simulation
truth is available, pairwise same-cluster precision/recall/F1, and clustering
agreement metrics such as NMI, AMI, and ARI. The retained-edge report supports
qualitative diagnosis of coupling traceability without changing the scoring
contract.

## Notes

Candidate weighting and hierarchy-quality improvements belong in the
[graph and anomaly plans](../plans/libs/README.md), not in this active design
description.
