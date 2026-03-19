# Graph

## Purpose

`libs/graph` owns graph-domain models built from telemetry windows and events.

It does not own:
- raw window generation
- backbone fitting
- anomaly scoring

## How To Use

- Use the graph nouns for in-memory reasoning where they still exist:
  - `PrecisionGraph`
  - `EventGraph`
  - `FusedGraph`
  - `GraphHierarchy`
- Use `libs/graph/pipeline.py` as the canonical Spark implementation surface for graph fitting stages.
- Use `build_graph_components_with_diagnostics_spark_table(...)` during development when you need explicit per-component timings and row counts.

## Contents

- `precision.py`
- `event.py`
- `fused.py`
- `lag.py`
  - lag-band specs, multi-band lag profile construction, and collapse to legacy lag graph
- `hierarchy_artifacts.py`
  - first-class graph objects and hierarchy logic
- `data.py`
  - graph data preparation helpers
- `transition.py`
  - immediate-precedence transition graph
- `validator.py`
  - graph and hierarchy validation
- `pipeline.py`
  - canonical Spark graph builders and diagnostics

## Model / Concepts

The graph layer separates graph types by meaning:
- precision for continuous conditional structure
- event for repeated event co-presence
- lag profile for per-band delayed temporal relation
- lag graph for collapsed lag compatibility weights
- transition for state-sequence structure
- fused for combined graph evidence
- hierarchy for discovered structure over the fused graph

## Data / Artifacts

Persisted graph artifacts are defined in `libs/io/schemas/graph.py`.

## Math / Methods

Graph construction mixes:
- correlation / partial-correlation structure
- event-pair counting
- nearest-prior lag profiling in Spark with optional candidate pruning
- collapsed lag-graph compatibility weighting
- transition counts in Spark
- weighted fusion and clustering

## Subject Matter View

This package turns local telemetry relationships into structural models that support hierarchy discovery, phase fitting, and anomaly interpretation.

## Testing / Validation

- unit tests cover remaining graph objects and hierarchy behavior
- integration tests cover Spark graph semantics, stage 50, and hierarchy validation
- hierarchy validation reports include:
  - exact match by system/subsystem/module
  - pairwise same-cluster precision/recall/F1
  - clustering agreement metrics: NMI, AMI, and ARI

## Notes

- `pipeline.py` is the canonical implementation surface for stage-50 graph orchestration.
- `lag.py` owns the first-class multi-band lag profile builder; `transition.py` owns immediate transition semantics.
