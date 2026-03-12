# Graph

## Purpose

`libs/graph` owns graph-domain models built from telemetry windows and events.

It does not own:
- raw window generation
- backbone fitting
- anomaly scoring

## How To Use

- Use the graph nouns for in-memory reasoning:
  - `PrecisionGraph`
  - `EventGraph`
  - `LagGraph`
  - `TransitionGraph`
  - `FusedGraph`
  - `GraphHierarchy`
- Use `libs/graph/pipeline.py` only as the Spark/table adapter layer for persisted stages.

## Contents

- `precision.py`
- `event.py`
- `lag.py`
- `transition.py`
- `fused.py`
- `hierarchy_model.py`
  - first-class graph objects and hierarchy logic
- `data.py`
  - graph data preparation helpers
- `validator.py`
  - graph and hierarchy validation
- `pipeline.py`
  - persisted Spark-facing adapter

## Model / Concepts

The graph layer separates graph types by meaning:
- precision for continuous conditional structure
- event for repeated event co-presence
- lag for delayed temporal relation
- transition for state-sequence structure
- fused for combined graph evidence
- hierarchy for discovered structure over the fused graph

## Data / Artifacts

Persisted graph artifacts are defined in `libs/io/schemas/graph.py`.

## Math / Methods

Graph construction mixes:
- correlation / partial-correlation structure
- event-pair counting
- lagged relation counting
- transition counts
- weighted fusion and clustering

## Subject Matter View

This package turns local telemetry relationships into structural models that support hierarchy discovery, phase fitting, and anomaly interpretation.

## Testing / Validation

- unit tests cover graph objects and hierarchy behavior
- integration tests cover stage 11 and hierarchy validation

## Notes

- `pipeline.py` is an adapter, not the conceptual home of the graph domain.
