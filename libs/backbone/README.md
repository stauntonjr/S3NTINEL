# Backbone

## Purpose

`libs/backbone` owns the continuous reconstruction backbone used to summarize normal multivariate structure and sensor importance.

It does not own:
- window creation
- graph fitting
- phase assignment

Those belong to `libs/windows`, `libs/graph`, and `libs/phase`.

## How To Use

- Use `BackboneModel.from_window_feature_rows(...)` for in-memory fitting.
- Use the package pipeline helpers for persisted Spark-facing adapters.

## Contents

- `artifacts.py`
  - backbone nouns and fitting results
- `energy.py`
  - sensor energy calculations and event-aware selection scoring
- `fit.py`
  - lower-level fitting helpers
- `pipeline.py`
  - thin table adapter for persisted stages

## Model / Concepts

Main nouns:
- `BackboneSpec`
- `BackboneModel`
- `BackboneSensorEnergy`

The backbone is fitted from window feature rows and used later by graph, phase, and scoring paths.

## Data / Artifacts

Persisted outputs are defined in `libs/io/schemas/backbone.py`:
- backbone coefficients / `G/H`
- backbone sensor energy

## Math / Methods

The backbone is a reconstruction-oriented continuous model over scaled end-of-window feature vectors. Sensor selection now combines:
- continuous energy from `continuous_vector_t_end_scaled`
- an additive event-derived prior from `window_features.continuous_event_summary`

The event-derived prior influences sensor ranking only. It does not enter the `G/H` aggregates or the ridge solve directly.

## Subject Matter View

This package captures “normal continuous relationships” between parameters before graph and phase enrichments are added.

## Testing / Validation

- unit tests cover model fitting and energy derivation
- integration tests cover stage 10 persisted outputs

## Notes

- The main pipeline path is Spark-backed, but the domain model is expressed in library nouns rather than in stage files.
