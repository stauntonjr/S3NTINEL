# Behavior

## Purpose

`libs/behavior` owns the parameter behavior families used by simulation.

It does not own:
- aircraft or flight orchestration
- telemetry pipelines
- anomaly outputs

## How To Use

- Behavior implementations are attached to `Parameter` objects during `Parameter.from_spec(...)`.
- Use the registry in `registry.py` to resolve behavior families by label.

## Contents

- `regulated.py`
- `inertial.py`
- `accumulative.py`
- `discrete_state.py`
  - concrete behavior families and violation handling
- `base.py`
  - core abstractions
- `registry.py`
  - behavior registration and lookup
- `tick.py`
  - shared behavior tick structures
- `validation.py`
  - family-level validation helpers

## Model / Concepts

Behavior families define:
- normal signal generation
- clean vs observed signal handling
- family-specific violation types

## Subject Matter View

These behaviors model how telemetry evolves physically or logically:
- regulated control loops
- inertial responses
- accumulative quantities
- discrete state machines

## Testing / Validation

- family contract tests and simulation realism tests validate behavior semantics

## Notes

- Behavior code is simulation-facing and Python-native by design.
