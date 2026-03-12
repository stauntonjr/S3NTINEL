# Events

## Purpose

`libs/events` owns canonical event detection and event validation.

It does not own:
- cooccurrence as an event type
- window lifecycle
- graph fitting

## How To Use

- Use `build_events_table(...)` from the package surface for the canonical persisted path.
- Use the validator for simulation-backed event truth checks.

## Contents

- `extrema.py`
  - continuous event detection
- `categorical.py`
  - categorical and state-based event detection
- `pipeline.py`
  - thin composition layer for event building
- `cooccur.py`
  - event-pair relation utilities for cooccurrence/lag contexts
- `validator.py`
  - event detection evaluation

## Model / Concepts

Events are detected changes or state observations derived from telemetry.

Cooccurrence is treated as a relation over windows or lag structure, not as an event type.

## Data / Artifacts

The canonical event artifact is defined in `libs/io/schemas/events.py`.

## Subject Matter View

Events turn raw telemetry into discrete structural signals that support windows, graphs, and later attribution.

## Testing / Validation

- unit tests cover event detectors and event relation utilities
- integration tests cover stage 20 and simulation-backed validation

## Notes

- Keep event extraction logic distinct from windowing and graph semantics.
