# Anomaly

## Purpose

`libs/anomaly` owns downstream anomaly attribution artifacts and attribution-vs-truth validation.

It does not own:
- injected simulation misbehaviors
- raw score computation
- score calibration

Those belong to `libs/simulation`, `libs/scoring`, and their validators.

## How To Use

- Use `AnomalyWindowAttributionTable`, `AnomalyTelemetryAttributionTable`, and `AnomalyEventAttributionTable` from the package surface for the persisted stage.
- Use `validate_attribution_against_misbehavior_truth(...)` as the canonical truth validator.
- `validate_attribution_against_fault_truth(...)` remains as a deprecated compatibility wrapper.

## Contents

- `frames.py`
  - reusable subsystem, panel, and combined attribution context frames
- `tables.py`
  - persisted anomaly attribution artifact owners
- `pipeline.py`
  - thin orchestration over typed anomaly artifacts
- `validator.py`
  - attribution-vs-truth evaluation

## Model / Concepts

Main nouns:
- `AnomalyWindowAttributionTable`
- `AnomalyTelemetryAttributionTable`
- `AnomalyEventAttributionTable`
- `AnomalyAttributionContextFrame`

These represent downstream anomaly outputs, not simulation truth.

## Data / Artifacts

The package produces the persisted anomaly artifacts defined in `libs/io/schemas/anomaly.py`:
- window attribution
- telemetry attribution
- event attribution

## Subject Matter View

This package answers: given an anomalous window, which subsystem, parameters, and events are the most plausible explanation?

## Testing / Validation

- unit tests cover the anomaly model objects
- integration tests cover stage 80 and simulation-backed runner flows
- validator logic compares attribution outputs to injected misbehavior truth

## Notes

- Use `misbehavior` for simulator/source truth.
- `fault` remains a deprecated compatibility alias in validator/report wrappers.
- Use `anomaly` for the downstream attribution domain.
