# Anomaly

## Purpose

`libs/anomaly` owns downstream anomaly attribution artifacts and attribution-vs-truth validation.

It does not own:
- injected simulation faults
- raw score computation
- score calibration

Those belong to `libs/simulation`, `libs/scoring`, and their validators.

## How To Use

- Use `build_anomaly_window_attribution_df(...)`, `build_anomaly_telemetry_attribution_df(...)`, and `build_anomaly_event_attribution_df(...)` from the package surface for the persisted stage.
- Use `validate_attribution_against_fault_truth(...)` to compare attribution outputs against injected fault truth.

## Contents

- `model.py`
  - anomaly attribution nouns and artifact semantics
- `subsystem.py`
  - subsystem and top-sensor context
- `panel.py`
  - panel/message context extraction
- `attribution.py`
  - thin dataframe adapter over the anomaly model
- `validator.py`
  - attribution-vs-truth evaluation

## Model / Concepts

Main nouns:
- `AnomalyWindowAttribution`
- `AnomalyTelemetryAttribution`
- `AnomalyEventAttribution`
- `AnomalyAttributionContext`

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
- validator logic compares attribution outputs to injected fault truth

## Notes

- Use `fault` only in validation/truth comparison.
- Use `anomaly` for the downstream attribution domain.
