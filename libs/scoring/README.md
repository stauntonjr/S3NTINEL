# Scoring

## Purpose

`libs/scoring` owns raw and calibrated anomaly scoring over fitted phase and structural artifacts.

It does not own:
- anomaly attribution
- injected simulation faults
- telemetry event extraction

## How To Use

- Use `WindowScoresRawTable` and `WindowScoresCalibratedTable` as the canonical production scoring path.
- Keep production scoring semantics in Spark-owned `Table` builders only.
- Use local pandas materialization only after score artifacts already exist, for bounded validation/reporting/test assertions.

## Contents

- `channels.py`
  - canonical score-component names and score-map helpers
- `tables.py`
  - canonical raw/calibrated score builders
- `validator.py`
  - score-vs-truth validation helpers

## Model / Concepts

Scoring is split into:
- raw score generation from fitted structure and phase context
- calibration of those scores for downstream emission decisions

## Data / Artifacts

Persisted score artifacts are defined in `libs/io/schemas/scoring.py`:
- raw scores
- calibrated scores

## Math / Methods

Scores summarize regime, reconstruction, event, behavior-mechanism, and coherence
deviation at the window level, then are calibrated into phase-conditioned rarity
(`p_value`) plus a conservative `emit_ready` flag.

The current canonical channel contract includes:
- `regime_deviation`
- `reconstruction_error`
- `event_discordance`
- `bound_violation`
- `accumulation_violation`
- `response_violation`
- `state_violation`
- `coherence_break`

`emit_ready` now means:
- the phase bucket is warm enough to calibrate
- either the upstream raw score is already `medium` / `high`
- or a `low`-severity window is rare within its detected phase

## Subject Matter View

Scoring answers: how unusual is this window relative to learned structure and current operating context?

## Testing / Validation

- unit tests cover score model behavior
- integration tests cover the stage-80 Spark scoring path

## Notes

- Local score assembly APIs were intentionally removed to prevent Spark-vs-local model drift.
- The raw score schema keeps `subsystem_scores` for compatibility, but the canonical scorer currently writes an empty map there.
- Calibration implementation currently lives in `libs/conformal`.
