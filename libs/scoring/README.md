# Scoring

## Purpose

`libs/scoring` owns raw and calibrated anomaly scoring over fitted phase and structural artifacts.

It does not own:
- anomaly attribution
- injected simulation faults
- telemetry event extraction

## How To Use

- Use `WindowScoreArtifacts` for in-memory score assembly semantics.
- Use `pipeline.py` for Spark-facing artifact construction.
- Use `rules.py` for lower-level score computations.

## Contents

- `artifacts.py`
  - score-domain nouns and rollup semantics
- `rules.py`
  - lower-level score calculations
- `pipeline.py`
  - persisted adapter
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

Scores summarize reconstruction and structural deviation at the window level, then are calibrated into phase-conditioned rarity (`p_value`) plus a conservative `emit_ready` flag.

`emit_ready` now means:
- the phase bucket is warm enough to calibrate
- either the upstream raw score is already `medium` / `high`
- or a `low`-severity window is rare within its detected phase

## Subject Matter View

Scoring answers: how unusual is this window relative to learned structure and current operating context?

## Testing / Validation

- unit tests cover score model behavior
- integration tests cover stage 60 and stage 70

## Notes

- Calibration implementation currently lives in `libs/conformal`.
