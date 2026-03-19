# Conformal

## Purpose

`libs/conformal` owns the current score-calibration implementation used by stage 70.

## How To Use

- Use `build_calibrated_window_scores_table(...)` through the package surface or the scoring stage.

## Contents

- `pipeline.py`
  - empirical score calibration over detected phase partitions

## Model / Concepts

This package is active, but it is not a broad conformal framework. In its current form it is a compact calibration implementation for window scores.

## Data / Artifacts

It produces the calibrated score artifact defined in `libs/io/schemas/scoring.py`.

## Math / Methods

Current calibration is empirical and phase-conditioned:
- partition by tail / flight / detected phase
- rank `global_score`
- estimate a tail probability / p-value
- gate emission readiness via warmup rules

## Notes

- Keep the docs honest: this package is narrower than its name suggests today.
