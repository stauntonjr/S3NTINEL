# Phase

## Purpose

`libs/phase` owns phase feature selection, phase detection, phase analysis, and phase validation.

It does not own:
- telemetry window creation
- backbone fitting
- anomaly scoring

## How To Use

- Use `PhaseFeatureConfig` as the canonical in-memory object that defines which window features drive detection.
- Use `pipeline.py` for the canonical Spark/table stage path.
- Use `validator.py` for detection-vs-truth evaluation and `analysis.py` for explanatory phase-separation analysis.

## Contents

- `feature_config.py`
  - `PhaseFeatureConfig`
- `validator.py`
  - evaluation and table validation
- `analysis.py`
  - explanatory analysis of phase-separating signals
- `pipeline.py`
  - canonical Spark/table adapter for persisted stages

## Model / Concepts

The phase model is intentionally split:
- feature selection and in-memory config modeling in `feature_config.py`
- artifact assembly in `artifacts.py`
- feature/observation frame construction in `frames.py`
- fit and decode logic in `fit.py` and `decode.py`
- canonical stage orchestration in `pipeline.py`
- evaluation in `validator.py`
- analysis in `analysis.py`

## Data / Artifacts

Persisted artifacts are defined in `libs/io/schemas/phase.py`:
- phase windows
- phase baselines

Inputs come from the `window_features` artifact in `libs/windows` plus the persisted backbone artifact from stage 10.

## Math / Methods

Phase detection combines:
- selected structure vectors from window features
- backbone-backed feature selection
- Spark-first per-flight clustering and ordered assignment
- assignment smoothing
- minimum-dwell enforcement
- baseline emission over assigned phases

## Subject Matter View

Phases represent recurring operating regimes such as ground, climb, cruise, or descent, inferred from telemetry behavior rather than only from labels.

## Testing / Validation

- unit tests cover phase detection behavior and analysis
- integration tests cover stage 50 artifact generation
- simulation-backed validation compares detected phases against known phase truth

## Notes

- `PhaseFeatureConfig` is the active feature-selection object.
- Plain `dict` phase configs are a persisted/package-boundary form; `libs/phase` normalizes them back into `PhaseFeatureConfig` for in-memory work.
- `pipeline.py` is the production phase stage and does not refit backbone state internally.
- Cross-flight phase history is intentionally out of scope in the current production stage.
