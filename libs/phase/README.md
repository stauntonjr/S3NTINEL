# Phase

## Purpose

`libs/phase` owns phase feature selection, phase detection runtime, phase analysis, and phase validation.

It does not own:
- telemetry window creation
- backbone fitting
- anomaly scoring

## How To Use

- Use `PhaseFeatureConfig` to define which window features drive detection.
- Use `PhaseDetectionPolicy` to detect phases from ordered window features.
- Use `PhaseFeatures` for phase artifact assembly.
- Use `validator.py` for detection-vs-truth evaluation and `analysis.py` for explanatory phase-separation analysis.

## Contents

- `model.py`
  - `PhaseFeatureConfig`
  - `PhaseFeatures`
- `runtime.py`
  - `Phase`
  - `PhaseBuffer`
  - `PhaseClustering`
  - `PhaseClusterAssignment`
  - `PhaseStream`
  - `PhaseDetectionPolicy`
- `validator.py`
  - evaluation and table validation
- `analysis.py`
  - explanatory analysis of phase-separating signals
- `pipeline.py`
  - Spark/table adapter for persisted stages

## Model / Concepts

The phase model is intentionally split:
- feature selection and artifact assembly in `model.py`
- ordered runtime detection and smoothing in `runtime.py`
- evaluation in `validator.py`
- analysis in `analysis.py`

## Data / Artifacts

Persisted artifacts are defined in `libs/io/schemas/phase.py`:
- phase windows
- phase baselines

Inputs come from the `WindowFeaturesDataFrame` path in `libs/windows`.

## Math / Methods

Phase detection combines:
- selected structure vectors from window features
- clustering over ordered windows
- assignment smoothing
- minimum-dwell enforcement
- baseline emission over assigned phases

## Subject Matter View

Phases represent recurring operating regimes such as ground, climb, cruise, or descent, inferred from telemetry behavior rather than only from labels.

## Testing / Validation

- unit tests cover runtime detection behavior and analysis
- integration tests cover stage 50 artifact generation
- simulation-backed validation compares detected phases against known phase truth

## Notes

- `PhaseFeatureConfig` is the active feature-selection object.
- `PhaseDetectionPolicy` is the active runtime policy object.
