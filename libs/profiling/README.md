# Profiling

## Purpose

`libs/profiling` owns parameter profiling over canonical telemetry:
- datatype profiling
- continuous scaling/profile statistics
- behavior-family profiling

It does not own:
- simulation behavior generation
- window creation
- anomaly scoring

## How To Use

- Use the profiling model objects for in-memory/profile reasoning.
- Use `pipeline.py` for the persisted Spark-facing adapter layer.
- Use `validator.py` to compare profiled outputs against known truth.

## Contents

- `model.py`
  - `ParameterProfile`
  - `ParameterDatatypeProfile`
  - `ContinuousScalingProfile`
  - `ParameterBehaviorProfile`
  - `CategoricalDistribution`
- `pipeline.py`
  - thin adapter over profiling model objects
- `validator.py`
  - profile-vs-truth validation

## Model / Concepts

Profiling is intentionally split into distinct model families:
- datatype classification
- continuous scaling/statistics
- behavior-family classification

Those profiles become reusable downstream structure for windows, scoring, and anomaly interpretation.

## Data / Artifacts

Persisted profiling artifacts are defined in `libs/io/schemas/profiling.py`.

## Math / Methods

Profiling uses a mix of:
- distribution summary statistics
- categorical frequency summaries
- behavior-family-specific classifier logic

## Subject Matter View

Profiling answers: what kind of signal is this parameter, how does it scale, and what behavior family best describes it?

## Testing / Validation

- unit and integration tests cover profile generation
- validators compare profiled datatype/behavior outputs to known truth

## Notes

- The profiling hot path is still Spark-oriented overall, but some behavior profiling remains Python-side because the family profilers are implemented there.
