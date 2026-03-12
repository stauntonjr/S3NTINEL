# Libraries

## Purpose

`libs/` contains the reusable domain and infrastructure code used by the persisted pipelines, simulation, validators, and operational scripts.

It does not contain:
- stage entrypoints
- command-line orchestration
- repo-level test cases

Those live under [`pipelines/`](./../pipelines/README.md), [`scripts/`](./../scripts/README.md), and [`tests/`](./../tests/README.md).

## How To Use

- Import domain logic from the owning package.
- Prefer package README files before drilling into individual modules.
- Treat Spark-facing `pipeline.py` modules as adapter layers unless the package README says otherwise.

## Library Split

- `simulation/`
  - aircraft, flight, phase, fault, coupling, and authored examples
- `windows/`
  - window lifecycle, buffering, feature extraction, and the `WindowFeaturesDataFrame`
- `profiling/`
  - datatype, scaling, and behavior profiling
- `backbone/`
  - backbone model fitting and sensor energy
- `graph/`
  - precision, event, lag, transition, fused graph, and hierarchy logic
- `phase/`
  - phase feature selection, runtime detection, analysis, and validation
- `scoring/`
  - raw and calibrated window scoring
- `anomaly/`
  - anomaly attribution artifacts and attribution-vs-truth validation
- `events/`
  - event detection and event validation
- `io/`
  - persisted artifact schemas, row contracts, Spark/pandas bridge utilities
- `behavior/`
  - parameter behavior implementations and violation mechanics
- `testing/`
  - shared test infrastructure only
- `perf/`
  - MLflow, wall-time, and stage-manifest support
- `conformal/`
  - score calibration implementation
- `common/`
  - narrow shared constants such as event types and sensor datatype helpers

## Data / Artifacts

- Persisted artifact schemas live under [`libs/io/schemas/`](./io/schemas/).
- In-memory row contracts live in [`libs/io/contracts.py`](./io/contracts.py).
- Package READMEs describe which artifacts each library owns or consumes.

## Subject Matter View

The repo is organized around a telemetry-processing stack:

1. simulation can produce telemetry-shaped rows
2. profiling characterizes parameter behavior
3. windows and events build structural context
4. backbone, graph, and phase infer normal structure
5. scoring and anomaly attribution evaluate deviation from that structure

## Testing / Validation

- Shared test helpers live in [`libs/testing/`](./testing/README.md).
- Actual tests live in [`tests/`](./../tests/README.md).

## Notes

- Read the package-level README in any `libs/*` directory before refactoring that package.
- Prefer current package nouns over older terms like `assembly`, `native`, or `window_x`.
