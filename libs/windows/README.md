# Windows

## Purpose

`libs/windows` owns:
- window lifecycle and closure semantics
- per-window signal buffering
- window streaming
- window feature extraction
- the `WindowFeaturesDataFrame` artifact

It does not own:
- raw event detection
- backbone fitting
- phase or scoring logic

## How To Use

- Use `WindowPolicy`, `Window`, and `WindowStream` for the runtime model.
- Use `WindowFeatures` for one-window feature semantics.
- Use `build_window_features_dataframe(...)` or `build_window_features_spark_dataframe(...)` for dataframe materialization.

## Contents

- `window.py`
  - `WindowPolicy`
  - `Window`
- `buffer.py`
  - `WindowSensorBuffer`
- `stream.py`
  - `WindowStream`
- `features.py`
  - `WindowFeatures`
  - `WindowScaler`
  - `WindowFeatureSelection`
- `context.py`
  - window-context resolution
- `coverage.py`
  - coverage sampling helpers
- `adaptive.py`
  - Spark/grouped adaptive window adapter
- `window_features_dataframe.py`
  - dataframe adapter over context and features
- `pipeline.py`
  - thin strategy dispatch

## Model / Concepts

The windows model is:
- `WindowPolicy` decides when a window closes
- `Window` owns one window’s mutable state
- `WindowSensorBuffer` owns last-seen sensor state within a window
- `WindowStream` manages windows over ordered events
- `WindowFeatures` represents the end-of-window feature semantics
- `WindowFeaturesDataFrame` is the many-window artifact used by backbone, graph, and phase

## Data / Artifacts

Persisted window artifacts are defined in `libs/io/schemas/windows.py`:
- windows
- window features

## Math / Methods

Window feature extraction includes:
- end-of-window continuous vectors
- scaled continuous vectors
- categorical end states
- event-type counts
- drift-oriented summaries

## Subject Matter View

Windows convert a stream of events and telemetry updates into analysis segments that are meaningful for structural learning and anomaly detection.

## Testing / Validation

- unit tests cover buffer, window, stream, and feature behavior
- integration tests cover adaptive windowing and feature dataframe construction

## Notes / Constraints

- The canonical artifact name is `WindowFeaturesDataFrame`, replacing older `window_x` terminology.
- Spark is the preferred hot-path execution model; pandas is tolerated only where there is a specific technical reason.
