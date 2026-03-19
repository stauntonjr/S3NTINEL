# Windows

## Purpose

`libs/windows` owns:
- window lifecycle and closure semantics
- per-window signal buffering
- the canonical segmented Spark window builder
- window feature extraction
- the `window_features` artifact

It does not own:
- raw event detection
- backbone fitting
- phase or scoring logic

## How To Use

- Use `WindowPolicy`, `Window`, and `AdaptiveWindowPlan` for the active runtime model.
- Use `build_windows_table(...)` for canonical window materialization.
- Use `build_window_features_spark_table(...)` for canonical dataframe materialization.
- Use `build_window_features_with_diagnostics_spark_table(...)` during development when you need explicit per-step timings and row counts.

## Contents

- `window.py`
  - `WindowPolicy`
  - `Window`
- `buffer.py`
  - `WindowSensorBuffer`
- `coverage.py`
  - coverage sampling helpers
- `features.py`
  - canonical Spark `window_features` builder
- `pipeline.py`
  - segmented Spark window orchestration

## Model / Concepts

The windows model is:
- `WindowPolicy` decides when a window closes
- `Window` owns one window's mutable state
- `WindowSensorBuffer` owns last-seen sensor state within a window
- `AdaptiveWindowPlan` is the canonical segmented Spark window builder
- `window_features` is the many-window artifact used by backbone, graph, and phase

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

- unit tests cover buffer, window, segmented window behavior, and feature behavior
- integration tests cover adaptive windowing and feature dataframe construction

## Notes / Constraints

- The canonical persisted artifact is `window_features`, replacing older `window_x` terminology.
- Spark is the canonical hot-path execution model.
