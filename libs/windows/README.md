# Windows

## Purpose

`libs/windows` owns:
- window lifecycle and closure semantics
- window-policy profiling and selection
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
- Use `WindowPolicyProfileSpec` and `build_window_policy_profile_table(...)` to fit candidate window policies from detected events.
- Use `WindowPolicyEvaluationSpec` and `build_window_policy_profile_evaluation_report_spark(...)` to evaluate the selected policy against closure mix, downstream cost proxies, and bounded flight-subset stability.
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
- `policy_profile.py`
  - window policy candidate generation, scoring, selected-policy resolution, and evaluation reporting
- `features.py`
  - canonical Spark `window_features` builder
- `pipeline.py`
  - segmented Spark window orchestration

## Model / Concepts

The windows model is:
- `WindowPolicy` decides when a window closes
- `WindowPolicyProfile` fits candidate `WindowPolicy` rows from the event stream
- `WindowPolicyEvaluationSpec` controls bounded evaluation/reporting for the selected policy
- `Window` owns one window's mutable state
- `WindowSensorBuffer` owns last-seen sensor state within a window
- `AdaptiveWindowPlan` is the canonical segmented Spark window builder
- `window_features` is the many-window artifact used by backbone, graph, and phase

## Data / Artifacts

Persisted window artifacts are defined in `libs/io/schemas/windows.py`:
- window policy profile
- windows
- window features

## Math / Methods

Window feature extraction includes:
- raw-telemetry zero-order-held continuous state at `t_end`
- scaled continuous vectors
- categorical end states
- event-type counts
- additive continuous-event summaries derived from events inside the window
- drift-oriented summaries

## Subject Matter View

Windows convert a stream of events and telemetry updates into analysis segments that are meaningful for structural learning and anomaly detection.

## Testing / Validation

- unit tests cover buffer, window, segmented window behavior, and feature behavior
- integration tests cover adaptive windowing and feature dataframe construction

## Notes / Constraints

- The canonical persisted artifact is `window_features`, replacing older `window_x` terminology.
- `continuous_vector_t_end` and `continuous_vector_t_end_scaled` are raw telemetry snapshot-at-`t_end` fields. Event payload values no longer override those vectors.
- `continuous_event_summary` is additive context for event-rich windows; it does not replace the window-end telemetry state.
- Spark is the canonical hot-path execution model.
