# Fitting Workflow

This note defines the intended one-off fitting workflow for S3NTINEL V2.

The purpose is to pin down:

- what gets profiled once up front
- what gets persisted as reusable artifacts
- what downstream stages should consume during inference

This is the canonical workflow reference for datatype profiling, robust scaling,
behavior profiling, and backbone fitting.

For the active pipeline path, see [v2_architecture.md](/home/jrs/code/S3NTINEL/sentinel/docs/v2_architecture.md).
For the mathematical interpretation of robust scaling and `window_x`, see [theory_foundations.md](/home/jrs/code/S3NTINEL/sentinel/docs/theory_foundations.md).

## 1. Signal semantics

Two value fields matter in the simulator path:

- `parameter_value_clean`
  - nominal clean value before observation noise and measurement effects
  - used for simulator-side labels, debug, and truth-oriented validation

- `parameter_value`
  - observed downstream signal
  - used by profilers, detectors, `window_x`, backbone fitting, phase fitting, and
    runtime scoring

The fitting workflow always operates on `parameter_value`, not
`parameter_value_clean`, unless a document explicitly says otherwise.

## 2. Design rule

Behavior family, datatype, and scaling statistics are primarily **parameter
metadata artifacts**, not per-window anomaly outputs.

The intended sequence is:

1. fit parameter metadata artifacts once from observed telemetry
2. persist them
3. use them during backbone/graph/phase fitting and inference
4. add streaming monitors later only where adaptation or drift detection is needed

This keeps nominal parameter semantics stable and prevents the inference path from
reclassifying every parameter from scratch on every run.

## 3. Sequential fitting stages

## 3.1 Datatype and rate profiling

Active stage:

- `pipelines/05_parameter_profiles_fit.py`

Input:

- raw telemetry on `parameter_value`

Output artifact:

- `parameter_datatype_profile`

Minimum contents:

- `parameter_name`
- `parameter_datatype_profiled`
- `sampling_rate_profiled_hz`
- `median_interval_ms`
- missingness and cardinality statistics

This artifact should establish:

- whether a parameter is numeric, binary, categorical, constant, or high-cardinality
- the observed sampling cadence

Downstream uses:

- gating continuous-only scaling and `window_x`
- routing behavior profiling
- detector configuration

## 3.2 Continuous robust scaling profile

Input:

- raw telemetry on `parameter_value`
- `parameter_datatype_profile`

Output artifact:

- `continuous_scaling_profile`

Minimum contents:

- `parameter_name`
- `scaling_center_median`
- `scaling_iqr`
- optional guard statistics such as low/high quantiles

This artifact should be fit only for parameters whose profiled datatype is
continuous/numeric-like.

Downstream uses:

- `window_x`
- drift magnitude
- backbone energy and `G/H`
- residual scoring

## 3.3 Behavior profiling

Input:

- raw telemetry on `parameter_value`
- `parameter_datatype_profile`
- optionally `continuous_scaling_profile`

Output artifact:

- `parameter_behavior_profile`

Minimum contents:

- `parameter_name`
- `behavior_family_profiled`
- `behavior_profile_confidence`
- per-family score columns

The profile should classify each parameter into a small nominal behavior taxonomy,
for example:

- `regulated`
- `inertial`
- `accumulative`
- `discrete_state`
- `derived_response`
- `mixed_unknown`

Downstream uses:

- simulation/profile validation
- detector configuration
- future behavior-aware feature selection
- future misbehavior classification

## 3.4 Window representation and backbone fitting

Input:

- raw telemetry on `parameter_value`
- windows
- `continuous_scaling_profile`

Output artifacts:

- `window_x` optional persisted intermediate
- `backbone`
- `backbone_sensor_energy`

This stage should not require recomputing datatype or behavior identity. It should
consume the persisted profiles.

## 3.5 Graph, phase, and scoring stages

These stages consume upstream profile artifacts and structural artifacts rather than
reprofiling parameters on the fly.

In particular:

- graph fitting consumes `window_x`, `events`, and `backbone`
- phase fitting consumes `window_x`
- scoring consumes `phase_windows`, `phase_baselines`, and `hierarchy_sensor_map`

## 4. Inference-time use

Inference should normally consume the fitted artifacts:

- `parameter_datatype_profile`
- `continuous_scaling_profile`
- `parameter_behavior_profile`

and should not depend on online re-fitting of those parameter semantics.

## 5. Optional streaming monitors

Streaming monitors are still useful, but they are secondary:

- datatype/profile drift checks
- behavior confidence collapse
- new/unseen parameter bootstrap

These should be treated as monitors or adaptation aids, not as the primary source of
nominal parameter semantics.

## 6. Artifact naming summary

Recommended fitting-time artifacts:

- `parameter_datatype_profile`
- `continuous_scaling_profile`
- `parameter_behavior_profile`
- `window_x`
- `backbone`
- `backbone_sensor_energy`

This is the intended sequence for the active V2 path and for the richer simulation
work that follows.
