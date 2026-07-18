# Behavior Profiling Design

This note records the design for the behavior-profiling layer that mirrors the richer
simulation semantics.

The motivation is symmetry:

- if simulation assigns behavior semantics to parameters
- then runtime detection/profiling should infer those same semantics from real telemetry

Without that symmetry, the simulator becomes richer than the runtime model and the
sim-to-real gap widens.

For the corresponding deviation/anomaly ontology, see [misbehavior_taxonomy.md](../reference/misbehavior_taxonomy.md).
For the intended fitting-stage placement of datatype profiling, robust scaling, and
behavior profiling, see [fitting_workflow.md](../current/fitting_workflow.md).

## 1. Purpose

The behavior profiler sits next to the datatype/rate profiler and infers
parameter-level behavioral semantics from observed telemetry.

This is not anomaly detection. It is structure inference and metadata generation.

Current profiler responsibilities already include:

- datatype inference
- sampling-rate estimation

See:
- [libs/profiling/README.md](../../libs/profiling/README.md)

The current layer now infers:

- behavior family
- behavioral traits
- confidence
- primitive evidence

## 1.1 Placement in the fitting workflow

Behavior profiling is implemented as a one-off fitting-stage artifact, not a
mandatory always-on inference dependency.

The intended sequence is:

1. fit `parameter_datatype_profile`
2. fit `continuous_scaling_profile`
3. fit `parameter_behavior_primitive_profile`
4. fit `parameter_behavior_profile`
5. reuse those artifacts during backbone/graph/phase fitting and inference

The active pipeline stages for this metadata are:

- `pipelines/10_parameter_profiles_fit.py`
- `pipelines/12_behavior_profiles_fit.py`

That means:

- the profiler operates on observed `parameter_value`
- it produces stable nominal metadata
- downstream stages normally consume the persisted profile rather than recomputing it
  continuously

An optional streaming behavior monitor, if introduced, would cover:

- new parameter bootstrap
- confidence collapse
- behavior drift monitoring

but that monitor is secondary to the fitted metadata artifact.

## 2. Canonical fields

Simulation/source side:

- `behavior_family_label`
- `behavior_traits_label`

Runtime/profile side:

- `behavior_family_profiled`
- `behavior_profile_confidence`

Artifact name:

- `parameter_behavior_primitive_profile`
- `parameter_behavior_profile`

Optional additional profiled fields:

- `regulated_score_profiled`
- `inertial_score_profiled`
- `accumulative_score_profiled`
- `discrete_state_score_profiled`
- `derived_response_score_profiled`
- `monotonicity_score_profiled`
- `statefulness_score_profiled`
- `oscillation_score_profiled`

The first two are the most important:

- `behavior_family_label`
- `behavior_family_profiled`

Those are enough to build a validator analogous to the datatype validator.

## 3. Primary behavior families

The first taxonomy should stay small.

Recommended primary families:

- `regulated`
- `tracking`
- `inertial`
- `accumulative`
- `discrete_state`
- `mixed_unknown`

### regulated

A channel that is actively held near a target or band, with comparatively small
nominal variation and transient excursions during switching or demand events.

Examples:

- bus voltage
- hydraulic pressure
- differential pressure under nominal pressurization control

### tracking

A channel that follows a target or command with bounded error and visible recovery
after target changes.

Examples:

- commanded valve position
- commanded pack flow
- target-following environmental variables in the simulator

### inertial

A channel that evolves smoothly with lag and persistence.

Examples:

- spool speed
- airspeed
- pitch/roll state
- cabin altitude

### accumulative

A channel that integrates over time and is slow-moving or monotone over long spans.

Examples:

- fuel quantity
- battery state of charge
- degradation state
- some temperatures

### discrete_state

A low-cardinality state channel with dwell and abrupt transitions.

Examples:

- contactor state
- gear/flap/slat mode
- pressurization mode
- generator online/offline

### mixed_unknown

A fallback family for channels that do not fit the current taxonomy cleanly.

This should remain explicit rather than forcing low-confidence assignments.

## 4. Secondary traits

Primary family alone is not enough. A parameter should also be describable by
secondary traits.

Recommended traits:

- `bounded`
- `monotone_like`
- `stateful`
- `oscillatory`
- `bursty`
- `lagged_response`
- `phase_sensitive`

These traits are useful even if the primary family is imperfect.

Example:

- a signal may be primarily `inertial` but also strongly `phase_sensitive`
- another may be `regulated` and `bursty`

## 5. Primitive-first Spark design

Behavior profiling should stay Spark-only on the hot path and use a two-layer model:

1. derive primitive evidence from raw telemetry and scaling metadata
2. score behavior families from that primitive artifact

The primitive layer should stay:

- interpretable
- cheap to compute
- compatible with Spark grouped/window aggregation

## 5.1 Shared primitive evidence

Useful features to compute per parameter:

- `total_count`
- `missing_rate`
- `distinct_value_count`
- `median_interval_ms`
- `sampling_rate_profiled_hz`
- numeric quantiles
- variance / MAD / IQR
- first-difference quantiles
- sign-flip rate of first differences
- lag-1 autocorrelation estimate
- central-band occupancy
- dwell statistics for low-cardinality values

Most of these can be computed with:

- grouped aggregates
- lag windows
- percentile approximations

No learned model is required for the first pass.

## 5.2 Discrete-state heuristic

Strong evidence for `discrete_state`:

- low distinct-value count
- long median dwell
- abrupt changes between states
- low within-state noise

This should be the cleanest family to detect.

It is closely related to, but not identical with, datatype:

- most `discrete_state` parameters are categorical/binary
- some numeric-coded channels may also behave as discrete state

## 5.3 Accumulative heuristic

Strong evidence for `accumulative`:

- very high lag-1 persistence
- low sign-flip rate in first differences
- near-monotone evolution over long windows
- relatively low high-frequency energy

This is where fuel quantity and state-of-charge-like channels should land.

## 5.4 Inertial heuristic

Strong evidence for `inertial`:

- high lag-1 autocorrelation
- smooth changes
- moderate first-difference magnitude
- not near-constant
- not strongly dwell-like

This family differs from `regulated` in that the channel is not merely held near a
setpoint. It carries genuine state evolution.

## 5.5 Regulated heuristic

Strong evidence for `regulated`:

- most samples lie inside a narrow central band
- excursions are relatively brief
- mean-reverting behavior after perturbation
- bounded range under nominal operation

A simple first pass can use:

- occupancy inside median ± k * IQR band
- excursion duration statistics
- low long-run drift relative to local variation

This will not be perfect, but it is operationally useful.

## 5.6 Derived-response heuristic

Strong evidence for `derived_response`:

- lower persistence than inertial state channels
- higher sign-flip rate
- higher diff-energy relative to level-energy
- near-zero mean around an operating point

This family is useful for channels like VSI or fast load/error terms.

## 6. Recommended first output schema

The first behavior profile table should stay flat and Qlik-friendly.

Suggested columns:

- `parameter_name`
- `behavior_family_profiled`
- `behavior_profile_confidence`
- `regulated_score_profiled`
- `inertial_score_profiled`
- `accumulative_score_profiled`
- `discrete_state_score_profiled`
- `derived_response_score_profiled`
- `monotonicity_score_profiled`
- `statefulness_score_profiled`
- `oscillation_score_profiled`
- `lag1_autocorr_profiled`
- `sign_flip_rate_profiled`
- `central_band_occupancy_profiled`
- `median_dwell_seconds_profiled`
- `sampling_rate_profiled_hz`

Do not start with nested JSON here.

## 7. Where it should live in code

Suggested additions:

- `libs/behavior/`
  - one file per behavior
  - each behavior owns:
    - `generator`
    - `profiler`
    - `validator`
    - `violator`
- `libs/behavior/base.py`
  - shared protocols
- `libs/behavior/registry.py`
  - shared registry
- `libs/profiling/validator.py`
  - extend or sibling-validator for:
    - `behavior_family_profiled` vs `behavior_family_label`

For the shared package and per-family file layout, see:

- [behavior_family_architecture.md](behavior_family_architecture.md)
- [behavior_family_skeletons.md](behavior_family_skeletons.md)

Suggested public API:

- `build_parameter_behavior_profile(...)`
- `stream_behavior_profile_validation(...)`

## 8. Simulation mirror

The simulator should eventually stamp:

- `behavior_family_label`
- optionally `behavior_traits_label`

Likely source:

- `ParameterSpec`
- or the local behavior container once specs exist

That keeps simulation and profiling aligned.

## 9. First validator

Add a validator analogous to the datatype validator:

- compare `behavior_family_profiled` to `behavior_family_label`
- emit cumulative TP/FP/FN/TN or a small confusion report

The better metric is likely:

- exact-match accuracy
- per-family precision/recall
- confusion matrix

rather than only one aggregate confusion count.

## 10. Why this helps the rest of the system

### event detection

Behavior family can influence detector choices:

- regulated -> tighter deviation/event thresholds
- inertial -> lag/slope-focused thresholds
- accumulative -> monotonicity and long-horizon trend checks
- discrete_state -> transition legality and dwell logic

### window representation

If V2.1 is adopted later, behavior-family detection is the natural routing signal for:

- rate-aware summaries
- family-aware continuous summaries

### anomaly attribution

Explaining an anomaly as:

- "regulated channel left its control band"
- "inertial channel lagged its expected response"
- "accumulative channel violated monotonicity"
- "discrete-state channel emitted an illegal transition"

is much stronger than generic residual language.

## 11. Recommended first implementation scope

Keep the first cut narrow:

1. profile only:
   - `discrete_state`
   - `accumulative`
   - `inertial`
   - `regulated`
   - `mixed_unknown`
2. defer `derived_response` if the heuristics are still weak
3. produce flat score columns
4. add a validator against simulation labels

That is enough to make the concept operational without overcomplicating the profiler.

## 12. Recommendation

The next extension should mirror the simulation architecture by introducing:

- `behavior_family_label`
- `behavior_family_profiled`

with a small interpretable heuristic profiler and a matching validator.

That is the cleanest way to keep richer simulation semantics and real-signal profiling
aligned.
