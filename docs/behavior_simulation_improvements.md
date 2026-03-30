# Behavior Simulation Improvements

This note records proposed simulator-side changes to improve behavior-family
separability in the emitted telemetry, with a near-term focus on:

- `regulated`
- `inertial`
- deferring unary `tracking` claims until the simulator and profiler expose a
  real tracked-signal relationship

It is a companion to:

- [behavior_profiling_design.md](/home/jrs/code/S3NTINEL/sentinel/docs/behavior_profiling_design.md)
- [simulation_medium_term_plan.md](/home/jrs/code/S3NTINEL/sentinel/docs/simulation_medium_term_plan.md)

## 1. Problem Statement

The current simulation labels several parameters with behavior-family semantics
that are internally plausible but not always externally identifiable from the
emitted time series alone.

That causes two downstream problems:

- behavior profiling quality is capped by waveform ambiguity rather than only by
  profiler weakness
- event-profile inference inherits that ambiguity because it uses the behavior
  profile as a prior

The clearest current ambiguity is `regulated` versus `inertial`.

## 2. What We Observed

Recent composite profiling runs showed that:

- `discrete_state` and `accumulative` are comparatively recoverable
- `tracking` is not recoverable from unary telemetry in the current design
- `regulated` and `inertial` are frequently mistaken for each other even after
  multiple profiler refinements

Representative ambiguous channels include:

- `actuator_load_pct`
- `bus_voltage_v_ess`
- `pack_temp_c`
- `compressor_speed_pct`
- `inverter_temp_c`
- `pack_flow_rate_pct_ctr`

These channels often look like:

- smooth ramp plus long plateau
- bounded settled value with only small visible correction
- lagged response whose target itself is already smooth

That means the waveform often does not expose whether the parameter is:

- actively trimmed around a target or band
- merely lagging behind a smooth latent driver

## 3. Near-Term Rule Changes

### 3.1 Defer `tracking` as a primary unary label

`tracking` should not be treated as a first-class primary family for unary
behavior profiling until the simulator and profiler both represent a tracked
relationship explicitly.

Near-term policy:

- do not expect unary behavior profiling to recover `tracking`
- do not treat `tracking` misses as the main behavior-profiling blocker
- reintroduce `tracking` as a primary target only after command/reference
  signals are available as paired evidence

Until then, simulator channels currently labeled `tracking` should be handled in
one of two ways:

- relabel them to their dominant observable unary family
- keep them marked as tracking internally but exclude them from unary
  behavior-family benchmark success criteria

### 3.2 Prefer observable behavior labels over mechanism labels

When a channel's emitted waveform is not distinguishable from another family,
the benchmark label should prefer observable nominal behavior over hidden
mechanism intent.

This is especially important for:

- thermal channels
- actuator/load channels
- smoothly driven environmental channels
- speed channels that spend most of the run on bounded plateaus

## 4. Proposed Simulator Changes

### 4.1 Make `regulated` observably regulated

Regulated channels should show visible evidence of active correction, not just
bounded smoothness.

Recommended changes:

- add sharper setpoint changes during phase transitions
- add small disturbances or load steps that require visible return-to-band
- make trim behavior visibly different from inertial settling
- create short overshoot-and-correction or undershoot-and-correction episodes
  where physically reasonable
- avoid making the target itself so smooth that regulation becomes visually
  indistinguishable from lag

Desired observable signatures:

- boundedness
- repeated correction toward a band or target
- recovery after disturbance
- return behavior stronger than simple passive lag

### 4.2 Make `inertial` observably inertial

Inertial channels should show visible lag and persistence, not only stable
bounded plateaus.

Recommended changes:

- apply clearer target changes with measurable lag
- reduce closed-loop trim-like behavior on inertial channels
- preserve ramp and settle structure instead of quickly converging to
  regulation-like steady state
- use time constants and phase excitation that produce visibly different delay
  from regulated channels
- prefer inertial channels whose latent drivers change enough to expose lag

Desired observable signatures:

- delayed response after driver changes
- smooth ramping and settling
- persistence after target movement
- less obvious correction-to-band behavior

### 4.3 Stop over-smoothing the latent drivers

A major source of ambiguity is that both regulated and inertial channels are
often driven by already-smooth latent targets.

Recommended changes:

- add more piecewise or stepped latent target programs where plausible
- keep some sharp context changes at phase boundaries
- expose disturbance-response episodes, not only gentle drifts
- make the excitation schedule part of the behavior benchmark contract

If the driver is already extremely smooth, both a controller and a first-order
lag can emit nearly identical telemetry.

### 4.4 Split component semantics from benchmark semantics

The simulator can still retain hidden mechanism semantics, but the benchmark
surface should distinguish:

- internal mechanism family
- observable waveform family

That enables a parameter to remain internally modeled as a regulated component
while being benchmarked as an observable inertial-like or mixed family if the
telemetry does not support clean unary separation.

Recommended additions:

- `behavior_family_label_internal`
- `behavior_family_label_observable`

Near-term validation should target the observable label.

## 5. Candidate Channel Actions

These are not final relabel decisions, but they are the clearest current review
targets.

### 5.1 Re-excitation candidates

These should first receive stronger simulator excitation before relabeling:

- `actuator_load_pct`
- `bus_voltage_v_ess`
- `pack_temp_c`
- `cabin_delta_p_psi`

Why:

- they are labeled `regulated`
- but their current traces often look like passive lag or smooth drift

### 5.2 Relabel-review candidates

These should be reviewed for observable-family relabeling if stronger excitation
still does not separate them:

- `compressor_speed_pct`
- `compressor_speed_pct_ess`
- `compressor_speed_pct_stby`
- `inverter_temp_c`
- `inverter_temp_c_ess`
- `pack_flow_rate_pct_ctr`

Why:

- they are currently labeled `inertial`
- but large portions of their waveform look like bounded settled channels

### 5.3 Tracking-defer candidates

These should not remain hard unary `tracking` evaluation targets in the near
term:

- `ambient_pressure_kpa`
- `ambient_temp_c`
- `outflow_cmd_pct`
- `outflow_cmd_pct_aft`
- `pack_flow_cmd_pct`
- `pack_flow_cmd_pct_aft`

These need paired command/reference context before `tracking` is a fair
profiling target.

## 6. Validation Additions

The simulator should not rely only on downstream profiler accuracy to tell us
whether the generated behavior families are separable.

Add simulator-side validation that measures waveform separability directly.

Recommended additions:

- per-family excitation coverage metrics
- per-parameter lag-versus-correction diagnostics
- family-level separability reports using primitive evidence
- a report of channels whose observable family is ambiguous under the current
  phase program

Useful diagnostics:

- number of visible target/disturbance changes per parameter
- recovery-to-band evidence
- lag strength after step changes
- plateau dominance versus corrective activity

## 7. Implementation Order

Recommended order:

1. Defer `tracking` from unary benchmark success criteria.
2. Add observable-family labeling alongside internal family labeling.
3. Strengthen phase and disturbance excitation for regulated candidates.
4. Strengthen lag exposure for inertial candidates.
5. Rerun behavior-profile validation on the golden composite scenario.
6. Only then continue behavior-profile scoring work against the updated
   benchmark.

## 8. Success Criteria

This work should be considered successful when:

- `regulated` and `inertial` are more clearly separable in raw telemetry
- unary behavior profiling no longer depends on forcing many numeric channels
  into `mixed_unknown`
- behavior-profile improvements produce cleaner downstream event-profile policy
  selection
- remaining ambiguity is concentrated in explicitly deferred `tracking`
  channels rather than spread across the core numeric families

## 9. Non-Goals

This note does not propose:

- immediate full implementation of tracked-signal profiling
- replacing current behavior simulation families with a learned model
- removing internal mechanism semantics from the simulator

The goal is narrower:

- make the simulator's emitted nominal telemetry a fairer benchmark for the
  current profiler and downstream detector stack
