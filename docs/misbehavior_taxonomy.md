# Misbehavior Taxonomy

This note defines the planned misbehavior taxonomy for the active S3NTINEL path.

The purpose is to separate:

- what a parameter normally is (`behavior_family_*`)
- how it is currently deviating (`misbehavior_family_*`)
- how severe that deviation is (`anomaly_score_*`)

This keeps anomaly logic structured and interpretable.

## 1. Canonical planned fields

Simulation/source side:

- `misbehavior_family_label`
- `misbehavior_detail_label`

Detector/runtime side:

- `misbehavior_family_detected`
- `misbehavior_detail_detected`
- `misbehavior_confidence_detected`

Severity remains separate:

- `anomaly_score_label`
- `anomaly_score_detected`

General anomaly type can remain as a broader field:

- `anomaly_type_label`
- `anomaly_type_detected`

But the preferred structured ontology should be `misbehavior_family_*`.

## 2. Relationship to behavior families

Behavior and misbehavior are not the same thing.

Examples:

- a `regulated` channel may exhibit:
  - `offset`
  - `noise_increase`
  - `stuck`
- an `inertial` channel may exhibit:
  - `timing_lag`
  - `drift`
  - `coupling_break`
- a `discrete_state` channel may exhibit:
  - `illegal_transition`
  - `dwell_violation`
  - `state_chatter`

So the intended representation is:

- `behavior_family_label` / `behavior_family_profiled`
- `misbehavior_family_label` / `misbehavior_family_detected`

## 3. Primary misbehavior families

The first taxonomy should stay compact and map onto existing score channels.

Recommended primary families:

- `offset`
- `drift`
- `stuck`
- `noise_increase`
- `dropout`
- `timing_lag`
- `timing_jitter`
- `illegal_transition`
- `dwell_violation`
- `state_chatter`
- `coupling_break`
- `coupling_inversion`
- `unsupported_cooccurrence`
- `unsupported_transition`
- `phase_context_violation`
- `persistent_degradation`
- `mixed_unknown`

## 4. Meaning of each family

### offset

Sustained displacement from nominal level or control band.

Typical evidence:

- residual bias
- high central tendency shift
- stable but displaced channel

### drift

Slow directional change inconsistent with nominal behavior or phase envelope.

Typical evidence:

- growing residual trend
- monotone deviation
- increasing mismatch over time

### stuck

Loss of expected variability or updates.

Typical evidence:

- near-zero first-difference energy
- abnormally long flat interval
- missing state changes

### noise_increase

Variance increase without a coherent mean shift.

Typical evidence:

- larger residual variance
- elevated diff-energy
- reduced predictability with stable mean

### dropout

Missing or null observations, or effective disappearance of updates.

Typical evidence:

- missing samples
- missing bursts
- loss of expected update cadence

### timing_lag

Response occurs too late relative to coupled drivers or expected dynamics.

Typical evidence:

- increased lag in coupled channels
- transition delay
- delayed actuator or process response

### timing_jitter

Timing becomes irregular without a stable lag shift.

Typical evidence:

- inconsistent inter-arrival intervals
- variable response timing
- increased temporal variance in coupled responses

### illegal_transition

Discrete-state transition violates the allowed state graph.

Typical evidence:

- event detector emits impossible transition
- transition graph weight near zero for observed edge

### dwell_violation

State dwell duration is implausibly short or long.

Typical evidence:

- dwell bucket anomaly
- repeated too-short or too-long occupancy

### state_chatter

Repeated rapid switching of a discrete-state channel.

Typical evidence:

- high transition rate
- low median dwell
- alternating states in short intervals

### coupling_break

Expected relationship between channels weakens or disappears.

Typical evidence:

- higher reconstruction residuals
- reduced precision/fused support
- lag/transition support collapse

### coupling_inversion

Relationship persists but with reversed or qualitatively incorrect response.

Typical evidence:

- wrong-sign response
- wrong-direction derived trend
- phase-conditional reversal

### unsupported_cooccurrence

Observed joint event pattern is not supported by the event graph.

Typical evidence:

- elevated graph-violation score from same-window event sets

### unsupported_transition

Observed event sequence is not supported by the lag/transition graph.

Typical evidence:

- low-probability or absent edge in transition/lag graph

### phase_context_violation

Behavior may be plausible globally but is implausible in the current phase.

Typical evidence:

- abnormal score only under detected/label phase
- wrong mode/state for flight phase

### persistent_degradation

Degradation is not local to one window; it persists or worsens across flights.

Typical evidence:

- long-horizon residual growth
- persistent lag increase
- cumulative deviation for a tail

### mixed_unknown

Fallback when a deviation is clearly present but cannot be mapped confidently to one
family.

## 5. Mapping to current pipeline channels

The current pipeline already contains partial support for several misbehavior families.

### residual / reconstruction channel

Current path:

- `window_x`
- backbone fit
- reconstruction residuals
- `reconstruction_score`
- `subsystem_scores`

Most naturally supports:

- `offset`
- `drift`
- `noise_increase`
- `coupling_break`
- `persistent_degradation`

### event detector channel

Current path:

- continuous/categorical event detection
- state transitions
- dwell-related signals

Most naturally supports:

- `illegal_transition`
- `dwell_violation`
- `state_chatter`
- some aspects of `stuck`

### graph channel

Current path:

- `event_graph`
- `lag_graph`
- `transition_graph`
- `fused_graph`
- `graph_violation_score`

Most naturally supports:

- `unsupported_cooccurrence`
- `unsupported_transition`
- `coupling_break`
- `timing_lag`
- `timing_jitter`

### phase channel

Current path:

- `phase_windows`
- `phase_baselines`
- `phase_id_detected`

Most naturally supports:

- `phase_context_violation`
- phase-conditioned forms of:
  - `offset`
  - `noise_increase`
  - `illegal_transition`

## 6. What new pipeline elements are needed

Yes, new elements are needed if `misbehavior_family_detected` is to become a real
first-class output.

The current pipeline can already produce *evidence* for misbehavior, but not a clean
misbehavior ontology.

Recommended additions:

### 6.1 Misbehavior rule/aggregation layer

Add a small layer that consumes:

- residual statistics
- event summaries
- graph-violation scores
- phase context
- behavior-family profile

and emits:

- `misbehavior_family_detected`
- `misbehavior_detail_detected`
- `misbehavior_confidence_detected`

This is best placed after raw window scoring and before final anomaly attribution.

### 6.2 Misbehavior score components

Introduce explicit score components for:

- residual shift
- variance increase
- timing lag
- dwell/transition illegality
- graph support violation
- persistent degradation

These can then vote into the misbehavior family.

### 6.3 Tail-persistent degradation tracker

`persistent_degradation` needs memory across flights.

Add a tail-level baseline/degradation tracker that can compare:

- current residual pattern
- recent flights
- historical tail baseline

### 6.4 Behavior-aware routing

Behavior family should influence misbehavior logic.

Examples:

- `regulated` + residual shift -> likely `offset`
- `inertial` + lag evidence -> likely `timing_lag`
- `accumulative` + monotonicity break -> likely `drift` or `mixed_unknown`
- `discrete_state` + bad edge -> likely `illegal_transition`

Without behavior-aware routing, misbehavior classification will be much weaker.

## 7. Recommended placement in code

The preferred placement is local to the behavior containers.

Suggested additions:

- `libs/behavior/`
  - one file per behavior
  - each behavior owns:
    - `generator`
    - `profiler`
    - `validator`
    - `violator`

In that layout, the `violator` is where behavior-local misbehavior generation and
family-specific deviation logic lives.

For pipeline aggregation beyond one local behavior, a thin scoring layer may still be
useful:

- `libs/scoring/misbehavior.py`

Suggested public API:

- `detect_window_misbehavior(...)`
- `classify_misbehavior_family(...)`
- `build_window_misbehavior_table(...)`

Suggested output table:

- `window_misbehavior`

or fields folded into `window_scores_raw` first:

- `misbehavior_family_detected`
- `misbehavior_detail_detected`
- `misbehavior_confidence_detected`

## 8. Suggested first implementation scope

Do not try to cover all families at once.

Start with the families that map most cleanly onto existing evidence:

1. `offset`
2. `drift`
3. `noise_increase`
4. `illegal_transition`
5. `dwell_violation`
6. `unsupported_cooccurrence`
7. `unsupported_transition`
8. `phase_context_violation`

Defer:

- `coupling_inversion`
- `persistent_degradation`
- refined `timing_lag` / `timing_jitter`

until the core behavior/misbehavior mirror is in place.

## 9. Recommendation

The pipeline does not need a wholly separate anomaly architecture.

But it does need one additional semantic layer:

- evidence channels already exist
- misbehavior classification does not

So the clean next step is:

1. define `misbehavior_family_label` in simulation
2. add `misbehavior_family_detected` to the scoring/anomaly path
3. implement behavior-local `violator` components and a small rule/aggregation layer
   that maps existing evidence to the taxonomy

That is enough to make "misbehavior detection" real rather than implicit.
