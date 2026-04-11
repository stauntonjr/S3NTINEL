# Simulation Library Plan

Status: Plan
Authority: Non-authoritative roadmap. Use package READMEs and `docs/current/` for current behavior.

This document defines the next 2-3 engineering milestones for the simulation and
 simulation pipeline work. It is intended to keep the codebase moving in one clear
direction instead of growing new parallel seams.

For current implementation ownership, prefer:
- [libs/simulation/README.md](/home/jrs/code/S3NTINEL/sentinel/libs/simulation/README.md)
- [libs/windows/README.md](/home/jrs/code/S3NTINEL/sentinel/libs/windows/README.md)
- [libs/phase/README.md](/home/jrs/code/S3NTINEL/sentinel/libs/phase/README.md)
- [scripts/README.md](/home/jrs/code/S3NTINEL/sentinel/scripts/README.md)

The standing priorities are:

1. continually clean the codebase and remove stale or redundant paths
2. improve simulation realism and feature depth
3. keep the hot path bounded and performant
4. keep phase detection and anomaly channels moving forward together

Related library coverage:

- `libs/simulation`
  - primary owner of this plan
- `libs/behavior`
  - current behavior-family observability work is simulator-driven and is tracked here
- `libs/phase`
  - coordinated phase-simulation semantics are tracked here and in
    [phase.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/libs/phase.md)

## Current State

The plan below is grounded in the current repo shape:

- the end-to-end runner exists and the persisted simulation pipeline is now a
  real operational path via `scripts.run_sim_pipeline`
- `libs.simulation` has already been heavily reduced and reorganized around the
  current object model, but still has room for readability and realism
  improvements
- the single-model-path refactor is now in place for the anomaly stack, so
  future simulation work should not assume a second local modeling path exists
- phase and anomaly logic now have cleaner ownership, but further simulation
  realism, scenario curation, and performance work are still needed

The next work should be organized into three workstreams and executed in this
order:

1. `A. Realism, phase context, and anomaly/violation integration`
2. `B. Golden scenarios and validation discipline`
3. `C. Remaining performance and hot-path profiling`

## A. Realism, Phase Context, And Anomaly/Violation Integration

### Objective

Increase realism without adding a second parallel anomaly or phase system.

The existing hooks should be normalized into one clear model instead of
continuing to grow independently.

### Core rule

Phase schedule and phase envelope remain the top-level operating-context layer.

Existing anomaly/abnormality features should build on the current seam:

- `violation_context_by_module_for_step`
- `apply_violations`
- behavior-local violator hooks
- phase gates
- mode gates

These should converge into one clear anomaly-conditioning model rather than
separate per-script or per-helper overrides.

### Planned realism growth

- expand phase-conditioned:
  - targets
  - mode states
  - couplings
  - noise envelopes
  - controller behavior
- keep authored flights as the realism proving ground before expanding
  fleet-scale complexity
- prefer realism that emerges from operating context over label stamping or
  direct telemetry hacks

### Violation integration model

Existing behavior-local violation hooks should become the primary anomaly
injection seam where appropriate.

Violation work should be organized by behavior family:

- `regulated`
  - setpoint offset
  - tracking degradation
  - saturation
  - oscillation
- `inertial`
  - lag increase
  - damping change
  - stuck/ramp distortion
- `accumulative`
  - leak
  - bias
  - drift
  - incorrect integration rate
- `discrete_state`
  - delayed transition
  - forbidden transition
  - chatter
  - stuck state

Violation scheduling should be tied to:

- phase context
- subsystem/module context
- persistence rules
- coupling structure

and not to ad hoc per-script overrides.

Separate anomaly helpers should not reappear as parallel seams. Violation-driven
simulation is the canonical injection path.

### Phase/anomaly channel coupling

The simulation and downstream detector should be advanced together:

- simulation phases should drive realistic operating envelopes
- violations should respect phase context and coupling structure
- downstream acceptance should track both:
  - phase detection quality on golden simulation runs
  - anomaly attribution quality against injected or known truth

The current clean-vs-observed signal split remains a hard rule:

- `parameter_value_clean`
  - simulator truth/debug
- `parameter_value`
  - fitting
  - detection
  - scoring
  - attribution

### Golden scenarios

At minimum, keep `power_chain` and `pressurization` as long-lived golden
scenarios.

Each golden scenario should eventually include:

- expected phase behavior
- at least one violation family
- expected graph/phase/anomaly downstream signals

See also:
- [phase.md](/home/jrs/code/S3NTINEL/sentinel/docs/plans/libs/phase.md)

## B. Golden Scenarios And Validation Discipline

### Objective

Keep simulation realism work tied to downstream evidence instead of only to
subjective waveform judgment.

### Golden-scenario rule

At minimum, maintain long-lived scenario coverage for:

- `power_chain`
- `power_pressurization_hierarchy_composite`

Each golden scenario should track:

- expected phase behavior
- expected event behavior
- at least one violation family
- expected downstream validation surfaces

### Validation rule

Simulation changes should be evaluated through the current validation harness,
not only through ad hoc plots.

When realism changes land, check:

- phase quality
- event quality
- anomaly detection quality
- parameter localization quality
- subsystem/module localization when relevant

## D. Behavior-Family Observability

### Objective

Make simulated behavior families more externally observable in the emitted
telemetry so downstream profiling and event interpretation are limited by model
quality rather than waveform ambiguity.

### Current ambiguity

The strongest current ambiguity is:

- `regulated` versus `inertial`

And unary `tracking` should still be treated cautiously until both simulator and
profiler expose a real tracked-signal relationship.

### Near-term rules

- do not treat unary `tracking` recovery as the main near-term benchmark target
- prefer observable waveform semantics over hidden internal mechanism labels
- use stronger simulator excitation before relabeling families

### Recommended simulator changes

- make `regulated` channels visibly corrective:
  - sharper setpoint changes
  - load steps or disturbances with return-to-band behavior
  - clearer recovery signatures
- make `inertial` channels visibly lagged:
  - measurable response delay
  - smoother ramp-and-settle structure
  - less controller-like correction
- stop over-smoothing latent drivers when that erases the distinction between
  control and lag

### Optional benchmark split

If needed, split:

- internal mechanism semantics
- observable waveform semantics

so simulator internals can remain rich while unary behavior benchmarks stay
scientifically defensible.

## C. Remaining Performance And Hot-Path Profiling

### Objective

Keep the remaining generator and persisted-stage hotspots visible and bounded.

### Current rule

- generator core stays Python-only and independent of pandas/Spark
- pandas is a bounded bridge only for:
  - tiny simulation proof paths
  - small reference artifacts
  - local debug outputs
- Spark owns persisted fact-table work
- every remaining bounded bridge should stay explicit and guarded

### Current profiling targets

- simulation generation wall time
- graph-stage evaluation/reporting cost
- full-run replay timing on the canonical simulation path
- any bounded artifact/report bridges that still show up in performance traces

### Hot-path rules

- no growth-table bridge without a hard row budget and fail-fast behavior
- no repeated rebuilding of:
  - raw
  - events
  - windows
  - `window_features`
  across adjacent stages when one bounded shared context or persisted seam is
  sufficient
- use the full-run path as the standard local performance and regression
  harness

### Performance ownership

The hot path should be treated as a first-class acceptance surface, not an
afterthought.

For each milestone touching simulation or replay-heavy stages:

- identify which bridges remain
- keep explicit row-limit guards where bridges still exist
- record wall times on the full-run path

## Milestone Ordering

### Milestone 1: realism and integrated violation model

Deliverables:

- phase schedule/envelope flow tightened
- violation injection normalized around the existing canonical seam
- authored golden scenarios expanded with explicit violation truth
- downstream regression signals defined for phase and anomaly quality

### Milestone 2: scenario and validation discipline

Deliverables:

- golden scenarios carry explicit downstream expectations
- simulation changes are read through the validation harness
- realism work is tied to named scenario coverage rather than one-off demos

### Milestone 3: performance and hotspot hardening

Deliverables:

- documented remaining bounded bridge seams
- full-run used as a repeatable local performance check
- timing reports remain part of the simulation acceptance surface

## Test And Acceptance Plan

### Realism gate

- golden runs for at least:
  - `power_chain`
  - `power_pressurization_hierarchy_composite`
- each scenario includes:
  - expected phase behavior
  - at least one injected violation family

### Phase/anomaly gate

- regression checks on detected phase outputs
- regression checks on calibrated scores and attribution outputs against known
  injected truth
- explicit checks that behavior violations flow through the intended canonical
  seam

### Performance gate

- bounded-bridge thresholds remain enforced
- full-run wall time and stage timing remain recorded
- each performance milestone documents which remaining hotspots or bridge seams
  were bounded, reduced, or accepted intentionally

## Assumptions

- medium-term means the next 2-3 engineering milestones, not a full architecture
  rewrite
- internal breaking cleanup is acceptable where it removes stale compatibility
  and improves taxonomy
- `scripts.run_sim_pipeline` remains the canonical simulation operational
  entrypoint
- existing violation features are retained and normalized into the main
  realism/anomaly design, not replaced by a second parallel injection model
