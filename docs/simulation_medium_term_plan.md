# Simulation Medium-Term Plan

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

## Current State

The plan below is grounded in the current repo shape:

- the end-to-end runner exists and the persisted simulation pipeline is now a
  real operational path via `scripts.run_sim_pipeline`
- `libs.simulation` has already been heavily reduced and reorganized around the
  current object model, but still has room for readability and realism
  improvements
- pandas and Spark boundaries are still duplicated across:
  - `pipelines/10_backbone_fit.py`
  - `pipelines/11_graph_fit.py`
  - `libs/phase/pipeline.py`
  - `libs/scoring/pipeline.py`
- phase and anomaly logic now have cleaner ownership, but further realism and
  performance work is still needed

The next work should be organized into three workstreams and executed in this
order:

1. `A. Taxonomy and contraction`
2. `B. Realism, phase context, and anomaly/violation integration`
3. `C. Spark-boundary reduction and hot-path hardening`

## A. Taxonomy And Contraction

### Objective

Reduce the amount of stale, repetitive, or ambiguous code and make the repo
layout reflect the actual execution model.

### Target shape

The simulation area should converge to four clear zones:

- `simulation core`
  - specs
  - runtime
  - stepping
  - coupling
  - flight orchestration
- `simulation scenarios`
  - authored flights
  - reusable examples
  - scenario builders
- `simulation pipeline handoff`
  - canonical telemetry row emission
  - persisted-run orchestration
  - simulation-to-pipeline handoff

### Planned changes

- reduce `libs.simulation` to a small obvious public surface
- move remaining legacy-only helpers and scripts into clearly marked legacy
  locations
- collapse redundant simulation scripts behind `scripts.run_sim_pipeline`
- remove duplicate event-labeling, experiment-setup, and demo glue where the
  canonical path already exists
- replace large procedural signatures with config dataclasses and orchestration
  objects

### Immediate cleanup targets

- `libs/simulation/__init__.py`
  - stop exposing a broad mixed surface as the default public API
- `scripts/`
  - keep `run_sim_pipeline` as the only canonical simulation entrypoint
  - remove duplicate workflow scripts instead of retaining compatibility layers

### Naming and placement rules

- spec types should live in spec-oriented modules, not in convenience/helper
  files
- avoid new modules that mix:
  - runtime state
  - static specs
  - pipeline handoff helpers
  - stale compatibility code

### Large-signature reduction rule

Where workflows are stateful or configuration-heavy, stop adding more functions
with large argument lists.

Instead introduce small config/value objects for:

- simulation run configuration
- structural fitting configuration
- graph fitting configuration
- phase fitting configuration
- scoring and calibration configuration

Prefer methods on orchestration/context objects where the call sequence is
stateful and the data naturally travels together.

## B. Realism, Phase Context, And Anomaly/Violation Integration

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

## C. Spark-Boundary Reduction And Hot-Path Hardening

### Objective

Make the generator core, bridge layers, and persisted Spark stages visibly
separate and enforce bounded behavior at their interfaces.

### Boundary rule

- generator core stays Python-only and independent of pandas/Spark
- pandas is a bounded bridge only for:
  - tiny simulation proof paths
  - small reference artifacts
  - local debug outputs
- Spark owns persisted fact-table work
- every remaining `toPandas()` and unbounded `collect()` in structural stages
  must either be removed or explicitly guarded

### Concrete reduction targets

- `pipelines/10_backbone_fit.py`
- `pipelines/11_graph_fit.py`
- `libs/phase/pipeline.py`
- `libs/scoring/pipeline.py`

These are the places where the repo still visibly crosses the generator core,
pandas, and Spark boundaries in repetitive or partially duplicated ways.

### Hot-path rules

- no growth-table bridge without a hard row budget and fail-fast behavior
- no repeated rebuilding of:
  - raw
  - events
  - windows
  - `WindowFeaturesDataFrame`
  across adjacent stages when one bounded shared context or persisted seam is
  sufficient
- use the full-run path as the standard local performance and regression
  harness

### Performance ownership

The hot path should be treated as a first-class acceptance surface, not an
afterthought.

For each milestone touching structural stages:

- identify which bridges remain
- identify which ones were removed
- keep explicit row-limit guards where bridges still exist
- record wall times on the full-run path

## Milestone Ordering

### Milestone 1: contraction and ownership cleanup

Deliverables:

- reduced `libs.simulation` public surface
- clear separation of core/scenario/bridge/legacy modules
- deprecated or removed duplicate simulation entrypoints
- phase spec placement plan executed or queued as a direct follow-up
- config objects introduced for the worst large-signature seams

### Milestone 2: realism and integrated violation model

Deliverables:

- phase schedule/envelope flow tightened
- violation injection normalized around the existing canonical seam
- authored golden scenarios expanded with explicit violation truth
- downstream regression signals defined for phase and anomaly quality

### Milestone 3: Spark-boundary and hot-path hardening

Deliverables:

- narrowed pandas/Spark bridge points
- improved reuse of intermediate structural artifacts
- documented remaining bounded bridge seams
- full-run used as a repeatable local performance check

## Test And Acceptance Plan

### Cleanup gate

- no stale wrapper exports
- no duplicate simulation entrypoints documented as canonical
- no ambiguous ownership between core/scenario/bridge/legacy modules

### Realism gate

- golden runs for at least:
  - `power_chain`
  - `pressurization`
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
- each bridge-reduction milestone documents which `toPandas()` or `collect()`
  calls were removed or newly bounded

## Assumptions

- medium-term means the next 2-3 engineering milestones, not a full architecture
  rewrite
- internal breaking cleanup is acceptable where it removes stale compatibility
  and improves taxonomy
- `scripts.run_sim_pipeline` remains the canonical simulation operational
  entrypoint
- existing violation features are retained and normalized into the main
  realism/anomaly design, not replaced by a second parallel injection model
