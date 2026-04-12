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
2. audit the current simulation as a localization benchmark before adding more detector logic
3. improve simulation realism and feature depth where the benchmark audit shows real ambiguity
4. keep the hot path bounded and performant
5. keep phase detection and anomaly channels moving forward together

## Generality Constraint

Plans here may use simulator scenarios to expose weaknesses, but detection
improvements must not overfit to simulator specifics.

That means:

- golden scenarios are validation harnesses, not templates for bespoke detector
  rules
- avoid scenario-name, parameter-name, or injected-label-specific detection
  logic
- prefer improvements that generalize through operating context, behavior
  families, coupling structure, and stable feature semantics
- treat simulator realism work as a way to improve signal quality, not as a
  justification for adding scenario-specific downstream fixes

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

The next work should be organized into four workstreams and executed in this
order:

1. `A. Localization benchmark audit and scenario review`
2. `B. Realism, phase context, and anomaly/violation integration`
3. `C. Golden scenarios and validation discipline`
4. `D. Remaining performance and hot-path profiling`

## A. Localization Benchmark Audit And Scenario Review

### Objective

Scrutinize the current simulator as a benchmark for anomaly attribution before
building more downstream heuristics or replacing the whole simulator.

The practical question is not just whether faults are injected. It is whether
the emitted telemetry and event stream make the source module identifiable at
all.

### Current concern

Repeated anomaly-channel changes are not materially moving module localization.
That now has to be treated as a benchmark question, not only a detector
question.

Possible explanations:

- the current anomaly stack is near its ceiling on the present benchmark
- the current simulation makes many fault sources only weakly identifiable from
  the observables it emits
- the truth target is sometimes finer-grained than the observable best answer

### Audit rule

Before introducing another broad anomaly-localization change, generate and read
an explicit simulation benchmark audit from the current validation surfaces.

That audit should classify each truth fault window by observed recoverability:

- `module_recoverable`
- `subsystem_recoverable`
- `parameter_visible_only`
- `detection_only`
- `undetected`

The audit should also aggregate by:

- fault family
- fault detail type
- source subsystem
- source module
- dominant score component

### What the audit should answer

For each truth window:

- was it detected at all
- did it become emit-ready
- was the truth parameter visible
- was the truth subsystem present in top candidates
- was the truth module present in top candidates
- which dominant score component surfaced it

For each fault family/detail:

- how often the current benchmark supports module-level recovery
- how often it only supports subsystem-level recovery
- how often it collapses to parameter-only or detection-only evidence

### Implementation direction

Implement the audit as a first-class simulation report, not a notebook-only
side analysis.

Canonical owners:

- `libs/simulation/reporting.py`
- `libs/simulation/full_run_report.py`
- `libs/simulation/validation_harness.py`

The report should be written into `reports/` and folded into the validation
harness so every simulation run produces the same benchmark view.

Encode benchmark intent on the authored misbehavior windows themselves.

That means the simulator should declare, per fault window, whether the scenario
is intended to be a:

- `module_recoverable`
- `subsystem_recoverable`
- `parameter_visible_only`
- `detection_only`

The audit should then compare observed recoverability against the declared
target and surface:

- windows that miss the declared target
- windows that meet the target
- windows whose current target is probably too coarse

The benchmark should stop treating every fault as if module recovery were the
same expectation.

### Acceptance

The audit is useful if it makes the next simulation decisions concrete:

- which current scenarios are valid module-localization benchmarks
- which are only subsystem-localization benchmarks
- which are effectively detection-only benchmarks
- which fault families deserve a new scenario pack rather than more detector
  tuning

### Outcome expectation

This workstream may justify one of two next moves:

- revise the current scenario design and truth framing
- build a narrower new localization-focused scenario pack before more anomaly
  work

### Immediate benchmark split

Do not keep using only the mixed composite bundle for every benchmark question.

The current scenario family should expose separate named benchmark packs for:

- module-localization-target windows
- subsystem-localization-target windows

Those packs should be thin filters over the canonical authored scenario first,
so the benchmark can be split immediately without forking a second simulator.

After that split is working, design a new localization-focused scenario pack for
the fault types that still miss their declared target badly.

The first new pack should be a smoke-topology localization sanity suite, not a
full second composite benchmark. It should:

- keep the canonical authored power/pressurization scenario family
- reduce stochastic ambiguity rather than adding simulator-specific labels
- use only clean module-target fault types first:
  - `bias`
  - `saturation`
  - `drift`
- expose a named entrypoint so the benchmark can answer a narrower question:
  - can the current stack recover module structure at all in a simpler,
    better-identified setting?

The next step after that first pack is not more detector tuning. It is a fault
family split:

- `bias` and `drift` should be benchmarked together as the cleaner
  module-localization family
- `saturation` should be benchmarked separately because it can remain only
  parameter-visible even when the cleaner family starts to recover structure

That split should decide whether `saturation` needs scenario redesign,
additional observability, or a downgraded benchmark target.

### Current measured benchmark split

The current smoke benchmark split has now been run and should guide the next
simulation work directly.

Observed outcomes:

- `power_pressurization_hierarchy_smoke_localization_focus_drift`
  - `drift` is now available as a dedicated module-localization gate
  - current measured result is `module_recoverable` and meets the declared
    target:
    - detected `1/1`
    - emit-ready `1/1`
    - telemetry parameter match `1/1`
    - selected telemetry parameter match `1/1`
    - top module candidate present `1/1`
    - benchmark tier alignment: `met_target`
- `power_pressurization_hierarchy_smoke_localization_focus_bias`
  - `bias` is now available as a dedicated subsystem-localization gate
  - current measured result is `subsystem_recoverable` and meets the declared
    target:
    - detected `1/1`
    - emit-ready `1/1`
    - telemetry parameter match `1/1`
    - dominant subsystem match `1/1`
    - top subsystem candidate present `1/1`
    - benchmark tier alignment: `met_target`
- `power_pressurization_hierarchy_smoke_localization_focus_bias_drift`
  - `drift` is a valid module-localization benchmark in the current stack
  - `bias` currently behaves as a subsystem-recoverable benchmark, not a clean
    module benchmark
  - benchmark intent should now encode that split directly:
    - `drift` as `module_recoverable`
    - `bias` as `subsystem_recoverable`
- `power_pressurization_hierarchy_smoke_localization_focus_bias_load_monitor`
  - rewriting `bias` onto local `electrical_load_pct` improves benchmark
    clarity, but it still misses the module target
  - current measured result is still `subsystem_recoverable`:
    - detected `1/1`
    - emit-ready `1/1`
    - telemetry parameter match `1/1`
    - dominant subsystem match `1/1`
    - dominant module match `0/1`
- `power_pressurization_hierarchy_smoke_localization_focus_saturation`
  - shared-supply saturation is stable as a `parameter_visible_only` benchmark
  - it should not be treated as a module-localization benchmark in the current
    benchmark family
- `power_pressurization_hierarchy_smoke_localization_focus_saturation_local`
  - rewriting saturation onto local `pack_temp_c` reduced the case further to
    `detection_only`
  - that local rewrite is still useful as a benchmark because it shows that
    simply moving saturation onto a more local observable can remove structural
    evidence instead of improving recoverability

### Updated immediate next move

Do not spend more anomaly-localization effort on the saturation family right
now.

The benchmark evidence says:

- `drift` should remain in the module-localization sanity suite
- the dedicated `power_pressurization_hierarchy_smoke_localization_focus_drift`
  pack should be treated as the clean module-localization acceptance gate
- the dedicated `power_pressurization_hierarchy_smoke_localization_focus_bias`
  pack should be treated as the clean subsystem-localization acceptance gate
- `bias` should still be treated as a subsystem-vs-module separation problem
  when the goal is module recovery
- local-monitor `bias` is a useful redesign probe, but not yet a
  module-localization acceptance gate
- `saturation` should live in explicit lower-tier benchmark packs:
  - shared-supply saturation as `parameter_visible_only`
  - local pack-temperature saturation as `detection_only`

Use a grouped gate-suite harness for anomaly acceptance:

- canonical runner:
  - `python -m scripts.run_sim_benchmark_tier_gates --base-dir ...`
- canonical suite report:
  - `reports/benchmark_tier_gate_suite_summary.json`
- intended use:
  - evaluate anomaly changes on the dedicated `bias` and `drift` packs before
    returning to the mixed composite benchmark

If saturation is revisited again, do it as a simulation-design problem:

- add stronger local observability
- add clearer downstream propagation from the saturated local variable
- or redesign the saturation scenario around a different module/parameter pair

Do not treat saturation as a current module-localization acceptance gate.

### Recoverability development phases

Simulation improvement should now proceed as an explicit recoverability ladder,
not as a mixed attempt to optimize every structural level at once.

The sequencing should be:

1. `parameter detectability and labeling`
2. `module recoverability`
3. `subsystem recoverability`
4. `system recoverability`

The practical meaning of each phase is:

- `parameter detectability and labeling`
  - optimize whether the correct faulted parameter or parameter family becomes
    detectable and attributable at all
  - current benchmark tiers already support this through:
    - `detection_only`
    - `parameter_visible_only`
- `module recoverability`
  - only after parameter visibility is stable, optimize whether the correct
    source module is structurally recoverable
  - current benchmark tier:
    - `module_recoverable`
- `subsystem recoverability`
  - only after module-local benchmark families are working, optimize whether
    subsystem rollups remain stable under harder or more shared scenarios
  - current benchmark tier:
    - `subsystem_recoverable`
- `system recoverability`
  - only after subsystem behavior is stable, add broader system-level
    benchmark intent for multi-subsystem or highly shared scenarios
  - this is a future extension and is not yet a first-class benchmark target
    encoded in the current simulation specs

### Working rule for future scenario design

New scenario packs should be introduced in that same order.

That means:

- do not author a new module-localization benchmark until the underlying fault
  family is at least parameter-visible in a stable way
- do not treat subsystem or system scenarios as the next optimization target
  when module recovery is still unproven for the cleaner family
- when a scenario fails at a lower phase, downgrade or redesign it there
  instead of keeping it as a higher-tier benchmark

### Immediate application of the phased ladder

The current smoke-family results already imply the next phase assignments:

- `drift`
  - stays in the module-recoverability phase
  - the dedicated `drift` pack is now the clean acceptance gate for that phase
- `bias`
  - now has a dedicated subsystem-recoverability acceptance gate
  - still straddles parameter/module work when the goal is module separation
  - the new local-monitor rewrite does not fix that yet; it remains a
    subsystem-recoverable benchmark
  - benchmark intent should be downgraded accordingly in the smoke packs now,
    rather than leaving `bias` declared as module-recoverable
- shared-supply `saturation`
  - stays in the parameter-detectability/labeling phase
- local `pack_temp_c` saturation
  - stays in the detection-only phase and should not be used to judge module
    localization

So the next simulation-design work should prioritize:

1. strengthening lower-tier parameter labeling where it is still weak
2. then building a cleaner `bias` path from parameter visibility to module
   recoverability, likely by separating `MOD_PWR_LOAD_MON` from sibling
   `MOD_COMP_DRIVE` evidence rather than staying on shared-source voltage
3. only after that, expanding harder subsystem- and system-level scenario packs

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
- tier gates
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

## C. Golden Scenarios And Validation Discipline

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

But do not accept a change just because it improves one golden scenario through
scenario-specific tuning. The intended target is broader detector generality.

## E. Behavior-Family Observability

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

## D. Remaining Performance And Hot-Path Profiling

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

### Milestone 1: localization benchmark audit

Deliverables:

- `reports/simulation_benchmark_audit_summary.json` emitted by the canonical
  simulation reporting path
- `reports/benchmark_scope_validation_summary.json` emitted by the same
  reporting path so benchmark intent becomes optimization denominators instead
  of audit-only metadata
- fault-window recoverability classification in the validation harness
- family/detail-level review surface for deciding whether the current simulator
  is a valid localization benchmark

### Milestone 2: realism and integrated violation model

Deliverables:

- phase schedule/envelope flow tightened
- violation injection normalized around the existing canonical seam
- authored golden scenarios expanded with explicit violation truth
- downstream regression signals defined for phase and anomaly quality

### Milestone 3: scenario and validation discipline

Deliverables:

- golden scenarios carry explicit downstream expectations
- simulation changes are read through the validation harness
- realism work is tied to named scenario coverage rather than one-off demos

### Milestone 4: performance and hotspot hardening

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

- simulation benchmark audit is emitted and readable from the canonical run
  reports
- the audit clearly shows which scenarios are module-recoverable versus only
  parameter-visible or detection-only
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
