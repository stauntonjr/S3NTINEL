# Simulation Library Plan

Status: Plan
Authority: Non-authoritative roadmap. Use package READMEs and `docs/current/` for current behavior.

This document defines the next 2-3 engineering milestones for the simulation and
 simulation pipeline work. It is intended to keep the codebase moving in one clear
direction instead of growing new parallel seams.

For current implementation ownership, prefer:
- [libs/simulation/README.md](../../../libs/simulation/README.md)
- [libs/windows/README.md](../../../libs/windows/README.md)
- [libs/phase/README.md](../../../libs/phase/README.md)
- [scripts/README.md](../../../scripts/README.md)

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
    [phase.md](phase.md)

## Current State

### 2026-07 resume checkpoint

The canonical simulation runner was revalidated after the tooling pause:

- `power_chain` is a no-fault operational smoke workload and is not suitable
  for positive anomaly attribution acceptance.
- `power_pressurization_hierarchy_composite` is the current authored-fault
  positive validation workload.
- Its latest full replay completed all `00` through `95` stages, with `18/18`
  detected fault windows and `15/18` emit-ready fault windows.
- The local `00` through `90` smoke remains a structural contract check. It may
  legitimately emit no `emit_ready` rows because its one-window fixture does
  not provide a conformal warm history.
- Smoke and simulation consistency requirements are maintained in
  `docs/testing/smoke_simulation_consistency.md`.

Do not use `power_chain` as evidence of detector recall or attribution quality.
Use the authored-fault composite and its persisted validation summaries for
positive claims.

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

The benchmark audit and tier-gate framework in section A are now implemented.
The next development pass below supersedes the earlier workstream ordering
where it conflicts with this current evidence.

## Next Development Pass: Benchmark-First Structural Localization

### Objective

Make the next anomaly decision against a scientifically defensible localization
benchmark rather than optimize the current mixed composite labels by default.

This pass must produce one of three evidence-backed outcomes:

1. retain a declared benchmark target because its source is observably
   distinguishable and the next issue is downstream modeling;
2. redesign one simulator fault path because the emitted telemetry does not
   make its declared source scope identifiable; or
3. explicitly lower a benchmark claim because the scenario is intended only
   for a lower recoverability tier.

It must not turn the current detector result into the definition of simulator
truth. A target can be revised only after reviewing the emitted source,
propagated observables, and the existing tiered evidence together.

### Scope

In scope:

- use the canonical composite and dedicated tier reports to classify each
  authored fault window by its appropriate validation role;
- align benchmark-target metadata, named benchmark packs, report expectations,
  and tests when the evidence supports a scope decision;
- make at most one simulator change that improves observable source-versus-
  consequence separation without injecting labels into telemetry;
- run one diagnostic-only evidence-preservation and reference-fit/inference
  comparison before changing anomaly behavior;
- make at most one generic canonical-path anomaly localization change, and only
  after the scenario decision and diagnostic comparison prove that a valid
  observable is being lost downstream;
- preserve full-run timings and bounded hot-path behavior as an acceptance
  surface.

Out of scope:

- A-MATS or AFDX ingestion, packet decoding, source adapters, payload mapping,
  and telemetry-provenance contracts;
- assumptions about an unavailable ICD or live source capture;
- broad phase-envelope work, rate-aware window-feature redesign, deep sequence
  models, a second scoring path, or broad stage-`90` rerankers;
- a generic top-k expansion that treats bounded-output absence as proof of
  candidate-generation failure;
- changing declared targets merely to hide an unresolved model failure.

### Baseline And Decision Inputs

Start from the persisted
`data/simulation_runs/20260718T024330Z_power_pressurization_hierarchy_composite`
bundle. Its canonical reports are:

- `simulation_benchmark_audit_summary.json` for declared versus observed
  recoverability;
- `benchmark_scope_validation_summary.json` for denominator-aware objectives;
- `benchmark_tier_validation_summary.json` for the per-window first-failure
  ledger;
- `misbehavior_attribution_validation_summary.json` for candidate and
  reconstruction-localization diagnostics.

The current composite declares `13` module-recoverable and `5`
subsystem-recoverable windows, while the audit records only one
module-recoverable and one subsystem-recoverable outcome. Dedicated smoke gates
already demonstrate a clean module case for `drift` and a clean subsystem case
for `bias`; they do not establish that those fault families remain equally
recoverable inside the mixed composite.

### Milestone 1: Establish The Benchmark Decision Ledger

Use existing reports as the canonical data source. Add a reporting field only
when the current reports cannot answer an actionable decision; do not build a
parallel notebook-only benchmark.

Status: implemented as the suite-level
`reports/benchmark_decision_ledger_summary.json` and Markdown companion.
Run `python -m scripts.run_sim_benchmark_tier_gates --composite-run-dir <run-dir>`
to join a completed composite run to the named clean gate results. The ledger
preserves the per-run reports as their canonical owners and adds only the
cross-run decision view required for this milestone.

For every canonical `fault_window_id`, record:

- declared target and observed recoverability tier;
- first failed benchmark scope and dominant score component;
- source parameter/module and the selected telemetry/candidate evidence;
- whether a clean dedicated pack agrees with the composite result;
- one decision: retain target, redesign scenario, lower target, or formulate a
  downstream-model hypothesis.

Decision rules:

- retain the target when a source-local observable and its intended propagation
  are present, even if the current detector misses it;
- redesign the scenario when the source cannot be separated from shared or
  sibling consequences in the emitted telemetry;
- lower a target only when that lower scope is the intended and defensible
  scenario contract, not because the current model failed;
- formulate a downstream-model hypothesis only when raw observability is
  present and the candidate evidence shows where the canonical path loses it.

The acceptance output is a reviewed per-window decision matrix and a clear
selection of one next change. The existing reports and
`libs/simulation/reporting.py` remain the owners of per-run report surfaces;
`libs/simulation/benchmark_tier_gates.py` owns this cross-run suite artifact.

### Milestone 2: Correct One Observable Fault Path Or Its Scope

Select one result from Milestone 1. The first investigation should explain why
the dedicated `drift` gate is module-recoverable while mixed-composite drift
windows are not. Then choose only one scenario family to change, if a change is
required.

Completed rejected experiment: staggered composite `drift` windows into
non-overlapping phase-local intervals and scaled their rates to preserve an
approximately comparable accumulated perturbation. The clean gates remained
green, but the composite regressed to one undetected drift window and did not
restore structural recovery. Retain the original scenario timing; overlap alone
does not explain the downstream structural loss.

### Candidate-Cut Sensitivity Diagnosis (Completed 2026-07-18)

Completed as a diagnostic-only pass in the canonical anomaly validation path.
The retained composite showed that both drift windows emit results, but neither
truth subsystem nor truth module appears in the top structural candidates. The
forward window retained the truth parameter in selected telemetry; the aft
window did not.

Current owners and cuts:

- `libs/anomaly/frames.py`, `AnomalyParameterLocalizationFrame`, ranks
  parameter support and retains the fixed top-five telemetry selection before
  applying the top-three structural input used for subsystem and module
  candidates;
- `libs/anomaly/pipeline.py` passes that fixed candidate breadth into the
  canonical stage-`90` attribution workflow;
- `libs/anomaly/validator.py` and `libs/simulation/reporting.py` own the
  bounded validation and benchmark reporting surfaces. They do not currently
  retain the full candidate universe.

Implemented:

1. Added `candidate_cut_validation` to both attribution-validation summaries.
   For every truth window it records the parameter support rank, selected
   telemetry status, top-three membership, support margin at the cut, minimum
   candidate breadth, and inferred-hierarchy mappability when ranked.
2. Retained inferred hierarchy identifiers in the bounded telemetry reporting
   view so candidate-cut evidence can distinguish a valid cut loss from an
   ambiguous inferred cluster.
3. Added focused validator, reporting-view, and Spark-frame coverage for
   inside-cut, below-cut, unranked, no-match, and empty cases.
4. Ran the clean localization gates at
   `/tmp/s3ntinel_candidate_cut_gates/20260718T185619Z_localization_benchmark_tier_gates`:
   both gates were `met_target`.
5. Ran the fresh full composite at
   `/tmp/s3ntinel_candidate_cut/20260718T190625Z_power_pressurization_hierarchy_composite`.
   Its localization and recoverability metrics exactly matched the retained
   baseline; no scoring, emitted candidate, or benchmark behavior changed.

Diagnosis from the fresh composite:

- among `18` truth windows, `9` truth parameters were within the structural
  top-three input, `2` were below it, `4` were absent from the persisted
  top-five telemetry payload, and `3` had no qualifying telemetry attribution;
- `FW_ACCUM_DRIFT_FORWARD` is a genuine structural-cut loss: its truth
  parameter is rank `4`, selected for telemetry, only `0.00109335` below the
  rank-three support boundary, and maps unambiguously to both truth subsystem
  and module;
- a full canonical localization rerun with `top_k_per_window=128` places
  `FW_ACCUM_DRIFT_AFT` at rank `6` or `7` in the late drift windows. It is a
  top-five telemetry-retention loss, not evidence that candidate generation
  omitted the parameter.

Decision: do not begin a generic rank-aware expansion in this branch. Both
drift parameters are outside the structural top three, while the aft parameter
is only outside the telemetry top five. Widening one cap cannot establish that
the other is the correct structural decision, and many windows already inside
the top three still fail structural recovery. The next pass must preserve the
candidate evidence and test the simulation fitting lifecycle before any model
or cap change.

Candidate families include:

- `timing_lag`, which currently fails at detection/emission;
- `bias`, which is cleanly subsystem-recoverable but not module-recoverable in
  the current smoke variants;
- `drift`, whose clean and composite behavior disagree;
- coupling families, if the source-to-consequence trace shows a valid
  subsystem-level observable;
- `saturation` only as a lower-tier scenario-design question, never as the
  current module-localization gate.

Rules for the selected change:

- change source behavior, coupling, phase context, or sampled observability;
  never copy fault identity into emitted values or labels;
- keep the canonical `Flight` and behavior-local violation path;
- add or update focused simulation tests, benchmark metadata, and report
  expectations in the same change;
- keep unaffected benchmark packs and the no-fault smoke structurally valid.

Promote a revised scenario only if the named tier gate and the canonical
composite both explain the expected scope without a regression in unrelated
tiers. If the evidence is still ambiguous after one focused scenario attempt,
record that conclusion and stop rather than accumulate simulator variants.

### Milestone 3: Evidence-Preservation And Reference-Inference Diagnosis

Status: in progress. Candidate-evidence preservation and the paired
reference-fit/faulted-inference orchestration are implemented without changing
anomaly behavior. Focused phase fit/apply tests and the canonical structural
smoke pass. A first 600-step paired attempt exposed cross-child Spark checkpoint
retention and was discarded; explicit Spark session resets are now applied at
child-run boundaries. The post-reset paired replay remains required before this
milestone can be completed.

The current grouped full simulation run fits artifacts and runs inference from
the same fault-bearing observed telemetry in one run directory. That path does
not use `parameter_value_clean` for production fitting or inference, but it can
make a synthetic fault period part of the fitted baseline. The intended fitting
lifecycle normally fits reusable artifacts once and reuses them during
inference, as documented in `docs/current/fitting_workflow.md`.

Build a paired simulation benchmark:

1. generate a nominal reference flight and fit reusable artifacts from its
   observed `parameter_value`;
2. generate the matching faulted flight and run inference against those fixed
   reference artifacts, recording the reference run/artifact lineage;
3. consume the bounded parameter/window candidate-evidence record from the
   canonical scoring and anomaly path, including candidate source, channel
   contribution, phase-conditioned residual evidence, profile/event evidence,
   support, rank, and both cap statuses;
4. compare self-fit and reference-fit support ranks, phase baselines, and
   localization results on the clean gates and the canonical composite.

The evidence record must retain a fixed union of global and per-channel
candidates. It must not materialize every parameter in every emitted window,
duplicate candidate evidence across raw sample rows, or alter the current
model result. The simulation harness must not pass clean values, labels, or
fault identifiers to production fitting, scoring, or attribution code.

This is a validation-architecture test, not a claim that deployment always
fits on a fault-bearing flight. If production intentionally refits per flight,
the later model decision must explicitly address slow-drift adaptation rather
than assuming reference artifacts are always available.

Acceptance:

- both drift ranks and their cap losses are traceable from score evidence to
  candidate output;
- the paired runs have explicit artifact lineage and comparable scenario
  topology, seed, timing, and observed-input contracts;
- all existing model outputs remain unchanged in the diagnostic-only pass;
- the report records the observed stage-`90` row counts and timings before a
  candidate-evidence artifact is considered for promotion.

### Milestone 4: Conditional Anomaly Experiment

Do not begin this milestone unless Milestones 1 through 3 leave a valid
declared target with a specific downstream loss.

Choose the implementation seam from the evidence:

- truth is absent from the bounded candidate union after the reference-fit
  comparison: one generic upstream signal or channel-maturation pass;
- truth is retained but has weak phase-conditioned or mechanism-specific
  evidence: one narrow canonical ranking/novelty change;
- truth is visible only below the telemetry or structural cap: assess that cap
  separately, with no generic expansion by default;
- truth-local candidates are strong but the final winner is wrong: reconsider
  the hierarchy gate only after candidate quality is demonstrated.

The experiment must stay on the canonical Spark path. It must not use scenario
names, parameter-name rules, injected labels, a new local score builder, or a
broad stage-`90` retention/reranking expansion.

### Promotion And Stop Rules

For every code-bearing milestone:

1. run affected unit and contract tests;
2. run the localization tier-gate suite before the mixed composite replay;
3. run the parameter tier suite when parameter evidence is touched;
4. compare the reference-fit and self-fit replay reports, artifact lineage,
   and stage timings against the baseline bundle;
5. retain the change only when it improves its declared target without a
   material regression in a lower tier, an unrelated benchmark pack, or hot-
   path runtime.

Stop the pass when one of these is true:

- a scenario target is now defensible and the selected anomaly experiment has
  a clear keep/reject outcome;
- the selected scenario remains intrinsically ambiguous after one focused
  observability change;
- no generic candidate signal is supported by the evidence after the
  reference-fit and candidate-evidence diagnostic.

In the latter two cases, preserve the reports and decision matrix, leave the
model unchanged, and use them to justify the following development pass.

## A. Completed Foundation: Localization Benchmark Audit And Scenario Review

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

Before introducing another broad anomaly-localization change, read the existing
simulation benchmark audit from the current validation surfaces and refresh it
only when the scenario or the canonical model path changes.

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

The audit is implemented as a first-class simulation report, not a
notebook-only side analysis.

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

Do not use only the mixed composite bundle for every benchmark question.

The current scenario family exposes separate named benchmark packs for:

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

### Parameter-tier benchmark status

The validation harness is now capable of measuring benchmark-aware parameter
scope performance, and the simulator now exposes a dedicated grouped
`parameter_visible_only` smoke suite.

Current implemented parameter-tier gate family:

- canonical grouped runner:
  - `python -m scripts.run_sim_benchmark_tier_gates --suite parameter --base-dir ...`
- grouped report:
  - `reports/benchmark_tier_gate_suite_summary.json`
- family packs:
  - `power_pressurization_hierarchy_smoke_parameter_focus_regulated`
  - `power_pressurization_hierarchy_smoke_parameter_focus_accumulative`
  - `power_pressurization_hierarchy_smoke_parameter_focus_discrete`
  - `power_pressurization_hierarchy_smoke_parameter_focus_coupling`

Current canonical grouped result on:

- `/tmp/s3ntinel_parameter_tier_gates_v2/20260413T021821Z_parameter_benchmark_tier_gates`

Observed outcome:

- suite status:
  - `all_gates_met_or_exceeded = true`
  - alignment counts:
    - `met_target = 1`
    - `exceeded_target = 3`
- by family:
  - regulated `saturation`
    - observed `parameter_visible_only`
    - detected `1/1`
    - emit-ready `1/1`
    - telemetry parameter match `1/1`
    - alignment `met_target`
  - accumulative `drift`
    - observed `module_recoverable`
    - detected `1/1`
    - emit-ready `1/1`
    - telemetry parameter match `1/1`
    - alignment `exceeded_target`
  - discrete `state_chatter`
    - observed `module_recoverable`
    - detected `1/1`
    - emit-ready `1/1`
    - telemetry parameter match `1/1`
    - selected telemetry parameter match `1/1`
    - event parameter match `1/1`
    - alignment `exceeded_target`
  - coupling `timing_jitter`
    - observed `module_recoverable`
    - detected `1/1`
    - emit-ready `1/1`
    - telemetry parameter match `1/1`
    - event parameter match `1/1`
    - alignment `exceeded_target`

Implication:

- the parameter tier is now real and useful, not hypothetical
- parameter-tier coverage is no longer blocked on missing benchmark supply
- the discrete family miss was traced to authored/sampled chatter cadence, not a
  generic absence of lower-tier benchmark supply
- after aligning `state_chatter` to the emitted sample cadence, the grouped
  suite now shows the discrete family as a working event-driven benchmark rather
  than the prior `undetected` miss
- raw signal confirmation from the repaired discrete rerun:
  - `/tmp/s3ntinel_parameter_discrete_fix_v4/20260413T020945Z_power_pressurization_hierarchy_smoke_parameter_focus_discrete`
  - sampled `pack_mode_state` alternates `LOW/OFF` across the window
  - extracted `transition` events rise from `3` baseline transitions to `47`
    transitions

### Next simulation work for the parameter tier

The dedicated parameter-tier suite now exists, and the discrete family is no
longer structurally broken, so the next work is narrower:

1. keep the regulated, accumulative, discrete, and coupling family packs as the
   canonical lower-tier acceptance set
2. only after that, decide whether parameter-tier coverage also needs an
   additional illegal-transition pack

Practical interpretation:

- regulated `saturation` is a valid parameter-tier gate
- accumulative `drift` and coupling `timing_jitter` currently overachieve and
  should stay in the parameter suite as lower-tier smoke screens, even though
  they can recover structure
- discrete `state_chatter` is now viable after aligning the chatter cadence to
  the emitted sample cadence; keep it as an event-driven lower-tier benchmark

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

So the current development pass should:

1. keep the existing parameter-tier suite as a required regression screen;
2. use the benchmark decision ledger before adding any new lower-tier pack;
3. investigate one source-versus-consequence scenario path under the
   benchmark-first sequence above;
4. defer harder subsystem- and system-level scenario expansion.

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
- [phase.md](phase.md)

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

## Completed Foundation And Deferred Follow-On

### Completed: localization benchmark audit

Deliverables:

- `reports/simulation_benchmark_audit_summary.json` emitted by the canonical
  simulation reporting path
- `reports/benchmark_scope_validation_summary.json` emitted by the same
  reporting path so benchmark intent becomes optimization denominators instead
  of audit-only metadata
- `reports/benchmark_tier_validation_summary.json` emitted by the same
  reporting path so mixed composite runs are readable by declared benchmark
  tier and eligible-window failure stage
- composite candidate diagnostics in
  `reports/benchmark_tier_validation_summary.json` for:
  - rollup inconsistency
  - detected-to-truth subsystem mapping gaps
  - detected-to-truth module mapping gaps
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
