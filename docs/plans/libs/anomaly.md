# Anomaly Library Plan

Status: Plan
Authority: Non-authoritative roadmap. Use package READMEs and `docs/current/` for current behavior.

This plan captures the next anomaly-focused work after the single-model-path
refactor, anomaly-channel expansion, and recent localization experiments.

Primary library owner:
- `libs/anomaly`

Shared library dependencies covered here:
- `libs/scoring`
- `libs/graph`

For current implementation ownership, prefer:
- [libs/scoring/README.md](/home/jrs/code/S3NTINEL/sentinel/libs/scoring/README.md)
- [libs/anomaly/README.md](/home/jrs/code/S3NTINEL/sentinel/libs/anomaly/README.md)
- [libs/graph/README.md](/home/jrs/code/S3NTINEL/sentinel/libs/graph/README.md)
- [docs/current/v2_architecture.md](/home/jrs/code/S3NTINEL/sentinel/docs/current/v2_architecture.md)
- [docs/current/phase_validation_semantics.md](/home/jrs/code/S3NTINEL/sentinel/docs/current/phase_validation_semantics.md)

## Current Baseline

The current kept baseline is good enough to support targeted anomaly work:

- phase macro F1: `0.7299398538418875`
- detected fault window rate: `0.8333333333333334`
- emit-ready fault window rate: `0.7777777777777778`
- telemetry parameter match rate: `0.7777777777777778`
- event parameter match rate: `0.16666666666666666`
- dominant subsystem match rate: `0.3333333333333333`
- dominant module match rate: `0.0`

Fresh replay confirming the current working-tree baseline and diagnostic surface:

- replay bundle:
  `/tmp/s3ntinel_accumulation_channel_v1/20260411T213154Z_power_pressurization_hierarchy_composite`
- attribution summary:
  `/tmp/s3ntinel_accumulation_channel_v1/20260411T213154Z_power_pressurization_hierarchy_composite/reports/attribution_validation_summary.json`

Interpretation:

- steady anomaly detection is no longer collapsed
- parameter localization is materially useful
- subsystem localization is weak but non-zero
- module localization is still the weakest active anomaly layer

## Current Architecture Constraints

### 1. Canonical scoring path is now Spark-only

The duplicate local/in-memory score builders were removed. All production score
semantics now live in:

- `WindowScoresRawTable`
- `WindowScoresCalibratedTable`

That rule should remain fixed during follow-on anomaly work.

### 2. The score-channel contract is broader than the active signal mix

The canonical raw channel surface now includes:

- `regime_deviation`
- `reconstruction_error`
- `event_discordance`
- `bound_violation`
- `accumulation_violation`
- `response_violation`
- `state_violation`
- `coherence_break`

But the most consequential localization failures are still concentrated in
`reconstruction_error`-dominated windows.

### 3. The immediate bottleneck is not phase, calibration, or emission

Recent work already made:

- phase validation explicit for transition regions
- score/emission semantics non-zero and measurable
- parameter localization visible in the reports

The next anomaly issue is narrower:

- reconstruction-led candidate generation and ranking still favor shared-source
  or sibling-consequence parameters too often

## Main Diagnosis

The anomaly stack is now in this state:

1. scoring detects useful windows
2. calibration/emission preserve useful windows
3. telemetry parameter localization often includes the truth signal
4. subsystem/module winners still lose too early in reconstruction-led cases

The system therefore needs:

- better diagnosis of reconstruction misses
- then targeted candidate-generation changes
- and only then a decision about whether to revisit hierarchy quality upstream

## Generality Constraint

Detection improvements in this plan must maintain generality and must not
overfit to simulation-specific artifacts.

That means:

- do not key scoring or localization logic to simulator scenario names,
  parameter names, fault labels, or handcrafted truth families
- do not optimize for one replay by adding special-case branches for the exact
  current miss set
- prefer generic mechanisms such as local support concentration, source versus
  consequence asymmetry, regime-conditioned baselines, and channel-aware
  evidence
- treat simulator replays as acceptance harnesses and diagnostic sources, not
  as the target taxonomy to memorize

Success for anomaly-detection improvement is:

- better replay metrics without degrading the current baseline
- while keeping the implementation plausible for real telemetry and unseen
  simulator scenarios

## Workstream A: Reconstruction Failure Taxonomy

### Objective

Stop guessing why reconstruction-led localization misses.

### Current status

The validator/reporting surface is now implemented and populated from a fresh
replay on current head.

Observed reconstruction-localization mix on the current replay:

- reconstruction truth windows: `10`
- reconstruction failures: `10`
- failure count by bucket:
  - `missing_truth_local_candidate`: `4`
  - `shared_source_won`: `3`
  - `sibling_consequence_won`: `1`
  - `truth_module_present_but_lost`: `2`
- candidate-quality rates:
  - truth subsystem present in selected telemetry: `0.6`
  - truth module present in selected telemetry: `0.5`
  - truth subsystem present in top subsystem candidates: `0.2`
  - truth module present in top module candidates: `0.2`
  - top-ranked selected parameter exact match: `0.1`
  - top-ranked selected parameter in truth subsystem: `0.2`
  - top-ranked selected parameter in truth module: `0.1`

Implication:

- the dominant failure is still candidate generation, not final rollup
- shared-source ranking is the second-order failure mode inside the generated
  candidate set
- there was no dominant `truth_subsystem_present_but_lost` bucket on this
  replay, so another winner-rollup pass should not be the next move

### Add a validator breakdown for misses

For reconstruction-dominated anomaly windows, classify misses into:

- `shared_source_won`
- `sibling_consequence_won`
- `truth_subsystem_present_but_lost`
- `truth_module_present_but_lost`
- `missing_truth_local_candidate`

These labels should be derived from existing persisted outputs:

- selected telemetry parameters
- ranked subsystem/module candidates
- truth parameter/module/subsystem mapping
- detected hierarchy map

### Acceptance

This pass is diagnostic-only. It should not change model behavior.

Success criteria:

- the new failure buckets appear in attribution validation
- the output is stable on empty or no-match cases
- the replay metrics above remain unchanged

## Workstream B: Dual-View Reconstruction Upgrade

### Objective

Upgrade reconstruction before adding more stage-`90` heuristics.

The current backbone only reconstructs the end-of-window continuous state.
That is enough to detect many abnormal windows, but it is still too weak on
source-versus-consequence localization for reconstruction-dominated cases.

The next model upgrade should therefore stay generic and should use already
persisted window information rather than simulator-specific rules or a heavier
sequence-model redesign.

### Proposed design

Implement one bounded reconstruction upgrade in the canonical Spark path:

- keep the existing end-state `level` backbone
- add a second `delta` backbone on
  `continuous_vector_t_end_scaled - continuous_vector_t_start_scaled`
- fit separate sensor selections and ridge weights for the `level` and `delta`
  views
- keep one backbone artifact row, not two backbones and not a second modeling
  path

Shared constraints:

- do not add simulator-specific logic
- do not add event-level reconstruction in this pass
- do not add a second local or in-memory reconstruction path
- do not change graph semantics in this pass; stage `50` should continue to use
  the existing level-view backbone selection
- do not persist a new delta vector artifact in `window_features`; compute it
  lazily inside the backbone and phase stages

### Scope

Primary code areas:

- stage `40` backbone fit
- stage `70` phase reconstruction assembly
- stage `80` raw scoring
- stage `90` anomaly-local reconstruction support

Schema additions for the kept implementation:

- backbone artifact:
  - `selected_sensors_delta_c`
  - `weights_delta_b`
  - `backbone_version = 3`
- phase output surface:
  - `backbone_level_reconstruction_error`
  - `backbone_level_residual_by_parameter`
  - `backbone_delta_reconstruction_error`
  - `backbone_delta_residual_by_parameter`

Keep the existing legacy fields by emitting fused views:

- `backbone_reconstruction_error`
- `backbone_residual_by_parameter`

Fusion semantics:

- scalar raw reconstruction error should be RMS over the level and delta view
  errors
- residual-by-parameter should be per-parameter RMS over level and delta
  residuals

### Scoring and localization semantics

Do not add a second public reconstruction score channel in this pass.

Keep the channel name:

- `reconstruction_error`

But calibrate both internal views:

- level reconstruction error
- delta reconstruction error
- combined reconstruction error

Final score semantics:

- positive robust z-score for the level view
- positive robust z-score for the delta view
- `reconstruction_error = max(level_z, delta_z)`

Localization semantics:

- use the new view-specific residual maps in the existing reconstruction-support
  path
- parameter support should prefer the stronger view rather than summing the two
  indiscriminately
- add a lightweight diagnostic field:
  - `dominant_reconstruction_view = level | delta | mixed`

### Phase 2 gate

Do not bundle local-adaptive calibration into the first code pass.

Phase 2 should only start if the dual-view backbone:

- keeps detection stable
- improves candidate presence or dominant subsystem quality
- but still leaves reconstruction-led separation too coarse

If that gate is met, the first adaptive-calibration step should be:

- phase centroid x drift bucket calibration using `drift_magnitude_profiled`

It should not start with:

- event-density buckets
- simulator-specific contexts
- parameter-family-specific calibration

### Acceptance

Required non-regressions:

- phase macro F1
- detected fault window rate
- emit-ready fault window rate
- telemetry parameter match rate

At least two of these should improve over the current kept baseline:

- dominant subsystem match rate `> 0.3333`
- top subsystem candidate presence `> 0.1111`
- truth subsystem present in selected telemetry `> 0.6`
- `missing_truth_local_candidate < 4`
- `shared_source_won < 3`

Runtime guardrails:

- stage `90` should stay within `1.25x` of the current replay runtime
- full replay should stay within `1.5x` of the current baseline unless the
  quality improvement is clearly material

## Workstream C: Post-Upgrade Replay Gate

### Objective

Use one clean replay to decide whether the dual-view reconstruction upgrade is
kept, revised, or reverted.

### Required checks

- `py_compile`
- targeted unit tests for backbone, phase, scoring, and anomaly
- targeted integration tests for phase/anomaly pipeline slices
- full replay in `sentinel-spark35`

### Decision rule

- keep the pass only if it clears the acceptance and runtime gates above
- if it misses the quality gates and breaches the runtime guardrail, revert it
- if it is neutral but cheap, reject it unless the new diagnostics reveal a
  clearer next targeted change

## Workstream D: Reconstruction-Led Candidate Generation

### Objective

Improve the selected local candidate set before subsystem/module rollup.

### Decision rule

Only start this pass after Workstream A shows which failure mode dominates.

Current decision from the fresh replay:

- `missing_truth_local_candidate` is the dominant bucket
- `shared_source_won` is the next largest bucket
- proceed with candidate generation first
- treat shared-source suppression as a coupled ranking constraint inside that
  pass, not as a separate first move
- keep the upstream `accumulation_violation` channel; it improved the replay
  without regressing detection or parameter-localization metrics

### Candidate-generation direction

Prefer targeted, generic signals over another large heuristic pile:

- reconstruction-local source vs consequence cues that improve candidate recall
  for the true local subsystem/module
- local support concentration over broad shared utility parameters such as power
  or bleed-supply sources
- channel-aware support that keeps `reconstruction_error` distinct from
  event-driven evidence

Immediate next implementation target:

- improve reconstruction-led selected telemetry candidate recall so the truth
  subsystem/module appears in the selected set more often than the current
  `0.5455` / `0.4545`
- while doing that, reduce the frequency of shared-source winners so the top
  ranked selected parameter lands in the truth subsystem more often than the
  current `0.1818`
- do both through generic locality/ranking signals rather than simulator- or
  scenario-specific exception handling

Current implementation note:

- the next upstream pass should prefer generic mechanism channels over more
  stage-`90` rerank complexity when a failure mode is missing a real score
  surface
- `accumulation_violation` is the first concrete example: quiet accumulative
  drift windows should not rely on event-gated behavior channels to become
  distinguishable from generic `reconstruction_error`
- the first kept `accumulation_violation` pass improved dominant subsystem
  match rate from `0.25` to `0.3333`, top subsystem candidate presence from
  `0.0556` to `0.1111`, top module candidate presence from `0.1111` to
  `0.1667`, and reduced the dominant `missing_truth_local_candidate` bucket
  from `5` to `4`

Rejected near-term direction:

- a broader reconstruction candidate-retention pass plus stronger residual
  cluster boosting did not move the replay metrics and pushed stage `90`
  runtime from about `22s` to about `98s` on replay
- a reconstruction reranking pass using phase-selected sensor/state metadata
  plus parameter behavior profiles also did not move the replay metrics or
  failure-bucket mix, and it pushed stage `90` runtime to about `89s`
- a quiet `bound_violation` broadening pass that mixed residual concentration
  with bound/saturation profile evidence without requiring nearby bound events
  improved telemetry parameter match rate from `0.7778` to `0.8333`, but it
  regressed the more important localization targets:
  - dominant subsystem match rate `0.3333 -> 0.1429`
  - top subsystem candidate presence `0.1111 -> 0.0556`
  - top module candidate presence `0.1667 -> 0.1111`
  and therefore was not kept
- a reconstruction-only module-representative retention pass in stage `90`
  kept one representative from the strongest secondary modules before the final
  selection cutoff, but it produced no replay change at all:
  - dominant subsystem match rate stayed `0.3333`
  - top subsystem candidate presence stayed `0.1111`
  - top module candidate presence stayed `0.1667`
  - reconstruction failure buckets stayed `4 / 3 / 1 / 2`
  for `missing_truth_local_candidate / shared_source_won /
  sibling_consequence_won / truth_module_present_but_lost`
  and therefore was not kept
- seeding stage `90` localization from the existing stage-`80` dominant
  subsystem/module winner does not look promising on the current replay;
  for the checked reconstruction misses, stage `80` and stage `90` were
  already collapsing onto the same wrong detected winners
- do not keep pursuing wider selected sets or heavier reconstruction-cluster
  amplification on the hot path without new evidence
- do not add phase-feature or parameter-profile reranking to stage `90`
  unless a future design can show a materially cheaper path or a clearly
  stronger generic signal
- do not widen retention again without a new upstream reconstruction signal
- do not add heavy rerankers to stage `90`
- do not jump to deep sequence models before the dual-view backbone is
  evaluated

Do not reintroduce:

- a second local scoring path
- simulator-specific anomaly rules
- simulator-scenario heuristics or parameter-name allow/deny lists
- broad graph penalties that slow stage 90 without moving the target metric
- broad reconstruction-retention heuristics that increase stage-90 runtime
  without improving candidate quality
- expensive reconstruction rerankers that only change which wrong winner
  appears at the top
- stage-`80` winner carry-forward as a substitute for better reconstruction
  candidate generation

## Workstream E: Channel Maturation

### Objective

Revisit the broader channel surface only after the reconstruction-localization
diagnostics are explicit.

### Current channel reality

The seven-channel contract is correct, but some channels are still conservative
or low-impact on the kept replay.

If channel work resumes, the order should be:

1. keep the canonical channel contract stable
2. validate which non-reconstruction channels are materially active
3. expand channel-specific diagnostics before changing weights or emission

### Good next targets

- make `event_discordance` more useful when event-stream mismatch is real
- strengthen behavior-mechanism evidence through:
  - `response_violation`
  - `state_violation`
- revisit `bound_violation` only with a more discriminating generic signal than
  the rejected quiet residual-plus-profile broadening pass above
- keep channel-aware validation visible in reports

## Workstream F: Upstream Hierarchy Revisit Gate

### Objective

Avoid bouncing back upstream too early.

Hierarchy should be revisited only if the reconstruction-local diagnostics show
that downstream candidate quality is already decent and the remaining loss is
structural.

Go back to stage `60` only if:

- truth subsystem/module presence in selected candidates is already reasonable
- but final subsystem/module quality remains poor
- and rollup/selection changes stop moving the needle

If those conditions are not met, keep working in stage `90`.

Current gate result:

- do not revisit stage `60` yet
- candidate quality is still too weak for hierarchy retuning to be the primary
  next move

## Next Narrowing Step

The next active anomaly sequence should now be:

1. dual-view reconstruction upgrade
2. post-upgrade replay gate
3. only then any further candidate-generation or hierarchy revisit

That ordering is intentional.

The current bottleneck still sits in reconstruction-led localization, but the
next move should be to strengthen the upstream reconstruction surface before
adding another stage-`90` selection heuristic.

If the dual-view pass is kept, then the next narrowing step should again focus
on reconstruction cases where the selected set is dominated by:

- shared upstream utility parameters
- sibling module copies of the same signal family
- shared control-state or environmental consequence parameters

At that point, a follow-on implementation may target high-precision filtering
or re-ranking of reconstruction candidates, but only after the new
reconstruction surface has been evaluated on replay.

## Test And Acceptance Plan

### Diagnostic pass

- new reconstruction miss taxonomy appears in attribution validation
- candidate-quality counters are emitted by report and harness paths
- no regression in current headline anomaly metrics
- no new detection rule should depend on simulator-specific identifiers or truth
  labels

### Modeling pass after the diagnostics

- replay still keeps:
  - detected fault window rate above the current non-zero baseline
  - telemetry parameter localization at or above current useful levels
- subsystem localization should improve before module localization is treated as
  the primary success target

## Non-Goals

This plan is not proposing:

- another phase taxonomy rewrite
- another local/in-memory model path
- a blind hierarchy retune without a failure taxonomy
- a simulator-specific anomaly-label expansion
