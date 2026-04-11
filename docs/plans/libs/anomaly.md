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
- dominant subsystem match rate: `0.25`
- dominant module match rate: `0.0`

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

## Workstream A: Reconstruction Failure Taxonomy

### Objective

Stop guessing why reconstruction-led localization misses.

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

## Workstream B: Reconstruction-Led Candidate Generation

### Objective

Improve the selected local candidate set before subsystem/module rollup.

### Decision rule

Only start this pass after Workstream A shows which failure mode dominates.

- if `missing_truth_local_candidate` dominates:
  - work candidate generation first
- if `truth_subsystem_present_but_lost` dominates:
  - work aggregation and ranking first
- if truth presence stays weak even with good parameter evidence:
  - move upstream into hierarchy/module quality

### Candidate-generation direction

Prefer targeted, generic signals over another large heuristic pile:

- reconstruction-local source vs consequence cues
- local support concentration over broad shared utility parameters
- channel-aware support that keeps `reconstruction_error` distinct from
  event-driven evidence

Do not reintroduce:

- a second local scoring path
- simulator-specific anomaly rules
- broad graph penalties that slow stage 90 without moving the target metric

## Workstream C: Channel Maturation

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
  - `bound_violation`
  - `response_violation`
  - `state_violation`
- keep channel-aware validation visible in reports

## Workstream D: Upstream Hierarchy Revisit Gate

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

## Test And Acceptance Plan

### Diagnostic pass

- new reconstruction miss taxonomy appears in attribution validation
- candidate-quality counters are emitted by report and harness paths
- no regression in current headline anomaly metrics

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
