# Anomaly

## Purpose

`libs/anomaly` owns downstream anomaly attribution artifacts and attribution-vs-truth validation.

It does not own:
- injected simulation misbehaviors
- raw score computation
- score calibration

Those belong to `libs/simulation`, `libs/scoring`, and their validators.

## How To Use

- Use `AnomalyWindowAttributionTable`, `AnomalyTelemetryAttributionTable`, `AnomalyEventAttributionTable`, and `AnomalyParameterCandidateEvidenceTable` from the package surface for the persisted stage.
- Use `validate_attribution_against_misbehavior_truth(...)` as the canonical truth validator.
- `validate_attribution_against_fault_truth(...)` remains as a deprecated compatibility wrapper.

## Contents

- `frames.py`
  - reusable subsystem, panel, and combined attribution context frames
- `tables.py`
  - persisted anomaly attribution artifact owners
- `pipeline.py`
  - thin orchestration over typed anomaly artifacts
- `validator.py`
  - attribution-vs-truth evaluation

## Model / Concepts

Main nouns:
- `AnomalyWindowAttributionTable`
- `AnomalyTelemetryAttributionTable`
- `AnomalyEventAttributionTable`
- `AnomalyParameterCandidateEvidenceTable`
- `AnomalyAttributionContextFrame`

These represent downstream anomaly outputs, not simulation truth.

## Data / Artifacts

The package produces the persisted anomaly artifacts defined in `libs/io/schemas/anomaly.py`:
- window attribution
- telemetry attribution
- event attribution
- bounded parameter candidate evidence

`anomaly_parameter_candidate_evidence` is one row per emitted
window/parameter candidate. It joins the score-owned evidence to stage-90
localization support and rank, hierarchy identifiers, and explicit
`telemetry_retained` and `structural_cut_retained` flags. It is intentionally
separate from raw telemetry attribution so evidence is not duplicated across
sample rows.

## Subject Matter View

This package answers: given an anomalous window, which subsystem, parameters, and events are the most plausible explanation?

## Testing / Validation

- unit tests cover the anomaly model objects
- integration tests cover stage 80 and simulation-backed runner flows
- validator logic compares attribution outputs to injected misbehavior truth

## Notes

- Use `misbehavior` for simulator/source truth.
- `fault` remains a deprecated compatibility alias in validator/report wrappers.
- Use `anomaly` for the downstream attribution domain.
- Attribution validation uses the same window-local strict-overlap semantics as score validation, so short emitted windows can still be credited against long truth intervals when they are well-aligned.
- Attribution validation now also emits a dedicated `parameter_localization_validation` block plus per-truth attributed-parameter name lists, so reports make exact parameter localization visible even when subsystem localization stays coarse.
- Reconstruction-dominated misses now also emit a dedicated `reconstruction_localization_validation` block, including failure buckets and candidate-quality diagnostics so stage-90 localization work can distinguish missing local candidates from rollup losses.
- Attribution validation emits `candidate_cut_validation`, which compares a truth parameter's persisted telemetry support rank with the fixed top-three structural rollup cut. It reports whether truth has no qualifying telemetry, was not ranked into bounded candidates, is inside the cut, or falls below it, together with the cut margin and required candidate breadth. This is diagnostic-only and does not alter emitted candidates.
- Stage-90 parameter localization now also accepts behavior-profile context for mechanism-specific channels that need residual-backed parameter ranking without relying on nearby events, such as `accumulation_violation`.
- Stage 90 persists the bounded score-to-localization evidence ledger without changing localization support, ranking, telemetry selection, or structural rollup cuts.
